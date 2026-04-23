import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — NuVizz CSV Ingest Background Worker (v2.40.34)
 *
 * Receives parsed NuVizz stops from the dispatcher and saves them to
 * Firestore in 500-doc batches using the :commit endpoint (same pattern
 * that fixed the audit rebuild in v2.40.28).
 *
 * Why this exists:
 *   Client-side ingest was dropping ~73% of stops (13,198 of 49,847 saved).
 *   Root cause: mobile Safari killed the tab during sequential Firestore
 *   writes, and the Firebase SDK had a known hang where batch.commit()
 *   promises silently never resolved. Server-side has a 15-min budget,
 *   uses the Firestore REST API directly (no SDK), and writes 500 docs
 *   per HTTP call instead of 25.
 *
 * Status doc (nuvizz_ingest_logs/{run_id}):
 *   state:            "queued" | "running" | "complete" | "failed"
 *   started_at:       ISO
 *   completed_at:     ISO
 *   stop_count:       total stops received
 *   filter_dropped:   stops without pro+date
 *   to_save:          stops that passed filter
 *   saved_ok:         successfully written to Firestore
 *   saved_failed:     write failures
 *   batches_total:    number of :commit calls made
 *   batches_failed:   :commit calls that returned non-200
 *   prefix_counts:    { SHP: 225, MCC: 77, ... } for verification
 *   weeks_rebuilt:    nuvizz_weekly rollup count
 *   progress_text:    human-readable status for UI polling
 *   error:            error message if state=failed
 *
 * Env vars:
 *   FIREBASE_API_KEY — Firestore REST auth
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

// ─── Firestore REST helpers ──────────────────────────────────────────────────

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") {
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (typeof v === "string") return { stringValue: v };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

function toFsFields(obj: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(obj)) out[k] = toFsValue(v);
  return out;
}

async function writeStatusDoc(runId: string, data: Record<string, any>): Promise<void> {
  const url = `${BASE}/nuvizz_ingest_logs/${runId}?key=${FIREBASE_API_KEY}`;
  await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: toFsFields(data) }),
  });
}

async function batchWriteDocs(
  collection: string,
  docs: Array<{ docId: string; fields: Record<string, any> }>
): Promise<{ ok: number; failed: number }> {
  if (docs.length === 0) return { ok: 0, failed: 0 };
  // Recursive split for >500 (Firestore :commit hard limit)
  if (docs.length > 500) {
    let ok = 0, failed = 0;
    for (let i = 0; i < docs.length; i += 500) {
      const r = await batchWriteDocs(collection, docs.slice(i, i + 500));
      ok += r.ok; failed += r.failed;
    }
    return { ok, failed };
  }
  const url = `${BASE}:commit?key=${FIREBASE_API_KEY}`;
  const writes = docs.map(d => ({
    update: {
      name: `projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${d.docId}`,
      fields: toFsFields(d.fields),
    },
  }));
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ writes }),
  });
  if (!resp.ok) {
    console.error(`batchWrite ${collection} ${docs.length} docs: ${resp.status} ${await resp.text()}`);
    return { ok: 0, failed: docs.length };
  }
  const data: any = await resp.json();
  const writeResults = data.writeResults || [];
  const statuses = data.status || [];
  let explicitFailed = 0;
  for (const s of statuses) {
    if (s?.code && s.code !== 0) explicitFailed++;
  }
  if (writeResults.length > 0) return { ok: writeResults.length - explicitFailed, failed: explicitFailed };
  return { ok: docs.length - explicitFailed, failed: explicitFailed };
}

async function listWeekStops(weekEnding: string): Promise<any[]> {
  const stops: any[] = [];
  let pt = "";
  do {
    const params = new URLSearchParams({ key: FIREBASE_API_KEY || "", pageSize: "300" });
    if (pt) params.set("pageToken", pt);
    // Query nuvizz_stops where week_ending == weekEnding
    const url = `${BASE}:runQuery?key=${FIREBASE_API_KEY}`;
    const body = {
      structuredQuery: {
        from: [{ collectionId: "nuvizz_stops" }],
        where: { fieldFilter: { field: { fieldPath: "week_ending" }, op: "EQUAL", value: { stringValue: weekEnding } } },
        limit: 1000,
      }
    };
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!resp.ok) break;
    const results: any[] = await resp.json();
    for (const item of results) {
      if (!item.document) continue;
      const f = item.document.fields || {};
      const out: Record<string, any> = {};
      for (const [k, v] of Object.entries(f)) {
        const vv = v as any;
        if ("stringValue" in vv) out[k] = vv.stringValue;
        else if ("integerValue" in vv) out[k] = Number(vv.integerValue);
        else if ("doubleValue" in vv) out[k] = vv.doubleValue;
        else if ("booleanValue" in vv) out[k] = vv.booleanValue;
      }
      stops.push(out);
    }
    // runQuery doesn't return pageToken — it returns all results up to limit
    break;
  } while (pt);
  return stops;
}

// ─── Date helpers ────────────────────────────────────────────────────────────

function weekEndingFriday(isoDate: string | null): string | null {
  if (!isoDate) return null;
  const d = new Date(isoDate + "T12:00:00Z");
  if (isNaN(d.getTime())) return null;
  const dow = d.getUTCDay(); // 0=Sun, 5=Fri
  const daysToFriday = (5 - dow + 7) % 7;
  const friday = new Date(d.getTime() + daysToFriday * 86400000);
  return friday.toISOString().slice(0, 10);
}

function dateToMonth(isoDate: string): string {
  return isoDate.slice(0, 7);
}

// ─── PRO normalizer (strip leading zeros) ───────────────────────────────────

function normalizePro(v: any): string | null {
  if (v == null) return null;
  const s = String(v).trim().replace(/\.0+$/, "");
  if (!s) return null;
  const stripped = s.replace(/^0+/, "");
  return stripped || s;
}

// ─── Prefix extractor ───────────────────────────────────────────────────────

function extractPrefix(pro: string): string {
  const m = pro.match(/^([A-Za-z][A-Za-z\-_]*)/);
  if (m) return m[1].toUpperCase();
  if (pro[0] >= "0" && pro[0] <= "9") return "(numeric)";
  return "(other)";
}

// ─── NuVizz weekly rollup builder ───────────────────────────────────────────

function buildWeeklyRollup(stops: any[]): any {
  let stops_total = 0, pay_base_total = 0, pay_at_40_total = 0;
  const drivers = new Set<string>();
  for (const s of stops) {
    stops_total++;
    pay_base_total += s.contractor_pay_base || 0;
    pay_at_40_total += s.contractor_pay_at_40 || 0;
    if (s.driver_name) drivers.add(s.driver_name);
  }
  const week_ending = stops[0]?.week_ending;
  return {
    week_ending,
    month: week_ending ? dateToMonth(week_ending) : null,
    stops_total,
    pay_base_total: Math.round(pay_base_total * 100) / 100,
    pay_at_40_total: Math.round(pay_at_40_total * 100) / 100,
    driver_count: drivers.size,
    rebuilt_from_stops: true,
    updated_at: new Date().toISOString(),
  };
}

// ─── Main ────────────────────────────────────────────────────────────────────

export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") {
    return new Response("POST required", { status: 405 });
  }

  let body: any;
  try { body = await req.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const stops: any[] = body.stops || [];
  const runId: string = body.run_id || `nuvizz_srv_${Date.now()}`;
  const source: string = body.source || "server";

  if (stops.length === 0) {
    await writeStatusDoc(runId, { state: "failed", error: "No stops received" });
    return new Response("No stops", { status: 400 });
  }

  const startedAt = new Date().toISOString();
  await writeStatusDoc(runId, {
    run_id: runId, source, state: "running",
    started_at: startedAt, stop_count: stops.length,
    progress_text: `Starting server-side ingest of ${stops.length.toLocaleString()} stops...`,
  });

  try {
    // ── Step 1: Filter ──────────────────────────────────────────────────────
    const CONTRACTOR_PAY_PCT = 0.40;
    const toSave: Array<{ docId: string; fields: Record<string, any> }> = [];
    let filterDropped = 0;
    const prefixCounts: Record<string, number> = {};

    for (const s of stops) {
      const stopNum = s.stop_number ? String(s.stop_number).trim() : null;
      const pro = normalizePro(stopNum);
      const delivDate = s.delivery_date || null;

      if (!pro && !stopNum) { filterDropped++; continue; }
      if (!delivDate) { filterDropped++; continue; }

      const payBase = parseFloat(String(s.contractor_pay_base || 0)) || 0;
      const docId = pro || stopNum!;
      const prefix = extractPrefix(docId);
      prefixCounts[prefix] = (prefixCounts[prefix] || 0) + 1;

      const weekEnding = weekEndingFriday(delivDate);
      const fields: Record<string, any> = {
        stop_number: stopNum,
        pro: docId,
        driver_name: s.driver_name || null,
        delivery_date: delivDate,
        week_ending: weekEnding,
        month: weekEnding ? dateToMonth(weekEnding) : null,
        status: s.status || null,
        ship_to: s.ship_to || null,
        city: s.city || null,
        zip: s.zip || null,
        contractor_pay_base: payBase,
        contractor_pay_at_40: Math.round(payBase * CONTRACTOR_PAY_PCT * 10000) / 10000,
      };
      toSave.push({ docId, fields });
    }

    await writeStatusDoc(runId, {
      state: "running",
      filter_dropped: filterDropped,
      to_save: toSave.length,
      prefix_counts: prefixCounts,
      progress_text: `Filtered: ${toSave.length.toLocaleString()} to save, ${filterDropped} dropped. Writing to Firestore...`,
    });

    // ── Step 2: Save stops in 500-doc batches ───────────────────────────────
    let savedOk = 0, savedFailed = 0, batchesTotal = 0, batchesFailed = 0;
    const BATCH = 500;

    for (let i = 0; i < toSave.length; i += BATCH) {
      batchesTotal++;
      const chunk = toSave.slice(i, i + BATCH);
      const r = await batchWriteDocs("nuvizz_stops", chunk);
      savedOk += r.ok;
      savedFailed += r.failed;
      if (r.failed > 0) batchesFailed++;

      // Update progress every 5 batches
      if (batchesTotal % 5 === 0 || i + BATCH >= toSave.length) {
        await writeStatusDoc(runId, {
          state: "running",
          saved_ok: savedOk,
          saved_failed: savedFailed,
          batches_total: batchesTotal,
          progress_text: `Saving... ${savedOk.toLocaleString()}/${toSave.length.toLocaleString()} stops written (${batchesTotal} batches)`,
        });
      }
    }

    // ── Step 3: Rebuild weekly rollups for affected weeks ──────────────────
    const touchedWeeks = new Set<string>();
    for (const s of toSave) {
      if (s.fields.week_ending) touchedWeeks.add(s.fields.week_ending as string);
    }

    await writeStatusDoc(runId, {
      state: "running",
      progress_text: `Rebuilding ${touchedWeeks.size} weekly rollups...`,
    });

    let weeksRebuilt = 0;
    for (const weekEnding of touchedWeeks) {
      const weekStops = await listWeekStops(weekEnding);
      if (weekStops.length === 0) continue;
      const rollup = buildWeeklyRollup(weekStops);
      await batchWriteDocs("nuvizz_weekly", [{ docId: weekEnding, fields: rollup }]);
      weeksRebuilt++;
    }

    // ── Step 4: Final status ────────────────────────────────────────────────
    const completedAt = new Date().toISOString();
    const pct = toSave.length > 0 ? Math.round(savedOk / toSave.length * 100) : 0;
    await writeStatusDoc(runId, {
      run_id: runId,
      source,
      state: "complete",
      started_at: startedAt,
      completed_at: completedAt,
      stop_count: stops.length,
      filter_dropped: filterDropped,
      to_save: toSave.length,
      saved_ok: savedOk,
      saved_failed: savedFailed,
      batches_total: batchesTotal,
      batches_failed: batchesFailed,
      weeks_rebuilt: weeksRebuilt,
      prefix_counts: prefixCounts,
      progress_text: `✓ Complete: ${savedOk.toLocaleString()} stops saved (${pct}%), ${savedFailed} failed, ${weeksRebuilt} weeks rebuilt`,
    });

    console.log(`NuVizz ingest ${runId}: ${savedOk} ok, ${savedFailed} failed, ${weeksRebuilt} weeks rebuilt`);
    return new Response(JSON.stringify({ ok: true, run_id: runId, saved_ok: savedOk }), {
      status: 200, headers: { "Content-Type": "application/json" },
    });

  } catch (e: any) {
    console.error("NuVizz ingest background error:", e);
    await writeStatusDoc(runId, {
      state: "failed",
      error: e?.message || String(e),
      progress_text: `✗ Failed: ${e?.message || String(e)}`,
    });
    return new Response(JSON.stringify({ error: e?.message || "Unknown error" }), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }
};


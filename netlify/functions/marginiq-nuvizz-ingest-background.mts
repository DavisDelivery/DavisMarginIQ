import type { Context } from "@netlify/functions";
import { gunzipSync } from "node:zlib";

/**
 * Davis MarginIQ — NuVizz CSV Ingest Background Worker (v2.40.36)
 *
 * Reads NuVizz stops from staged Firestore chunks (written by the dispatcher)
 * and saves them to Firestore in 500-doc batches using the :commit endpoint
 * (same pattern that fixed the audit rebuild in v2.40.28).
 *
 * v2.40.36 change: chunks are now pulled from nuvizz_ingest_payloads/{run_id}__NNN
 * instead of being POSTed in the trigger body. This works around Lambda's
 * 256KB async-invocation body limit that was silently killing every weekly
 * ingest at HTTP 500. The chunks are gzipped+base64 and decoded here, then
 * deleted at the end of a successful run.
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
// 1099 contractors are paid 40% of each stop's SealNbr base. W2 drivers are
// paid hourly via CyberPay — pay_at_40 is meaningless for them. Used at
// rollup time only; per-stop docs no longer carry a precomputed pay_at_40.
const CONTRACTOR_PAY_PCT = 0.40;

// ─── Firestore REST helpers ──────────────────────────────────────────────────

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") {
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (typeof v === "string") return { stringValue: v };
  // v2.42.4: arrays must use arrayValue, not mapValue. Without this check,
  // typeof v === "object" matches arrays AND objects, so top_drivers (an
  // array) was being encoded as a map with stringified integer keys ("0",
  // "1", ...) instead of a proper Firestore array. The data was technically
  // present but downstream UI (sortable top_drivers list) couldn't read it.
  if (Array.isArray(v)) {
    return { arrayValue: { values: v.map(toFsValue) } };
  }
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
  // NuVizz weeks max out at ~3,500 stops historically. Firestore's runQuery
  // limit caps at 5000 docs per call. We use 5000 as a generous safety
  // margin without needing pagination — pagination on runQuery requires a
  // composite index (week_ending + orderBy field) that we'd have to create.
  // If a single week ever exceeds 5000 stops, this will silently truncate;
  // the safety check below catches that case.
  const url = `${BASE}:runQuery?key=${FIREBASE_API_KEY}`;
  const body = {
    structuredQuery: {
      from: [{ collectionId: "nuvizz_stops" }],
      where: { fieldFilter: { field: { fieldPath: "week_ending" }, op: "EQUAL", value: { stringValue: weekEnding } } },
      limit: 5000,
    }
  };
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    console.error(`listWeekStops failed for ${weekEnding}: HTTP ${resp.status}`);
    return [];
  }
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
  if (stops.length === 5000) {
    console.warn(`listWeekStops(${weekEnding}) hit the 5000-doc cap. Rollup may be incomplete.`);
  }
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
//
// v2.41.17: replaced slim 8-field rollup with the full rich schema produced
// by the client-side buildNuVizzWeekly() in MarginIQ.jsx so downstream UI
// (top_drivers, unique_customers, stops_completed/manually_completed split,
// effective-only pay totals) keeps working after server-side ingest. Prior
// version was overwriting rich rollups with downgraded stops_total/pay
// only, breaking the weekly summary tab.

function buildWeeklyRollup(stops: any[]): any {
  let stops_total = 0;
  let stops_completed = 0;
  let stops_manually_completed = 0;
  let stops_effective = 0;
  let pay_base_total = 0;
  let contractor_pay_if_all_1099 = 0;
  const unique_drivers = new Set<string>();
  const unique_customers = new Set<string>();
  const drivers: Record<string, { stops: number; pay_base: number; pay_at_40: number }> = {};

  for (const s of stops) {
    stops_total++;
    const st = String(s.status || "").toLowerCase().trim();
    const isCompleted = st === "completed";
    const isManual = st === "manually completed";
    const isEffective = isCompleted || isManual;
    if (isCompleted) stops_completed++;
    if (isManual) stops_manually_completed++;
    if (isEffective) {
      stops_effective++;
      const stopBase = Number(s.contractor_pay_base) || 0;
      pay_base_total += stopBase;
      contractor_pay_if_all_1099 += stopBase * CONTRACTOR_PAY_PCT;
      if (s.driver_name) {
        unique_drivers.add(s.driver_name);
        if (!drivers[s.driver_name]) {
          drivers[s.driver_name] = { stops: 0, pay_base: 0, pay_at_40: 0 };
        }
        drivers[s.driver_name].stops++;
        drivers[s.driver_name].pay_base += stopBase;
        // v2.46.2: pay_at_40 computed inline from base ratio. Per-driver
        // figure remains in the rollup as a "pay if 1099" hypothetical;
        // LaborReality and DriverPerformanceTab gate it on
        // driver_classifications before treating as actual cost.
        drivers[s.driver_name].pay_at_40 += stopBase * CONTRACTOR_PAY_PCT;
      }
      if (s.ship_to) unique_customers.add(s.ship_to);
    }
  }

  const week_ending = stops[0]?.week_ending;
  const top_drivers = Object.entries(drivers)
    .sort((a, b) => b[1].stops - a[1].stops)
    .slice(0, 60)
    .map(([name, v]) => ({
      name,
      stops: v.stops,
      pay_base: Math.round(v.pay_base * 100) / 100,
      pay_at_40: Math.round(v.pay_at_40 * 100) / 100,
    }));

  return {
    week_ending,
    month: week_ending ? dateToMonth(week_ending) : null,
    stops_total,
    stops_completed,
    stops_manually_completed,
    stops_effective,
    pay_base_total: Math.round(pay_base_total * 100) / 100,
    contractor_pay_if_all_1099: Math.round(contractor_pay_if_all_1099 * 100) / 100,
    unique_drivers: unique_drivers.size,
    unique_customers: unique_customers.size,
    top_drivers,
    rebuilt_from_stops: true,
    updated_at: new Date().toISOString(),
  };
}

// ─── Payload chunk reader (v2.40.36) ─────────────────────────────────────────
//
// Loads all nuvizz_ingest_payloads/{run_id}__NNN chunks staged by the
// dispatcher, decodes the gzipped base64 data, and reconstructs the full
// stops array. Returns { stops, chunkDocIds } so caller can delete the
// chunks after successful processing.

async function loadStagedPayload(runId: string): Promise<{ stops: any[]; chunkDocIds: string[]; chunkCount: number }> {
  const allStops: any[] = [];
  const chunkDocIds: string[] = [];
  let expectedChunkCount: number | null = null;

  // Walk pages of nuvizz_ingest_payloads, filtering by docId prefix in code
  // (Firestore REST has no docId-prefix query, so we filter client-side).
  let pageToken: string | undefined;
  const prefix = `${runId}__`;
  let safety = 0;
  do {
    const params = new URLSearchParams({
      key: FIREBASE_API_KEY || "",
      pageSize: "50",
    });
    if (pageToken) params.set("pageToken", pageToken);
    const url = `${BASE}/nuvizz_ingest_payloads?${params.toString()}`;
    const resp = await fetch(url);
    if (!resp.ok) {
      throw new Error(`Failed to list payload chunks: HTTP ${resp.status}`);
    }
    const data: any = await resp.json();
    for (const doc of (data.documents || [])) {
      const id: string = String(doc.name).split("/").pop() || "";
      if (!id.startsWith(prefix)) continue;
      chunkDocIds.push(id);
      const f = doc.fields || {};
      if (expectedChunkCount === null && f.chunk_count?.integerValue) {
        expectedChunkCount = Number(f.chunk_count.integerValue);
      }
      const b64: string = f.data_b64?.stringValue || "";
      if (!b64) continue;
      const gz = Buffer.from(b64, "base64");
      const raw = gunzipSync(gz).toString("utf8");
      const chunkStops = JSON.parse(raw);
      if (Array.isArray(chunkStops)) {
        for (const s of chunkStops) allStops.push(s);
      }
    }
    pageToken = data.nextPageToken;
    safety++;
    if (safety > 200) break; // hard guardrail
  } while (pageToken);

  // Sort chunkDocIds so deletes happen in order (purely cosmetic for log clarity)
  chunkDocIds.sort();

  return {
    stops: allStops,
    chunkDocIds,
    chunkCount: expectedChunkCount ?? chunkDocIds.length,
  };
}

async function deleteStagedChunks(chunkDocIds: string[]): Promise<{ ok: number; failed: number }> {
  let ok = 0, failed = 0;
  // Parallelize 10-at-a-time for speed; chunks are independent
  const CONC = 10;
  for (let i = 0; i < chunkDocIds.length; i += CONC) {
    const batch = chunkDocIds.slice(i, i + CONC);
    const results = await Promise.all(batch.map(async (id) => {
      const url = `${BASE}/nuvizz_ingest_payloads/${encodeURIComponent(id)}?key=${FIREBASE_API_KEY}`;
      const r = await fetch(url, { method: "DELETE" });
      return r.ok;
    }));
    for (const r of results) {
      if (r) ok++; else failed++;
    }
  }
  return { ok, failed };
}

// ─── Main ────────────────────────────────────────────────────────────────────

export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") {
    return new Response("POST required", { status: 405 });
  }

  let body: any;
  try { body = await req.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const runId: string = body.run_id || `nuvizz_srv_${Date.now()}`;
  const source: string = body.source || "server";

  // v2.40.36: stops are now loaded from Firestore chunks, not the request body.
  // The dispatcher writes nuvizz_ingest_payloads/{run_id}__NNN chunks before
  // calling us with just { run_id }. Backward compat: if body.stops is present
  // (old-style call), use it directly.
  let stops: any[] = body.stops || [];
  let chunkDocIds: string[] = [];

  if (stops.length === 0) {
    try {
      const staged = await loadStagedPayload(runId);
      stops = staged.stops;
      chunkDocIds = staged.chunkDocIds;
      if (stops.length === 0) {
        await writeStatusDoc(runId, {
          state: "failed",
          error: `No stops received and no payload chunks found for run_id=${runId}`,
        });
        return new Response("No stops", { status: 400 });
      }
    } catch (e: any) {
      await writeStatusDoc(runId, { state: "failed", error: `Chunk load failed: ${e.message}` });
      return new Response(`Chunk load failed: ${e.message}`, { status: 500 });
    }
  }

  const startedAt = new Date().toISOString();
  await writeStatusDoc(runId, {
    run_id: runId, source, state: "running",
    started_at: startedAt, stop_count: stops.length,
    chunk_count: chunkDocIds.length,
    progress_text: chunkDocIds.length > 0
      ? `Loaded ${stops.length.toLocaleString()} stops from ${chunkDocIds.length} chunk(s). Filtering...`
      : `Starting server-side ingest of ${stops.length.toLocaleString()} stops...`,
  });

  try {
    // ── Step 1: Filter ──────────────────────────────────────────────────────
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
      // v2.46.0: pass through every field the client sent (including `raw`,
      // delivery_end_at, delivery_start_at, and any future additions),
      // then override with server-canonical/derived fields. This means we
      // don't have to update this function every time we add a stop attribute.
      // v2.46.2: contractor_pay_at_40 is no longer written per-stop — the
      // 40% rate is only meaningful for 1099-classified drivers, applied
      // downstream at query time. Per-stop we keep only the base (SealNbr).
      const fields: Record<string, any> = {
        ...s,
        stop_number: stopNum,
        pro: docId,
        delivery_date: delivDate,
        week_ending: weekEnding,
        month: weekEnding ? dateToMonth(weekEnding) : null,
        contractor_pay_base: payBase,
      };
      // Strip any inbound contractor_pay_at_40 from older clients
      delete (fields as any).contractor_pay_at_40;
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

    // ── Step 4: Cleanup staged payload chunks ──────────────────────────────
    // Now that everything is durably written to nuvizz_stops/nuvizz_weekly,
    // the staged chunks in nuvizz_ingest_payloads can be deleted. Best-effort:
    // failures here just leave orphan chunks; they're harmless and a future
    // cleanup pass can remove them.
    let chunksDeleted = 0, chunksDeleteFailed = 0;
    if (chunkDocIds.length > 0) {
      const cleanup = await deleteStagedChunks(chunkDocIds);
      chunksDeleted = cleanup.ok;
      chunksDeleteFailed = cleanup.failed;
    }

    // ── Step 5: Final status ────────────────────────────────────────────────
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
      chunks_deleted: chunksDeleted,
      chunks_delete_failed: chunksDeleteFailed,
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


import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Audit Queue Rebuild (v2.40.10)
 *
 * Replaces the in-browser rebuild that was silently dropping ~84% of payment
 * data and ~37% of unpaid stops due to client-side caps. Runs server-side
 * with no caps.
 *
 * Algorithm:
 *   1. Page every row of `ddis_payments` → paymentByPro map (PRO → total paid)
 *   2. Page every row of `unpaid_stops`
 *   3. For each unpaid stop: variance = billed - paid, categorize by ratio + age
 *   4. Write all audit items in batchWrite (500/call) to `audit_items`
 *   5. Write status doc to `marginiq_config/audit_rebuild_status` so the UI
 *      can poll and display progress without a WebSocket.
 *
 * Why background:
 *   - ddis_payments is 122K+ rows. A client-side .get() truncates at ~20K
 *     (the old code's cap — and Firestore's client SDK also rate-limits).
 *   - Background function has 15-min budget vs 10s sync.
 *   - Dispatcher marginiq-audit-rebuild.mts returns 202 immediately; the
 *     actual work runs here. Matches the v2.40.5 backup pattern.
 *
 * Status doc schema (marginiq_config/audit_rebuild_status):
 *   state:            "running" | "complete" | "failed"
 *   started_at:       ISO timestamp
 *   completed_at:     ISO timestamp (when state != "running")
 *   phase:            "reading_payments" | "reading_stops" | "computing" | "writing" | "done"
 *   progress_text:    human string shown in the UI
 *   payment_rows_read: integer
 *   unpaid_stops_read: integer
 *   items_generated:  integer
 *   items_written:    integer
 *   total_variance:   number
 *   with_payments:    boolean (true when paymentByPro is populated)
 *   error:            string | null
 *
 * Env vars:
 *   FIREBASE_API_KEY  — Firestore REST
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

// ─── Firestore REST helpers ────────────────────────────────────────

async function listAllDocsMask(
  collection: string,
  fieldPaths: string[],
  onPage?: (count: number) => Promise<void> | void,
): Promise<any[]> {
  const out: any[] = [];
  let pageToken: string | undefined;
  let pages = 0;
  do {
    const params = new URLSearchParams({
      key: FIREBASE_API_KEY || "",
      pageSize: "300",
    });
    for (const fp of fieldPaths) params.append("mask.fieldPaths", fp);
    if (pageToken) params.set("pageToken", pageToken);
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}?${params.toString()}`;
    const resp = await fetch(url);
    if (!resp.ok) {
      if (resp.status === 404) return out;
      throw new Error(`List ${collection} failed: HTTP ${resp.status} ${await resp.text()}`);
    }
    const data: any = await resp.json();
    const docs = data.documents || [];
    out.push(...docs);
    pageToken = data.nextPageToken;
    pages++;
    if (onPage) await onPage(out.length);
    if (pages > 2000) {
      console.warn(`list ${collection}: hit page ceiling 2000, stopping`);
      break;
    }
  } while (pageToken);
  return out;
}

async function patchDoc(collection: string, docId: string, patch: Record<string, any>): Promise<boolean> {
  const fieldPaths = Object.keys(patch);
  const params = new URLSearchParams({ key: FIREBASE_API_KEY || "" });
  for (const fp of fieldPaths) params.append("updateMask.fieldPaths", fp);
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?${params.toString()}`;
  const fields: Record<string, any> = {};
  for (const [k, v] of Object.entries(patch)) fields[k] = toFsValue(v);
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  return resp.ok;
}

async function batchWriteDocs(
  collection: string,
  docs: Array<{ docId: string; fields: any }>,
): Promise<{ ok: number; failed: number }> {
  if (docs.length === 0) return { ok: 0, failed: 0 };
  if (docs.length > 500) {
    let ok = 0, failed = 0;
    for (let i = 0; i < docs.length; i += 500) {
      const r = await batchWriteDocs(collection, docs.slice(i, i + 500));
      ok += r.ok; failed += r.failed;
    }
    return { ok, failed };
  }
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default):batchWrite?key=${FIREBASE_API_KEY}`;
  const writes = docs.map(d => ({
    update: {
      name: `projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${d.docId}`,
      fields: d.fields,
    },
  }));
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ writes }),
  });
  if (!resp.ok) {
    console.error(`batchWrite ${collection} failed: ${resp.status} ${await resp.text()}`);
    return { ok: 0, failed: docs.length };
  }
  const data: any = await resp.json();
  const statuses = data.status || [];
  let ok = 0, failed = 0;
  for (const s of statuses) {
    if (!s.code || s.code === 0) ok++; else failed++;
  }
  return { ok, failed };
}

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "string") return { stringValue: v };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") {
    if (!isFinite(v)) return { nullValue: null };
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (Array.isArray(v)) return { arrayValue: { values: v.map(toFsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

function fromFsValue(v: any): any {
  if (!v) return null;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return v.doubleValue;
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("arrayValue" in v) return (v.arrayValue?.values || []).map(fromFsValue);
  if ("mapValue" in v) {
    const out: Record<string, any> = {};
    for (const [k, val] of Object.entries(v.mapValue?.fields || {})) out[k] = fromFsValue(val);
    return out;
  }
  return null;
}

// ─── Audit domain helpers (mirror MarginIQ.jsx) ────────────────────

function driverKey(name: string | null): string | null {
  if (!name) return null;
  return (String(name).trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 140)) || null;
}
const customerKey = driverKey;

function categorize(billed: number, paid: number, hasAccessorial: boolean, accessorialPaid: number, ageDays: number): string {
  if (billed <= 0) return "orphan";
  const ratio = paid / billed;
  if (ratio >= 0.99) return "paid_in_full";
  if (ratio >= 0.5 && ratio < 0.98) return "short_paid";
  if (paid <= 0 && ageDays >= 45) return "zero_pay";
  if (paid <= 0) return "short_paid";
  if (ratio > 1.01) return "overpaid";
  if (hasAccessorial && accessorialPaid <= 0 && ratio >= 0.5) return "accessorial_ignored";
  return "short_paid";
}
function ageBucket(ageDays: number | null): string {
  if (ageDays == null || ageDays < 0) return "unknown";
  if (ageDays <= 30) return "0-30";
  if (ageDays <= 60) return "31-60";
  if (ageDays <= 90) return "61-90";
  if (ageDays <= 180) return "91-180";
  if (ageDays <= 365) return "181-365";
  return "365+";
}
function daysBetween(fromISO: string | null, toISO: string | null): number | null {
  if (!fromISO) return null;
  const a = new Date(fromISO + "T00:00:00");
  const b = new Date((toISO || new Date().toISOString().slice(0, 10)) + "T00:00:00");
  if (isNaN(a.getTime()) || isNaN(b.getTime())) return null;
  return Math.floor((b.getTime() - a.getTime()) / (1000 * 60 * 60 * 24));
}

// ─── Status writer ──────────────────────────────────────────────────

async function writeStatus(patch: Record<string, any>): Promise<void> {
  // We include started_at on every write only if the caller passes it.
  await patchDoc("marginiq_config", "audit_rebuild_status", patch);
}

// ─── Main rebuild ───────────────────────────────────────────────────

async function rebuild(): Promise<{ ok: true; generated: number; written: number; totalVariance: number; withPayments: boolean } | { ok: false; error: string }> {
  const startedAt = new Date().toISOString();
  await writeStatus({
    state: "running",
    started_at: startedAt,
    completed_at: null,
    phase: "reading_payments",
    progress_text: "Reading ddis_payments…",
    payment_rows_read: 0,
    unpaid_stops_read: 0,
    items_generated: 0,
    items_written: 0,
    total_variance: 0,
    with_payments: false,
    error: null,
  });

  try {
    // 1) Read all ddis_payments → paymentByPro map
    const paymentByPro: Record<string, number> = {};
    let paymentRowsRead = 0;
    const payDocs = await listAllDocsMask(
      "ddis_payments",
      ["pro", "paid_amount", "paid"],
      async (count) => {
        paymentRowsRead = count;
        if (count % 3000 < 300) {
          // Update status ~every 3000 rows (every ~10 pages of 300)
          await writeStatus({
            phase: "reading_payments",
            progress_text: `Reading ddis_payments… ${count.toLocaleString()} rows`,
            payment_rows_read: count,
          }).catch(() => {});
        }
      },
    );
    for (const doc of payDocs) {
      const fields = doc.fields || {};
      const pro = fields.pro?.stringValue;
      if (!pro) continue;
      const paidRaw = fields.paid_amount ?? fields.paid;
      let amt = 0;
      if (paidRaw) {
        amt = Number(paidRaw.doubleValue ?? paidRaw.integerValue ?? paidRaw.stringValue ?? 0);
      }
      if (amt > 0) paymentByPro[pro] = (paymentByPro[pro] || 0) + amt;
    }
    const prosWithPayments = Object.keys(paymentByPro).length;
    const hasPerProPayments = prosWithPayments > 0;
    console.log(`audit-rebuild: ${payDocs.length} payment rows read, ${prosWithPayments} unique PROs with payments`);
    await writeStatus({
      phase: "reading_stops",
      progress_text: `Found ${prosWithPayments.toLocaleString()} PROs with payments. Reading ddis_files…`,
      payment_rows_read: payDocs.length,
      with_payments: hasPerProPayments,
    });

    // v2.40.12: load ddis_files to build the set of Friday-weeks that have a
    // matching payment file. A stop whose week_ending isn't in this set (and
    // isn't in the "recent" grace window below) is exempted from the audit —
    // we're missing the DDIS for its week, so we can't know if it's truly
    // unpaid or just unmatched.
    const ddisFilesDocs = await listAllDocsMask(
      "ddis_files",
      ["bill_week_ending", "covers_weeks", "week_ambiguous"],
    );
    const coveredWeeks = new Set<string>();
    for (const fdoc of ddisFilesDocs) {
      const f = fdoc.fields || {};
      // covers_weeks is the authoritative multi-week list (v2.40.12).
      // Fallback to bill_week_ending for pre-backfill files.
      const cw = f.covers_weeks?.arrayValue?.values || [];
      if (cw.length > 0) {
        for (const v of cw) {
          const s = v?.stringValue;
          if (s) coveredWeeks.add(s);
        }
      } else {
        const bwe = f.bill_week_ending?.stringValue;
        if (bwe) coveredWeeks.add(bwe);
      }
    }
    console.log(`audit-rebuild: ${ddisFilesDocs.length} ddis_files docs, ${coveredWeeks.size} unique covered weeks`);

    // 2) Read all unpaid_stops
    const stopDocs = await listAllDocsMask(
      "unpaid_stops",
      ["pro", "billed", "customer", "city", "state", "zip", "pu_date", "week_ending", "month", "code", "weight", "order", "service_type"],
    );
    console.log(`audit-rebuild: ${stopDocs.length} unpaid_stops read`);
    await writeStatus({
      phase: "computing",
      progress_text: `Computing variances against ${stopDocs.length.toLocaleString()} unpaid stops…`,
      unpaid_stops_read: stopDocs.length,
    });

    // v2.40.12: recent-week cutoff. Stops with week_ending within the last
    // RECENT_DAYS days are exempt from the audit queue regardless of DDIS
    // match — the DDIS file for that week may not have arrived yet.
    const RECENT_DAYS = 21;
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - RECENT_DAYS);
    const recentCutoffISO = cutoffDate.toISOString().slice(0, 10);

    // 3) Compute audit items
    const today = new Date().toISOString().slice(0, 10);
    const items: Array<{ docId: string; item: any }> = [];
    let totalVariance = 0;
    let skippedRecent = 0;       // exempted — within last RECENT_DAYS
    let skippedNoDdis = 0;       // no matching DDIS file for this week
    let skippedNoWeek = 0;       // unpaid_stop has no week_ending at all
    const awaitingWeeks = new Map<string, { stops: number; billed: number }>(); // per-week sideline
    const recentWeeks = new Map<string, { stops: number; billed: number }>();
    for (const sdoc of stopDocs) {
      const f = sdoc.fields || {};
      const pro = f.pro?.stringValue;
      if (!pro) continue;
      const billed = Number(f.billed?.doubleValue ?? f.billed?.integerValue ?? 0);
      if (billed <= 0) continue;
      const weekEnding = f.week_ending?.stringValue || null;
      // v2.40.12 filters —
      if (!weekEnding) {
        skippedNoWeek += 1;
        continue;
      }
      if (weekEnding >= recentCutoffISO) {
        // Too recent — exempt from audit, count on sideline
        skippedRecent += 1;
        const agg = recentWeeks.get(weekEnding) || { stops: 0, billed: 0 };
        agg.stops += 1; agg.billed += billed;
        recentWeeks.set(weekEnding, agg);
        continue;
      }
      if (!coveredWeeks.has(weekEnding)) {
        // Older week but no matching DDIS file — can't match payments
        skippedNoDdis += 1;
        const agg = awaitingWeeks.get(weekEnding) || { stops: 0, billed: 0 };
        agg.stops += 1; agg.billed += billed;
        awaitingWeeks.set(weekEnding, agg);
        continue;
      }
      const paid = hasPerProPayments ? (paymentByPro[pro] || 0) : 0;
      const variance = billed - paid;
      if (variance <= 1) continue; // skip dust / fully-paid
      const customer = f.customer?.stringValue || "Unknown";
      const pu_date = f.pu_date?.stringValue || null;
      const ageDays = daysBetween(pu_date, today) || 0;
      const category = categorize(billed, paid, false, 0, ageDays);
      const variance_pct = billed > 0 ? (variance / billed * 100) : null;
      const item = {
        pro,
        customer,
        customer_key: customerKey(customer),
        city: f.city?.stringValue || null,
        state: f.state?.stringValue || null,
        zip: f.zip?.stringValue || null,
        pu_date,
        week_ending: weekEnding,
        month: f.month?.stringValue || null,
        billed,
        paid,
        variance,
        variance_pct,
        accessorial_amount: 0,
        base_cost: 0,
        code: f.code?.stringValue || null,
        weight: Number(f.weight?.doubleValue ?? f.weight?.integerValue ?? 0) || null,
        order: f.order?.stringValue || null,
        service_type: f.service_type?.stringValue || "delivery", // v2.40.14: for TK filter
        age_days: ageDays,
        age_bucket: ageBucket(ageDays),
        category,
        dispute_status: "new",
        notes: [] as any[],
        rebuilt_from_firestore: true,
        rebuild_source: "background_v2.40.14",
        updated_at: new Date().toISOString(),
      };
      totalVariance += variance;
      items.push({ docId: pro, item });
    }
    // v2.40.12: flatten the sideline maps into sortable arrays for the status doc
    const recentWeeksArr = [...recentWeeks.entries()]
      .map(([wk, v]) => ({ week: wk, stops: v.stops, billed: Math.round(v.billed * 100) / 100 }))
      .sort((a, b) => (a.week < b.week ? 1 : -1));
    const awaitingWeeksArr = [...awaitingWeeks.entries()]
      .map(([wk, v]) => ({ week: wk, stops: v.stops, billed: Math.round(v.billed * 100) / 100 }))
      .sort((a, b) => (a.week < b.week ? 1 : -1));
    const recentTotalBilled = recentWeeksArr.reduce((s, r) => s + r.billed, 0);
    const awaitingTotalBilled = awaitingWeeksArr.reduce((s, r) => s + r.billed, 0);
    console.log(
      `audit-rebuild: ${items.length} audit items, $${totalVariance.toFixed(2)} variance | ` +
      `sidelined — recent:${skippedRecent} stops across ${recentWeeksArr.length}wks ($${recentTotalBilled.toFixed(0)}) · ` +
      `awaiting-ddis:${skippedNoDdis} stops across ${awaitingWeeksArr.length}wks ($${awaitingTotalBilled.toFixed(0)}) · ` +
      `no-week:${skippedNoWeek}`
    );
    await writeStatus({
      phase: "writing",
      progress_text: `Writing ${items.length.toLocaleString()} audit items…`,
      items_generated: items.length,
      total_variance: totalVariance,
      // v2.40.12 sideline counts
      sidelined_recent_stops: skippedRecent,
      sidelined_recent_billed: recentTotalBilled,
      sidelined_recent_weeks: recentWeeksArr,
      sidelined_awaiting_stops: skippedNoDdis,
      sidelined_awaiting_billed: awaitingTotalBilled,
      sidelined_awaiting_weeks: awaitingWeeksArr,
      sidelined_noweek_stops: skippedNoWeek,
      recent_cutoff_iso: recentCutoffISO,
      covered_weeks_count: coveredWeeks.size,
    });

    // 4) Convert items to Firestore field shape and batch-write
    const toWrite = items.map(({ docId, item }) => ({
      docId,
      fields: toFsValue(item).mapValue.fields,
    }));
    const result = await batchWriteDocs("audit_items", toWrite);
    console.log(`audit-rebuild: wrote ${result.ok} / ${toWrite.length} items (${result.failed} failed)`);

    const sidelineNote = (skippedRecent || skippedNoDdis || skippedNoWeek)
      ? ` · sidelined: ${skippedRecent} recent, ${skippedNoDdis} awaiting DDIS, ${skippedNoWeek} no-week`
      : "";
    await writeStatus({
      state: "complete",
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: `✓ Rebuilt ${result.ok.toLocaleString()} audit items ($${Math.round(totalVariance).toLocaleString()} variance) from ${payDocs.length.toLocaleString()} payment rows × ${stopDocs.length.toLocaleString()} unpaid stops${sidelineNote}`,
      items_written: result.ok,
      with_payments: hasPerProPayments,
    });

    return {
      ok: true,
      generated: items.length,
      written: result.ok,
      totalVariance,
      withPayments: hasPerProPayments,
    };
  } catch (e: any) {
    const msg = e?.message || String(e);
    console.error("audit-rebuild FAILED:", msg);
    await writeStatus({
      state: "failed",
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: `✗ Rebuild failed: ${msg}`,
      error: msg,
    }).catch(() => {});
    return { ok: false, error: msg };
  }
}

// ─── Entry point ────────────────────────────────────────────────────

export default async (_req: Request, _context: Context) => {
  if (!FIREBASE_API_KEY) {
    console.error("audit-rebuild-background: missing FIREBASE_API_KEY env var");
    return;
  }
  const t0 = Date.now();
  console.log("audit-rebuild-background: start");
  const r = await rebuild();
  const elapsed = Math.round((Date.now() - t0) / 1000);
  if (r.ok) {
    console.log(`audit-rebuild-background: done in ${elapsed}s — ${r.written} items, $${r.totalVariance.toFixed(2)} variance, withPayments=${r.withPayments}`);
  } else {
    console.error(`audit-rebuild-background: FAILED in ${elapsed}s — ${(r as any).error}`);
  }
};

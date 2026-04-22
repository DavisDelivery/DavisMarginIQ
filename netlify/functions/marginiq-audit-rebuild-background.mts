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
  // v2.40.15: Firestore REST batchWrite returns writeResults[] + status[].
  // When everything succeeds, status may be empty/missing — only populated
  // for failures. Prior logic counted only status entries with code===0,
  // which meant all-success returned ok=0 (and the dashboard never updated).
  // Fix: ground truth is writeResults.length; subtract explicit failures.
  const writeResults = data.writeResults || [];
  const statuses = data.status || [];
  let explicitFailed = 0;
  for (const s of statuses) {
    if (s && s.code && s.code !== 0) explicitFailed++;
  }
  // If we have writeResults, use that as the source of truth. Otherwise,
  // fall back to statuses (older API surface or partial responses).
  if (writeResults.length > 0) {
    return { ok: writeResults.length - explicitFailed, failed: explicitFailed };
  }
  // No writeResults AND no statuses — unclear, assume no-op
  if (statuses.length === 0) return { ok: docs.length, failed: 0 };
  // Only statuses populated — count OK from those
  let okFromStatus = 0;
  for (const s of statuses) {
    if (!s || !s.code || s.code === 0) okFromStatus++;
  }
  return { ok: okFromStatus, failed: statuses.length - okFromStatus };
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

    // v2.40.18: Read existing audit_items so we can:
    //   (a) preserve human-set fields (dispute_status, notes, original_variance,
    //       recovered_at/recovered_amount) through rebuilds
    //   (b) detect PROs that were in the queue before but have now been paid
    //       (they don't generate a new item this pass) → transition those to
    //       status="recovered_paid" with recovered_amount = prior variance
    const priorAuditDocs = await listAllDocsMask(
      "audit_items",
      ["pro", "variance", "billed", "paid", "dispute_status", "notes",
       "recovered_at", "recovered_amount", "original_variance",
       "first_seen_at", "created_at", "week_ending"],
    );
    const priorByPro = new Map<string, any>();
    for (const pdoc of priorAuditDocs) {
      const f = pdoc.fields || {};
      const pro = f.pro?.stringValue || pdoc.name?.split("/").pop();
      if (!pro) continue;
      priorByPro.set(pro, {
        variance: Number(f.variance?.doubleValue ?? f.variance?.integerValue ?? 0),
        billed: Number(f.billed?.doubleValue ?? f.billed?.integerValue ?? 0),
        paid: Number(f.paid?.doubleValue ?? f.paid?.integerValue ?? 0),
        dispute_status: f.dispute_status?.stringValue || "new",
        notes: f.notes?.arrayValue?.values || [],
        recovered_at: f.recovered_at?.stringValue || null,
        recovered_amount: Number(f.recovered_amount?.doubleValue ?? f.recovered_amount?.integerValue ?? 0) || null,
        original_variance: Number(f.original_variance?.doubleValue ?? f.original_variance?.integerValue ?? 0) || null,
        first_seen_at: f.first_seen_at?.stringValue || f.created_at?.stringValue || null,
        week_ending: f.week_ending?.stringValue || null,
      });
    }
    console.log(`audit-rebuild: ${priorByPro.size} prior audit_items loaded (for history/recovered-paid tracking)`);

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
    let skippedTruckload = 0;    // v2.40.19: TK stops don't settle through DDIS
    const awaitingWeeks = new Map<string, { stops: number; billed: number }>(); // per-week sideline
    const recentWeeks = new Map<string, { stops: number; billed: number }>();
    const truckloadWeeks = new Map<string, { stops: number; billed: number }>();
    const truckloadPros = new Set<string>(); // track which PROs were sidelined as TK

    // v2.40.19: A stop is considered a truckload if its PRO has the Uline
    // truckload prefix (ULI-) OR its service_type was tagged "truckload" at
    // ingest time. Uline settles LTL and TK through separate payment streams
    // — ddis_payments only covers LTL. Until we ingest a TK payment feed,
    // TK stops cannot be audited against DDIS and must be sidelined.
    const isTruckload = (pro: string, serviceType: string | null) =>
      /^ULI-/i.test(pro) || serviceType === "truckload";

    for (const sdoc of stopDocs) {
      const f = sdoc.fields || {};
      const pro = f.pro?.stringValue;
      if (!pro) continue;
      const billed = Number(f.billed?.doubleValue ?? f.billed?.integerValue ?? 0);
      if (billed <= 0) continue;
      const serviceType = f.service_type?.stringValue || null;
      // v2.40.19: TK sideline first — takes priority over all other reasons.
      // Even if the TK stop is in a recent or uncovered week, we still mark it
      // TK-sidelined rather than recent/awaiting, because the blocking issue
      // is the payment-stream gap, not the DDIS file timing.
      if (isTruckload(pro, serviceType)) {
        skippedTruckload += 1;
        truckloadPros.add(pro);
        const weekKey = f.week_ending?.stringValue || "(no-week)";
        const agg = truckloadWeeks.get(weekKey) || { stops: 0, billed: 0 };
        agg.stops += 1; agg.billed += billed;
        truckloadWeeks.set(weekKey, agg);
        continue;
      }
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
      // v2.40.18: preserve human-set fields across rebuilds.
      const prior = priorByPro.get(pro);
      const priorStatus = prior?.dispute_status || "new";
      // Dispute status: never downgrade a human-set status back to "new".
      // If user had marked this "queued"/"sent"/etc, keep it.
      const keepStatuses = new Set(["queued", "sent", "won", "lost", "partial", "written_off", "recovered_paid"]);
      const dispute_status = keepStatuses.has(priorStatus) ? priorStatus : "new";
      // If an item was previously recovered_paid and variance reappeared,
      // something changed (maybe Uline clawed back? or new DDIS revealed they
      // didn't pay it all). Reset to "new" so it gets triaged fresh.
      const effectiveStatus = (priorStatus === "recovered_paid" && variance > 1) ? "new" : dispute_status;
      const item: any = {
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
        dispute_status: effectiveStatus,
        notes: prior?.notes || [],
        // v2.40.18: track original variance (first time we saw this PRO owe money)
        // so a later recovered_paid event can show how much we actually recovered.
        original_variance: prior?.original_variance || variance,
        first_seen_at: prior?.first_seen_at || new Date().toISOString(),
        rebuilt_from_firestore: true,
        rebuild_source: "background_v2.40.18",
        updated_at: new Date().toISOString(),
      };
      // If item had been recovered_paid but variance reappeared, clear those fields
      // so they don't show phantom recovered data in the UI.
      if (priorStatus === "recovered_paid" && variance > 1) {
        item.recovered_at = null;
        item.recovered_amount = null;
      }
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
      `truckload:${skippedTruckload} stops across ${truckloadWeeks.size}wks · ` +
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
      // v2.40.19: truckload sideline counts (early preview for UI progress)
      sidelined_truckload_stops: skippedTruckload,
      recent_cutoff_iso: recentCutoffISO,
      covered_weeks_count: coveredWeeks.size,
    });

    // 4) Convert items to Firestore field shape and batch-write
    // v2.40.18: before converting, walk priorByPro for PROs that did NOT
    // produce a new item this pass. Possibilities:
    //   (a) Still has variance > 1 but got sidelined (recent/awaiting-DDIS/no-week)
    //       → keep the prior doc as-is so dispute tracking isn't lost. We
    //         just upsert the prior fields back (no-op effectively, but
    //         guarantees the doc stays present).
    //   (b) Variance dropped to ≤ 1 (Uline finally paid)
    //       → transition to recovered_paid with recovered_amount = prior variance
    //         UNLESS the prior status was already in a terminal state
    //         (won/lost/partial/written_off/recovered_paid) — then preserve.
    const newProSet = new Set(items.map(i => i.docId));
    const stopsByPro = new Map<string, any>();
    for (const sd of stopDocs) {
      const pro = sd.fields?.pro?.stringValue;
      if (pro) stopsByPro.set(pro, sd);
    }
    let recoveredCount = 0;
    let recoveredAmount = 0;
    let preservedCount = 0;
    let sidelinedTkCount = 0;  // v2.40.19: prior items transitioned to sidelined_tk
    const nowIso = new Date().toISOString();
    const sidelinedTkItems: Array<{ docId: string; item: any }> = [];
    for (const [pro, prior] of priorByPro) {
      if (newProSet.has(pro)) continue; // already handled by normal loop
      const terminalStatuses = new Set(["won", "lost", "partial", "written_off"]);
      if (terminalStatuses.has(prior.dispute_status)) {
        // Already finalized — preserve exactly. We don't write these back at
        // all (they stay in Firestore untouched). Skip to next.
        preservedCount++;
        continue;
      }
      if (prior.dispute_status === "recovered_paid") {
        // Already marked recovered. Don't rewrite — it stays as-is.
        preservedCount++;
        continue;
      }
      // v2.40.19: if this prior item's PRO is TK-shaped OR the underlying
      // stop was sidelined as TK this run, transition it to sidelined_tk.
      // This cleans up pre-v2.40.19 TK items that leaked into the LTL queue.
      const stopDoc = stopsByPro.get(pro);
      const sf0 = stopDoc?.fields || {};
      const priorIsTk = isTruckload(pro, sf0.service_type?.stringValue || null) || truckloadPros.has(pro);
      if (priorIsTk && prior.dispute_status !== "sidelined_tk") {
        const sidelinedItem: any = {
          pro,
          customer: sf0.customer?.stringValue || "Unknown",
          customer_key: customerKey(sf0.customer?.stringValue || "Unknown"),
          city: sf0.city?.stringValue || null,
          state: sf0.state?.stringValue || null,
          zip: sf0.zip?.stringValue || null,
          pu_date: sf0.pu_date?.stringValue || null,
          week_ending: sf0.week_ending?.stringValue || prior.week_ending || null,
          month: sf0.month?.stringValue || null,
          billed: Number(sf0.billed?.doubleValue ?? sf0.billed?.integerValue ?? 0) || prior.billed,
          paid: 0,
          variance: 0,  // zero out — TK payment stream not wired yet
          variance_pct: null,
          accessorial_amount: 0,
          base_cost: 0,
          code: sf0.code?.stringValue || null,
          weight: Number(sf0.weight?.doubleValue ?? sf0.weight?.integerValue ?? 0) || null,
          order: sf0.order?.stringValue || null,
          service_type: "truckload",
          age_days: daysBetween(sf0.pu_date?.stringValue || null, today) || 0,
          age_bucket: ageBucket(daysBetween(sf0.pu_date?.stringValue || null, today) || 0),
          category: "truckload_sidelined",
          dispute_status: "sidelined_tk",
          notes: prior.notes || [],
          original_variance: prior.original_variance || prior.variance,
          first_seen_at: prior.first_seen_at || nowIso,
          rebuilt_from_firestore: true,
          rebuild_source: "background_v2.40.19",
          updated_at: nowIso,
          sidelined_reason: "truckload_payments_not_wired",
        };
        sidelinedTkItems.push({ docId: pro, item: sidelinedItem });
        sidelinedTkCount++;
        continue;
      }
      if (prior.dispute_status === "sidelined_tk") {
        // Already sidelined — preserve.
        preservedCount++;
        continue;
      }
      // Did we see this PRO in unpaid_stops this run? If yes, the reason it
      // didn't produce an item is sidelining or variance ≤ 1 (paid up).
      if (stopDoc) {
        const sf = stopDoc.fields || {};
        const billed = Number(sf.billed?.doubleValue ?? sf.billed?.integerValue ?? 0);
        const paid = hasPerProPayments ? (paymentByPro[pro] || 0) : 0;
        const variance = billed - paid;
        if (variance <= 1 && prior.variance > 1) {
          // Uline paid. Transition to recovered_paid.
          const recoveredItem: any = {
            pro,
            customer: sf.customer?.stringValue || "Unknown",
            customer_key: customerKey(sf.customer?.stringValue || "Unknown"),
            city: sf.city?.stringValue || null,
            state: sf.state?.stringValue || null,
            zip: sf.zip?.stringValue || null,
            pu_date: sf.pu_date?.stringValue || null,
            week_ending: sf.week_ending?.stringValue || prior.week_ending || null,
            month: sf.month?.stringValue || null,
            billed,
            paid,
            variance: 0,
            variance_pct: 0,
            accessorial_amount: 0,
            base_cost: 0,
            code: sf.code?.stringValue || null,
            weight: Number(sf.weight?.doubleValue ?? sf.weight?.integerValue ?? 0) || null,
            order: sf.order?.stringValue || null,
            service_type: sf.service_type?.stringValue || "delivery",
            age_days: daysBetween(sf.pu_date?.stringValue || null, today) || 0,
            age_bucket: ageBucket(daysBetween(sf.pu_date?.stringValue || null, today) || 0),
            category: "recovered",
            dispute_status: "recovered_paid",
            notes: prior.notes || [],
            original_variance: prior.original_variance || prior.variance,
            first_seen_at: prior.first_seen_at || nowIso,
            recovered_at: nowIso,
            recovered_amount: prior.variance,
            rebuilt_from_firestore: true,
            rebuild_source: "background_v2.40.18",
            updated_at: nowIso,
          };
          items.push({ docId: pro, item: recoveredItem });
          recoveredCount++;
          recoveredAmount += prior.variance;
        }
        // else: still has variance but was sidelined — prior doc stays
        // untouched. Don't write it back.
      }
      // else: prior PRO not in unpaid_stops at all (stop was deleted/pruned).
      // Leave prior doc alone — it either shows the user a historical record
      // or a stale pointer they can manually clean up.
    }
    if (recoveredCount > 0) {
      console.log(`audit-rebuild: ${recoveredCount} PROs transitioned to recovered_paid ($${recoveredAmount.toFixed(2)} recovered)`);
    }
    if (preservedCount > 0) {
      console.log(`audit-rebuild: ${preservedCount} prior items preserved in terminal status (won/lost/partial/written_off/recovered_paid/sidelined_tk)`);
    }
    if (sidelinedTkCount > 0) {
      console.log(`audit-rebuild: ${sidelinedTkCount} prior items transitioned to sidelined_tk (truckload payments not wired)`);
    }

    // v2.40.19: merge sidelined_tk transitions into the write batch so they
    // get persisted alongside the regular + recovered items.
    const allItems = items.concat(sidelinedTkItems);
    const toWrite = allItems.map(({ docId, item }) => ({
      docId,
      fields: toFsValue(item).mapValue.fields,
    }));
    const result = await batchWriteDocs("audit_items", toWrite);
    console.log(`audit-rebuild: wrote ${result.ok} / ${toWrite.length} items (${result.failed} failed)`);

    // v2.40.19: flatten truckload sideline map for the status doc
    const truckloadWeeksArr = [...truckloadWeeks.entries()]
      .map(([wk, v]) => ({ week: wk, stops: v.stops, billed: Math.round(v.billed * 100) / 100 }))
      .sort((a, b) => (a.week < b.week ? 1 : -1));
    const truckloadTotalBilled = truckloadWeeksArr.reduce((s, r) => s + r.billed, 0);

    const sidelineNote = (skippedRecent || skippedNoDdis || skippedNoWeek || skippedTruckload)
      ? ` · sidelined: ${skippedRecent} recent, ${skippedNoDdis} awaiting DDIS, ${skippedNoWeek} no-week, ${skippedTruckload} TK`
      : "";
    const recoveredNote = recoveredCount > 0
      ? ` · 🏆 ${recoveredCount} newly recovered ($${Math.round(recoveredAmount).toLocaleString()})`
      : "";
    const tkNote = sidelinedTkCount > 0
      ? ` · 🚛 ${sidelinedTkCount} prior items auto-sidelined as TK`
      : "";
    await writeStatus({
      state: "complete",
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: `✓ Rebuilt ${result.ok.toLocaleString()} audit items ($${Math.round(totalVariance).toLocaleString()} variance) from ${payDocs.length.toLocaleString()} payment rows × ${stopDocs.length.toLocaleString()} unpaid stops${sidelineNote}${recoveredNote}${tkNote}`,
      items_written: result.ok,
      with_payments: hasPerProPayments,
      newly_recovered_count: recoveredCount,
      newly_recovered_amount: recoveredAmount,
      preserved_terminal_count: preservedCount,
      // v2.40.19: truckload sideline reporting
      sidelined_truckload_stops: skippedTruckload,
      sidelined_truckload_billed: truckloadTotalBilled,
      sidelined_truckload_weeks: truckloadWeeksArr,
      sidelined_tk_transitions: sidelinedTkCount,
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

import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Stop Economics Rollup (v2.51.1)
 *
 * Pre-aggregates stops + payments + classifications into per-(month, ZIP),
 * per-(month, driver), per-(month, customer) summary docs. The UI then
 * reads these aggregates instead of trying to join 200K+ stops in the browser.
 *
 * Why background:
 *   - nuvizz_stops is 214K+ rows. Sync function would time out.
 *   - For a single month, it's ~12-16K stops, joins to up to 16K DDIS payments.
 *
 * Calling pattern:
 *   POST /.netlify/functions/marginiq-stop-economics
 *     { "month": "2026-01", "test": false }  (writes all rollup docs)
 *     { "month": "2026-01", "test": true  }  (returns counts but doesn't write)
 *     { "all_months": true }                  (queues every month)
 *
 * Output collections:
 *   stop_economics_zip      / {month}_{zip}      — by ZIP per month
 *   stop_economics_driver   / {month}_{driverkey} — by driver per month
 *   stop_economics_customer / {month}_{custkey}   — by customer per month
 *   stop_economics_summary  / {month}             — aggregate of the month
 *
 * Status doc: marginiq_config/stop_economics_status
 *
 * Critical revenue logic:
 *   - For Uline stops (customer matches /uline/i):
 *       revenue_paid   = ddis_payments.paid_amount summed for matching PRO
 *       revenue_unpaid = unpaid_stops.billed for matching PRO
 *       (both contribute to total Uline revenue)
 *   - For non-Uline stops:
 *       Customer charge isn't in any data source today. We approximate
 *       implied_revenue = contractor_pay_base / 0.4 (since 1099 drivers
 *       are paid 40% of the customer charge).
 *       This is flagged as `nonuline_revenue_implied` so the UI can
 *       show "(estimated)" and a future rate-card upload can replace it.
 *
 * Cost logic:
 *   - contractor_pay_base = sum of stops.contractor_pay_base for the bucket
 *   - For 1099 drivers: pay_at_40 = contractor_pay_base * 0.4 (actual cost)
 *   - For W2 drivers: actual cost is hourly via timeclock — not yet attributed
 *     per stop in this rollup. Tracked separately in driver_classifications join.
 *
 * Env vars:
 *   FIREBASE_API_KEY  — Firestore REST
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

const NONULINE_RATE_DEFLATOR = 0.4; // 1099 drivers paid 40% of customer charge

// ─── Firestore REST helpers ────────────────────────────────────────

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

function fieldsToObject(fields: Record<string, any> | undefined): Record<string, any> {
  if (!fields) return {};
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) out[k] = fromFsValue(v);
  return out;
}

async function listDocsByField(
  collection: string,
  field: string,
  op: "EQUAL" | "GREATER_THAN_OR_EQUAL" | "LESS_THAN_OR_EQUAL",
  value: any,
  fieldPaths?: string[],
  extraFilters?: Array<{ field: string; op: string; value: any }>,
): Promise<any[]> {
  // Use runQuery for filtered reads. Pages by setting startAfter on cursor field.
  const out: any[] = [];
  let pageCount = 0;
  let lastCursor: any = null;

  while (true) {
    const filters: any[] = [{ fieldFilter: { field: { fieldPath: field }, op, value: toFsValue(value) } }];
    for (const ef of extraFilters || []) {
      filters.push({ fieldFilter: { field: { fieldPath: ef.field }, op: ef.op, value: toFsValue(ef.value) } });
    }
    const where = filters.length === 1
      ? filters[0]
      : { compositeFilter: { op: "AND", filters } };

    const body: any = {
      structuredQuery: {
        from: [{ collectionId: collection }],
        where,
        orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
        limit: 300,
      },
    };
    if (fieldPaths && fieldPaths.length) {
      body.structuredQuery.select = { fields: fieldPaths.map(fp => ({ fieldPath: fp })) };
    }
    if (lastCursor) body.structuredQuery.startAt = { values: [lastCursor], before: false };

    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:runQuery?key=${FIREBASE_API_KEY}`;
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      throw new Error(`runQuery ${collection} failed: HTTP ${resp.status} ${await resp.text()}`);
    }
    const rows: any[] = await resp.json();
    const docs = rows.filter(r => r.document).map(r => r.document);
    if (docs.length === 0) break;
    out.push(...docs);
    pageCount++;
    if (docs.length < 300) break;
    // Cursor = last doc's __name__ as ref
    const last = docs[docs.length - 1];
    lastCursor = { referenceValue: last.name };
    if (pageCount > 200) {
      console.warn(`runQuery ${collection}: hit page ceiling, returning ${out.length}`);
      break;
    }
  }
  return out;
}

// Read all paid amounts for a list of PROs, batched 30/in-clause
async function loadPaymentsByPros(pros: string[]): Promise<Map<string, number>> {
  const result = new Map<string, number>();
  if (!pros.length) return result;
  const proSet = [...new Set(pros.map(String))];
  const chunks: string[][] = [];
  for (let i = 0; i < proSet.length; i += 30) chunks.push(proSet.slice(i, i + 30));

  for (const chunk of chunks) {
    const body = {
      structuredQuery: {
        from: [{ collectionId: "ddis_payments" }],
        where: {
          fieldFilter: {
            field: { fieldPath: "pro" },
            op: "IN",
            value: { arrayValue: { values: chunk.map(p => toFsValue(p)) } },
          },
        },
        select: { fields: [{ fieldPath: "pro" }, { fieldPath: "paid_amount" }] },
        limit: 300,
      },
    };
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:runQuery?key=${FIREBASE_API_KEY}`;
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      console.warn(`payments chunk failed: ${resp.status} ${await resp.text()}`);
      continue;
    }
    const rows: any[] = await resp.json();
    for (const r of rows) {
      if (!r.document) continue;
      const f = fieldsToObject(r.document.fields);
      const pro = String(f.pro);
      const amt = typeof f.paid_amount === "number" ? f.paid_amount : 0;
      result.set(pro, (result.get(pro) || 0) + amt);
    }
  }
  return result;
}

async function loadUnpaidByPros(pros: string[]): Promise<Map<string, number>> {
  const result = new Map<string, number>();
  if (!pros.length) return result;
  const proSet = [...new Set(pros.map(String))];
  const chunks: string[][] = [];
  for (let i = 0; i < proSet.length; i += 30) chunks.push(proSet.slice(i, i + 30));

  for (const chunk of chunks) {
    const body = {
      structuredQuery: {
        from: [{ collectionId: "unpaid_stops" }],
        where: {
          fieldFilter: {
            field: { fieldPath: "pro" },
            op: "IN",
            value: { arrayValue: { values: chunk.map(p => toFsValue(p)) } },
          },
        },
        select: { fields: [{ fieldPath: "pro" }, { fieldPath: "billed" }] },
        limit: 300,
      },
    };
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:runQuery?key=${FIREBASE_API_KEY}`;
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!resp.ok) continue;
    const rows: any[] = await resp.json();
    for (const r of rows) {
      if (!r.document) continue;
      const f = fieldsToObject(r.document.fields);
      const pro = String(f.pro);
      const billed = typeof f.billed === "number" ? f.billed : 0;
      result.set(pro, (result.get(pro) || 0) + billed);
    }
  }
  return result;
}

async function loadDriverClassifications(): Promise<Map<string, string>> {
  const result = new Map<string, string>();
  let pageToken: string | undefined;
  do {
    const params = new URLSearchParams({
      key: FIREBASE_API_KEY || "",
      pageSize: "300",
    });
    params.append("mask.fieldPaths", "name");
    params.append("mask.fieldPaths", "classification");
    if (pageToken) params.set("pageToken", pageToken);
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/driver_classifications?${params.toString()}`;
    const resp = await fetch(url);
    if (!resp.ok) break;
    const data: any = await resp.json();
    for (const doc of data.documents || []) {
      const f = fieldsToObject(doc.fields);
      const key = doc.name.split("/").pop();
      if (key && f.classification) result.set(key, f.classification);
    }
    pageToken = data.nextPageToken;
  } while (pageToken);
  return result;
}

// Driver-name → key normalizer (must match MarginIQ.jsx)
function driverKey(name: string | null | undefined): string {
  if (!name) return "";
  return String(name).trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 140);
}

function customerKey(c: string | null | undefined): string {
  if (!c) return "";
  return String(c).trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 140);
}

function isUlineCustomer(c: string | null | undefined): boolean {
  // Legacy detection by name; not used as primary signal anymore.
  // ship_to is the end recipient (e.g. "ATLANTA HAWKS"), not the carrier customer.
  // Uline-as-carrier is detected via PRO format instead. See isUlinePro.
  if (!c) return false;
  return /uline/i.test(c);
}

// Uline PROs are 7-9 digit purely numeric (DAS=7-digit, DDIS=8-digit format).
// Non-Uline PROs are alpha-prefixed: SHP, MCC, ARY, ATT (redelivery prefix), etc.
// This is the authoritative way to attribute revenue source.
function isUlinePro(pro: string | null | undefined): boolean {
  if (!pro) return false;
  const s = String(pro).trim();
  return /^\d{7,9}$/.test(s);
}

async function batchWriteDocs(collection: string, docs: Array<{ docId: string; fields: any }>): Promise<{ ok: number; failed: number }> {
  if (docs.length === 0) return { ok: 0, failed: 0 };
  if (docs.length > 500) {
    let ok = 0, failed = 0;
    for (let i = 0; i < docs.length; i += 500) {
      const r = await batchWriteDocs(collection, docs.slice(i, i + 500));
      ok += r.ok; failed += r.failed;
    }
    return { ok, failed };
  }
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:commit?key=${FIREBASE_API_KEY}`;
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
  const writeResults = data.writeResults || [];
  return { ok: writeResults.length, failed: 0 };
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

// ─── Aggregator ────────────────────────────────────────────────────

interface RollupBucket {
  // Volume
  stops_total: number;
  uline_stops: number;
  nonuline_stops: number;
  // Cost
  contractor_pay_base: number;        // raw seal_nbr from NuVizz (all stops)
  // Revenue (Uline)
  uline_revenue_paid: number;
  uline_revenue_unpaid: number;
  // Revenue (non-Uline implied)
  nonuline_contractor_pay_base: number; // for the deflator math
  // Tracking
  pro_set: Set<string>;
  // Optional: top items for context (can populate later)
  metadata?: Record<string, any>;
}

function emptyBucket(): RollupBucket {
  return {
    stops_total: 0,
    uline_stops: 0,
    nonuline_stops: 0,
    contractor_pay_base: 0,
    uline_revenue_paid: 0,
    uline_revenue_unpaid: 0,
    nonuline_contractor_pay_base: 0,
    pro_set: new Set(),
  };
}

function bucketToFields(bucket: RollupBucket, extra: Record<string, any> = {}): Record<string, any> {
  const uline_revenue = bucket.uline_revenue_paid + bucket.uline_revenue_unpaid;
  const nonuline_revenue_implied = bucket.nonuline_contractor_pay_base / NONULINE_RATE_DEFLATOR;
  const total_revenue = uline_revenue + nonuline_revenue_implied;
  const contractor_cost_at_40 = bucket.contractor_pay_base * 0.4;
  const gross_margin = total_revenue - contractor_cost_at_40;
  const gross_margin_pct = total_revenue > 0 ? gross_margin / total_revenue * 100 : null;
  const uline_match_rate = bucket.uline_stops > 0 ? bucket.pro_set.size / bucket.uline_stops : null;

  const obj = {
    ...extra,
    stops_total: bucket.stops_total,
    uline_stops: bucket.uline_stops,
    nonuline_stops: bucket.nonuline_stops,
    contractor_pay_base: round(bucket.contractor_pay_base),
    contractor_cost_at_40: round(contractor_cost_at_40),
    uline_revenue_paid: round(bucket.uline_revenue_paid),
    uline_revenue_unpaid: round(bucket.uline_revenue_unpaid),
    uline_revenue: round(uline_revenue),
    nonuline_revenue_implied: round(nonuline_revenue_implied),
    nonuline_revenue_source: "implied_from_contractor_pay_at_40pct",
    total_revenue: round(total_revenue),
    rev_per_stop: bucket.stops_total > 0 ? round(total_revenue / bucket.stops_total) : null,
    gross_margin: round(gross_margin),
    gross_margin_pct: gross_margin_pct != null ? round(gross_margin_pct) : null,
    uline_pro_match_rate: uline_match_rate,
    computed_at: new Date().toISOString(),
  };
  // Convert to Firestore field map
  const fields: Record<string, any> = {};
  for (const [k, v] of Object.entries(obj)) fields[k] = toFsValue(v);
  return fields;
}

function round(n: number): number {
  return Math.round(n * 100) / 100;
}

async function rollupMonth(month: string, dryRun: boolean = false): Promise<any> {
  const startTime = Date.now();
  const status: any = {
    month, dryRun, phase: "starting",
    stops_read: 0, payments_matched: 0, unpaid_matched: 0,
    zips: 0, drivers: 0, customers: 0,
  };

  // 1. Load all nuvizz_stops for this month-bucket
  // NOTE: nuvizz_stops.month is the week-ending Friday's month, so a Dec 29
  // delivery in a week ending Jan 2 is bucketed as 2026-01. We load by that
  // bucket but filter at aggregate time by actual delivery_date so revenue
  // is attributed to the correct delivery month.
  status.phase = "loading_stops";
  const stopsAll = await listDocsByField("nuvizz_stops", "month", "EQUAL", month);
  // Plus the prior-month bucket because some stops with delivery_date in
  // THIS month were bucketed in the prior month's week (e.g. Feb 1 delivery
  // in week ending Jan 31).
  const [py, pm] = month.split("-").map(Number);
  const priorMonth = pm === 1 ? `${py-1}-12` : `${py}-${String(pm-1).padStart(2,"0")}`;
  const nextMy = pm === 12 ? py+1 : py;
  const nextMm = pm === 12 ? 1 : pm+1;
  const nextMonth = `${nextMy}-${String(nextMm).padStart(2,"0")}`;
  const [stopsPrior, stopsNext] = await Promise.all([
    listDocsByField("nuvizz_stops", "month", "EQUAL", priorMonth),
    listDocsByField("nuvizz_stops", "month", "EQUAL", nextMonth),
  ]);
  // Filter strictly by delivery_date YYYY-MM == requested month
  const stops = [...stopsAll, ...stopsPrior, ...stopsNext].filter(sd => {
    const f = fieldsToObject(sd.fields);
    const dd = f.delivery_date;
    return typeof dd === "string" && dd.length >= 7 && dd.slice(0, 7) === month;
  });
  status.stops_read = stops.length;
  status.stops_loaded_raw = stopsAll.length + stopsPrior.length + stopsNext.length;
  if (stops.length === 0) {
    return { ...status, phase: "no_stops", elapsed_ms: Date.now() - startTime };
  }

  // 2. Load all DDIS payments + unpaid_stops for this month's Uline PROs
  status.phase = "loading_payments";
  const allPros: string[] = [];
  const ulineStops: any[] = [];
  for (const sd of stops) {
    const f = fieldsToObject(sd.fields);
    const pro = f.pro;
    if (pro) allPros.push(String(pro));
    // Uline detection by PRO format, not ship_to (which is the end recipient).
    if (isUlinePro(pro)) ulineStops.push({ pro: String(pro), stop: f });
  }
  const ulinePros = ulineStops.map(x => x.pro);
  const [paymentsByPro, unpaidByPro] = await Promise.all([
    loadPaymentsByPros(ulinePros),
    loadUnpaidByPros(ulinePros),
  ]);
  status.payments_matched = paymentsByPro.size;
  status.unpaid_matched = unpaidByPro.size;

  // 3. Load driver classifications (small)
  const driverClass = await loadDriverClassifications();

  // 4. Aggregate
  status.phase = "aggregating";
  const byZip      = new Map<string, RollupBucket>();
  const byDriver   = new Map<string, RollupBucket>();
  const byCustomer = new Map<string, RollupBucket>();
  const driverNames: Map<string, string> = new Map();
  const customerNames: Map<string, string> = new Map();

  // Cache: which stop has matched payment, used for accurate uline_pro_match_rate
  for (const sd of stops) {
    const f = fieldsToObject(sd.fields);
    const pro = f.pro ? String(f.pro) : "";
    const ship = f.ship_to;
    const zip = f.zip;
    const driverName = f.driver_name;
    const driverK = driverKey(driverName);
    const custK = customerKey(ship);
    // Uline = numeric 7-9 digit PRO (DAS 7-digit / DDIS 8-digit), not by ship_to
    const isUline = isUlinePro(pro);
    const payBase = typeof f.contractor_pay_base === "number" ? f.contractor_pay_base : 0;

    const paid   = isUline ? (paymentsByPro.get(pro) || 0) : 0;
    const unpaid = isUline ? (unpaidByPro.get(pro)   || 0) : 0;

    // Helper: update one bucket
    const update = (bucket: RollupBucket) => {
      bucket.stops_total += 1;
      bucket.contractor_pay_base += payBase;
      if (isUline) {
        bucket.uline_stops += 1;
        bucket.uline_revenue_paid += paid;
        bucket.uline_revenue_unpaid += unpaid;
        if (paid > 0 || unpaid > 0) bucket.pro_set.add(pro);
      } else {
        bucket.nonuline_stops += 1;
        bucket.nonuline_contractor_pay_base += payBase;
      }
    };

    if (zip) {
      if (!byZip.has(zip)) byZip.set(zip, emptyBucket());
      update(byZip.get(zip)!);
    }
    if (driverK) {
      if (!byDriver.has(driverK)) {
        byDriver.set(driverK, emptyBucket());
        if (driverName) driverNames.set(driverK, driverName);
      }
      update(byDriver.get(driverK)!);
    }
    if (custK) {
      if (!byCustomer.has(custK)) {
        byCustomer.set(custK, emptyBucket());
        if (ship) customerNames.set(custK, ship);
      }
      update(byCustomer.get(custK)!);
    }
  }

  status.zips = byZip.size;
  status.drivers = byDriver.size;
  status.customers = byCustomer.size;

  // Aggregate month summary
  const monthBucket = emptyBucket();
  for (const b of byZip.values()) {
    monthBucket.stops_total += b.stops_total;
    monthBucket.uline_stops += b.uline_stops;
    monthBucket.nonuline_stops += b.nonuline_stops;
    monthBucket.contractor_pay_base += b.contractor_pay_base;
    monthBucket.uline_revenue_paid += b.uline_revenue_paid;
    monthBucket.uline_revenue_unpaid += b.uline_revenue_unpaid;
    monthBucket.nonuline_contractor_pay_base += b.nonuline_contractor_pay_base;
    for (const p of b.pro_set) monthBucket.pro_set.add(p);
  }

  if (dryRun) {
    status.phase = "dry_run_complete";
    status.summary = fieldsToObject(bucketToFields(monthBucket, { month }));
    status.elapsed_ms = Date.now() - startTime;
    // Sample 5 ZIPs for preview
    const zipPreview = [...byZip.entries()].sort((a,b) => b[1].stops_total - a[1].stops_total).slice(0, 5);
    status.top_zips = zipPreview.map(([zip, b]) => ({
      zip,
      ...fieldsToObject(bucketToFields(b, {})),
    }));
    return status;
  }

  // 5. Write all docs
  status.phase = "writing";
  const zipDocs = [...byZip.entries()].map(([zip, b]) => ({
    docId: `${month}_${zip}`,
    fields: bucketToFields(b, { month, zip, dimension: "zip" }),
  }));
  const driverDocs = [...byDriver.entries()].map(([key, b]) => ({
    docId: `${month}_${key}`,
    fields: bucketToFields(b, { month, driver_key: key, driver_name: driverNames.get(key) || key, dimension: "driver" }),
  }));
  const customerDocs = [...byCustomer.entries()].map(([key, b]) => ({
    docId: `${month}_${key}`,
    fields: bucketToFields(b, { month, customer_key: key, customer: customerNames.get(key) || key, dimension: "customer" }),
  }));

  const [zipR, drvR, custR, sumR] = await Promise.all([
    batchWriteDocs("stop_economics_zip", zipDocs),
    batchWriteDocs("stop_economics_driver", driverDocs),
    batchWriteDocs("stop_economics_customer", customerDocs),
    batchWriteDocs("stop_economics_summary", [{
      docId: month,
      fields: bucketToFields(monthBucket, { month, dimension: "summary" }),
    }]),
  ]);

  status.phase = "complete";
  status.zip_writes = zipR;
  status.driver_writes = drvR;
  status.customer_writes = custR;
  status.summary_writes = sumR;
  status.elapsed_ms = Date.now() - startTime;
  return status;
}

// ─── Handler ──────────────────────────────────────────────────────

export default async (req: Request, _context: Context) => {
  if (req.method === "GET") {
    return new Response(
      JSON.stringify({
        ok: true,
        endpoint: "stop_economics_rollup",
        usage: {
          POST: {
            body: { month: "YYYY-MM", test: "boolean (default false)" },
            description: "Computes stop_economics rollup for one month, optionally dry-run",
          },
        },
      }),
      { headers: { "Content-Type": "application/json" } },
    );
  }

  if (req.method !== "POST") return new Response("method not allowed", { status: 405 });

  try {
    const body = await req.json();
    const month = body.month;
    const test = !!body.test;

    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return new Response(JSON.stringify({ ok: false, error: "month required (YYYY-MM)" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    // Always write status doc so the UI can poll for completion (test or full)
    await patchDoc("marginiq_config", "stop_economics_status", {
      month,
      state: "running",
      test_mode: test,
      started_at: new Date().toISOString(),
    });

    const result = await rollupMonth(month, test);

    await patchDoc("marginiq_config", "stop_economics_status", {
      month,
      state: "complete",
      test_mode: test,
      completed_at: new Date().toISOString(),
      result_summary: JSON.stringify({
        stops_read: result.stops_read,
        zips: result.zips,
        drivers: result.drivers,
        customers: result.customers,
        payments_matched: result.payments_matched,
        unpaid_matched: result.unpaid_matched,
        elapsed_ms: result.elapsed_ms,
      }),
    });

    return new Response(JSON.stringify({ ok: true, ...result }), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (e: any) {
    console.error("stop-economics rollup failed:", e);
    // Best-effort: write failed status so UI poll resolves
    try {
      const body2 = await req.clone().json().catch(() => ({}));
      const m2 = body2.month;
      if (m2) {
        await patchDoc("marginiq_config", "stop_economics_status", {
          month: m2,
          state: "failed",
          error: e?.message || String(e),
          failed_at: new Date().toISOString(),
        });
      }
    } catch { /* swallow */ }
    return new Response(
      JSON.stringify({ ok: false, error: e?.message || String(e) }),
      { status: 500, headers: { "Content-Type": "application/json" } },
    );
  }
};

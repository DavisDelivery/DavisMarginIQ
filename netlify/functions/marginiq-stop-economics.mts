import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Stop Economics Rollup Dispatcher (v2.51.3)
 *
 * Sync function that:
 *   - For test=true: runs synchronously and returns the dry-run result
 *     (skips the heavy writes; just reads + aggregates + returns top ZIPs)
 *   - For test=false: forwards to the background function and returns 202
 *     immediately so the UI doesn't time out.
 *
 * The dry-run path can complete inside the 26s sync budget for one month
 * (~12K stops, ~16K payment lookups). The full write path runs in the
 * 15-min background budget.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const NONULINE_RATE_DEFLATOR = 0.4;

// Inline copy of helpers — kept identical between sync and background
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

function isUlinePro(pro: string | null | undefined): boolean {
  if (!pro) return false;
  return /^\d{7,9}$/.test(String(pro).trim());
}

function customerKey(c: string | null | undefined): string {
  if (!c) return "";
  return String(c).trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 140);
}

function driverKey(name: string | null | undefined): string {
  if (!name) return "";
  return String(name).trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 140);
}

function round(n: number): number { return Math.round(n * 100) / 100; }

async function listStopsByMonth(month: string): Promise<any[]> {
  const out: any[] = [];
  let lastCursor: any = null;
  let pageCount = 0;

  while (true) {
    const body: any = {
      structuredQuery: {
        from: [{ collectionId: "nuvizz_stops" }],
        where: { fieldFilter: { field: { fieldPath: "month" }, op: "EQUAL", value: { stringValue: month } } },
        orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
        limit: 300,
        select: { fields: ["pro","ship_to","zip","driver_name","contractor_pay_base"].map(f => ({ fieldPath: f })) },
      },
    };
    if (lastCursor) body.structuredQuery.startAt = { values: [lastCursor], before: false };

    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:runQuery?key=${FIREBASE_API_KEY}`;
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!resp.ok) throw new Error(`runQuery failed: ${resp.status}`);
    const rows: any[] = await resp.json();
    const docs = rows.filter(r => r.document).map(r => r.document);
    if (docs.length === 0) break;
    out.push(...docs);
    pageCount++;
    if (docs.length < 300) break;
    lastCursor = { referenceValue: docs[docs.length - 1].name };
    if (pageCount > 200) break;
  }
  return out;
}

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
        where: { fieldFilter: { field: { fieldPath: "pro" }, op: "IN", value: { arrayValue: { values: chunk.map(p => toFsValue(p)) } } } },
        select: { fields: [{ fieldPath: "pro" }, { fieldPath: "paid_amount" }] },
        limit: 300,
      },
    };
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:runQuery?key=${FIREBASE_API_KEY}`;
    const resp = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    if (!resp.ok) continue;
    const rows: any[] = await resp.json();
    for (const r of rows) {
      if (!r.document) continue;
      const f = fieldsToObject(r.document.fields);
      const pro = String(f.pro);
      result.set(pro, (result.get(pro) || 0) + (typeof f.paid_amount === "number" ? f.paid_amount : 0));
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
        where: { fieldFilter: { field: { fieldPath: "pro" }, op: "IN", value: { arrayValue: { values: chunk.map(p => toFsValue(p)) } } } },
        select: { fields: [{ fieldPath: "pro" }, { fieldPath: "billed" }] },
        limit: 300,
      },
    };
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:runQuery?key=${FIREBASE_API_KEY}`;
    const resp = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    if (!resp.ok) continue;
    const rows: any[] = await resp.json();
    for (const r of rows) {
      if (!r.document) continue;
      const f = fieldsToObject(r.document.fields);
      result.set(String(f.pro), (result.get(String(f.pro)) || 0) + (typeof f.billed === "number" ? f.billed : 0));
    }
  }
  return result;
}

async function dryRunMonth(month: string): Promise<any> {
  const startTime = Date.now();
  const stops = await listStopsByMonth(month);
  if (!stops.length) return { stops_read: 0, phase: "no_stops", elapsed_ms: Date.now() - startTime };

  const ulinePros: string[] = [];
  for (const sd of stops) {
    const f = fieldsToObject(sd.fields);
    if (isUlinePro(f.pro)) ulinePros.push(String(f.pro));
  }
  const [paymentsByPro, unpaidByPro] = await Promise.all([
    loadPaymentsByPros(ulinePros),
    loadUnpaidByPros(ulinePros),
  ]);

  const byZip = new Map<string, any>();
  let totalUlineStops = 0, totalNonUlineStops = 0;
  let totalUlinePaid = 0, totalUlineUnpaid = 0;
  let totalContractorPay = 0, totalNonUlineContractorPay = 0;

  for (const sd of stops) {
    const f = fieldsToObject(sd.fields);
    const pro = f.pro ? String(f.pro) : "";
    const zip = f.zip;
    const isU = isUlinePro(pro);
    const payBase = typeof f.contractor_pay_base === "number" ? f.contractor_pay_base : 0;
    const paid = isU ? (paymentsByPro.get(pro) || 0) : 0;
    const unpaid = isU ? (unpaidByPro.get(pro) || 0) : 0;

    totalContractorPay += payBase;
    if (isU) {
      totalUlineStops++; totalUlinePaid += paid; totalUlineUnpaid += unpaid;
    } else {
      totalNonUlineStops++; totalNonUlineContractorPay += payBase;
    }

    if (zip) {
      if (!byZip.has(zip)) byZip.set(zip, { zip, stops_total: 0, uline_stops: 0, nonuline_stops: 0, contractor_pay_base: 0, uline_paid: 0, uline_unpaid: 0, nonuline_pay: 0 });
      const b = byZip.get(zip);
      b.stops_total++;
      b.contractor_pay_base += payBase;
      if (isU) { b.uline_stops++; b.uline_paid += paid; b.uline_unpaid += unpaid; }
      else { b.nonuline_stops++; b.nonuline_pay += payBase; }
    }
  }

  const buildSummary = (b: any, base: any = {}) => {
    const ulineRev = b.uline_paid + b.uline_unpaid;
    const nonUlineImplied = b.nonuline_pay / NONULINE_RATE_DEFLATOR;
    const totalRev = ulineRev + nonUlineImplied;
    const cost = b.contractor_pay_base * 0.4;
    const margin = totalRev - cost;
    return {
      ...base,
      stops_total: b.stops_total, uline_stops: b.uline_stops, nonuline_stops: b.nonuline_stops,
      uline_revenue_paid: round(b.uline_paid),
      uline_revenue_unpaid: round(b.uline_unpaid),
      uline_revenue: round(ulineRev),
      nonuline_revenue_implied: round(nonUlineImplied),
      total_revenue: round(totalRev),
      rev_per_stop: b.stops_total > 0 ? round(totalRev / b.stops_total) : null,
      contractor_cost_at_40: round(cost),
      gross_margin: round(margin),
      gross_margin_pct: totalRev > 0 ? round(margin / totalRev * 100) : null,
    };
  };

  const monthBucket = {
    stops_total: stops.length,
    uline_stops: totalUlineStops,
    nonuline_stops: totalNonUlineStops,
    contractor_pay_base: totalContractorPay,
    uline_paid: totalUlinePaid,
    uline_unpaid: totalUlineUnpaid,
    nonuline_pay: totalNonUlineContractorPay,
  };

  return {
    phase: "dry_run_complete",
    stops_read: stops.length,
    payments_matched: paymentsByPro.size,
    unpaid_matched: unpaidByPro.size,
    zips: byZip.size,
    summary: buildSummary(monthBucket, { month }),
    top_zips: [...byZip.values()]
      .sort((a, b) => b.stops_total - a.stops_total)
      .slice(0, 10)
      .map(b => buildSummary(b, { zip: b.zip })),
    elapsed_ms: Date.now() - startTime,
  };
}

export default async (req: Request, _context: Context) => {
  if (req.method === "GET") {
    return new Response(JSON.stringify({
      ok: true, endpoint: "stop_economics", usage: "POST { month, test: bool }",
    }), { headers: { "Content-Type": "application/json" } });
  }
  if (req.method !== "POST") return new Response("method not allowed", { status: 405 });

  try {
    const body = await req.json();
    const month = body.month;
    const test = !!body.test;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return new Response(JSON.stringify({ ok: false, error: "month required (YYYY-MM)" }), { status: 400, headers: { "Content-Type": "application/json" } });
    }

    if (test) {
      const result = await dryRunMonth(month);
      return new Response(JSON.stringify({ ok: true, ...result }), { headers: { "Content-Type": "application/json" } });
    }

    // Forward to background function (returns 202 immediately)
    const url = new URL(req.url);
    const baseUrl = `${url.protocol}//${url.host}`;
    fetch(`${baseUrl}/.netlify/functions/marginiq-stop-economics-background`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ month }),
    }).catch(e => console.error("dispatch failed:", e));

    return new Response(JSON.stringify({ ok: true, dispatched: true, month, message: "Background rollup started. Check back in ~30-60 seconds." }), {
      status: 202,
      headers: { "Content-Type": "application/json" },
    });
  } catch (e: any) {
    return new Response(JSON.stringify({ ok: false, error: e?.message || String(e) }), { status: 500, headers: { "Content-Type": "application/json" } });
  }
};

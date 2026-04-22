import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Audit Payment Trace (v2.40.21)
 *
 * For a single PRO, return the full diagnostic trace of the billed-vs-paid
 * join: what we looked for, what we found, which DDIS files cover the
 * relevant week, and what near-miss payments exist. This answers "why is
 * this PRO showing up as unpaid?" with receipts.
 *
 * Primary motivator: Chad confirmed some ULI- TK shipments are settled via
 * DDIS under the bare numeric PRO (ULI-1511957 → PRO 1511957), but others
 * aren't found anywhere in the 820 stream even when their week is fully
 * covered by ingested DDIS. The trace view surfaces that gap so he can
 * decide: dispute with Uline, or recognize it as genuinely unpaid.
 *
 * GET /.netlify/functions/marginiq-audit-trace?pro=<PRO>
 *
 * Response:
 *   {
 *     ok: true,
 *     pro: "ULI-1543672",
 *     numeric_core: "1543672",  // null if no transform applied
 *     audit_item: {...} | null,
 *     unpaid_stop: {...} | null,
 *     direct_payments: [ { pro, paid_amount, bill_date, check, voucher, source_file }, ... ],
 *     candidate_files: [                  // DDIS files whose bill_date range
 *       {                                 // covers the stop's pu_date OR week_ending
 *         filename, bill_week_ending,
 *         earliest_bill_date, latest_bill_date,
 *         covers_weeks: [...],
 *         covers_this_week: bool          // true iff either covers_weeks or
 *                                         // bill_week_ending matches stop.week_ending
 *       }
 *     ],
 *     near_misses: [                      // payments in same week within ±$5
 *       { pro, paid_amount, bill_date, source_file, delta }
 *     ],
 *     verdict: "paid" | "paid_under_core" | "short_paid" | "unpaid_ddis_present" |
 *              "awaiting_ddis" | "unpaid_no_ddis_for_week",
 *     explanation: string,
 *   }
 *
 * Env: FIREBASE_API_KEY
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

function fs(v: any): any {
  if (!v) return null;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return v.doubleValue;
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("arrayValue" in v) return (v.arrayValue?.values || []).map(fs);
  if ("mapValue" in v) {
    const out: Record<string, any> = {};
    for (const [k, val] of Object.entries(v.mapValue?.fields || {})) out[k] = fs(val);
    return out;
  }
  return null;
}

function fieldsToObj(fields: any): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields || {})) out[k] = fs(v);
  return out;
}

async function getDoc(collection: string, docId: string): Promise<Record<string, any> | null> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url);
  if (resp.status === 404) return null;
  if (!resp.ok) throw new Error(`getDoc ${collection}/${docId} failed: HTTP ${resp.status}`);
  const data: any = await resp.json();
  return fieldsToObj(data.fields || {});
}

async function runQuery(body: any): Promise<any[]> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:runQuery?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`runQuery failed: HTTP ${resp.status} ${await resp.text()}`);
  const rows: any[] = await resp.json();
  return rows.filter(r => r.document).map(r => ({ id: r.document.name.split("/").pop(), ...fieldsToObj(r.document.fields || {}) }));
}

async function findPaymentsByPro(pro: string): Promise<any[]> {
  return runQuery({
    structuredQuery: {
      from: [{ collectionId: "ddis_payments" }],
      where: { fieldFilter: { field: { fieldPath: "pro" }, op: "EQUAL", value: { stringValue: pro } } },
      limit: 25,
    },
  });
}

// List ddis_files whose range overlaps or whose bwe/covers_weeks matches.
// We fetch all files (typically <200) and filter client-side — simpler than
// composite queries for this edit-distance-style filter.
async function findCandidateFiles(puDate: string | null, weekEnding: string | null): Promise<any[]> {
  const all = await runQuery({
    structuredQuery: {
      from: [{ collectionId: "ddis_files" }],
      limit: 500,
    },
  });
  const out: any[] = [];
  for (const f of all) {
    const bwe = f.bill_week_ending || null;
    const earliest = f.earliest_bill_date || null;
    const latest = f.latest_bill_date || null;
    const coversArr: string[] = Array.isArray(f.covers_weeks) ? f.covers_weeks : [];
    const coversThisWeek = !!weekEnding && (coversArr.includes(weekEnding) || bwe === weekEnding);
    const inRange = !!puDate && !!earliest && !!latest && puDate >= earliest && puDate <= latest;
    if (coversThisWeek || inRange) {
      out.push({
        filename: f.filename || f.id,
        bill_week_ending: bwe,
        earliest_bill_date: earliest,
        latest_bill_date: latest,
        covers_weeks: coversArr,
        covers_this_week: coversThisWeek,
        in_bill_date_range: inRange,
      });
    }
  }
  // Sort: files covering the exact week first, then by bwe desc
  out.sort((a, b) => {
    if (a.covers_this_week && !b.covers_this_week) return -1;
    if (!a.covers_this_week && b.covers_this_week) return 1;
    return (b.bill_week_ending || "").localeCompare(a.bill_week_ending || "");
  });
  return out;
}

// Near-misses: payments from a candidate file whose amount is within ±$5 of
// billed and whose PRO we haven't already directly matched.
async function findNearMissPayments(candidateFiles: any[], billed: number, directPros: Set<string>): Promise<any[]> {
  if (!billed || billed <= 0) return [];
  const near: any[] = [];
  const target = billed;
  const tolerance = Math.max(5, billed * 0.02); // $5 or 2% whichever greater
  for (const cf of candidateFiles.slice(0, 6)) {
    // Pull all payments from this source file and filter by amount client-side.
    // Firestore doesn't support range queries on doubleValue via runQuery without
    // a composite index we haven't built, so just grab by source_file and filter.
    const payments = await runQuery({
      structuredQuery: {
        from: [{ collectionId: "ddis_payments" }],
        where: { fieldFilter: { field: { fieldPath: "source_file" }, op: "EQUAL", value: { stringValue: cf.filename } } },
        limit: 500,
      },
    });
    for (const p of payments) {
      const amt = Number(p.paid_amount || 0);
      const delta = Math.abs(amt - target);
      if (delta <= tolerance && !directPros.has(p.pro) && amt > 0) {
        near.push({
          pro: p.pro,
          paid_amount: amt,
          bill_date: p.bill_date,
          source_file: cf.filename,
          check: p.check,
          delta: Math.round(delta * 100) / 100,
        });
      }
    }
    if (near.length >= 15) break;
  }
  near.sort((a, b) => a.delta - b.delta);
  return near.slice(0, 10);
}

function deriveVerdict(opts: {
  billed: number;
  paid: number;
  directPayments: any[];
  corePaid: number;
  candidateFiles: any[];
  weekEnding: string | null;
}): { verdict: string; explanation: string } {
  const { billed, paid, directPayments, corePaid, candidateFiles, weekEnding } = opts;
  const weekCovered = candidateFiles.some(f => f.covers_this_week);

  if (paid >= billed * 0.99 && directPayments.length > 0) {
    const usedCore = corePaid > 0 && directPayments.some(p => !/^ULI-/i.test(p.pro));
    return {
      verdict: usedCore ? "paid_under_core" : "paid",
      explanation: usedCore
        ? `Paid in full via DDIS under the bare numeric PRO (ULI- prefix was stripped to find the match).`
        : `Paid in full — direct PRO match in DDIS.`,
    };
  }
  if (paid > 0 && paid < billed * 0.99) {
    return {
      verdict: "short_paid",
      explanation: `Paid ${((paid/billed)*100).toFixed(0)}% of billed. Variance $${(billed-paid).toFixed(2)} to dispute.`,
    };
  }
  if (paid === 0 && directPayments.length === 0) {
    if (weekCovered) {
      return {
        verdict: "unpaid_ddis_present",
        explanation: `The DDIS file for this week (${weekEnding}) has been ingested but contains no payment row for this PRO (or its numeric core). Either genuinely unpaid by Uline, or settled through a non-820 channel. Worth disputing with Uline AP.`,
      };
    }
    if (!weekCovered && weekEnding) {
      return {
        verdict: "awaiting_ddis",
        explanation: `No ingested DDIS file covers week ${weekEnding}. Import the remit for that week via Gmail Sync before treating this as unpaid.`,
      };
    }
    return {
      verdict: "unpaid_no_ddis_for_week",
      explanation: `No week_ending recorded on this stop, can't determine DDIS coverage.`,
    };
  }
  return {
    verdict: "unknown",
    explanation: `Could not classify — check raw data below.`,
  };
}

export default async (req: Request, _context: Context) => {
  if (!FIREBASE_API_KEY) {
    return new Response(JSON.stringify({ error: "FIREBASE_API_KEY not configured" }), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }
  const url = new URL(req.url);
  const pro = url.searchParams.get("pro");
  if (!pro) {
    return new Response(JSON.stringify({ error: "pro query param required" }), {
      status: 400, headers: { "Content-Type": "application/json" },
    });
  }

  try {
    // Derive the numeric-core variant (if ULI-prefixed)
    const core = /^ULI-/i.test(pro) ? pro.replace(/^ULI-/i, "") : null;

    // Fetch audit_item + unpaid_stop in parallel
    const [auditItem, unpaidStop] = await Promise.all([
      getDoc("audit_items", pro),
      getDoc("unpaid_stops", pro),
    ]);

    const billed = Number((auditItem?.billed ?? unpaidStop?.billed ?? 0));
    const puDate: string | null = auditItem?.pu_date || unpaidStop?.pu_date || null;
    const weekEnding: string | null = auditItem?.week_ending || unpaidStop?.week_ending || null;

    // Look up payments under both raw PRO and numeric core
    const [rawHits, coreHits] = await Promise.all([
      findPaymentsByPro(pro),
      core ? findPaymentsByPro(core) : Promise.resolve([]),
    ]);
    const directPayments = [...rawHits, ...coreHits];
    // Dedupe by id
    const seen = new Set<string>();
    const dedup: any[] = [];
    for (const p of directPayments) {
      const k = p.id || `${p.pro}_${p.bill_date}_${p.check}`;
      if (seen.has(k)) continue;
      seen.add(k);
      dedup.push(p);
    }
    const totalPaid = dedup.reduce((s, p) => s + Number(p.paid_amount || 0), 0);
    const corePaid = coreHits.reduce((s, p) => s + Number(p.paid_amount || 0), 0);

    // Candidate DDIS files
    const candidateFiles = await findCandidateFiles(puDate, weekEnding);

    // Near-miss scan (only if we didn't find a direct payment)
    const directPros = new Set(dedup.map(p => p.pro));
    const nearMisses = dedup.length === 0 && billed > 0
      ? await findNearMissPayments(candidateFiles, billed, directPros)
      : [];

    const { verdict, explanation } = deriveVerdict({
      billed, paid: totalPaid, directPayments: dedup, corePaid, candidateFiles, weekEnding,
    });

    return new Response(JSON.stringify({
      ok: true,
      pro,
      numeric_core: core,
      audit_item: auditItem,
      unpaid_stop: unpaidStop,
      direct_payments: dedup,
      total_paid: totalPaid,
      candidate_files: candidateFiles,
      near_misses: nearMisses,
      verdict,
      explanation,
    }), {
      status: 200, headers: { "Content-Type": "application/json" },
    });
  } catch (e: any) {
    console.error("audit-trace failed:", e?.message || e);
    return new Response(JSON.stringify({ error: e?.message || String(e) }), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }
};

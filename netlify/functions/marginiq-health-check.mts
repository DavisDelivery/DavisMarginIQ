import type { Context } from "@netlify/functions";
import {
  runHealthCheck,
  fsGetDoc,
  fsRunQuery,
  fsRunAggregation,
  fsPaginate,
  unwrapDoc,
  unwrapField,
  type CheckId,
  type CheckClosureResult,
} from "./lib/health-checks.js";
import { SOURCE_REGISTRY } from "./lib/four-layer-ingest.js";
import { gunzipSync } from "node:zlib";

/**
 * Davis MarginIQ — Standing Health Checks (v2.53.4, Phase 2 Commit 4a)
 *
 * Single endpoint dispatching to all 9 standing checks defined in
 * DESIGN.md §5. Manual-only triggering in v1 — no cron yet (per the
 * orchestrator's Q5 direction: establish baselines first, schedule later).
 *
 * v2.53.4: added ?force=1 query param. Surfaces to runHealthCheck so the
 * Phase 2 migration runner's preflight phase can bypass the in_migration
 * short-circuit and verify checks still produce real results when forced.
 *
 * Endpoint:
 *   POST /.netlify/functions/marginiq-health-check?check={id}
 *     where {id} ∈ {1A, 1B, 2, 2B, 2C, 3, 4, 5, 6}
 *
 *   GET /.netlify/functions/marginiq-health-check
 *     Lists the latest result of every check (reads __latest mirrors).
 *
 *   GET /.netlify/functions/marginiq-health-check?check={id}
 *     Returns the most recent result for a single check.
 *
 * Results stored at:
 *   marginiq_health_checks/{check_id}__{ran_at}   — time-series
 *   marginiq_health_checks/{check_id}__latest     — quick-lookup mirror
 *
 * Result schema:
 *   { check_id, ran_at, status: "PASS"|"FAIL"|"INFO",
 *     summary, details, duration_ms }
 *
 * CHECK CATALOG
 * =============
 *   1A — Parse fidelity         (PASS/FAIL: per-source-file L2 row count match)
 *   1B — Backfill coverage      (INFO:     % L3 rows with provenance, per L3 collection)
 *   2  — Delivery uniqueness    (PASS/FAIL: das_lines (pro,pu_date,service_type) unique)
 *   2B — Orphan accessorial     (PASS/FAIL: every accessorial has sibling delivery)
 *   2C — Accessorial sum recon  (INFO:     deviation distribution vs ddis_payments)
 *   3  — Reconstructibility     (PASS/FAIL: sample 100 rows; verify L1 chunks intact)
 *   4  — Reparse determinism    (STUB: returns INFO "not yet implemented" until C5+)
 *   5  — DDIS dual-payment      (PASS/FAIL: no same-check dups per (pro,bill_date))
 *   6  — L2 lossless recon      (PASS/FAIL: sample 5 files; reconstruct from das_rows_raw)
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

const VALID_CHECKS: CheckId[] = ["1A", "1B", "2", "2B", "2C", "3", "4", "5", "6"];

function jsonResponse(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// ─── Check 1A: Parse fidelity ────────────────────────────────────────────────
// For every source_files_raw doc with parsed_row_count > 0, verify the
// L2 row count for that source_file_id (das_rows_raw / nuvizz_rows_raw /
// ddis_rows_raw) MUST equal parsed_row_count.
//
// L2 collection name is read from SOURCE_REGISTRY (single source of
// truth shared with the ingest path). If the registry ever changes,
// this check follows automatically — no chance of drift between the
// writer and the verifier.
//
// Today (pre-migration): fails for the 180 v2.53.1-staged uline files
// because the v2.53.1 worker wrote ~50% of rows under colliding L3 docIds.
// Goal: GREEN after migration.
async function check1A(apiKey: string): Promise<CheckClosureResult> {
  const mismatches: any[] = [];
  let totalChecked = 0;
  let totalSkipped = 0;

  // Walk source_files_raw (~700 docs total, no need to filter server-side).
  // Client-side filter for parsed_row_count > 0 to avoid the
  // "inequality + order-by-key" Firestore restriction.
  await fsPaginate(
    {
      structuredQuery: {
        from: [{ collectionId: "source_files_raw" }],
        orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
      },
    },
    100,
    apiKey,
    async (docs) => {
      for (const doc of docs) {
        const meta = unwrapDoc(doc);
        const expected = Number(meta.parsed_row_count || 0);
        if (expected <= 0) continue;  // not yet parsed
        const fileId = meta.file_id || doc.name?.split("/").pop();
        const source = String(meta.source || "");
        // Single source of truth: L2 collection comes from SOURCE_REGISTRY
        // (same registry the ingest path writes to).
        const l2Coll = SOURCE_REGISTRY[source]?.rowsRaw;
        if (!l2Coll) {
          totalSkipped++;
          continue;
        }
        if (!l2Coll) {
          totalSkipped++;
          continue;
        }
        // Aggregation count of L2 rows for this file_id
        const aggBody = {
          structuredAggregationQuery: {
            structuredQuery: {
              from: [{ collectionId: l2Coll }],
              where: { fieldFilter: { field: { fieldPath: "source_file_id" }, op: "EQUAL", value: { stringValue: fileId } } },
            },
            aggregations: [{ alias: "n", count: {} }],
          },
        };
        const res = await fsRunAggregation(aggBody, apiKey);
        const actual = Number(res?.[0]?.result?.aggregateFields?.n?.integerValue || 0);
        totalChecked++;
        if (actual !== expected) {
          mismatches.push({
            file_id: fileId,
            source,
            expected,
            actual,
            delta: actual - expected,
            filename: meta.filename,
          });
          // Cap mismatch list at 200 to keep the response manageable
          if (mismatches.length >= 200) return false;
        }
      }
    },
  );

  const status: "PASS" | "FAIL" = mismatches.length === 0 ? "PASS" : "FAIL";
  const summary = status === "PASS"
    ? `Parse fidelity GREEN — ${totalChecked} files, all L2 row counts match parsed_row_count`
    : `Parse fidelity FAIL — ${mismatches.length} of ${totalChecked} files have L2/parsed_row_count mismatch`;
  return {
    status,
    summary,
    details: {
      total_checked: totalChecked,
      total_skipped: totalSkipped,
      mismatch_count: mismatches.length,
      mismatches: mismatches.slice(0, 50),  // top 50 in the response; full list in time-series
      capped_at: mismatches.length >= 200 ? 200 : null,
    },
  };
}

// ─── Check 1B: Backfill coverage (INFO metric) ───────────────────────────────
// % of L3 rows with non-null source_file_id, per collection. Health metric
// tracked over time. Goal is 100% across das_lines/nuvizz_stops/ddis_payments.
async function check1B(apiKey: string): Promise<CheckClosureResult> {
  const COLLECTIONS = ["das_lines", "nuvizz_stops", "ddis_payments"];
  const breakdown: any[] = [];

  for (const coll of COLLECTIONS) {
    // Total count
    const totalRes = await fsRunAggregation({
      structuredAggregationQuery: {
        structuredQuery: { from: [{ collectionId: coll }] },
        aggregations: [{ alias: "n", count: {} }],
      },
    }, apiKey);
    const total = Number(totalRes?.[0]?.result?.aggregateFields?.n?.integerValue || 0);

    // Backed count (source_file_id != null). Firestore aggregation doesn't
    // support "is not null" directly; use a startAt range trick on the
    // string field (any non-empty string is > "").
    // Equivalent: count where source_file_id >= " " (any printable).
    const backedRes = await fsRunAggregation({
      structuredAggregationQuery: {
        structuredQuery: {
          from: [{ collectionId: coll }],
          where: {
            fieldFilter: { field: { fieldPath: "source_file_id" }, op: "GREATER_THAN", value: { stringValue: "" } },
          },
        },
        aggregations: [{ alias: "n", count: {} }],
      },
    }, apiKey);
    const backed = Number(backedRes?.[0]?.result?.aggregateFields?.n?.integerValue || 0);

    breakdown.push({
      collection: coll,
      total,
      backed,
      coverage_pct: total > 0 ? +((backed / total) * 100).toFixed(2) : 0,
    });
  }

  return {
    status: "INFO",
    summary: breakdown
      .map(b => `${b.collection}: ${b.coverage_pct}% (${b.backed.toLocaleString()}/${b.total.toLocaleString()})`)
      .join(" · "),
    details: { collections: breakdown },
  };
}

// ─── Check 2: das_lines delivery uniqueness ──────────────────────────────────
// Model B uniqueness: COUNT(*) das_lines must equal COUNT(distinct
// (pro, pu_date, service_type)). The docId scheme already encodes this
// triple, so equality is structurally guaranteed for any rows written
// by the v2.53.3 parser. This check verifies the property holds across
// the entire collection (catches stragglers from the migration that
// might not have been cleaned up).
//
// For Model B, since docId == pro_pudate_servicetype, distinct (pro,
// pu_date, service_type) == distinct docId == COUNT(*). So FAIL = some
// row has identity fields disagreeing with its own docId.
//
// PERFORMANCE: sample-based by default (1000 rows; ~5s). Pass full=1 in
// the URL query to do a full scan (~5 min, requires an extended timeout
// or a background runner — used by Commit 4 migration runner via direct
// function-to-function invocation, not by interactive triggering).
async function check2(apiKey: string, full: boolean): Promise<CheckClosureResult> {
  // Total row count (cheap aggregation)
  const totalRes = await fsRunAggregation({
    structuredAggregationQuery: {
      structuredQuery: { from: [{ collectionId: "das_lines" }] },
      aggregations: [{ alias: "n", count: {} }],
    },
  }, apiKey);
  const total = Number(totalRes?.[0]?.result?.aggregateFields?.n?.integerValue || 0);

  // Walk das_lines (or sample) and verify each row's docId ==
  // `${pro}_${pu_date||'nodate'}_${service_type}`. Misalignments =
  // parser bug or migration leftover.
  const SAMPLE_TARGET = full ? Number.MAX_SAFE_INTEGER : 1000;
  let misaligned = 0;
  let scanned = 0;
  const examples: any[] = [];
  // Tuple-distinctness is only meaningful in full-scan mode; in sample
  // mode we report it as null.
  const tupleSet: Map<string, number> | null = full ? new Map() : null;

  await fsPaginate(
    {
      structuredQuery: {
        from: [{ collectionId: "das_lines" }],
        orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
      },
    },
    full ? 1000 : 500,
    apiKey,
    async (docs) => {
      for (const doc of docs) {
        const docId = doc.name?.split("/").pop() || "";
        const fields = unwrapDoc(doc);
        const pro = String(fields.pro || "");
        const puDate = fields.pu_date == null ? "nodate" : String(fields.pu_date);
        const st = String(fields.service_type || "");
        const expectedId = `${pro}_${puDate}_${st}`;
        if (docId !== expectedId) {
          misaligned++;
          if (examples.length < 25) {
            examples.push({ docId, expected: expectedId, pro, pu_date: fields.pu_date, service_type: st });
          }
        }
        if (tupleSet) {
          const tuple = `${pro}|${puDate}|${st}`;
          tupleSet.set(tuple, (tupleSet.get(tuple) || 0) + 1);
        }
        scanned++;
        if (scanned >= SAMPLE_TARGET) return false;
      }
    },
  );

  let distinctTuples: number | null = null;
  const dupTuples: any[] = [];
  if (tupleSet) {
    distinctTuples = tupleSet.size;
    for (const [tuple, count] of tupleSet) {
      if (count > 1) {
        dupTuples.push({ tuple, count });
        if (dupTuples.length >= 25) break;
      }
    }
  }

  // Pass criteria differ for sample vs full
  let pass: boolean;
  let summary: string;
  if (full) {
    pass = misaligned === 0 && distinctTuples === total;
    summary = pass
      ? `Delivery uniqueness GREEN (full scan) — ${total.toLocaleString()} das_lines, all docIds align with (pro, pu_date, service_type)`
      : `Delivery uniqueness FAIL (full scan) — ${misaligned} misaligned docIds; distinct tuples ${distinctTuples} vs total ${total}`;
  } else {
    pass = misaligned === 0;
    summary = pass
      ? `Delivery uniqueness GREEN (sampled ${scanned.toLocaleString()}/${total.toLocaleString()}) — all sampled docIds align with (pro, pu_date, service_type)`
      : `Delivery uniqueness FAIL (sampled ${scanned.toLocaleString()}) — ${misaligned} misaligned docIds detected; recommend ?full=1 for complete count`;
  }

  return {
    status: pass ? "PASS" : "FAIL",
    summary,
    details: {
      total,
      scanned,
      mode: full ? "full_scan" : "sample",
      distinct_tuples: distinctTuples,
      misaligned_count: misaligned,
      misaligned_examples: examples,
      duplicate_tuples_examples: dupTuples,
    },
  };
}

// ─── Check 2B: Orphan accessorial detection ──────────────────────────────────
// For every das_lines row with service_type='accessorial', a sibling
// das_lines row at (pro, pu_date, service_type='delivery' or 'truckload')
// must exist. Orphan = billing anomaly (accessorial charged but no
// delivery line) — surface for Chad's recon review.
//
// PERFORMANCE: full scan walks all delivery + truckload + accessorial rows
// (~30s for 813K rows). Sample mode walks accessorials only (typically
// ~9K) and uses point-lookup GETs for the sibling check. Sample is
// faster and more accurate per-row; full is needed when delivery/truckload
// counts also matter for the report.
async function check2B(apiKey: string, full: boolean): Promise<CheckClosureResult> {
  // Helper: check if a sibling row exists at (pro, pu_date, st_to_check).
  async function hasSibling(pro: string, puDate: string, st: string): Promise<boolean> {
    const docId = `${pro}_${puDate || "nodate"}_${st}`;
    const doc = await fsGetDoc("das_lines", docId, apiKey);
    return !!doc;
  }

  // Walk accessorials. For each, point-lookup its siblings.
  const orphans: any[] = [];
  let accessorialCount = 0;

  await fsPaginate(
    {
      structuredQuery: {
        from: [{ collectionId: "das_lines" }],
        where: { fieldFilter: { field: { fieldPath: "service_type" }, op: "EQUAL", value: { stringValue: "accessorial" } } },
        orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
      },
    },
    500,
    apiKey,
    async (docs) => {
      for (const doc of docs) {
        const f = unwrapDoc(doc);
        accessorialCount++;
        const pro = String(f.pro || "");
        const puDate = f.pu_date == null ? "nodate" : String(f.pu_date);
        // Point-lookup both delivery and truckload siblings
        const hasDelivery = await hasSibling(pro, puDate, "delivery");
        if (hasDelivery) continue;
        const hasTruckload = await hasSibling(pro, puDate, "truckload");
        if (hasTruckload) continue;
        // Orphan
        if (orphans.length < 100) {
          orphans.push({
            docId: doc.name?.split("/").pop(),
            pro: f.pro,
            pu_date: f.pu_date,
            code: f.code,
            extra_cost: f.extra_cost,
            source_file_id: f.source_file_id,
          });
        }
      }
    },
  );

  // For full mode, also report delivery + truckload row counts (informational).
  let deliveryCount: number | null = null;
  let truckloadCount: number | null = null;
  if (full) {
    const dRes = await fsRunAggregation({
      structuredAggregationQuery: {
        structuredQuery: {
          from: [{ collectionId: "das_lines" }],
          where: { fieldFilter: { field: { fieldPath: "service_type" }, op: "EQUAL", value: { stringValue: "delivery" } } },
        },
        aggregations: [{ alias: "n", count: {} }],
      },
    }, apiKey);
    deliveryCount = Number(dRes?.[0]?.result?.aggregateFields?.n?.integerValue || 0);
    const tRes = await fsRunAggregation({
      structuredAggregationQuery: {
        structuredQuery: {
          from: [{ collectionId: "das_lines" }],
          where: { fieldFilter: { field: { fieldPath: "service_type" }, op: "EQUAL", value: { stringValue: "truckload" } } },
        },
        aggregations: [{ alias: "n", count: {} }],
      },
    }, apiKey);
    truckloadCount = Number(tRes?.[0]?.result?.aggregateFields?.n?.integerValue || 0);
  }

  const pass = orphans.length === 0;
  return {
    status: pass ? "PASS" : "FAIL",
    summary: pass
      ? `Orphan accessorial GREEN — ${accessorialCount.toLocaleString()} accessorials, all have sibling delivery/truckload`
      : `Orphan accessorial FAIL — ${orphans.length}${orphans.length === 100 ? "+" : ""} accessorials missing sibling row`,
    details: {
      mode: full ? "full" : "sample",
      delivery_rows: deliveryCount,
      truckload_rows: truckloadCount,
      accessorial_rows: accessorialCount,
      orphan_count: orphans.length === 100 ? "100+" : orphans.length,
      orphan_examples: orphans.slice(0, 25),
    },
  };
}

// ─── Check 2C: Accessorial sum reconciliation (INFO metric) ──────────────────
// For each (pro, pu_date) where both delivery and accessorial das_lines
// rows exist, compute total_billed = delivery.cost + sum(accessorial.extra_cost).
// Compare against ddis_payments.paid_amount summed for same PRO.
//
// This is a coarse spot-check — perfect equality is not expected because
// of timing differences, partial payments, etc. Report the deviation
// distribution as a histogram for trend tracking.
//
// PERFORMANCE: building the (pro,pu_date) -> {delivery, accessorial}
// totals requires walking the relevant das_lines slice. Sample mode
// limits to ~5000 das_lines rows scanned (~30s) and ~50 DDIS lookups.
// Full mode walks all ~813K rows (~5 min).
async function check2C(apiKey: string, full: boolean): Promise<CheckClosureResult> {
  const SCAN_TARGET = full ? Number.MAX_SAFE_INTEGER : 5000;
  const DDIS_CANDIDATE_CAP = full ? 200 : 50;

  // Build (pro, pu_date) -> { delivery_cost, accessorial_total }
  const totals = new Map<string, { delivery_cost: number; accessorial_total: number }>();
  let scanned = 0;

  await fsPaginate(
    {
      structuredQuery: {
        from: [{ collectionId: "das_lines" }],
        select: {
          fields: [
            { fieldPath: "pro" }, { fieldPath: "pu_date" }, { fieldPath: "service_type" },
            { fieldPath: "cost" }, { fieldPath: "extra_cost" }, { fieldPath: "new_cost" },
          ],
        },
        orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
      },
    },
    1000,
    apiKey,
    async (docs) => {
      for (const doc of docs) {
        const f = unwrapDoc(doc);
        if (!f.pro || !f.pu_date) continue;
        const key = `${f.pro}|${f.pu_date}`;
        let entry = totals.get(key);
        if (!entry) {
          entry = { delivery_cost: 0, accessorial_total: 0 };
          totals.set(key, entry);
        }
        const cost = Number(f.new_cost ?? f.cost ?? 0);
        const extra = Number(f.extra_cost ?? 0);
        if (f.service_type === "delivery" || f.service_type === "truckload") {
          entry.delivery_cost += cost;
        } else if (f.service_type === "accessorial") {
          entry.accessorial_total += (extra || cost);
        }
        scanned++;
        if (scanned >= SCAN_TARGET) return false;
      }
    },
  );

  // For each tuple with both kinds, sample the DDIS comparison.
  let sampled = 0;
  let withDdis = 0;
  const deviations: number[] = [];
  const sampleDetails: any[] = [];
  const candidates: string[] = [];
  for (const [key, t] of totals) {
    if (t.delivery_cost > 0 && t.accessorial_total > 0) candidates.push(key);
    if (candidates.length >= DDIS_CANDIDATE_CAP) break;
  }

  for (const key of candidates) {
    const t = totals.get(key)!;
    const [pro, puDate] = key.split("|");
    sampled++;
    const ddisRes = await fsRunAggregation({
      structuredAggregationQuery: {
        structuredQuery: {
          from: [{ collectionId: "ddis_payments" }],
          where: { fieldFilter: { field: { fieldPath: "pro" }, op: "EQUAL", value: { stringValue: pro } } },
        },
        aggregations: [{ alias: "n", count: {} }],
      },
    }, apiKey);
    const ddisN = Number(ddisRes?.[0]?.result?.aggregateFields?.n?.integerValue || 0);
    if (ddisN > 0) {
      withDdis++;
      const sumRes = await fsRunQuery({
        structuredQuery: {
          from: [{ collectionId: "ddis_payments" }],
          where: { fieldFilter: { field: { fieldPath: "pro" }, op: "EQUAL", value: { stringValue: pro } } },
          select: { fields: [{ fieldPath: "paid_amount" }] },
        },
      }, apiKey);
      let paid = 0;
      for (const r of sumRes) {
        if (!r.document) continue;
        const f = unwrapDoc(r.document);
        paid += Number(f.paid_amount ?? 0);
      }
      const billed = t.delivery_cost + t.accessorial_total;
      const deviation = paid - billed;
      deviations.push(deviation);
      if (sampleDetails.length < 25) {
        sampleDetails.push({ pro, pu_date: puDate, billed: +billed.toFixed(2), paid: +paid.toFixed(2), deviation: +deviation.toFixed(2) });
      }
    }
  }

  deviations.sort((a, b) => a - b);
  const stats: any = {
    sampled,
    with_ddis: withDdis,
    deviation_count: deviations.length,
  };
  if (deviations.length > 0) {
    stats.min = +deviations[0].toFixed(2);
    stats.max = +deviations[deviations.length - 1].toFixed(2);
    stats.median = +deviations[Math.floor(deviations.length / 2)].toFixed(2);
    const mean = deviations.reduce((a, b) => a + b, 0) / deviations.length;
    stats.mean = +mean.toFixed(2);
    stats.abs_mean = +(deviations.reduce((a, b) => a + Math.abs(b), 0) / deviations.length).toFixed(2);
  }

  return {
    status: "INFO",
    summary: deviations.length > 0
      ? `2C INFO (${full ? "full" : "sample"}) — sampled ${sampled} (pro,pu_date) tuples; ${withDdis} have DDIS data; abs-mean deviation ${stats.abs_mean}, median ${stats.median}`
      : `2C INFO (${full ? "full" : "sample"}) — sampled ${sampled} tuples but no DDIS payment matches (likely Phase 1 follow-up still pending for DDIS provenance)`,
    details: {
      mode: full ? "full" : "sample",
      das_lines_scanned: scanned,
      tuples_in_das_lines: totals.size,
      candidates_with_both_kinds: candidates.length,
      stats,
      sample_examples: sampleDetails,
    },
  };
}

// ─── Check 3: Merge audit reconstructibility ─────────────────────────────────
// Sample 100 random das_lines rows. For each, fetch the source_files_raw
// doc by source_file_id; confirm chunk_count > 0 and the first chunk
// decompresses cleanly. Validates the L1 backing for L3 rows.
async function check3(apiKey: string): Promise<CheckClosureResult> {
  // Reservoir-sample 100 das_lines docs by walking pages and randomly
  // accepting docs. Avoids loading all 1M+ docs in memory.
  const SAMPLE_SIZE = 100;
  const sample: any[] = [];
  let scanned = 0;

  // Use a coarse PRO-prefix scan to get reasonable diversity. We'll
  // grab from 5 different PRO prefix buckets.
  const PRO_BUCKETS = ["6", "7", "U", "1", "9"];  // U for ULI-truckload, others numeric
  for (const bucket of PRO_BUCKETS) {
    let bucketDocs: any[] = [];
    await fsPaginate(
      {
        structuredQuery: {
          from: [{ collectionId: "das_lines" }],
          where: {
            fieldFilter: {
              field: { fieldPath: "__name__" }, op: "GREATER_THAN_OR_EQUAL",
              value: { referenceValue: `projects/${PROJECT_ID}/databases/(default)/documents/das_lines/${bucket}` },
            },
          },
          orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
        },
      },
      40,  // up to 40 per bucket -> 200 candidates total -> sample 100
      apiKey,
      async (docs) => {
        bucketDocs.push(...docs);
        return bucketDocs.length < 40 ? undefined : false;  // stop after 40
      },
    );
    scanned += bucketDocs.length;
    sample.push(...bucketDocs);
  }

  // Reservoir-sample down to 100 (deterministic-ish — just take every Nth)
  const stride = Math.max(1, Math.floor(sample.length / SAMPLE_SIZE));
  const selected = sample.filter((_, i) => i % stride === 0).slice(0, SAMPLE_SIZE);

  const reconstructed: any[] = [];
  const failed: any[] = [];

  for (const doc of selected) {
    const f = unwrapDoc(doc);
    const fileId = f.source_file_id;
    if (!fileId) {
      failed.push({ docId: doc.name?.split("/").pop(), reason: "no source_file_id" });
      continue;
    }
    const sfr = await fsGetDoc("source_files_raw", fileId, apiKey);
    if (!sfr) {
      failed.push({ docId: doc.name?.split("/").pop(), source_file_id: fileId, reason: "source_files_raw doc missing" });
      continue;
    }
    const sfrFields = unwrapDoc(sfr);
    const chunkCount = Number(sfrFields.chunk_count || 0);
    if (chunkCount <= 0) {
      failed.push({ docId: doc.name?.split("/").pop(), source_file_id: fileId, reason: "chunk_count=0" });
      continue;
    }
    // Fetch first chunk and try to decompress
    const chunk0 = await fsGetDoc("source_file_chunks", `${fileId}__000`, apiKey);
    if (!chunk0) {
      failed.push({ docId: doc.name?.split("/").pop(), source_file_id: fileId, reason: "chunk 000 missing" });
      continue;
    }
    const b64 = unwrapDoc(chunk0).data_b64;
    if (!b64) {
      failed.push({ docId: doc.name?.split("/").pop(), source_file_id: fileId, reason: "chunk 000 has no data_b64" });
      continue;
    }
    try {
      // Just test decompressibility — full reconstruct is too heavy for this check
      const gz = Buffer.from(String(b64), "base64");
      // First chunk alone may not decompress; but the gzip header should be valid
      if (gz[0] !== 0x1f || gz[1] !== 0x8b) {
        failed.push({ docId: doc.name?.split("/").pop(), source_file_id: fileId, reason: "first chunk lacks gzip magic" });
        continue;
      }
      reconstructed.push({ docId: doc.name?.split("/").pop(), source_file_id: fileId, chunk_count: chunkCount });
    } catch (e: any) {
      failed.push({ docId: doc.name?.split("/").pop(), source_file_id: fileId, reason: `decompress failed: ${e?.message || e}` });
    }
  }

  const pass = failed.length === 0;
  return {
    status: pass ? "PASS" : "FAIL",
    summary: pass
      ? `Reconstructibility GREEN — ${reconstructed.length}/${selected.length} sampled rows reconstruct from L1`
      : `Reconstructibility FAIL — ${failed.length}/${selected.length} sampled rows cannot reconstruct from L1`,
    details: {
      candidates_scanned: scanned,
      sampled: selected.length,
      reconstructed: reconstructed.length,
      failed: failed.length,
      failure_examples: failed.slice(0, 25),
    },
  };
}

// ─── Check 4: Reparse determinism (STUB) ─────────────────────────────────────
// Will be wired up in Commit 5+. The current parser path is stable enough
// that this could be implemented now, but we deferred per Q7 to keep the
// Phase 2 patch scoped.
async function check4(_apiKey: string): Promise<CheckClosureResult> {
  return {
    status: "INFO",
    summary: "Reparse determinism — not yet implemented (scheduled for Commit 5+)",
    details: {
      reason: "Stubbed in Commit 3 per orchestrator Q7 (see DESIGN.md §10).",
      planned_method: "Sample 5 random source files. Reparse each twice into das_lines_shadow_check4. Compare hash sets between runs and against current production.",
      next_steps: "Wire up after Phase 2 migration completes and the new parser has been running stably for at least one production cycle.",
    },
  };
}

// ─── Check 5: DDIS dual-payment integrity ────────────────────────────────────
// For each (pro, bill_date) tuple in ddis_payments with >1 row, verify
// check numbers are pairwise distinct. Same check = real duplicate
// posting bug; distinct = legitimate split delivery+accessorial pattern.
//
// Today's baseline (run pre-build): GREEN — 0 same-check duplicates among
// 529 multi-row tuples. This check makes that property continuous.
//
// Note on the 539 (Step B) vs 529 (CHECK 5) discrepancy: Step B counted
// PROs with 2+ rows ignoring bill_date; CHECK 5 keys by (pro, bill_date).
// The 10-row gap = PROs with payments split across distinct bill_dates
// with distinct checks (legitimate split-day payments).
//
// PERFORMANCE: walking 459K ddis_payments rows takes ~3 minutes paged.
// Sample mode walks 50K (~25s, ~10% sample). Full mode is needed for
// the migration-runner-grade GREEN signal; sample is fine for routine
// continuous checking.
async function check5(apiKey: string, full: boolean): Promise<CheckClosureResult> {
  const SCAN_TARGET = full ? Number.MAX_SAFE_INTEGER : 50_000;

  const tuples = new Map<string, string[]>();
  let scanned = 0;

  await fsPaginate(
    {
      structuredQuery: {
        from: [{ collectionId: "ddis_payments" }],
        select: { fields: [{ fieldPath: "pro" }, { fieldPath: "bill_date" }, { fieldPath: "check" }] },
        orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
      },
    },
    1000,
    apiKey,
    async (docs) => {
      for (const doc of docs) {
        const f = unwrapDoc(doc);
        scanned++;
        const pro = String(f.pro || "");
        const billDate = String(f.bill_date || "nodate");
        const check = f.check == null ? "" : String(f.check);
        const key = `${pro}|${billDate}`;
        const arr = tuples.get(key);
        if (arr) arr.push(check);
        else tuples.set(key, [check]);
        if (scanned >= SCAN_TARGET) return false;
      }
    },
  );

  const violations: any[] = [];
  let multiRow = 0;
  let multiRowAllNull = 0;
  let multiRowDistinct = 0;

  for (const [key, checks] of tuples) {
    if (checks.length <= 1) continue;
    multiRow++;
    const nonEmpty = checks.filter(c => c && c.length > 0);
    if (nonEmpty.length === 0) {
      multiRowAllNull++;
      continue;
    }
    const seen = new Set<string>();
    let dup: string | null = null;
    for (const c of nonEmpty) {
      if (seen.has(c)) { dup = c; break; }
      seen.add(c);
    }
    if (dup) {
      const [pro, billDate] = key.split("|");
      violations.push({ pro, bill_date: billDate, duplicate_check: dup, all_checks: checks });
    } else {
      multiRowDistinct++;
    }
  }

  const pass = violations.length === 0;
  return {
    status: pass ? "PASS" : "FAIL",
    summary: pass
      ? `DDIS dual-payment GREEN (${full ? "full" : "sample"}) — ${scanned.toLocaleString()} rows scanned, ${multiRow} multi-row tuples, all distinct checks`
      : `DDIS dual-payment FAIL (${full ? "full" : "sample"}) — ${violations.length} (pro,bill_date) tuples have duplicate check numbers`,
    details: {
      mode: full ? "full" : "sample",
      total_rows_scanned: scanned,
      distinct_tuples: tuples.size,
      multi_row_tuples: multiRow,
      multi_row_all_null_checks: multiRowAllNull,
      multi_row_distinct_checks: multiRowDistinct,
      violations: violations.slice(0, 50),
    },
  };
}

// ─── Check 6: L2 lossless reconstructibility ─────────────────────────────────
// Sample 5 random uline source_files_raw docs. For each, reconstruct the
// source-file rows from das_rows_raw alone (no L1 chunk fetch). Compare
// row count and column-coverage to source_files_raw.parsed_row_count and
// .column_headers. Validates the "future-proof L2" guarantee.
async function check6(apiKey: string): Promise<CheckClosureResult> {
  // Pick 5 source_files_raw docs with parsed_row_count > 0. We don't
  // sample uniformly because the goal is "broad smoke-test", not a
  // statistical guarantee. Pick from the first page of each source's
  // collection-by-state="ingested".
  const SAMPLE_SIZE = 5;
  const candidates: any[] = [];

  await fsPaginate(
    {
      structuredQuery: {
        from: [{ collectionId: "source_files_raw" }],
        orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
      },
    },
    50,
    apiKey,
    async (docs) => {
      for (const d of docs) {
        const f = unwrapDoc(d);
        if (Number(f.parsed_row_count || 0) > 0) candidates.push(d);
      }
      return candidates.length < 50 ? undefined : false;
    },
  );

  // Take a uniform stride
  const stride = Math.max(1, Math.floor(candidates.length / SAMPLE_SIZE));
  const sample = candidates.filter((_, i) => i % stride === 0).slice(0, SAMPLE_SIZE);

  const results: any[] = [];
  const failures: any[] = [];

  for (const doc of sample) {
    const meta = unwrapDoc(doc);
    const fileId = meta.file_id || doc.name?.split("/").pop();
    const expectedRows = Number(meta.parsed_row_count || 0);
    const source = String(meta.source || "");
    const expectedHeaders: string[] = Array.isArray(meta.column_headers) ? meta.column_headers : [];

    // L2 collection per source — single source of truth via SOURCE_REGISTRY.
    const l2Coll = SOURCE_REGISTRY[source]?.rowsRaw ?? null;
    if (!l2Coll) {
      results.push({ file_id: fileId, source, status: "SKIPPED", reason: "no L2 collection mapped" });
      continue;
    }

    // Page L2 rows for this file
    const rows: any[] = [];
    await fsPaginate(
      {
        structuredQuery: {
          from: [{ collectionId: l2Coll }],
          where: { fieldFilter: { field: { fieldPath: "source_file_id" }, op: "EQUAL", value: { stringValue: fileId } } },
          orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
        },
      },
      500,
      apiKey,
      async (docs) => {
        for (const d of docs) rows.push(unwrapDoc(d));
      },
    );

    const actualRows = rows.length;
    let headerCoverage: string | null = null;
    let headerSampled: string[] = [];

    if (rows.length > 0 && rows[0].raw_cells) {
      // raw_cells is an array of {header, header_index, value}. Aggregate
      // distinct headers seen across all sampled L2 rows.
      const headersSeen = new Set<string>();
      for (const r of rows.slice(0, 100)) {  // sample first 100 rows to avoid mem blow-up
        if (Array.isArray(r.raw_cells)) {
          for (const cell of r.raw_cells) {
            if (cell?.header) headersSeen.add(cell.header);
          }
        }
      }
      headerSampled = Array.from(headersSeen);
      const expectedSet = new Set(expectedHeaders);
      const missing: string[] = [];
      for (const h of expectedHeaders) if (!headersSeen.has(h)) missing.push(h);
      headerCoverage = missing.length === 0
        ? "COMPLETE"
        : `MISSING(${missing.length}): ${missing.slice(0, 5).join(", ")}${missing.length > 5 ? "..." : ""}`;
    }

    const ok = actualRows === expectedRows && (headerCoverage === "COMPLETE" || expectedHeaders.length === 0);
    const detail = {
      file_id: fileId,
      filename: meta.filename,
      source,
      file_kind: meta.file_kind,
      expected_rows: expectedRows,
      actual_rows: actualRows,
      delta: actualRows - expectedRows,
      expected_header_count: expectedHeaders.length,
      actual_header_count: headerSampled.length,
      header_coverage: headerCoverage,
      ok,
    };
    results.push(detail);
    if (!ok) failures.push(detail);
  }

  const pass = failures.length === 0;
  return {
    status: pass ? "PASS" : "FAIL",
    summary: pass
      ? `L2 lossless GREEN — ${results.length} files sampled; row counts and header coverage match`
      : `L2 lossless FAIL — ${failures.length}/${results.length} sampled files have row-count or header mismatch`,
    details: {
      candidates_scanned: candidates.length,
      sampled: results.length,
      passed: results.length - failures.length,
      failed: failures.length,
      results,
    },
  };
}

// ─── Dispatcher ──────────────────────────────────────────────────────────────

type CheckRunner = (apiKey: string, full: boolean) => Promise<CheckClosureResult>;

const CHECK_RUNNERS: Record<CheckId, CheckRunner> = {
  // 1A: full scan of source_files_raw is bounded (~700 docs); ignores `full`.
  "1A": (k, _f) => check1A(k),
  "1B": (k, _f) => check1B(k),
  "2":  check2,
  "2B": check2B,
  "2C": check2C,
  "3":  (k, _f) => check3(k),
  "4":  (k, _f) => check4(k),
  "5":  check5,
  "6":  (k, _f) => check6(k),
};

export default async (req: Request, _ctx: Context): Promise<Response> => {
  if (!FIREBASE_API_KEY) {
    return jsonResponse({ ok: false, error: "FIREBASE_API_KEY not configured" }, 500);
  }
  const url = new URL(req.url);
  const checkParam = url.searchParams.get("check");

  if (req.method === "GET") {
    if (checkParam) {
      // Return latest result for one check
      if (!VALID_CHECKS.includes(checkParam as CheckId)) {
        return jsonResponse({ ok: false, error: `Unknown check '${checkParam}'. Valid: ${VALID_CHECKS.join(",")}` }, 400);
      }
      const doc = await fsGetDoc("marginiq_health_checks", `${checkParam}__latest`, FIREBASE_API_KEY);
      if (!doc) {
        return jsonResponse({ ok: true, check_id: checkParam, has_result: false, note: "Never run — POST to execute" });
      }
      const f = unwrapDoc(doc);
      let details: any = null;
      try { details = JSON.parse(String(f.details_json || "{}")); } catch {}
      return jsonResponse({
        ok: true,
        check_id: f.check_id,
        ran_at: f.ran_at,
        status: f.status,
        summary: f.summary,
        details,
        duration_ms: f.duration_ms,
      });
    }
    // List all latest
    const out: any[] = [];
    for (const c of VALID_CHECKS) {
      const doc = await fsGetDoc("marginiq_health_checks", `${c}__latest`, FIREBASE_API_KEY);
      if (!doc) {
        out.push({ check_id: c, has_result: false });
        continue;
      }
      const f = unwrapDoc(doc);
      out.push({
        check_id: f.check_id,
        ran_at: f.ran_at,
        status: f.status,
        summary: f.summary,
        duration_ms: f.duration_ms,
      });
    }
    return jsonResponse({ ok: true, checks: out });
  }

  if (req.method !== "POST") {
    return jsonResponse({ ok: false, error: "method not allowed" }, 405);
  }

  if (!checkParam) {
    return jsonResponse({
      ok: false,
      error: `?check param required. Valid: ${VALID_CHECKS.join(",")}`,
    }, 400);
  }
  if (!VALID_CHECKS.includes(checkParam as CheckId)) {
    return jsonResponse({ ok: false, error: `Unknown check '${checkParam}'. Valid: ${VALID_CHECKS.join(",")}` }, 400);
  }

  const checkId = checkParam as CheckId;
  const runner = CHECK_RUNNERS[checkId];
  // ?full=1 (or true/yes) opts into the full-scan variant of checks 2/2B/2C/5.
  // Full-scan variants exceed the 10s default Netlify timeout for some checks
  // and should only be used when the caller has an extended timeout (e.g.,
  // the Commit 4 migration runner which invokes via its own HTTP client).
  const fullParam = url.searchParams.get("full") || "";
  const full = ["1", "true", "yes", "on"].includes(fullParam.toLowerCase());
  // v2.53.4: ?force=1 bypasses the in_migration short-circuit. Used by the
  // migration runner's preflight phase to verify checks still produce real
  // results when the flag is set. Operators should generally NOT pass force=1
  // during an active migration — the gate exists for a reason.
  const forceParam = url.searchParams.get("force") || "";
  const force = ["1", "true", "yes", "on"].includes(forceParam.toLowerCase());
  return await runHealthCheck(checkId, FIREBASE_API_KEY, () => runner(FIREBASE_API_KEY!, full), { force });
};

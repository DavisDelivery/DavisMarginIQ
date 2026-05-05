import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Phase 0 Data Inventory (v2.52.9)
 *
 * One-shot diagnostic for Foundation Rebuild Phase 0.
 *
 * Walks the canonical collection list from §5 of the rebuild brief and,
 * for each collection, captures:
 *   - doc_count  (paginated walk, capped at MAX_COUNT)
 *   - earliest_at / latest_at  (best-effort, scanning common date fields)
 *   - schema_sample  (field names + types from up to SAMPLE_SIZE docs)
 *   - sampled_at  (ISO timestamp)
 *
 * Writes the result to: marginiq_config/data_inventory_2026-05
 *
 * Endpoint: GET /.netlify/functions/marginiq-data-inventory
 *   Optional query params:
 *     ?collections=das_lines,nuvizz_stops   scope to a subset
 *     ?max=10000                            override count cap
 *     ?dry_run=1                            run + return, do not write
 *
 * NOTE: Uses the Firebase REST API with apiKey. Firestore's listCollectionIds
 * requires OAuth, so we use a hardcoded canonical list from §5 of the brief.
 * Phase 3 (Data Coverage tab) will replace this with continuous auto-discovery.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
const INVENTORY_DOC_ID = "data_inventory_2026-05";
const APP_VERSION = "2.52.9";

const DEFAULT_MAX_COUNT = 50_000;
const SAMPLE_SIZE = 5;
const PAGE_SIZE = 300;

// Canonical list from §5 of the Foundation Rebuild brief.
// Add to this list any collection observed in the wild that isn't here.
const CANONICAL_COLLECTIONS: string[] = [
  // High-trust core
  "audited_financials_v2",
  "ddis_payments",
  "ddis_rows_raw",
  "nuvizz_stops",
  "nuvizz_rows_raw",
  "das_lines",
  "das_rows_raw",
  // Variance / audit subsets
  "unpaid_stops",
  "audit_items",
  // Layer 1 raw
  "source_files_raw",
  "source_file_chunks",
  // Operational queues / processed markers
  "pending_uline_files",
  "uline_processed_emails",
  // External feeds (Phase 0 inventory will reveal exact names)
  "payroll_runs",
  "payroll_employees",
  "motive_drivers",
  "motive_vehicles",
  "motive_locations",
  "b600_punches",
  "zoom_call_logs",
  // Config / status
  "marginiq_config",
  // Stop economics rollup
  "stop_economics",
];

// Fields likely to carry meaningful dates, in priority order.
// First match wins per collection.
const DATE_FIELD_CANDIDATES = [
  "pu_date",
  "payment_date",
  "ingested_at",
  "processed_at",
  "created_at",
  "stop_date",
  "punch_in",
  "call_start_time",
  "month",
  "week_ending",
  "date",
  "internalDate",
];

// ─── Firestore helpers ────────────────────────────────────────────────────────

function unwrapValue(v: any): any {
  if (!v || typeof v !== "object") return v;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return parseInt(v.integerValue);
  if ("doubleValue" in v) return v.doubleValue;
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("timestampValue" in v) return v.timestampValue;
  if ("arrayValue" in v) return (v.arrayValue?.values || []).map(unwrapValue);
  if ("mapValue" in v) {
    const out: Record<string, any> = {};
    for (const [k, val] of Object.entries(v.mapValue?.fields || {})) out[k] = unwrapValue(val);
    return out;
  }
  return v;
}

function fsTypeOf(v: any): string {
  if (!v || typeof v !== "object") return typeof v;
  for (const k of ["stringValue", "integerValue", "doubleValue", "booleanValue", "nullValue", "timestampValue", "arrayValue", "mapValue"]) {
    if (k in v) return k.replace("Value", "");
  }
  return "unknown";
}

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") {
    if (!isFinite(v)) return { nullValue: null };
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (typeof v === "string") return { stringValue: v };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(toFsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

async function fsPatchDoc(coll: string, docId: string, fields: Record<string, any>, apiKey: string): Promise<boolean> {
  const url = `${FS_BASE}/${coll}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const body: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) body[k] = toFsValue(v);
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: body }),
  });
  if (!r.ok) {
    console.error(`fsPatchDoc ${coll}/${docId} failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  }
  return r.ok;
}

/**
 * List a page of docs from a collection.
 * Uses pageToken for cursor-based pagination.
 */
async function listPage(
  collection: string,
  apiKey: string,
  pageToken: string | null,
  pageSize: number,
): Promise<{ docs: any[]; nextPageToken: string | null }> {
  const params = new URLSearchParams({ pageSize: String(pageSize), key: apiKey });
  if (pageToken) params.set("pageToken", pageToken);
  const url = `${FS_BASE}/${collection}?${params.toString()}`;
  const r = await fetch(url);
  if (!r.ok) {
    if (r.status === 404) return { docs: [], nextPageToken: null };
    console.warn(`listPage ${collection} failed: ${r.status}`);
    return { docs: [], nextPageToken: null };
  }
  const data: any = await r.json();
  return {
    docs: data.documents || [],
    nextPageToken: data.nextPageToken || null,
  };
}

/**
 * Run a Firestore structured query (orderBy + limit) to find earliest/latest
 * doc by a given field. Returns null if field doesn't exist or no docs.
 */
async function findByOrder(
  collection: string,
  field: string,
  direction: "ASCENDING" | "DESCENDING",
  apiKey: string,
): Promise<any | null> {
  const url = `${FS_BASE}:runQuery?key=${apiKey}`;
  const body = {
    structuredQuery: {
      from: [{ collectionId: collection }],
      orderBy: [{ field: { fieldPath: field }, direction }],
      limit: 1,
    },
  };
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) return null;
  const data: any = await r.json();
  if (!Array.isArray(data) || data.length === 0) return null;
  const result = data[0];
  if (!result.document) return null;
  return result.document;
}

// ─── Inventory logic ──────────────────────────────────────────────────────────

interface CollectionReport {
  name: string;
  doc_count: number;
  count_capped: boolean;
  earliest_at: string | null;
  earliest_field: string | null;
  latest_at: string | null;
  latest_field: string | null;
  schema_sample: Array<{ field: string; type: string; example: string }>;
  sample_doc_ids: string[];
  error: string | null;
  sampled_at: string;
}

async function inventoryOne(
  collection: string,
  maxCount: number,
  apiKey: string,
): Promise<CollectionReport> {
  const sampledAt = new Date().toISOString();
  const report: CollectionReport = {
    name: collection,
    doc_count: 0,
    count_capped: false,
    earliest_at: null,
    earliest_field: null,
    latest_at: null,
    latest_field: null,
    schema_sample: [],
    sample_doc_ids: [],
    error: null,
    sampled_at: sampledAt,
  };

  try {
    // Step 1: count + collect first SAMPLE_SIZE docs for schema
    let pageToken: string | null = null;
    let count = 0;
    const samples: any[] = [];

    while (true) {
      const page = await listPage(collection, apiKey, pageToken, PAGE_SIZE);
      if (page.docs.length === 0 && pageToken === null) {
        // collection doesn't exist or is empty
        break;
      }
      if (samples.length < SAMPLE_SIZE) {
        for (const d of page.docs) {
          if (samples.length >= SAMPLE_SIZE) break;
          samples.push(d);
        }
      }
      count += page.docs.length;
      if (count >= maxCount) {
        report.count_capped = true;
        break;
      }
      if (!page.nextPageToken) break;
      pageToken = page.nextPageToken;
    }

    report.doc_count = count;

    // Step 2: schema sample from first doc (other samples checked too)
    if (samples.length > 0) {
      report.sample_doc_ids = samples.map(s => {
        const name: string = s.name || "";
        return name.split("/").pop() || "";
      });

      // Union of fields across all sampled docs, with first-seen example
      const fieldMap = new Map<string, { type: string; example: string }>();
      for (const doc of samples) {
        const fields = doc.fields || {};
        for (const [key, val] of Object.entries(fields)) {
          if (fieldMap.has(key)) continue;
          const type = fsTypeOf(val);
          const unwrapped = unwrapValue(val);
          let example = "";
          if (typeof unwrapped === "object") {
            try {
              example = JSON.stringify(unwrapped).slice(0, 80);
            } catch { example = "[object]"; }
          } else {
            example = String(unwrapped).slice(0, 80);
          }
          fieldMap.set(key, { type, example });
        }
      }
      report.schema_sample = Array.from(fieldMap.entries())
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([field, info]) => ({ field, type: info.type, example: info.example }));
    }

    // Step 3: earliest/latest by best-fit date field
    if (count > 0) {
      const sampleFields = new Set(report.schema_sample.map(s => s.field));
      const dateField = DATE_FIELD_CANDIDATES.find(f => sampleFields.has(f)) || null;

      if (dateField) {
        const earliestDoc = await findByOrder(collection, dateField, "ASCENDING", apiKey);
        const latestDoc = await findByOrder(collection, dateField, "DESCENDING", apiKey);

        if (earliestDoc?.fields?.[dateField]) {
          report.earliest_at = String(unwrapValue(earliestDoc.fields[dateField])).slice(0, 60);
          report.earliest_field = dateField;
        }
        if (latestDoc?.fields?.[dateField]) {
          report.latest_at = String(unwrapValue(latestDoc.fields[dateField])).slice(0, 60);
          report.latest_field = dateField;
        }
      }
    }
  } catch (e: any) {
    report.error = e?.message || String(e);
    console.warn(`inventoryOne(${collection}) error: ${report.error}`);
  }

  return report;
}

// ─── Entry point ──────────────────────────────────────────────────────────────

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default async (req: Request, _context: Context) => {
  const url = new URL(req.url);

  if (!FIREBASE_API_KEY) {
    return json({ ok: false, error: "FIREBASE_API_KEY not configured" }, 500);
  }

  const collectionsParam = url.searchParams.get("collections");
  const targetCollections = collectionsParam
    ? collectionsParam.split(",").map(s => s.trim()).filter(Boolean)
    : CANONICAL_COLLECTIONS;

  const maxParam = url.searchParams.get("max");
  const maxCount = maxParam ? Math.max(100, Math.min(500_000, parseInt(maxParam) || DEFAULT_MAX_COUNT)) : DEFAULT_MAX_COUNT;

  const dryRun = url.searchParams.get("dry_run") === "1" || url.searchParams.get("dry_run") === "true";

  const generatedAt = new Date().toISOString();
  const startMs = Date.now();

  console.log(`marginiq-data-inventory: scanning ${targetCollections.length} collections (max=${maxCount}, dry_run=${dryRun})`);

  const reports: Record<string, CollectionReport> = {};
  let totalDocs = 0;
  let collectionsWithData = 0;
  let collectionsEmpty = 0;
  let collectionsErrored = 0;

  for (const collection of targetCollections) {
    const report = await inventoryOne(collection, maxCount, FIREBASE_API_KEY!);
    reports[collection] = report;
    totalDocs += report.doc_count;
    if (report.error) collectionsErrored++;
    else if (report.doc_count > 0) collectionsWithData++;
    else collectionsEmpty++;
    console.log(`  ${collection}: ${report.doc_count} docs${report.count_capped ? " (capped)" : ""}${report.error ? ` ERROR: ${report.error}` : ""}`);
  }

  const elapsedSec = (Date.now() - startMs) / 1000;

  const summary = {
    generated_at: generatedAt,
    app_version: APP_VERSION,
    elapsed_seconds: Math.round(elapsedSec * 10) / 10,
    max_count_per_collection: maxCount,
    collection_count: targetCollections.length,
    collections_with_data: collectionsWithData,
    collections_empty: collectionsEmpty,
    collections_errored: collectionsErrored,
    total_docs_counted: totalDocs,
    canonical_list_source: "Foundation Rebuild brief §5, May 4 2026",
    collections: reports,
  };

  if (!dryRun) {
    const written = await fsPatchDoc("marginiq_config", INVENTORY_DOC_ID, summary, FIREBASE_API_KEY!);
    if (!written) {
      return json({ ok: false, error: "Failed to write inventory doc", summary }, 500);
    }
    console.log(`marginiq-data-inventory: wrote marginiq_config/${INVENTORY_DOC_ID}`);
  }

  return json({
    ok: true,
    dry_run: dryRun,
    written_to: dryRun ? null : `marginiq_config/${INVENTORY_DOC_ID}`,
    elapsed_seconds: summary.elapsed_seconds,
    collection_count: summary.collection_count,
    collections_with_data: summary.collections_with_data,
    collections_empty: summary.collections_empty,
    collections_errored: summary.collections_errored,
    total_docs_counted: summary.total_docs_counted,
    summary_table: targetCollections.map(c => ({
      name: c,
      docs: reports[c].doc_count,
      capped: reports[c].count_capped,
      earliest: reports[c].earliest_at,
      latest: reports[c].latest_at,
      fields: reports[c].schema_sample.length,
      error: reports[c].error,
    })),
  });
};

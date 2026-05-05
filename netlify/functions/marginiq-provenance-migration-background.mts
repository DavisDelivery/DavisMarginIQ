import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Phase 1 Provenance Migration (Background Worker, v2.53.0)
 *
 * See marginiq-provenance-migration.mts for the contract and strategy.
 *
 * This worker:
 *   1. Loads the report doc + checkpoint from marginiq_config
 *   2. Picks up where it left off (by collection + last_doc_id)
 *   3. Processes up to MAX_DOCS_PER_RUN rows
 *   4. Writes back report doc + new checkpoint
 *   5. Returns
 *
 * Hit the trigger repeatedly until report state=complete.
 *
 * The worker is intentionally bounded per invocation. Avoids the
 * inventory function's failure mode (one giant run that times out
 * at 15 min and writes nothing).
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
const REPORT_DOC = "provenance_migration_2026-05";
const APP_VERSION = "2.53.0";
const SCHEMA_VERSION_DEFAULT = "1.0.0-legacy";
const PAGE_SIZE = 100;
const MAX_DOCS_PER_RUN = 5000; // ~10 minutes worst case at ~120ms/doc patches

// Order matters: process small collections first so we can stamp
// checkpoint progress quickly, then move to large ones.
const MIGRATION_ORDER = [
  "audit_items",          // 26 docs
  "source_files_raw",     // 249 docs - rename staged_by → ingested_by
  "ddis_rows_raw",        // 3.3K
  "nuvizz_rows_raw",      // 3.3K
  "unpaid_stops",         // 4.7K
  "ddis_payments",        // 50K+
  "nuvizz_stops",         // 50K+ (mostly unprovenanced)
  "das_rows_raw",         // 50K+
  "das_lines",            // 50K+
];

// ─── Firestore primitives ─────────────────────────────────────────────────────

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

function unwrap(v: any): any {
  if (!v || typeof v !== "object") return v;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return parseInt(v.integerValue);
  if ("doubleValue" in v) return v.doubleValue;
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("timestampValue" in v) return v.timestampValue;
  if ("arrayValue" in v) return (v.arrayValue?.values || []).map(unwrap);
  if ("mapValue" in v) {
    const out: Record<string, any> = {};
    for (const [k, val] of Object.entries(v.mapValue?.fields || {})) out[k] = unwrap(val);
    return out;
  }
  return v;
}

async function getDoc(collection: string, docId: string): Promise<any | null> {
  const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
}

async function patchDoc(collection: string, docId: string, fields: Record<string, any>): Promise<boolean> {
  const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const fsFields: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) fsFields[k] = toFsValue(v);
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: fsFields }),
  });
  return r.ok;
}

async function listPage(
  collection: string,
  pageToken: string | null,
  pageSize: number,
): Promise<{ docs: any[]; nextPageToken: string | null }> {
  const params = new URLSearchParams({ pageSize: String(pageSize), key: FIREBASE_API_KEY || "" });
  if (pageToken) params.set("pageToken", pageToken);
  const url = `${FS_BASE}/${collection}?${params.toString()}`;
  const r = await fetch(url);
  if (!r.ok) return { docs: [], nextPageToken: null };
  const data: any = await r.json();
  return { docs: data.documents || [], nextPageToken: data.nextPageToken || null };
}

// Find a source_files_raw doc by filename + source.
// Cached per migration run since the same file maps to many rows.
const _filenameToFileId = new Map<string, string | null>();
async function findFileIdByFilename(filename: string, source: string): Promise<string | null> {
  const cacheKey = `${source}::${filename}`;
  if (_filenameToFileId.has(cacheKey)) return _filenameToFileId.get(cacheKey) || null;

  const url = `${FS_BASE}:runQuery?key=${FIREBASE_API_KEY}`;
  const body = {
    structuredQuery: {
      from: [{ collectionId: "source_files_raw" }],
      where: {
        compositeFilter: {
          op: "AND",
          filters: [
            { fieldFilter: { field: { fieldPath: "filename" }, op: "EQUAL", value: { stringValue: filename } } },
            { fieldFilter: { field: { fieldPath: "source" }, op: "EQUAL", value: { stringValue: source } } },
          ],
        },
      },
      limit: 1,
    },
  };
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    _filenameToFileId.set(cacheKey, null);
    return null;
  }
  const data: any = await r.json();
  if (!Array.isArray(data) || !data[0]?.document) {
    _filenameToFileId.set(cacheKey, null);
    return null;
  }
  const fileId = unwrap(data[0].document.fields?.file_id) as string | null;
  _filenameToFileId.set(cacheKey, fileId);
  return fileId;
}

// ─── Per-collection reconstruction logic ──────────────────────────────────────

interface PerCollectionStats {
  processed: number;
  enriched: number;
  already_provenanced: number;
  unprovenanced: number;
  errored: number;
  last_doc_id: string | null;
  next_page_token: string | null;
  complete: boolean;
}

function emptyStats(): PerCollectionStats {
  return {
    processed: 0,
    enriched: 0,
    already_provenanced: 0,
    unprovenanced: 0,
    errored: 0,
    last_doc_id: null,
    next_page_token: null,
    complete: false,
  };
}

interface ReconstructResult {
  outcome: "enriched" | "already_provenanced" | "unprovenanced" | "errored";
  patch?: Record<string, any>;
  error?: string;
}

async function reconstructForRow(collection: string, docId: string, fields: any): Promise<ReconstructResult> {
  const f = fields || {};
  const sourceFileId = unwrap(f.source_file_id);
  const ingestedBy = unwrap(f.ingested_by);
  const schemaVersion = unwrap(f.schema_version);

  // Already fully provenanced?
  if (sourceFileId && ingestedBy && schemaVersion) {
    return { outcome: "already_provenanced" };
  }

  // Per-collection logic
  switch (collection) {
    case "ddis_payments": {
      const sourceFile = unwrap(f.source_file) as string | null;
      if (!sourceFile) return { outcome: "unprovenanced", error: "no source_file field" };
      let fileId = sourceFileId as string | null;
      if (!fileId) fileId = await findFileIdByFilename(sourceFile, "ddis");
      if (!fileId) return { outcome: "unprovenanced", error: `source_files_raw lookup miss for ${sourceFile}` };
      return {
        outcome: "enriched",
        patch: {
          source_file_id: fileId,
          ingested_by: ingestedBy || "ddis-auto-ingest@pre-2.53.0",
          schema_version: schemaVersion || SCHEMA_VERSION_DEFAULT,
          ingested_at: unwrap(f.uploaded_at) || unwrap(f.ingested_at) || new Date().toISOString(),
        },
      };
    }

    case "ddis_rows_raw": {
      // No source_file on rows_raw — use pro + bill_date to find a payment
      const pro = unwrap(f.pro);
      const billDate = unwrap(f.bill_date);
      if (!pro) return { outcome: "unprovenanced", error: "no pro" };
      // Look up a matching payment
      const url = `${FS_BASE}:runQuery?key=${FIREBASE_API_KEY}`;
      const body = {
        structuredQuery: {
          from: [{ collectionId: "ddis_payments" }],
          where: billDate
            ? {
                compositeFilter: {
                  op: "AND",
                  filters: [
                    { fieldFilter: { field: { fieldPath: "pro" }, op: "EQUAL", value: { stringValue: String(pro) } } },
                    { fieldFilter: { field: { fieldPath: "bill_date" }, op: "EQUAL", value: { stringValue: String(billDate) } } },
                  ],
                },
              }
            : { fieldFilter: { field: { fieldPath: "pro" }, op: "EQUAL", value: { stringValue: String(pro) } } },
          limit: 1,
        },
      };
      const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      if (!r.ok) return { outcome: "errored", error: `lookup query failed ${r.status}` };
      const data: any = await r.json();
      if (!Array.isArray(data) || !data[0]?.document) {
        return { outcome: "unprovenanced", error: "no matching ddis_payments row" };
      }
      const fileId = unwrap(data[0].document.fields?.source_file_id) as string | null;
      if (!fileId) {
        return { outcome: "unprovenanced", error: "matching payment also lacks source_file_id" };
      }
      return {
        outcome: "enriched",
        patch: {
          source_file_id: fileId,
          ingested_by: "ddis-ingest-background@pre-2.53.0",
          schema_version: SCHEMA_VERSION_DEFAULT,
        },
      };
    }

    case "nuvizz_stops":
    case "nuvizz_rows_raw":
      // No reliable backlink. Flag as unprovenanced.
      return { outcome: "unprovenanced", error: "NuVizz pre-Phase-1 rows have no backlink to source_files_raw; rebuild via Phase 2 NuVizz historical backfill" };

    case "das_lines": {
      // v2.52.8 already wrote source_file_id. Just patch missing companions.
      if (!sourceFileId) return { outcome: "unprovenanced", error: "no source_file_id (pre-v2.52.8 row?)" };
      const patch: Record<string, any> = {};
      if (!ingestedBy) patch.ingested_by = "uline-historical-backfill@v2.52.8";
      if (!schemaVersion) patch.schema_version = SCHEMA_VERSION_DEFAULT;
      if (Object.keys(patch).length === 0) return { outcome: "already_provenanced" };
      return { outcome: "enriched", patch };
    }

    case "das_rows_raw": {
      // v2.52.8 wrote `file_id` (note: not `source_file_id`). Migration
      // adds source_file_id field aliasing file_id, plus companions.
      const fileId = (sourceFileId as string) || (unwrap(f.file_id) as string | null);
      if (!fileId) return { outcome: "unprovenanced", error: "no file_id or source_file_id" };
      const patch: Record<string, any> = {};
      if (!sourceFileId) patch.source_file_id = fileId;
      if (!ingestedBy) patch.ingested_by = "uline-historical-backfill@v2.52.8";
      if (!schemaVersion) patch.schema_version = SCHEMA_VERSION_DEFAULT;
      if (Object.keys(patch).length === 0) return { outcome: "already_provenanced" };
      return { outcome: "enriched", patch };
    }

    case "unpaid_stops": {
      // Match by pro + pu_date to das_lines.
      const pro = unwrap(f.pro);
      const puDate = unwrap(f.pu_date);
      if (!pro) return { outcome: "unprovenanced", error: "no pro" };
      const url = `${FS_BASE}:runQuery?key=${FIREBASE_API_KEY}`;
      const body = {
        structuredQuery: {
          from: [{ collectionId: "das_lines" }],
          where: puDate
            ? { compositeFilter: { op: "AND", filters: [
                { fieldFilter: { field: { fieldPath: "pro" }, op: "EQUAL", value: { stringValue: String(pro) } } },
                { fieldFilter: { field: { fieldPath: "pu_date" }, op: "EQUAL", value: { stringValue: String(puDate) } } },
              ] } }
            : { fieldFilter: { field: { fieldPath: "pro" }, op: "EQUAL", value: { stringValue: String(pro) } } },
          limit: 1,
        },
      };
      const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      if (!r.ok) return { outcome: "errored", error: `lookup failed ${r.status}` };
      const data: any = await r.json();
      if (!Array.isArray(data) || !data[0]?.document) {
        return { outcome: "unprovenanced", error: "no matching das_lines row" };
      }
      const fileId = unwrap(data[0].document.fields?.source_file_id) as string | null;
      if (!fileId) return { outcome: "unprovenanced", error: "match found but no source_file_id" };
      return {
        outcome: "enriched",
        patch: {
          source_file_id: fileId,
          ingested_by: "unpaid_stops-derived@pre-2.53.0",
          schema_version: SCHEMA_VERSION_DEFAULT,
        },
      };
    }

    case "audit_items": {
      // Tiny set, match by pro to das_lines.
      const pro = unwrap(f.pro);
      if (!pro) return { outcome: "unprovenanced", error: "no pro" };
      const url = `${FS_BASE}:runQuery?key=${FIREBASE_API_KEY}`;
      const body = {
        structuredQuery: {
          from: [{ collectionId: "das_lines" }],
          where: { fieldFilter: { field: { fieldPath: "pro" }, op: "EQUAL", value: { stringValue: String(pro) } } },
          limit: 1,
        },
      };
      const r = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      if (!r.ok) return { outcome: "errored", error: `lookup failed ${r.status}` };
      const data: any = await r.json();
      if (!Array.isArray(data) || !data[0]?.document) {
        return { outcome: "unprovenanced", error: "no das_lines row for pro" };
      }
      const fileId = unwrap(data[0].document.fields?.source_file_id) as string | null;
      if (!fileId) return { outcome: "unprovenanced", error: "match found but no source_file_id" };
      return {
        outcome: "enriched",
        patch: {
          source_file_id: fileId,
          ingested_by: "audit-rebuild@pre-2.53.0",
          schema_version: SCHEMA_VERSION_DEFAULT,
        },
      };
    }

    case "source_files_raw": {
      // Pre-Phase-1 may have staged_by but not ingested_by. Normalize.
      const stagedBy = unwrap(f.staged_by) as string | null;
      const patch: Record<string, any> = {};
      if (!ingestedBy) patch.ingested_by = stagedBy || "auto-ingest@pre-2.53.0";
      if (!schemaVersion) patch.schema_version = SCHEMA_VERSION_DEFAULT;
      if (Object.keys(patch).length === 0) return { outcome: "already_provenanced" };
      return { outcome: "enriched", patch };
    }

    default:
      return { outcome: "unprovenanced", error: `no migration logic for ${collection}` };
  }
}

// ─── Migration loop ───────────────────────────────────────────────────────────

interface ReportDoc {
  state: "running" | "complete" | "failed";
  per_collection: Record<string, PerCollectionStats>;
  started_at: string;
  last_run_at: string;
  completed_at: string | null;
  app_version: string;
  checkpoint: { collection: string; next_page_token: string | null };
  total_processed_so_far: number;
}

async function loadReport(): Promise<ReportDoc | null> {
  const doc = await getDoc("marginiq_config", REPORT_DOC);
  if (!doc) return null;
  // Unwrap fields
  const f = doc.fields || {};
  const result: any = {};
  for (const [k, v] of Object.entries(f)) result[k] = unwrap(v);
  return result as ReportDoc;
}

async function saveReport(r: ReportDoc): Promise<boolean> {
  return await patchDoc("marginiq_config", REPORT_DOC, r as unknown as Record<string, any>);
}

function freshReport(): ReportDoc {
  const now = new Date().toISOString();
  const per: Record<string, PerCollectionStats> = {};
  for (const c of MIGRATION_ORDER) per[c] = emptyStats();
  return {
    state: "running",
    per_collection: per,
    started_at: now,
    last_run_at: now,
    completed_at: null,
    app_version: APP_VERSION,
    checkpoint: { collection: MIGRATION_ORDER[0], next_page_token: null },
    total_processed_so_far: 0,
  };
}

async function migrateOnePage(
  collection: string,
  pageToken: string | null,
  budget: number,
): Promise<{ stats: PerCollectionStats; budgetSpent: number; pageDocsCount: number }> {
  const stats = emptyStats();
  let budgetSpent = 0;

  const page = await listPage(collection, pageToken, Math.min(PAGE_SIZE, budget));
  if (page.docs.length === 0) {
    stats.complete = true;
    stats.next_page_token = null;
    return { stats, budgetSpent: 0, pageDocsCount: 0 };
  }

  for (const doc of page.docs) {
    if (budgetSpent >= budget) break;
    const docId: string = String(doc.name).split("/").pop() || "";
    stats.last_doc_id = docId;
    stats.processed++;
    budgetSpent++;

    try {
      const result = await reconstructForRow(collection, docId, doc.fields);
      if (result.outcome === "enriched") {
        const ok = await patchDoc(collection, docId, result.patch || {});
        if (ok) stats.enriched++;
        else stats.errored++;
      } else if (result.outcome === "already_provenanced") {
        stats.already_provenanced++;
      } else if (result.outcome === "unprovenanced") {
        stats.unprovenanced++;
      } else {
        stats.errored++;
      }
    } catch (e: any) {
      console.warn(`migration ${collection}/${docId}: ${e?.message || e}`);
      stats.errored++;
    }
  }

  stats.next_page_token = page.nextPageToken;
  if (!page.nextPageToken && page.docs.length === 0) {
    stats.complete = true;
  } else if (!page.nextPageToken) {
    stats.complete = true;
  }
  return { stats, budgetSpent, pageDocsCount: page.docs.length };
}

function mergeStats(prev: PerCollectionStats, next: PerCollectionStats): PerCollectionStats {
  return {
    processed: prev.processed + next.processed,
    enriched: prev.enriched + next.enriched,
    already_provenanced: prev.already_provenanced + next.already_provenanced,
    unprovenanced: prev.unprovenanced + next.unprovenanced,
    errored: prev.errored + next.errored,
    last_doc_id: next.last_doc_id || prev.last_doc_id,
    next_page_token: next.next_page_token,
    complete: next.complete,
  };
}

// ─── Entry point ──────────────────────────────────────────────────────────────

export default async (req: Request, _context: Context) => {
  if (!FIREBASE_API_KEY) {
    console.error("provenance-migration-background: missing FIREBASE_API_KEY");
    return;
  }

  const url = new URL(req.url);
  const reset = url.searchParams.get("reset") === "1";
  const onlyCollection = url.searchParams.get("collection");
  const limitParam = url.searchParams.get("limit");
  const customBudget = limitParam ? Math.max(50, Math.min(MAX_DOCS_PER_RUN, parseInt(limitParam) || MAX_DOCS_PER_RUN)) : MAX_DOCS_PER_RUN;

  let report = await loadReport();
  if (reset || !report) {
    report = freshReport();
    console.log("provenance-migration: starting fresh");
  }

  const t0 = Date.now();
  let budget = customBudget;

  // Pick up checkpoint
  let checkpointCollection = report.checkpoint.collection;
  let checkpointToken = report.checkpoint.next_page_token;

  if (onlyCollection) checkpointCollection = onlyCollection;

  while (budget > 0) {
    const idx = MIGRATION_ORDER.indexOf(checkpointCollection);
    if (idx < 0) break;

    const existing = report.per_collection[checkpointCollection] || emptyStats();
    if (existing.complete && !onlyCollection) {
      // Move to next
      const nextIdx = idx + 1;
      if (nextIdx >= MIGRATION_ORDER.length) {
        report.state = "complete";
        report.completed_at = new Date().toISOString();
        break;
      }
      checkpointCollection = MIGRATION_ORDER[nextIdx];
      checkpointToken = null;
      continue;
    }

    console.log(`provenance-migration: ${checkpointCollection} (token=${checkpointToken?.slice(0,12) || "start"}, budget=${budget})`);
    const { stats, budgetSpent, pageDocsCount } = await migrateOnePage(checkpointCollection, checkpointToken, budget);

    report.per_collection[checkpointCollection] = mergeStats(existing, stats);
    report.total_processed_so_far += stats.processed;
    budget -= budgetSpent;

    checkpointToken = stats.next_page_token;
    if (stats.complete || pageDocsCount === 0) {
      report.per_collection[checkpointCollection].complete = true;
      // Move to next collection on next loop iter (unless scoped)
      if (onlyCollection) {
        break;
      }
      const nextIdx = idx + 1;
      if (nextIdx >= MIGRATION_ORDER.length) {
        report.state = "complete";
        report.completed_at = new Date().toISOString();
        break;
      }
      checkpointCollection = MIGRATION_ORDER[nextIdx];
      checkpointToken = null;
    }

    // Save mid-run after every page so we never lose work
    report.last_run_at = new Date().toISOString();
    report.checkpoint = { collection: checkpointCollection, next_page_token: checkpointToken };
    await saveReport(report);

    // Hard cap on wall time per invocation (safety net)
    if (Date.now() - t0 > 12 * 60 * 1000) {
      console.log("provenance-migration: 12 min wall budget hit, stopping");
      break;
    }
  }

  const elapsed = Math.round((Date.now() - t0) / 1000);
  console.log(`provenance-migration: stopped after ${elapsed}s. state=${report.state}, total=${report.total_processed_so_far}`);
  await saveReport(report);
};

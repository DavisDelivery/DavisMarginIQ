/**
 * Davis MarginIQ — Four-Layer Ingest Module (v2.53.0)
 *
 * Phase 1 of the Foundation Rebuild. This module is the SINGLE SANCTIONED
 * write path for any data that originates from a parseable file (Gmail
 * attachment, manual upload, or future feeds).
 *
 * THE CONTRACT
 * ============
 * Every file that lands in MarginIQ is preserved at FOUR LAYERS:
 *
 *   Layer 1 — Original bytes, gzipped
 *     source_files_raw/{file_id}              parent doc with metadata
 *     source_file_chunks/{file_id}__{NNN}     base64 chunks (≤700KB each)
 *
 *   Layer 2 — Every column, verbatim
 *     {source}_rows_raw/{row_doc_id}          one doc per parsed row
 *     Carries: source_file_id, ingested_at, ingested_by, schema_version
 *
 *   Layer 3 — Normalized rows
 *     {source}_{primary}/{row_doc_id}         one doc per parsed row
 *     Carries: source_file_id, ingested_at, ingested_by, schema_version
 *
 *   Layer 4 — Aggregations (computed downstream by other functions)
 *     stop_economics, weekly_rollups, etc.
 *     Always derivable from L3, never a primary source of truth.
 *
 * WHY THIS MODULE EXISTS
 * ======================
 * Pre-v2.53.0, each ingest function reimplemented its own write path.
 * DDIS preserved every column to ddis_rows_raw. NuVizz did the same to
 * nuvizz_rows_raw. Uline DAS dropped 73% of stops via a mobile-Safari
 * timeout because the parsing happened client-side. There was no
 * source_file_id field connecting any normalized row back to its
 * Layer 1 origin file.
 *
 * Phase 1 fixes this. Every write to a normalized collection (das_lines,
 * ddis_payments, nuvizz_stops, and any future feeds like payroll_runs,
 * b600_punches, etc.) MUST go through ingestFile() in this module.
 * Direct fs:commit writes from inside ingest functions are a Phase 1
 * CI violation — see scripts/check-direct-writes.mjs.
 *
 * SCHEMA VERSIONING
 * =================
 * schema_version starts at "1.0.0" and is bumped any time the parser
 * output shape changes. Format is semver-ish:
 *   - patch (1.0.x): bug fix in parsing, no field changes
 *   - minor (1.x.0): new fields added (backward-compatible)
 *   - major (x.0.0): breaking change to existing field meaning
 *
 * USAGE
 * =====
 *   import { ingestFile } from "./lib/four-layer-ingest.js";
 *
 *   const result = await ingestFile({
 *     source: "ddis",
 *     filename: "DDIS_Payment_Vouchers_2026-04-25.xlsx",
 *     binary: xlsxBytesBuffer,
 *     parser: (bytes) => myParser(bytes),
 *     metadata: {
 *       messageId: gmailMessageId,
 *       emailDate: emailHeaderDate,
 *       account: "chad@davisdelivery.com",
 *       subject: emailSubject,
 *       schemaVersion: "1.0.0",
 *       ingestedBy: "marginiq-ddis-ingest-background@2.53.0",
 *     },
 *     apiKey: FIREBASE_API_KEY,
 *   });
 *
 *   if (!result.ok) {
 *     console.error("Ingest failed:", result.error);
 *     return;
 *   }
 *
 *   console.log(`File ${result.fileId} ingested:`,
 *     `L1=${result.layer1.chunks} chunks,`,
 *     `L2=${result.layer2.written}/${result.layer2.attempted} rows,`,
 *     `L3=${result.layer3.written}/${result.layer3.attempted} rows`);
 */

import { gzipSync as nodeGzipSync } from "node:zlib";
import { createHash } from "node:crypto";

// ─── Configuration ────────────────────────────────────────────────────────────

const PROJECT_ID = "davismarginiq";
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

const RAW_CHUNK_BUDGET = 700_000; // base64 chars per chunk (~700KB, under 1MB Firestore doc cap)
const BATCH_WRITE_LIMIT = 500;    // Firestore :commit endpoint cap
const FIRESTORE_DOC_ID_MAX = 1400; // Firestore actual cap is 1500, leave headroom

// Map source → collection names. Any source listed here is "known."
// Adding a new source is a deliberate edit to this table.
export const SOURCE_REGISTRY: Record<string, { rowsRaw: string; primary: string }> = {
  ddis: { rowsRaw: "ddis_rows_raw", primary: "ddis_payments" },
  nuvizz: { rowsRaw: "nuvizz_rows_raw", primary: "nuvizz_stops" },
  uline: { rowsRaw: "das_rows_raw", primary: "das_lines" },
  payroll: { rowsRaw: "payroll_rows_raw", primary: "payroll_runs" },
  b600: { rowsRaw: "b600_rows_raw", primary: "b600_punches" },
};

// ─── Types ────────────────────────────────────────────────────────────────────

export interface IngestMetadata {
  messageId: string;        // Gmail message id (or other source id)
  emailDate: string;        // ISO date string of source email/event
  account: string;          // email account or other source identifier
  subject?: string;         // email subject (optional)
  schemaVersion: string;    // parser output schema version, e.g. "1.0.0"
  ingestedBy: string;       // calling function id, e.g. "marginiq-ddis-ingest-background@2.53.0"
}

export interface ParsedRow {
  /** Stable doc ID for this row in BOTH L2 and L3. Required. */
  docId: string;
  /** Layer 2 representation: every column from the source file, verbatim. */
  rawFields: Record<string, any>;
  /** Layer 3 representation: parsed/typed/normalized fields. */
  normalizedFields: Record<string, any>;
}

export type ParserFn = (binary: Buffer) => Promise<ParsedRow[]> | ParsedRow[];

export interface IngestParams {
  source: string;          // "ddis" | "nuvizz" | "uline" | "payroll" | "b600" | ...
  filename: string;
  binary: Buffer;
  parser: ParserFn;
  metadata: IngestMetadata;
  apiKey: string;
  /** If true, write Layer 1 only (preserve original bytes, skip parsing). */
  layer1Only?: boolean;
}

export interface IngestResult {
  ok: boolean;
  fileId: string;
  source: string;
  layer1: { chunks: number; rawBytes: number; gzBytes: number; ok: boolean; error?: string };
  layer2: { collection: string; attempted: number; written: number; failed: number };
  layer3: { collection: string; attempted: number; written: number; failed: number };
  schemaVersion: string;
  ingestedAt: string;
  ingestedBy: string;
  duration_ms: number;
  error?: string;
}

// ─── Firestore primitives (private to this module) ───────────────────────────

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

function toFsFields(obj: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(obj)) out[k] = toFsValue(v);
  return out;
}

async function fsPatchDoc(
  collection: string,
  docId: string,
  fields: Record<string, any>,
  apiKey: string,
): Promise<boolean> {
  const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: toFsFields(fields) }),
  });
  if (!r.ok) {
    console.error(`four-layer-ingest: fsPatchDoc ${collection}/${docId} failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  }
  return r.ok;
}

async function batchWriteDocs(
  collection: string,
  docs: Array<{ docId: string; fields: any }>,
  apiKey: string,
): Promise<{ ok: number; failed: number }> {
  if (docs.length === 0) return { ok: 0, failed: 0 };
  if (docs.length > BATCH_WRITE_LIMIT) {
    let ok = 0, failed = 0;
    for (let i = 0; i < docs.length; i += BATCH_WRITE_LIMIT) {
      const r = await batchWriteDocs(collection, docs.slice(i, i + BATCH_WRITE_LIMIT), apiKey);
      ok += r.ok; failed += r.failed;
    }
    return { ok, failed };
  }
  // :commit, not :batchWrite — Firestore rules treat them differently.
  // :commit is the canonical idempotent-write endpoint. See ddis-bg.mts
  // commit history for full rationale (v2.42.16).
  const url = `${FS_BASE}:commit?key=${apiKey}`;
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
    console.error(`four-layer-ingest: commit ${collection} failed: ${resp.status} ${(await resp.text()).slice(0, 300)}`);
    return { ok: 0, failed: docs.length };
  }
  const data: any = await resp.json();
  const writeResults = data.writeResults || [];
  return { ok: writeResults.length, failed: docs.length - writeResults.length };
}

// ─── ID and shape helpers ─────────────────────────────────────────────────────

function sanitizeId(id: string): string {
  // Firestore: no '/', cannot be '.' or '..', max 1500 bytes UTF-8.
  return String(id).replace(/\//g, "_").replace(/^\.+$/, "_").slice(0, FIRESTORE_DOC_ID_MAX);
}

/**
 * Deterministic file_id from source + filename + messageId. Same inputs
 * produce the same file_id forever. This means re-ingesting the same
 * file is idempotent — Layer 1 patches itself, Layers 2/3 overwrite.
 */
function deriveFileId(source: string, filename: string, messageId: string): string {
  const cleanSource = source.replace(/[^a-z0-9_-]/gi, "_");
  const cleanFilename = filename.replace(/[^a-z0-9._-]/gi, "_").slice(0, 60);
  const hash = createHash("sha256")
    .update(`${source}|${filename}|${messageId}`)
    .digest("hex")
    .slice(0, 12);
  return sanitizeId(`${cleanSource}_${cleanFilename}_${hash}`);
}

// ─── Layer 1 writer ───────────────────────────────────────────────────────────

async function writeLayer1(
  source: string,
  fileId: string,
  filename: string,
  bytes: Buffer,
  metadata: IngestMetadata,
  apiKey: string,
): Promise<{ ok: boolean; chunks: number; rawBytes: number; gzBytes: number; error?: string }> {
  const gz = nodeGzipSync(bytes);
  const b64 = gz.toString("base64");

  const chunks: string[] = [];
  for (let i = 0; i < b64.length; i += RAW_CHUNK_BUDGET) {
    chunks.push(b64.substring(i, i + RAW_CHUNK_BUDGET));
  }

  // Parent doc
  const headerOk = await fsPatchDoc("source_files_raw", fileId, {
    file_id: fileId,
    source,
    filename,
    size_bytes: bytes.length,
    gz_bytes: gz.length,
    chunk_count: chunks.length,
    state: "staged",
    staged_at: new Date().toISOString(),
    email_message_id: metadata.messageId,
    email_subject: metadata.subject || "",
    email_account: metadata.account,
    email_date: metadata.emailDate,
    schema_version: metadata.schemaVersion,
    ingested_by: metadata.ingestedBy,
  }, apiKey);
  if (!headerOk) {
    return { ok: false, chunks: 0, rawBytes: bytes.length, gzBytes: gz.length, error: "Failed to write source_files_raw header doc" };
  }

  // Chunk docs
  let chunkErrors = 0;
  for (let i = 0; i < chunks.length; i++) {
    const chunkId = `${fileId}__${String(i).padStart(3, "0")}`;
    const ok = await fsPatchDoc("source_file_chunks", chunkId, {
      file_id: fileId,
      chunk_index: i,
      chunk_count: chunks.length,
      data_b64: chunks[i],
      created_at: new Date().toISOString(),
    }, apiKey);
    if (!ok) chunkErrors++;
  }

  if (chunkErrors > 0) {
    return { ok: false, chunks: chunks.length, rawBytes: bytes.length, gzBytes: gz.length, error: `${chunkErrors}/${chunks.length} chunks failed` };
  }
  return { ok: true, chunks: chunks.length, rawBytes: bytes.length, gzBytes: gz.length };
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Ingest a file at all four layers (well, three — L4 aggregations are
 * computed downstream). This is the ONLY function any ingest worker
 * should call to write data into MarginIQ.
 */
export async function ingestFile(params: IngestParams): Promise<IngestResult> {
  const { source, filename, binary, parser, metadata, apiKey, layer1Only } = params;
  const startMs = Date.now();
  const ingestedAt = new Date().toISOString();

  // Validate source
  const registryEntry = SOURCE_REGISTRY[source];
  if (!registryEntry) {
    return {
      ok: false,
      fileId: "",
      source,
      layer1: { chunks: 0, rawBytes: 0, gzBytes: 0, ok: false, error: "Unknown source" },
      layer2: { collection: "", attempted: 0, written: 0, failed: 0 },
      layer3: { collection: "", attempted: 0, written: 0, failed: 0 },
      schemaVersion: metadata.schemaVersion,
      ingestedAt,
      ingestedBy: metadata.ingestedBy,
      duration_ms: Date.now() - startMs,
      error: `Unknown source "${source}". Add to SOURCE_REGISTRY in lib/four-layer-ingest.ts.`,
    };
  }

  const fileId = deriveFileId(source, filename, metadata.messageId);

  // Layer 1: original bytes
  const l1 = await writeLayer1(source, fileId, filename, binary, metadata, apiKey);
  if (!l1.ok) {
    return {
      ok: false,
      fileId,
      source,
      layer1: l1,
      layer2: { collection: registryEntry.rowsRaw, attempted: 0, written: 0, failed: 0 },
      layer3: { collection: registryEntry.primary, attempted: 0, written: 0, failed: 0 },
      schemaVersion: metadata.schemaVersion,
      ingestedAt,
      ingestedBy: metadata.ingestedBy,
      duration_ms: Date.now() - startMs,
      error: `Layer 1 write failed: ${l1.error}`,
    };
  }

  if (layer1Only) {
    // Mark file complete with a no-parse note and return.
    await fsPatchDoc("source_files_raw", fileId, {
      state: "preserved_l1_only",
      l1_completed_at: new Date().toISOString(),
    }, apiKey);
    return {
      ok: true,
      fileId,
      source,
      layer1: l1,
      layer2: { collection: registryEntry.rowsRaw, attempted: 0, written: 0, failed: 0 },
      layer3: { collection: registryEntry.primary, attempted: 0, written: 0, failed: 0 },
      schemaVersion: metadata.schemaVersion,
      ingestedAt,
      ingestedBy: metadata.ingestedBy,
      duration_ms: Date.now() - startMs,
    };
  }

  // Parse
  let parsedRows: ParsedRow[];
  try {
    parsedRows = await parser(binary);
  } catch (e: any) {
    await fsPatchDoc("source_files_raw", fileId, {
      state: "parse_failed",
      parse_error: e?.message || String(e),
      parse_failed_at: new Date().toISOString(),
    }, apiKey);
    return {
      ok: false,
      fileId,
      source,
      layer1: l1,
      layer2: { collection: registryEntry.rowsRaw, attempted: 0, written: 0, failed: 0 },
      layer3: { collection: registryEntry.primary, attempted: 0, written: 0, failed: 0 },
      schemaVersion: metadata.schemaVersion,
      ingestedAt,
      ingestedBy: metadata.ingestedBy,
      duration_ms: Date.now() - startMs,
      error: `Parser threw: ${e?.message || e}`,
    };
  }

  // Provenance fields injected into every L2 + L3 doc
  const provenance = {
    source_file_id: fileId,
    ingested_at: ingestedAt,
    ingested_by: metadata.ingestedBy,
    schema_version: metadata.schemaVersion,
  };

  // Layer 2: every column verbatim
  const l2Docs = parsedRows.map(row => ({
    docId: sanitizeId(row.docId),
    fields: toFsFields({ ...row.rawFields, ...provenance }),
  }));

  const l2Result = await batchWriteDocs(registryEntry.rowsRaw, l2Docs, apiKey);

  // Layer 3: normalized
  const l3Docs = parsedRows.map(row => ({
    docId: sanitizeId(row.docId),
    fields: toFsFields({ ...row.normalizedFields, ...provenance }),
  }));

  const l3Result = await batchWriteDocs(registryEntry.primary, l3Docs, apiKey);

  // Mark file complete with summary
  const l2Failed = l2Result.failed;
  const l3Failed = l3Result.failed;
  const allOk = l2Failed === 0 && l3Failed === 0;

  await fsPatchDoc("source_files_raw", fileId, {
    state: allOk ? "ingested" : "ingested_with_errors",
    completed_at: new Date().toISOString(),
    parsed_row_count: parsedRows.length,
    l2_written: l2Result.ok,
    l2_failed: l2Failed,
    l3_written: l3Result.ok,
    l3_failed: l3Failed,
  }, apiKey);

  return {
    ok: allOk,
    fileId,
    source,
    layer1: l1,
    layer2: { collection: registryEntry.rowsRaw, attempted: l2Docs.length, written: l2Result.ok, failed: l2Failed },
    layer3: { collection: registryEntry.primary, attempted: l3Docs.length, written: l3Result.ok, failed: l3Failed },
    schemaVersion: metadata.schemaVersion,
    ingestedAt,
    ingestedBy: metadata.ingestedBy,
    duration_ms: Date.now() - startMs,
    error: allOk ? undefined : `Partial failure: L2 ${l2Failed} failed, L3 ${l3Failed} failed`,
  };
}

/**
 * Read the provenance chain for a single L3 doc.
 * Used by the marginiq-provenance endpoint and Phase 4 KPI source pills.
 */
export async function getProvenanceChain(
  collection: string,
  docId: string,
  apiKey: string,
): Promise<{
  ok: boolean;
  doc?: { collection: string; doc_id: string; source_file_id: string | null; ingested_at: string | null; ingested_by: string | null; schema_version: string | null };
  source_file?: { file_id: string; source: string; filename: string; email_message_id: string; email_account: string; email_date: string; size_bytes: number; staged_at: string; state: string };
  error?: string;
}> {
  const docUrl = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const docResp = await fetch(docUrl);
  if (!docResp.ok) {
    return { ok: false, error: `Doc ${collection}/${docId} not found (${docResp.status})` };
  }
  const docData: any = await docResp.json();
  const fields = docData.fields || {};

  const unwrap = (v: any) => {
    if (!v) return null;
    if ("stringValue" in v) return v.stringValue;
    if ("integerValue" in v) return parseInt(v.integerValue);
    if ("doubleValue" in v) return v.doubleValue;
    if ("timestampValue" in v) return v.timestampValue;
    return null;
  };

  const sourceFileId = unwrap(fields.source_file_id);
  const docSummary = {
    collection,
    doc_id: docId,
    source_file_id: sourceFileId as string | null,
    ingested_at: unwrap(fields.ingested_at) as string | null,
    ingested_by: unwrap(fields.ingested_by) as string | null,
    schema_version: unwrap(fields.schema_version) as string | null,
  };

  if (!sourceFileId) {
    return {
      ok: false,
      doc: docSummary,
      error: "No source_file_id on doc — pre-Phase-1 row, no provenance chain available",
    };
  }

  const fileUrl = `${FS_BASE}/source_files_raw/${encodeURIComponent(sourceFileId as string)}?key=${apiKey}`;
  const fileResp = await fetch(fileUrl);
  if (!fileResp.ok) {
    return {
      ok: false,
      doc: docSummary,
      error: `source_files_raw/${sourceFileId} not found (${fileResp.status})`,
    };
  }
  const fileData: any = await fileResp.json();
  const ff = fileData.fields || {};

  return {
    ok: true,
    doc: docSummary,
    source_file: {
      file_id: unwrap(ff.file_id) as string,
      source: unwrap(ff.source) as string,
      filename: unwrap(ff.filename) as string,
      email_message_id: unwrap(ff.email_message_id) as string,
      email_account: unwrap(ff.email_account) as string,
      email_date: unwrap(ff.email_date) as string,
      size_bytes: unwrap(ff.size_bytes) as number,
      staged_at: unwrap(ff.staged_at) as string,
      state: unwrap(ff.state) as string,
    },
  };
}

/**
 * Sanity helper for callers: confirm a source is registered.
 * Allows ingest functions to fail-fast at startup if their source isn't wired.
 */
export function isRegisteredSource(source: string): boolean {
  return source in SOURCE_REGISTRY;
}

// ─── Lower-level helper for split L1/L2-3 architectures ───────────────────────

/**
 * Write Layer 2 + Layer 3 rows for a file whose Layer 1 was already written
 * elsewhere (typically by an auto-ingest function that has the raw bytes,
 * with the L2/L3 work happening in a separate background worker that only
 * has parsed records).
 *
 * Use this ONLY when the architecture genuinely requires splitting L1 from
 * L2/L3 (Gmail attachment fetch is in one function, chunked-staging hand-off
 * to a bg worker, bg worker has no access to binary). For new ingest paths,
 * prefer the all-in-one ingestFile() above.
 *
 * Caller's contract:
 *   - source_files_raw/{fileId} must already exist (this function patches it
 *     with completion fields but does not create it)
 *   - Each ParsedRow must have a stable docId (used for both L2 and L3)
 *
 * Every L2 and L3 doc written by this function carries:
 *   source_file_id, ingested_at, ingested_by, schema_version
 */
export async function writeProvenancedRows(params: {
  source: string;
  fileId: string;
  rows: ParsedRow[];
  metadata: Pick<IngestMetadata, "schemaVersion" | "ingestedBy">;
  apiKey: string;
}): Promise<{
  ok: boolean;
  layer2: { collection: string; attempted: number; written: number; failed: number };
  layer3: { collection: string; attempted: number; written: number; failed: number };
  ingestedAt: string;
  error?: string;
}> {
  const { source, fileId, rows, metadata, apiKey } = params;
  const ingestedAt = new Date().toISOString();

  const registryEntry = SOURCE_REGISTRY[source];
  if (!registryEntry) {
    return {
      ok: false,
      layer2: { collection: "", attempted: 0, written: 0, failed: 0 },
      layer3: { collection: "", attempted: 0, written: 0, failed: 0 },
      ingestedAt,
      error: `Unknown source "${source}". Add to SOURCE_REGISTRY.`,
    };
  }

  const provenance = {
    source_file_id: fileId,
    ingested_at: ingestedAt,
    ingested_by: metadata.ingestedBy,
    schema_version: metadata.schemaVersion,
  };

  const l2Docs = rows.map(row => ({
    docId: sanitizeId(row.docId),
    fields: toFsFields({ ...row.rawFields, ...provenance }),
  }));
  const l3Docs = rows.map(row => ({
    docId: sanitizeId(row.docId),
    fields: toFsFields({ ...row.normalizedFields, ...provenance }),
  }));

  const l2Result = await batchWriteDocs(registryEntry.rowsRaw, l2Docs, apiKey);
  const l3Result = await batchWriteDocs(registryEntry.primary, l3Docs, apiKey);

  const allOk = l2Result.failed === 0 && l3Result.failed === 0;

  // Patch the L1 doc with completion summary
  await fsPatchDoc("source_files_raw", fileId, {
    state: allOk ? "ingested" : "ingested_with_errors",
    completed_at: new Date().toISOString(),
    parsed_row_count: rows.length,
    l2_written: l2Result.ok,
    l2_failed: l2Result.failed,
    l3_written: l3Result.ok,
    l3_failed: l3Result.failed,
  }, apiKey);

  return {
    ok: allOk,
    layer2: { collection: registryEntry.rowsRaw, attempted: l2Docs.length, written: l2Result.ok, failed: l2Result.failed },
    layer3: { collection: registryEntry.primary, attempted: l3Docs.length, written: l3Result.ok, failed: l3Result.failed },
    ingestedAt,
    error: allOk ? undefined : `Partial failure: L2 ${l2Result.failed} failed, L3 ${l3Result.failed} failed`,
  };
}

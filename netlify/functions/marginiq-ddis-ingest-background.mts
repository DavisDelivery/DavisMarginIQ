import type { Context } from "@netlify/functions";
import { gunzipSync } from "node:zlib";
import { writeProvenancedRows, type ParsedRow } from "./lib/four-layer-ingest.js";

/**
 * Davis MarginIQ — DDIS Ingest Background Worker (v2.42.13)
 *
 * Persists DDIS payment + file metadata to Firestore after the client has
 * already parsed the CSV(s).
 *
 * v2.42.13 — Chunked payload support
 *   Now supports two input modes:
 *   1. Legacy direct: { ddisFileRecords: [...], ddisPayments: [...] }
 *      Used for very small files where the dispatcher elects to skip
 *      chunking. Backward-compat with any caller still using this shape.
 *   2. Chunked: { run_id: "ddis_srv_..." }
 *      Used by the v2.42.13 dispatcher and the auto-ingest path. Reads
 *      gzipped chunks from ddis_ingest_payloads/{run_id}__NNN, decodes
 *      them, processes, and deletes the chunks at the end.
 *
 * Why background (unchanged from v2.40.17):
 *   - A DDIS CSV commonly has ~3000 payment rows. The old client flow wrote
 *     ddis_payments 25 at a time over the Firebase JS SDK from a mobile
 *     browser → ~120 sequential round trips → minutes of stall and occasional
 *     outright hangs. REST batchWrite takes 500/call from Netlify's egress.
 *   - Background functions get ~15 min vs the 10s sync budget, so we're safe
 *     if a user uploads a full quarter of DDIS files at once.
 *
 * Input (JSON POST body from marginiq-ddis-ingest):
 *   {
 *     ddisFileRecords: Array<{
 *       file_id: string, filename: string, record_count: number,
 *       total_paid: number, earliest_bill_date: string | null,
 *       latest_bill_date: string | null, bill_week_ending: string | null,
 *       week_ambiguous: boolean, ambiguous_candidates: string[],
 *       top5_bill_dates: string[], covers_weeks: string[],
 *       checks: string[], uploaded_at: string,
 *     }>,
 *     ddisPayments: Array<{
 *       id: string, pro: string, paid_amount: number,
 *       bill_date: string | null, check: string | null,
 *       voucher: string | null, source_file: string, uploaded_at: string,
 *     }>,
 *   }
 *
 * Status doc schema (marginiq_config/ddis_ingest_status):
 *   state:           "running" | "complete" | "failed"
 *   started_at:      ISO timestamp
 *   completed_at:    ISO timestamp (when state != "running")
 *   phase:           "writing_files" | "writing_payments" | "done"
 *   progress_text:   human string shown in the UI
 *   file_records_in: integer  (records received)
 *   file_records_ok: integer  (records written)
 *   payment_rows_in: integer  (rows received)
 *   payment_rows_ok: integer  (rows written)
 *   failed_writes:   integer
 *   error:           string | null
 *
 * Env vars:
 *   FIREBASE_API_KEY  — Firestore REST
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

// Hard cap on payments we'll write per ingest. Mirrors the v2.5 client cap of
// 10,000 so behavior stays consistent if someone uploads a year at once.
const PAYMENT_CAP = 10000;

const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

// v2.42.13 — Chunked-payload reader. Loads all
// ddis_ingest_payloads/{run_id}__NNN chunks staged by the dispatcher,
// decodes the gzipped base64 content, and returns the concatenated
// ddisFileRecords[] + ddisPayments[]. Returns chunkDocIds so the caller
// can delete the chunks after successful processing.
async function loadStagedPayload(runId: string): Promise<{
  ddisFileRecords: any[];
  ddisPayments: any[];
  chunkDocIds: string[];
}> {
  const ddisFileRecords: any[] = [];
  const ddisPayments: any[] = [];
  const chunkDocIds: string[] = [];

  let pageToken: string | undefined;
  const prefix = `${runId}__`;
  let safety = 0;
  do {
    const params = new URLSearchParams({ key: FIREBASE_API_KEY || "", pageSize: "50" });
    if (pageToken) params.set("pageToken", pageToken);
    const url = `${FS_BASE}/ddis_ingest_payloads?${params.toString()}`;
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
      const b64: string = f.data_b64?.stringValue || "";
      if (!b64) continue;
      const gz = Buffer.from(b64, "base64");
      const raw = gunzipSync(gz).toString("utf8");
      const chunkData = JSON.parse(raw);
      if (Array.isArray(chunkData.ddisFileRecords)) {
        for (const fr of chunkData.ddisFileRecords) ddisFileRecords.push(fr);
      }
      if (Array.isArray(chunkData.ddisPayments)) {
        for (const p of chunkData.ddisPayments) ddisPayments.push(p);
      }
    }
    pageToken = data.nextPageToken;
    safety++;
    if (safety > 200) break;
  } while (pageToken);

  chunkDocIds.sort();
  return { ddisFileRecords, ddisPayments, chunkDocIds };
}

async function deleteStagedChunks(chunkDocIds: string[]): Promise<{ ok: number; failed: number }> {
  let ok = 0, failed = 0;
  const CONC = 10;
  for (let i = 0; i < chunkDocIds.length; i += CONC) {
    const batch = chunkDocIds.slice(i, i + CONC);
    const results = await Promise.all(batch.map(async (id) => {
      const url = `${FS_BASE}/ddis_ingest_payloads/${encodeURIComponent(id)}?key=${FIREBASE_API_KEY}`;
      const r = await fetch(url, { method: "DELETE" });
      return r.ok;
    }));
    for (const r of results) {
      if (r) ok++; else failed++;
    }
  }
  return { ok, failed };
}

// v2.53.0 Phase 1 — read run header doc carrying provenance from dispatcher.
// Returns null if no header was written (legacy callers).
interface RunHeader {
  source_file_id: string;
  email_message_id: string;
  email_account: string;
  email_date: string;
  email_subject: string;
  schema_version: string;
}
async function loadRunHeader(runId: string): Promise<RunHeader | null> {
  if (!runId) return null;
  const url = `${FS_BASE}/ddis_ingest_runs/${encodeURIComponent(runId)}?key=${FIREBASE_API_KEY}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  const data: any = await r.json();
  const f = data.fields || {};
  const get = (k: string) => f[k]?.stringValue || "";
  const sourceFileId = get("source_file_id");
  if (!sourceFileId) return null;
  return {
    source_file_id: sourceFileId,
    email_message_id: get("email_message_id"),
    email_account: get("email_account"),
    email_date: get("email_date"),
    email_subject: get("email_subject"),
    schema_version: get("schema_version") || "1.0.0",
  };
}

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
  // v2.42.16: switched from :batchWrite to :commit because the public web
  // API key + missing collection rule combination produces PERMISSION_DENIED
  // on :batchWrite but works fine on :commit (Firestore rules treat the two
  // endpoints differently — :commit is idempotent-write-friendly, :batchWrite
  // requires explicit allow rules even for collections that PATCH can hit
  // unauthenticated). NuVizz BG has been using :commit successfully since
  // v2.40.34; aligning DDIS to match.
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
    console.error(`commit ${collection} failed: ${resp.status} ${await resp.text()}`);
    return { ok: 0, failed: docs.length };
  }
  const data: any = await resp.json();
  // :commit is transactional — either all writes succeed (writeResults filled)
  // or the whole call fails with non-200 (handled above). No partial-failure
  // status array like :batchWrite has.
  const writeResults = data.writeResults || [];
  return { ok: writeResults.length, failed: docs.length - writeResults.length };
}

// ─── Status writer ──────────────────────────────────────────────────

async function writeStatus(patch: Record<string, any>): Promise<void> {
  await patchDoc("marginiq_config", "ddis_ingest_status", patch);
}

// ─── Doc shaping ───────────────────────────────────────────────────
//
// The client already sends plain JSON objects matching the Firestore doc
// shape for each collection. We just need to convert each object into the
// Firestore REST "fields" map via toFsValue, plus sanitize the docId.

function sanitizeDocId(id: string): string {
  // Firestore doc IDs: cannot contain '/', cannot be '.' or '..', max 1500
  // bytes UTF-8. Keep generous — DDIS ids like "5479236_2025-10-31_12345"
  // should pass straight through, but defend against weird filenames.
  return String(id).replace(/\//g, "_").replace(/^\.+$/, "_").slice(0, 1400);
}

function shapeFileDoc(rec: any): { docId: string; fields: any } {
  const docId = sanitizeDocId(rec.file_id || rec.filename || `file_${Date.now()}`);
  // Drop file_id from the fields — it's the docId. Everything else passes
  // through as-is.
  const { file_id: _unused, ...rest } = rec;
  return { docId, fields: toFsValue(rest).mapValue.fields };
}

function shapePaymentDoc(p: any): { docId: string; fields: any } {
  const docId = sanitizeDocId(p.id || `${p.pro}_${p.bill_date || "nodate"}_${p.check || "nocheck"}`);
  const { id: _unused, ...rest } = p;
  return { docId, fields: toFsValue(rest).mapValue.fields };
}

// ─── Main ingest ───────────────────────────────────────────────────

async function ingest(
  ddisFileRecords: any[],
  ddisPayments: any[],
  runHeader: RunHeader | null,
): Promise<{ ok: true; fileOk: number; paymentOk: number; failed: number } | { ok: false; error: string }> {
  const startedAt = new Date().toISOString();

  // Enforce the same 10K cap the client used pre-v2.40.17 so a runaway upload
  // can't chew through the Firestore daily write quota.
  const cappedPayments = ddisPayments.slice(0, PAYMENT_CAP);
  const droppedPayments = ddisPayments.length - cappedPayments.length;

  await writeStatus({
    state: "running",
    started_at: startedAt,
    completed_at: null,
    phase: "writing_files",
    progress_text: `Writing ${ddisFileRecords.length} DDIS file record(s)…`,
    file_records_in: ddisFileRecords.length,
    file_records_ok: 0,
    payment_rows_in: cappedPayments.length,
    payment_rows_ok: 0,
    failed_writes: 0,
    payment_rows_dropped: droppedPayments,
    error: null,
  });

  try {
    // 1) ddis_files (DDIS-specific metadata, NOT a provenanced collection —
    //    it's not in SOURCE_REGISTRY and isn't subject to the four-layer rule)
    const fileDocs = ddisFileRecords.map(shapeFileDoc);
    const fileResult = await batchWriteDocs("ddis_files", fileDocs);
    console.log(`ddis-ingest: wrote ${fileResult.ok}/${fileDocs.length} ddis_files (${fileResult.failed} failed)`);

    await writeStatus({
      phase: "writing_payments",
      progress_text: `Writing ${cappedPayments.length.toLocaleString()} DDIS payment row(s)…`,
      file_records_ok: fileResult.ok,
      failed_writes: fileResult.failed,
    });

    // 2) v2.53.0 Phase 1 — sanctioned write path via lib/four-layer-ingest.
    //    Every L2/L3 row carries source_file_id + provenance. If the run
    //    header is missing, fail loudly. The auto-ingest path always writes
    //    the run header in the dispatcher; any caller without one is a
    //    contract violation.
    if (!runHeader || !runHeader.source_file_id) {
      const err = "ddis-ingest: missing run header / source_file_id (Phase 1 contract violation)";
      console.error(err);
      await writeStatus({
        state: "failed",
        completed_at: new Date().toISOString(),
        phase: "done",
        progress_text: "✗ Refused write: caller did not provide a run header. See lib/four-layer-ingest.ts.",
        error: err,
      });
      return { ok: false, error: err };
    }

    console.log(`ddis-ingest: using four-layer module with source_file_id=${runHeader.source_file_id}`);
    const rows: ParsedRow[] = cappedPayments
      .filter(p => p.pro && p.paid_amount > 0)
      .map(p => ({
        docId: p.id || `${p.pro}_${p.bill_date || "nodate"}_${p.check || "nocheck"}`,
        rawFields: {
          pro: p.pro,
          bill_date: p.bill_date || null,
          paid_amount: p.paid_amount || 0,
          raw: p.raw || {},
        },
        normalizedFields: {
          pro: p.pro,
          paid_amount: p.paid_amount,
          bill_date: p.bill_date || null,
          check: p.check || null,
          voucher: p.voucher || null,
          source_file: p.source_file || null,
          uploaded_at: p.uploaded_at,
        },
      }));

    const result = await writeProvenancedRows({
      source: "ddis",
      fileId: runHeader.source_file_id,
      rows,
      metadata: {
        schemaVersion: runHeader.schema_version || "1.0.0",
        ingestedBy: "marginiq-ddis-ingest-background@2.53.0",
      },
      apiKey: FIREBASE_API_KEY!,
    });

    const payOk = result.layer3.written;
    const rawWritten = result.layer2.written;
    console.log(`ddis-ingest: four-layer result — L2=${rawWritten}/${result.layer2.attempted}, L3=${payOk}/${result.layer3.attempted}, ok=${result.ok}`);

    const totalFailed = fileResult.failed + (cappedPayments.length - payOk);
    const totalAttempted = fileDocs.length + cappedPayments.length;
    const totalOk = fileResult.ok + payOk;
    const droppedNote = droppedPayments > 0 ? ` · ${droppedPayments} payment rows dropped (over ${PAYMENT_CAP} cap)` : "";

    const allFailed = totalAttempted > 0 && totalOk === 0;
    const finalState = allFailed ? "failed" : "complete";
    const progressText = allFailed
      ? `✗ All ${totalAttempted} writes failed — check Firestore rules`
      : `✓ Wrote ${fileResult.ok} file record(s) + ${payOk.toLocaleString()} payment row(s)${droppedNote} · provenanced via four-layer module`;

    await writeStatus({
      state: finalState,
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: progressText,
      file_records_ok: fileResult.ok,
      payment_rows_ok: payOk,
      failed_writes: totalFailed,
      error: allFailed ? "All writes failed (likely Firestore rules issue)" : null,
    });

    if (allFailed) {
      return {
        ok: false,
        error: "All writes failed (likely Firestore rules issue)",
      };
    }
    return {
      ok: true,
      fileOk: fileResult.ok,
      paymentOk: payOk,
      failed: totalFailed,
    };
  } catch (e: any) {
    const msg = e?.message || String(e);
    console.error("ddis-ingest FAILED:", msg);
    await writeStatus({
      state: "failed",
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: `✗ DDIS ingest failed: ${msg}`,
      error: msg,
    }).catch(() => {});
    return { ok: false, error: msg };
  }
}

// ─── Entry point ────────────────────────────────────────────────────

export default async (req: Request, _context: Context) => {
  if (!FIREBASE_API_KEY) {
    console.error("ddis-ingest-background: missing FIREBASE_API_KEY env var");
    return;
  }

  let body: any = {};
  try {
    body = await req.json();
  } catch (e: any) {
    console.error("ddis-ingest-background: bad JSON body", e?.message || String(e));
    await writeStatus({
      state: "failed",
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: `✗ Bad JSON body from dispatcher: ${e?.message || String(e)}`,
      error: String(e?.message || e),
    }).catch(() => {});
    return;
  }

  let ddisFileRecords: any[] = Array.isArray(body.ddisFileRecords) ? body.ddisFileRecords : [];
  let ddisPayments: any[] = Array.isArray(body.ddisPayments) ? body.ddisPayments : [];
  let chunkDocIds: string[] = [];
  let runId: string = body.run_id || "";

  // v2.42.13: chunked-payload mode. If body has only run_id (no inline arrays),
  // load the staged chunks from Firestore.
  if ((ddisFileRecords.length === 0 && ddisPayments.length === 0) && runId) {
    try {
      const staged = await loadStagedPayload(runId);
      ddisFileRecords = staged.ddisFileRecords;
      ddisPayments = staged.ddisPayments;
      chunkDocIds = staged.chunkDocIds;
      console.log(`ddis-ingest-background: loaded ${ddisFileRecords.length} files + ${ddisPayments.length} payments from ${chunkDocIds.length} chunks (run_id=${runId})`);
    } catch (e: any) {
      console.error(`ddis-ingest-background: chunk load failed: ${e?.message || String(e)}`);
      await writeStatus({
        state: "failed",
        completed_at: new Date().toISOString(),
        phase: "done",
        progress_text: `✗ Chunk load failed: ${e?.message || String(e)}`,
        error: String(e?.message || e),
      }).catch(() => {});
      return;
    }
  }

  const t0 = Date.now();
  // v2.53.0 Phase 1 — load the run header (provenance) doc if present
  const runHeader = await loadRunHeader(runId);
  if (runHeader) {
    console.log(`ddis-ingest-background: run_header loaded — source_file_id=${runHeader.source_file_id}`);
  } else if (runId) {
    console.warn(`ddis-ingest-background: no run_header for run_id=${runId} (legacy caller — rows will be written via legacy path)`);
  }

  console.log(`ddis-ingest-background: start — ${ddisFileRecords.length} files, ${ddisPayments.length} payments`);
  const r = await ingest(ddisFileRecords, ddisPayments, runHeader);
  const elapsed = Math.round((Date.now() - t0) / 1000);
  if (r.ok) {
    console.log(`ddis-ingest-background: done in ${elapsed}s — ${r.fileOk} files, ${r.paymentOk} payments, ${r.failed} failed`);
  } else {
    console.error(`ddis-ingest-background: FAILED in ${elapsed}s — ${(r as any).error}`);
  }

  // v2.42.13: clean up staged chunks after successful ingest. Best-effort
  // — orphan chunks are harmless and a future cleanup pass can remove them.
  if (chunkDocIds.length > 0) {
    const cleanup = await deleteStagedChunks(chunkDocIds);
    console.log(`ddis-ingest-background: cleanup deleted ${cleanup.ok}/${chunkDocIds.length} chunks (${cleanup.failed} failed)`);
  }
};

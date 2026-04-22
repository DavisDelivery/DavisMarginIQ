import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — DDIS Ingest Background Worker (v2.40.17)
 *
 * Persists DDIS payment + file metadata to Firestore after the client has
 * already parsed the CSV(s). Client POSTs the parsed arrays to the dispatcher
 * marginiq-ddis-ingest; the dispatcher forwards them here and returns 202.
 *
 * Why background:
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
  // v2.40.15 counter fix: ground truth is writeResults.length; status[] is
  // populated only on failures. See marginiq-audit-rebuild-background.mts for
  // the full write-up.
  const writeResults = data.writeResults || [];
  const statuses = data.status || [];
  let explicitFailed = 0;
  for (const s of statuses) {
    if (s && s.code && s.code !== 0) explicitFailed++;
  }
  if (writeResults.length > 0) {
    return { ok: writeResults.length - explicitFailed, failed: explicitFailed };
  }
  if (statuses.length === 0) return { ok: docs.length, failed: 0 };
  let okFromStatus = 0;
  for (const s of statuses) {
    if (!s || !s.code || s.code === 0) okFromStatus++;
  }
  return { ok: okFromStatus, failed: statuses.length - okFromStatus };
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
    // 1) ddis_files (usually 1–10 rows, one batch is plenty)
    const fileDocs = ddisFileRecords.map(shapeFileDoc);
    const fileResult = await batchWriteDocs("ddis_files", fileDocs);
    console.log(`ddis-ingest: wrote ${fileResult.ok}/${fileDocs.length} ddis_files (${fileResult.failed} failed)`);

    await writeStatus({
      phase: "writing_payments",
      progress_text: `Writing ${cappedPayments.length.toLocaleString()} DDIS payment row(s)…`,
      file_records_ok: fileResult.ok,
      failed_writes: fileResult.failed,
    });

    // 2) ddis_payments — batched 500/call by batchWriteDocs automatically
    const paymentDocs = cappedPayments.map(shapePaymentDoc);
    const payResult = await batchWriteDocs("ddis_payments", paymentDocs);
    console.log(`ddis-ingest: wrote ${payResult.ok}/${paymentDocs.length} ddis_payments (${payResult.failed} failed)`);

    const totalFailed = fileResult.failed + payResult.failed;
    const droppedNote = droppedPayments > 0 ? ` · ${droppedPayments} payment rows dropped (over ${PAYMENT_CAP} cap)` : "";

    await writeStatus({
      state: "complete",
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: `✓ Wrote ${fileResult.ok} file record(s) + ${payResult.ok.toLocaleString()} payment row(s)${droppedNote}`,
      file_records_ok: fileResult.ok,
      payment_rows_ok: payResult.ok,
      failed_writes: totalFailed,
    });

    return {
      ok: true,
      fileOk: fileResult.ok,
      paymentOk: payResult.ok,
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

  const ddisFileRecords = Array.isArray(body.ddisFileRecords) ? body.ddisFileRecords : [];
  const ddisPayments = Array.isArray(body.ddisPayments) ? body.ddisPayments : [];

  const t0 = Date.now();
  console.log(`ddis-ingest-background: start — ${ddisFileRecords.length} files, ${ddisPayments.length} payments`);
  const r = await ingest(ddisFileRecords, ddisPayments);
  const elapsed = Math.round((Date.now() - t0) / 1000);
  if (r.ok) {
    console.log(`ddis-ingest-background: done in ${elapsed}s — ${r.fileOk} files, ${r.paymentOk} payments, ${r.failed} failed`);
  } else {
    console.error(`ddis-ingest-background: FAILED in ${elapsed}s — ${(r as any).error}`);
  }
};

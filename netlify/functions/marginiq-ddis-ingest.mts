import type { Context } from "@netlify/functions";
import { gzipSync } from "node:zlib";

/**
 * Davis MarginIQ — DDIS Ingest Dispatcher (v2.42.13)
 *
 * Moves DDIS persistence off the mobile browser. Client still parses the
 * CSV(s) in-browser (fast, ~1s for a 3000-row file), then POSTs the already-
 * parsed ddisFileRecords[] + ddisPayments[] payload to this dispatcher.
 *
 * v2.42.13 — Chunked-payload fix
 *   Identical to the v2.41.17 fix for the NuVizz dispatcher. The original
 *   v2.40.17 dispatcher fired the BG with the full payload as the request
 *   body via plain fetch(). For any DDIS file > ~800 payments (~256 KB
 *   serialized), AWS Lambda's async-invocation body limit silently
 *   rejected the BG fetch — the dispatcher returned 202 'queued' but the
 *   BG never started. End-to-end testing of the new auto-ingest function
 *   surfaced this: an auto-ingest of a 3,302-payment file reported success
 *   but the data never landed in Firestore.
 *
 * Fix: stage the payload to Firestore as gzipped+base64 chunks
 * (collection: ddis_ingest_payloads, doc IDs: {run_id}__NNN) BEFORE
 * firing the BG. Each chunk is sized to fit comfortably under
 * Firestore's 1 MiB doc cap. Then fire the BG with just { run_id } —
 * tiny ~100-byte payload, well under the 256 KB Lambda limit. BG reads
 * chunks back, processes them, and deletes the staged chunks at the end.
 *
 * Two endpoints on one function:
 *   POST /.netlify/functions/marginiq-ddis-ingest
 *        body: { ddisFileRecords: [...], ddisPayments: [...] }
 *     → Stages chunks, fires BG with run_id, returns 202 with run_id.
 *   GET  /.netlify/functions/marginiq-ddis-ingest?action=status
 *     → Reads marginiq_config/ddis_ingest_status (last completed run).
 *
 * Backward compat: BG still accepts the legacy { ddisFileRecords,
 * ddisPayments } shape via direct body. New chunked path used when the
 * BG receives only { run_id }.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

// Target chunk size: 700 KB raw JSON, gzips to ~150 KB, well under
// Firestore's 1 MiB doc cap. A typical DDIS file (~3K payments, ~700 KB
// JSON) ends up in 1 chunk. Larger files split into multiple chunks.
const RAW_CHUNK_BUDGET = 700_000;

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

async function readStatus(): Promise<any | null> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/ddis_ingest_status?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url);
  if (resp.status === 404) return null;
  if (!resp.ok) throw new Error(`status read failed: HTTP ${resp.status}`);
  const data: any = await resp.json();
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(data.fields || {})) out[k] = fromFsValue(v);
  return out;
}

// v2.42.13: Stage the dispatcher's payload to Firestore as gzipped chunks
// before firing the BG, then fire the BG with just { run_id }. This works
// around AWS Lambda's 256 KB async-invocation body limit that silently
// killed any DDIS file > ~800 payments.
async function writePayloadChunk(
  runId: string,
  chunkIndex: number,
  chunkCount: number,
  ddisFileRecords: any[],
  ddisPayments: any[],
): Promise<boolean> {
  const docId = `${runId}__${String(chunkIndex).padStart(3, "0")}`;
  // v2.53.1 — append updateMask.fieldPaths per key so PATCH is partial-merge,
  // not full-doc replace. Doc is unique per chunk so first-write semantics are
  // unchanged; fix added for consistency with sibling functions.
  const fieldNames = [
    "run_id", "chunk_index", "chunk_count",
    "file_records_in", "payments_in",
    "raw_bytes", "gz_bytes", "data_b64", "created_at",
  ];
  const params = new URLSearchParams();
  params.set("key", FIREBASE_API_KEY || "");
  for (const k of fieldNames) params.append("updateMask.fieldPaths", k);
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/ddis_ingest_payloads/${docId}?${params.toString()}`;
  const raw = JSON.stringify({ ddisFileRecords, ddisPayments });
  const gz = gzipSync(Buffer.from(raw, "utf8"));
  const b64 = gz.toString("base64");
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      fields: {
        run_id:           { stringValue: runId },
        chunk_index:      { integerValue: String(chunkIndex) },
        chunk_count:      { integerValue: String(chunkCount) },
        file_records_in:  { integerValue: String(ddisFileRecords.length) },
        payments_in:      { integerValue: String(ddisPayments.length) },
        raw_bytes:        { integerValue: String(raw.length) },
        gz_bytes:         { integerValue: String(gz.length) },
        data_b64:         { stringValue: b64 },
        created_at:       { stringValue: new Date().toISOString() },
      }
    }),
  });
  return resp.ok;
}

// Chunk the payments array so each chunk's raw JSON stays under
// RAW_CHUNK_BUDGET. ddisFileRecords (typically 1 entry) goes with chunk 0.
function chunkPayload(
  ddisFileRecords: any[],
  ddisPayments: any[],
): { fileRecords: any[]; payments: any[] }[] {
  const chunks: { fileRecords: any[]; payments: any[] }[] = [];
  let current: any[] = [];
  let currentSize = JSON.stringify(ddisFileRecords).length + 100;
  for (const p of ddisPayments) {
    const pJson = JSON.stringify(p);
    if (currentSize + pJson.length + 1 > RAW_CHUNK_BUDGET && current.length > 0) {
      chunks.push({
        fileRecords: chunks.length === 0 ? ddisFileRecords : [],
        payments: current,
      });
      current = [];
      currentSize = 100;
    }
    current.push(p);
    currentSize += pJson.length + 1;
  }
  if (current.length > 0 || ddisFileRecords.length > 0) {
    chunks.push({
      fileRecords: chunks.length === 0 ? ddisFileRecords : [],
      payments: current,
    });
  }
  return chunks;
}

export default async (req: Request, context: Context) => {
  if (!FIREBASE_API_KEY) {
    return new Response(JSON.stringify({ error: "FIREBASE_API_KEY not configured" }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }

  const url = new URL(req.url);
  const action = url.searchParams.get("action");

  if (action === "status") {
    try {
      const status = await readStatus();
      return new Response(JSON.stringify({ ok: true, status }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    } catch (e: any) {
      return new Response(JSON.stringify({ error: e?.message || String(e) }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  }

  let body: any = {};
  try {
    body = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "POST body must be JSON" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const ddisFileRecords = Array.isArray(body.ddisFileRecords) ? body.ddisFileRecords : [];
  const ddisPayments = Array.isArray(body.ddisPayments) ? body.ddisPayments : [];
  // v2.53.0 Phase 1 — provenance plumbing
  const sourceFileId: string = typeof body.source_file_id === "string" ? body.source_file_id : "";
  const metadata = body.metadata || {};
  if (ddisFileRecords.length === 0 && ddisPayments.length === 0) {
    return new Response(JSON.stringify({ error: "nothing to ingest — both arrays empty" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  // v2.42.13: stage the payload to Firestore in chunks so the BG fetch
  // payload stays under 256 KB.
  const runId = `ddis_srv_${new Date().toISOString().replace(/[:.]/g, "-")}`;

  // v2.53.0 Phase 1 — write a run header doc carrying provenance fields.
  // The BG worker reads this on entry to know the source_file_id for the
  // L2/L3 writes via writeProvenancedRows().
  if (sourceFileId) {
    // v2.53.1 — append updateMask.fieldPaths per key. Doc is unique per run so
    // first-write semantics are unchanged; fix for consistency.
    const headerFieldNames = [
      "run_id", "source_file_id", "email_message_id",
      "email_account", "email_date", "email_subject",
      "schema_version", "created_at",
    ];
    const headerParams = new URLSearchParams();
    headerParams.set("key", FIREBASE_API_KEY || "");
    for (const k of headerFieldNames) headerParams.append("updateMask.fieldPaths", k);
    const headerUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/ddis_ingest_runs/${runId}?${headerParams.toString()}`;
    await fetch(headerUrl, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        fields: {
          run_id:           { stringValue: runId },
          source_file_id:   { stringValue: sourceFileId },
          email_message_id: { stringValue: metadata.messageId || "" },
          email_account:    { stringValue: metadata.account || "" },
          email_date:       { stringValue: metadata.emailDate || "" },
          email_subject:    { stringValue: metadata.subject || "" },
          schema_version:   { stringValue: "1.0.0" },
          created_at:       { stringValue: new Date().toISOString() },
        },
      }),
    }).catch(e => console.error("ddis-ingest dispatcher: failed to write run header", e?.message || e));
  }

  const chunks = chunkPayload(ddisFileRecords, ddisPayments);
  let chunkWriteErrors = 0;
  for (let i = 0; i < chunks.length; i++) {
    const ok = await writePayloadChunk(runId, i, chunks.length, chunks[i].fileRecords, chunks[i].payments);
    if (!ok) chunkWriteErrors++;
  }
  if (chunkWriteErrors > 0) {
    return new Response(JSON.stringify({
      error: `Failed to stage ${chunkWriteErrors} of ${chunks.length} payload chunks`,
      run_id: runId,
    }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }

  // Fire BG with a tiny payload — just the run_id. The BG reads the
  // staged chunks back from Firestore.
  //
  // v2.42.17: wrap in context.waitUntil so Lambda keeps the runtime alive
  // long enough for the BG-fire fetch to complete its handoff. Bare
  // fire-and-forget `fetch().catch()` was being killed when the parent
  // function returned 202 — the BG never received the trigger. Same fix
  // we used in the NuVizz dispatcher (v2.41.17). The trigger payload
  // here is ~80 bytes (just { run_id }) so it's well under Lambda's
  // 256 KB async-invocation body limit.
  const bgUrl = `${url.origin}/.netlify/functions/marginiq-ddis-ingest-background`;
  context.waitUntil(
    fetch(bgUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ run_id: runId }),
    }).catch((e) => {
      console.error("ddis-ingest dispatch: background fetch failed", e?.message || String(e));
    })
  );

  return new Response(
    JSON.stringify({
      ok: true,
      queued: true,
      run_id: runId,
      file_records: ddisFileRecords.length,
      payment_rows: ddisPayments.length,
      chunk_count: chunks.length,
      message: "DDIS ingest queued. Poll ?action=status for progress.",
      dispatched_at: new Date().toISOString(),
    }),
    {
      status: 202,
      headers: { "Content-Type": "application/json" },
    },
  );
};

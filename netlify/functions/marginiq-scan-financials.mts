import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — Audited Financials PDF scanner DISPATCHER (v2.49.3).
 *
 * v2.49.3 fix: chunk the messages payload into scan_job_payloads/{jobId}__NNN
 * docs. The previous single-doc approach hit Firestore's 1MiB doc-field cap
 * the moment a real multi-page CPA PDF (3-7MB base64) was posted. Same
 * pattern as marginiq-nuvizz-ingest (which solved the same class of problem
 * for stops payloads).
 *
 * Modes:
 *   POST                                 → dispatch job, returns {job_id} 202
 *   GET ?action=status&job_id=…          → poll job state
 *   GET ?action=health                   → smoke test (no Anthropic call)
 *
 * Payload chunking:
 *   - Target raw chunk size: 700KB (gives ~30% headroom under 1MiB doc cap
 *     even after Firestore field-name overhead)
 *   - Doc IDs: scan_job_payloads/{jobId}__001, __002, ... (zero-padded for
 *     lexicographic ordering on read-back)
 *   - Each chunk doc: {job_id, chunk_index, chunk_count, data_b64}
 *   - On success the worker deletes the chunks; on failure they remain
 *     for inspection and a future cleanup pass can remove them.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

// Per-chunk raw-bytes budget. Firestore's hard cap is ~1 MiB per doc. We
// pad in 30%+ headroom for field-name overhead and the encoding wrapper.
const CHUNK_BUDGET_BYTES = 700_000;

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
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

async function readFsDoc(path: string): Promise<any | null> {
  const url = `${BASE}/${path}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url);
  if (!resp.ok) return null;
  const data: any = await resp.json();
  const f = data.fields || {};
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(f)) {
    const vv = v as any;
    if ("stringValue" in vv) out[k] = vv.stringValue;
    else if ("integerValue" in vv) out[k] = Number(vv.integerValue);
    else if ("doubleValue" in vv) out[k] = vv.doubleValue;
    else if ("booleanValue" in vv) out[k] = vv.booleanValue;
  }
  return out;
}

async function writeFsDoc(path: string, fields: Record<string, any>): Promise<{ ok: boolean; status: number; body?: string }> {
  const url = `${BASE}/${path}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: toFsFields(fields) }),
  });
  if (!resp.ok) {
    const body = await resp.text();
    return { ok: false, status: resp.status, body: body.slice(0, 500) };
  }
  return { ok: true, status: resp.status };
}

// Split a base64 string into chunks of CHUNK_BUDGET_BYTES each. Returns
// an array of substrings preserving order. Base64 is safe to split at
// arbitrary positions; we don't need to align to 4-char boundaries
// because the worker concatenates back before decoding.
function chunkString(s: string, max: number): string[] {
  const out: string[] = [];
  for (let i = 0; i < s.length; i += max) {
    out.push(s.slice(i, i + max));
  }
  return out;
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  const action = url.searchParams.get("action");

  // ── Health check ─────────────────────────────────────────────────────────
  if (req.method === "GET" && action === "health") {
    return json({
      ok: true,
      version: "2.49.3",
      firebase_configured: !!FIREBASE_API_KEY,
      anthropic_configured: !!process.env["ANTHROPIC_API_KEY"],
    });
  }

  // ── Status poll ──────────────────────────────────────────────────────────
  if (req.method === "GET" && action === "status") {
    const jobId = url.searchParams.get("job_id") || "";
    if (!jobId) return json({ error: "job_id required" }, 400);
    const doc = await readFsDoc(`scan_jobs/${jobId}`);
    if (!doc) return json({ error: "Job not found", job_id: jobId }, 404);
    return json(doc);
  }

  if (req.method !== "POST") return json({ error: "POST or GET with ?action= required" }, 405);
  if (!FIREBASE_API_KEY) return json({ error: "FIREBASE_API_KEY not configured" }, 500);

  // ── Dispatch new scan job ────────────────────────────────────────────────
  let body: any;
  try { body = await req.json(); }
  catch { return json({ error: "Invalid JSON body" }, 400); }

  const messages = body.messages;
  if (!Array.isArray(messages) || messages.length === 0) {
    return json({ error: "messages array required" }, 400);
  }
  const maxTokens = Math.min(body.max_tokens || 8192, 8192);

  const jobId = `scan_${new Date().toISOString().replace(/[:.]/g, "-")}_${Math.random().toString(36).slice(2, 8)}`;

  // Stage payload — chunked to fit Firestore's 1MiB doc cap
  const messagesB64 = Buffer.from(JSON.stringify(messages), "utf8").toString("base64");
  const chunks = chunkString(messagesB64, CHUNK_BUDGET_BYTES);
  const chunkCount = chunks.length;

  console.log(`[scan-dispatch] jobId=${jobId} payload=${messagesB64.length}b in ${chunkCount} chunk(s)`);

  for (let i = 0; i < chunks.length; i++) {
    const docId = `${jobId}__${String(i).padStart(3, "0")}`;
    const writeResult = await writeFsDoc(`scan_job_payloads/${docId}`, {
      job_id: jobId,
      chunk_index: i,
      chunk_count: chunkCount,
      data_b64: chunks[i],
      created_at: new Date().toISOString(),
    });
    if (!writeResult.ok) {
      // Surface the actual Firestore error so the client can see what failed
      console.error(`[scan-dispatch] chunk ${i+1}/${chunkCount} write failed: ${writeResult.status} ${writeResult.body}`);
      return json({
        error: `Failed to stage scan payload chunk ${i+1}/${chunkCount} to Firestore (${writeResult.status})`,
        firestore_error: writeResult.body,
      }, 500);
    }
  }

  // Status doc — small, fits comfortably in single doc
  const statusWrite = await writeFsDoc(`scan_jobs/${jobId}`, {
    job_id: jobId,
    state: "queued",
    queued_at: new Date().toISOString(),
    max_tokens: maxTokens,
    chunk_count: chunkCount,
    payload_bytes: messagesB64.length,
  });
  if (!statusWrite.ok) {
    return json({ error: `Failed to create status doc (${statusWrite.status})`, firestore_error: statusWrite.body }, 500);
  }

  // Fire background worker — passes only jobId; worker reassembles chunks.
  const bgUrl = `${url.origin}/.netlify/functions/marginiq-scan-financials-background`;
  context.waitUntil(
    fetch(bgUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ job_id: jobId }),
    }).catch(e => console.error("Background dispatch failed:", e))
  );

  return json({ job_id: jobId, state: "queued", chunks: chunkCount }, 202);
};

export const config: Config = {
  timeout: 26,
};

import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Audited Financials PDF scanner DISPATCHER (v2.48.3).
 *
 * Three modes (selected by request method + query string):
 *
 *   POST /.netlify/functions/marginiq-scan-financials
 *     Body: { messages: [...], max_tokens?: number }
 *     → stages payload to scan_job_payloads/{jobId}, writes scan_jobs/{jobId}
 *       with state=queued, fires the background worker, returns { jobId }.
 *       HTTP 202 Accepted.
 *
 *   GET /.netlify/functions/marginiq-scan-financials?action=status&job_id=xxx
 *     → returns the scan_jobs/{jobId} doc as JSON. Client polls this until
 *       state !== "queued"|"running". On state=complete, response_b64 is the
 *       base64-encoded Anthropic response body that the client decodes and
 *       parses for extracted financials.
 *
 * The previous implementation was a synchronous proxy that called Anthropic
 * directly. Vision calls past Netlify's 10s function wall returned
 * HTTP 504 with HTML body (the "Inactivity Timeout" page). This rewrite
 * decouples the request from the model call so the wall doesn't apply.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

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

async function writeFsDoc(path: string, fields: Record<string, any>): Promise<boolean> {
  const url = `${BASE}/${path}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: toFsFields(fields) }),
  });
  return resp.ok;
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);

  // ── Status poll ──────────────────────────────────────────────────────────
  if (req.method === "GET" && url.searchParams.get("action") === "status") {
    const jobId = url.searchParams.get("job_id") || "";
    if (!jobId) return json({ error: "job_id required" }, 400);
    const doc = await readFsDoc(`scan_jobs/${jobId}`);
    if (!doc) return json({ error: "Job not found" }, 404);
    return json(doc);
  }

  if (req.method !== "POST") return json({ error: "POST required" }, 405);

  // ── Dispatch new scan job ────────────────────────────────────────────────
  let body: any;
  try { body = await req.json(); }
  catch { return json({ error: "Invalid JSON body" }, 400); }

  const messages = body.messages;
  if (!Array.isArray(messages) || messages.length === 0) {
    return json({ error: "messages array required" }, 400);
  }
  const maxTokens = Math.min(body.max_tokens || 8192, 8192);

  // Generate a job ID with timestamp for sortability
  const jobId = `scan_${new Date().toISOString().replace(/[:.]/g, "-")}_${Math.random().toString(36).slice(2, 8)}`;

  // Stage the messages payload (separate collection because the base64
  // image data can be several MB; Firestore doc field cap is ~1MB but the
  // overall doc cap is also ~1MB so we keep status separate from payload).
  const messagesB64 = Buffer.from(JSON.stringify(messages), "utf8").toString("base64");
  const payloadOk = await writeFsDoc(`scan_job_payloads/${jobId}`, {
    job_id: jobId,
    messages_b64: messagesB64,
    max_tokens: maxTokens,
    created_at: new Date().toISOString(),
  });
  if (!payloadOk) {
    return json({ error: "Failed to stage scan payload to Firestore" }, 500);
  }

  // Status doc — visible to the polling client immediately
  await writeFsDoc(`scan_jobs/${jobId}`, {
    job_id: jobId,
    state: "queued",
    queued_at: new Date().toISOString(),
    max_tokens: maxTokens,
  });

  // Fire background worker. context.waitUntil keeps the dispatcher's
  // response decoupled from the worker's lifetime, so we return 202
  // immediately with the jobId.
  const bgUrl = `${url.origin}/.netlify/functions/marginiq-scan-financials-background`;
  context.waitUntil(
    fetch(bgUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ job_id: jobId }),
    }).catch(e => console.error("Background dispatch failed:", e))
  );

  return json({ job_id: jobId, state: "queued" }, 202);
};

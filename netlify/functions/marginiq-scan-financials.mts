import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — Audited Financials PDF scanner DISPATCHER (v2.49.0).
 *
 * Mode 1 — POST /.netlify/functions/marginiq-scan-financials
 *   Body: { messages: [...], max_tokens?: number }
 *   Action:
 *     1. Generate unique jobId
 *     2. Stage messages payload to scan_job_payloads/{jobId} (separate
 *        collection because base64 image data may exceed Firestore's
 *        per-doc field budget; status doc stays small)
 *     3. Create scan_jobs/{jobId} status doc with state=queued
 *     4. Fire marginiq-scan-financials-background via context.waitUntil()
 *        — does the actual Anthropic vision call, has a 15-min wall clock
 *     5. Return { job_id } 202 immediately
 *
 * Mode 2 — GET /.netlify/functions/marginiq-scan-financials?action=status&job_id=xxx
 *   Returns scan_jobs/{jobId} doc as JSON. Client polls this until
 *   state !== "queued"|"running". On state=complete, response_b64 is the
 *   base64-encoded Anthropic response body.
 *
 * Mode 3 — GET /.netlify/functions/marginiq-scan-financials?action=health
 *   Smoke test that confirms the function is alive and FIREBASE_API_KEY is
 *   set without making an Anthropic call. Useful for first-deploy verify.
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
  const action = url.searchParams.get("action");

  // ── Health check ─────────────────────────────────────────────────────────
  if (req.method === "GET" && action === "health") {
    return json({
      ok: true,
      version: "2.49.0",
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

  // jobId: ISO timestamp (sortable) + random suffix (multiple PDFs in same ms)
  const jobId = `scan_${new Date().toISOString().replace(/[:.]/g, "-")}_${Math.random().toString(36).slice(2, 8)}`;

  // Stage messages payload — separate collection so the status doc can be
  // small and pollable while the payload may be multi-MB.
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

  // Status doc — visible to polling client immediately
  await writeFsDoc(`scan_jobs/${jobId}`, {
    job_id: jobId,
    state: "queued",
    queued_at: new Date().toISOString(),
    max_tokens: maxTokens,
  });

  // Fire background worker. context.waitUntil keeps the dispatcher response
  // decoupled from the worker's lifetime — we return 202 with jobId
  // immediately, the worker runs up to 15 minutes independently.
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

export const config: Config = {
  timeout: 26,
};

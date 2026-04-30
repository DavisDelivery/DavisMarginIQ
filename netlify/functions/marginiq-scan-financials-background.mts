import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Audited Financials PDF scanner (BACKGROUND worker, v2.48.3).
 *
 * Why this exists: synchronous Netlify Functions have a 10s wall, but vision
 * calls against multi-page CPA financial PDFs routinely take 30-60s. Every
 * scan past ~10s was returning HTTP 504 "Inactivity Timeout" with HTML body.
 *
 * Architecture (mirrors marginiq-nuvizz-ingest pattern):
 *   1. Client POSTs { messages, max_tokens } to /marginiq-scan-financials
 *      → dispatcher writes scan_job_payloads/{jobId} with the messages,
 *        scan_jobs/{jobId} with state=queued, fires this background function
 *        via context.waitUntil(), returns { jobId } 202.
 *   2. This function reads the payload doc, calls Anthropic, writes result
 *      back to scan_jobs/{jobId} with state=complete or failed.
 *   3. Client polls /marginiq-scan-financials?action=status&job_id=xxx
 *      until state !== "queued"|"running".
 *
 * Background functions get a 15-min wall clock. Vision calls won't blow that.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const ANTHROPIC_API_KEY = process.env["ANTHROPIC_API_KEY"];
const BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

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

async function writeJobDoc(jobId: string, data: Record<string, any>): Promise<void> {
  const url = `${BASE}/scan_jobs/${jobId}?key=${FIREBASE_API_KEY}`;
  await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: toFsFields(data) }),
  });
}

export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") return new Response("POST required", { status: 405 });
  if (!ANTHROPIC_API_KEY) return new Response("ANTHROPIC_API_KEY not set", { status: 500 });

  let body: any;
  try { body = await req.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const jobId: string = body.job_id;
  if (!jobId) return new Response("job_id required", { status: 400 });

  // Inbound payload staged in scan_job_payloads/{jobId}. Separate collection
  // because Firestore doc fields max ~1MB and we may be passing several MB
  // of base64 image data — payload doc holds messages JSON, scan_jobs doc
  // holds status fields.
  const payloadUrl = `${BASE}/scan_job_payloads/${jobId}?key=${FIREBASE_API_KEY}`;
  const payloadResp = await fetch(payloadUrl);
  if (!payloadResp.ok) {
    await writeJobDoc(jobId, {
      state: "failed",
      error: `Payload doc not found (${payloadResp.status})`,
      completed_at: new Date().toISOString(),
    });
    return new Response("Payload not found", { status: 404 });
  }
  const payloadDoc: any = await payloadResp.json();
  const messagesB64 = payloadDoc.fields?.messages_b64?.stringValue;
  const maxTokens = Number(payloadDoc.fields?.max_tokens?.integerValue || 8192);
  if (!messagesB64) {
    await writeJobDoc(jobId, {
      state: "failed",
      error: "Payload missing messages_b64 field",
      completed_at: new Date().toISOString(),
    });
    return new Response("Bad payload", { status: 400 });
  }
  let messages: any;
  try { messages = JSON.parse(Buffer.from(messagesB64, "base64").toString("utf8")); }
  catch (e: any) {
    await writeJobDoc(jobId, {
      state: "failed",
      error: `Could not decode messages: ${e.message}`,
      completed_at: new Date().toISOString(),
    });
    return new Response("Bad payload", { status: 400 });
  }

  await writeJobDoc(jobId, {
    state: "running",
    started_at: new Date().toISOString(),
  });

  try {
    const anthropicResp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-6",
        max_tokens: Math.min(maxTokens, 8192),
        messages,
      }),
    });

    const respText = await anthropicResp.text();

    if (!anthropicResp.ok) {
      // Surface the actual error from Anthropic so the client can react.
      let errMsg = `Vision API ${anthropicResp.status}`;
      try {
        const j = JSON.parse(respText);
        if (j.error?.message) errMsg = `Vision API ${anthropicResp.status}: ${j.error.message}`;
        else if (j.error) errMsg = `Vision API ${anthropicResp.status}: ${typeof j.error === "string" ? j.error : JSON.stringify(j.error).slice(0, 300)}`;
      } catch { /* respText wasn't JSON, stick with status code only */ }

      await writeJobDoc(jobId, {
        state: "failed",
        error: errMsg,
        http_status: anthropicResp.status,
        completed_at: new Date().toISOString(),
      });
      await fetch(payloadUrl, { method: "DELETE" }).catch(() => {});
      return new Response(JSON.stringify({ error: errMsg }), { status: 500 });
    }

    // Success — store full Anthropic response (a few hundred KB at most,
    // well under Firestore's 1MB doc limit).
    await writeJobDoc(jobId, {
      state: "complete",
      response_b64: Buffer.from(respText, "utf8").toString("base64"),
      completed_at: new Date().toISOString(),
    });
    await fetch(payloadUrl, { method: "DELETE" }).catch(() => {});

    return new Response(JSON.stringify({ ok: true, job_id: jobId }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (e: any) {
    await writeJobDoc(jobId, {
      state: "failed",
      error: `Background worker error: ${e.message || String(e)}`,
      completed_at: new Date().toISOString(),
    });
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
};

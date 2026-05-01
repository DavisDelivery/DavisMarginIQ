import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Audited Financials PDF scanner BACKGROUND worker (v2.49.3).
 *
 * Reassembles chunked payload from scan_job_payloads/{jobId}__NNN docs,
 * calls Anthropic vision, writes result to scan_jobs/{jobId}.
 *
 * Background functions get a 15-minute wall — vision will fit comfortably.
 *
 * v2.49.3 change: payload now arrives as N chunks (was 1 doc). Worker lists
 * scan_job_payloads filtered by job_id prefix, sorts by chunk_index,
 * concatenates data_b64, then base64-decodes the result to get back the
 * original messages JSON.
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

// Walk scan_job_payloads listing and pick out chunks whose docId starts with
// `${jobId}__`. Firestore REST has no docId-prefix filter, so we paginate
// the listing and filter client-side. The whole payload is bounded by what
// the dispatcher just wrote (typically 1-10 chunks per job), so this is fast.
async function loadPayloadChunks(jobId: string): Promise<{ chunks: any[]; chunkDocIds: string[]; chunkCount: number }> {
  const chunks: any[] = [];
  const chunkDocIds: string[] = [];
  let expectedCount: number | null = null;
  let pageToken: string | undefined;
  const prefix = `${jobId}__`;

  for (let safety = 0; safety < 200; safety++) {
    const params = new URLSearchParams({
      key: FIREBASE_API_KEY || "",
      pageSize: "100",
    });
    if (pageToken) params.set("pageToken", pageToken);
    const url = `${BASE}/scan_job_payloads?${params.toString()}`;
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
      const idx = Number(f.chunk_index?.integerValue || 0);
      const total = Number(f.chunk_count?.integerValue || 0);
      const data_b64: string = f.data_b64?.stringValue || "";
      if (expectedCount === null && total > 0) expectedCount = total;
      chunks.push({ idx, data_b64 });
    }
    if (!data.nextPageToken) break;
    pageToken = data.nextPageToken;
  }

  // Sort by chunk_index (ascending) so concatenation order is correct
  chunks.sort((a, b) => a.idx - b.idx);
  chunkDocIds.sort();

  return {
    chunks,
    chunkDocIds,
    chunkCount: expectedCount ?? chunks.length,
  };
}

async function deleteChunks(chunkDocIds: string[]): Promise<void> {
  // Best effort, parallel — chunks are independent
  await Promise.all(chunkDocIds.map(async (id) => {
    const url = `${BASE}/scan_job_payloads/${encodeURIComponent(id)}?key=${FIREBASE_API_KEY}`;
    await fetch(url, { method: "DELETE" }).catch(() => {});
  }));
}

export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") return new Response("POST required", { status: 405 });
  if (!ANTHROPIC_API_KEY) return new Response("ANTHROPIC_API_KEY not set", { status: 500 });

  let body: any;
  try { body = await req.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const jobId: string = body.job_id;
  if (!jobId) return new Response("job_id required", { status: 400 });

  // Reassemble payload from chunked docs
  let messages: any;
  let chunkDocIds: string[] = [];
  try {
    const { chunks, chunkDocIds: ids, chunkCount } = await loadPayloadChunks(jobId);
    chunkDocIds = ids;
    if (chunks.length === 0) {
      await writeJobDoc(jobId, {
        state: "failed",
        error: `No payload chunks found for job ${jobId}`,
        completed_at: new Date().toISOString(),
      });
      return new Response("Payload not found", { status: 404 });
    }
    if (chunkCount > 0 && chunks.length < chunkCount) {
      await writeJobDoc(jobId, {
        state: "failed",
        error: `Expected ${chunkCount} chunks but only loaded ${chunks.length}`,
        completed_at: new Date().toISOString(),
      });
      return new Response("Incomplete payload", { status: 500 });
    }
    const messagesB64 = chunks.map(c => c.data_b64).join("");
    const messagesJson = Buffer.from(messagesB64, "base64").toString("utf8");
    messages = JSON.parse(messagesJson);
  } catch (e: any) {
    await writeJobDoc(jobId, {
      state: "failed",
      error: `Could not load/decode payload: ${e.message}`,
      completed_at: new Date().toISOString(),
    });
    return new Response(`Bad payload: ${e.message}`, { status: 400 });
  }

  // Read max_tokens from the status doc (the dispatcher recorded it there)
  const statusDocResp = await fetch(`${BASE}/scan_jobs/${jobId}?key=${FIREBASE_API_KEY}`);
  let maxTokens = 8192;
  if (statusDocResp.ok) {
    const sd: any = await statusDocResp.json();
    maxTokens = Number(sd.fields?.max_tokens?.integerValue || 8192);
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
      await deleteChunks(chunkDocIds);
      return new Response(JSON.stringify({ error: errMsg }), { status: 500 });
    }

    // Anthropic responses are typically a few hundred KB — well under
    // Firestore's 1MiB single-doc-field cap. If a future model adds verbose
    // output that pushes over, we'll need to chunk this too. Not now.
    await writeJobDoc(jobId, {
      state: "complete",
      response_b64: Buffer.from(respText, "utf8").toString("base64"),
      completed_at: new Date().toISOString(),
    });
    await deleteChunks(chunkDocIds);

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

import type { Context } from "@netlify/functions";
import { gzipSync } from "node:zlib";

/**
 * Davis MarginIQ — NuVizz CSV Ingest Dispatcher (v2.40.36)
 *
 * Fixes the 256KB AWS Lambda async-invocation body limit that was silently
 * killing every weekly ingest. Instead of POSTing a 1-4MB JSON body to the
 * background function via context.waitUntil() (which Lambda rejects with
 * HTTP 500 above 256KB), we:
 *
 *   1. Write the parsed-stops payload to Firestore as gzipped+base64
 *      chunks (collection: nuvizz_ingest_payloads, doc IDs: {run_id}__NNN).
 *      Each chunk is sized to fit comfortably under Firestore's 1MiB doc cap.
 *   2. Fire the background function with just { run_id } — a tiny ~100 byte
 *      payload that easily fits under Lambda's 256KB async limit.
 *   3. Background function reads the chunks, deserializes, processes them,
 *      and deletes the payload chunks at the end.
 *
 * Two endpoints (unchanged from v2.40.34):
 *
 *   POST /.netlify/functions/marginiq-nuvizz-ingest
 *     Body: { stops: NuVizzStop[], source: string }
 *     → Stages payload to Firestore, fires BG, returns 202 with { run_id }
 *
 *   GET  /.netlify/functions/marginiq-nuvizz-ingest?action=status&run_id=xxx
 *     → Reads nuvizz_ingest_logs/{run_id} and returns it as JSON
 *       The UI polls this to show live progress.
 *
 * NuVizzStop schema (unchanged):
 *   stop_number, pro, driver_name, delivery_date, week_ending, month,
 *   status, ship_to, city, zip, contractor_pay_base, contractor_pay_at_40
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

// Target chunk size: 700KB raw JSON, which gzips to ~150KB and gives us
// safety margin under the 1MiB Firestore doc cap. Typical weekly CSV
// (~3K stops, ~1MB raw) ends up in 2 chunks.
const RAW_CHUNK_BUDGET = 700_000;

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

async function readStatusDoc(runId: string): Promise<any> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/nuvizz_ingest_logs/${runId}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url);
  if (!resp.ok) return { error: `Status doc not found (${resp.status})` };
  const data: any = await resp.json();
  const f = data.fields || {};
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(f)) {
    const vv = v as any;
    if ("stringValue" in vv) out[k] = vv.stringValue;
    else if ("integerValue" in vv) out[k] = Number(vv.integerValue);
    else if ("doubleValue" in vv) out[k] = vv.doubleValue;
    else if ("booleanValue" in vv) out[k] = vv.booleanValue;
    else if ("mapValue" in vv) {
      const inner: Record<string, any> = {};
      for (const [mk, mv] of Object.entries((vv.mapValue?.fields || {}) as Record<string, any>)) {
        if ("stringValue" in mv) inner[mk] = mv.stringValue;
        else if ("integerValue" in mv) inner[mk] = Number(mv.integerValue);
        else if ("doubleValue" in mv) inner[mk] = mv.doubleValue;
        else inner[mk] = null;
      }
      out[k] = inner;
    }
  }
  return out;
}

// Chunk the stops array so each chunk's raw JSON is ≤ RAW_CHUNK_BUDGET bytes.
// Returns array of stop sub-arrays.
function chunkStops(stops: any[]): any[][] {
  const chunks: any[][] = [];
  let current: any[] = [];
  let currentSize = 2; // [] wrapper
  for (const s of stops) {
    const sJson = JSON.stringify(s);
    if (currentSize + sJson.length + 1 > RAW_CHUNK_BUDGET && current.length > 0) {
      chunks.push(current);
      current = [];
      currentSize = 2;
    }
    current.push(s);
    currentSize += sJson.length + 1;
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

// Write one chunk to nuvizz_ingest_payloads/{run_id}__{NNN}.
// Stores gzipped+base64 payload under a 'data_b64' field — same format
// the backup system uses, so the BG can decode with the same helper.
async function writePayloadChunk(
  runId: string,
  chunkIndex: number,
  chunkCount: number,
  stops: any[],
): Promise<boolean> {
  const docId = `${runId}__${String(chunkIndex).padStart(3, "0")}`;
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/nuvizz_ingest_payloads/${docId}?key=${FIREBASE_API_KEY}`;
  const raw = JSON.stringify(stops);
  const gz = gzipSync(Buffer.from(raw, "utf8"));
  const b64 = gz.toString("base64");
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      fields: {
        run_id:       { stringValue: runId },
        chunk_index:  { integerValue: String(chunkIndex) },
        chunk_count:  { integerValue: String(chunkCount) },
        stop_count:   { integerValue: String(stops.length) },
        raw_bytes:    { integerValue: String(raw.length) },
        gz_bytes:     { integerValue: String(gz.length) },
        data_b64:     { stringValue: b64 },
        created_at:   { stringValue: new Date().toISOString() },
      }
    }),
  });
  return resp.ok;
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);

  // ── Status poll ──────────────────────────────────────────────────────────
  if (req.method === "GET" && url.searchParams.get("action") === "status") {
    const runId = url.searchParams.get("run_id") || "";
    if (!runId) return json({ error: "run_id required" }, 400);
    const doc = await readStatusDoc(runId);
    return json(doc);
  }

  if (req.method !== "POST") return json({ error: "POST required" }, 405);

  // ── Dispatch ingest ──────────────────────────────────────────────────────
  let body: any;
  try {
    body = await req.json();
  } catch {
    return json({ error: "Invalid JSON body" }, 400);
  }

  const stops = body.stops;
  if (!Array.isArray(stops) || stops.length === 0) {
    return json({ error: "stops array required" }, 400);
  }

  // Soft cap to keep individual ingests bounded. 100K stops is ~30MB raw
  // → ~50 chunks of 700KB. Beyond this we'd exceed the BG function's
  // 15-minute budget anyway.
  if (stops.length > 100000) {
    return json({ error: "Too many stops in one request (max 100,000). Split into multiple uploads." }, 413);
  }

  const runId = `nuvizz_srv_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const source = body.source || "client_upload";

  // ── Stage payload to Firestore in chunks ─────────────────────────────────
  // This is the fix for the 256KB Lambda async limit. Instead of POSTing
  // the full payload to the BG (which Lambda rejects), we write it to
  // Firestore first and have the BG read it back.
  const chunks = chunkStops(stops);
  let chunkWriteErrors = 0;
  for (let i = 0; i < chunks.length; i++) {
    const ok = await writePayloadChunk(runId, i, chunks.length, chunks[i]);
    if (!ok) chunkWriteErrors++;
  }
  if (chunkWriteErrors > 0) {
    return json({
      error: `Failed to stage ${chunkWriteErrors} of ${chunks.length} payload chunks to Firestore`,
      run_id: runId,
    }, 500);
  }

  // Write a "queued" status doc immediately so UI can show something
  const statusUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/nuvizz_ingest_logs/${runId}?key=${FIREBASE_API_KEY}`;
  await fetch(statusUrl, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      fields: {
        run_id:        { stringValue: runId },
        source:        { stringValue: source },
        state:         { stringValue: "queued" },
        queued_at:     { stringValue: new Date().toISOString() },
        stop_count:    { integerValue: String(stops.length) },
        chunk_count:   { integerValue: String(chunks.length) },
        progress_text: { stringValue: `Queued ${stops.length.toLocaleString()} stops in ${chunks.length} chunk(s)...` },
      }
    }),
  });

  // ── Fire the background function with a TINY trigger payload ─────────────
  // No more 4MB body — just the run_id. BG fetches the actual payload
  // chunks from Firestore via the run_id.
  const bgUrl = `${url.origin}/.netlify/functions/marginiq-nuvizz-ingest-background`;
  context.waitUntil(
    fetch(bgUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ run_id: runId, source }),
    }).catch(e => console.error("Background dispatch failed:", e))
  );

  return json({
    run_id: runId,
    stop_count: stops.length,
    chunk_count: chunks.length,
    state: "queued",
  }, 202);
};


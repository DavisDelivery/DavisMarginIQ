import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — NuVizz CSV Ingest Dispatcher (v2.40.34)
 *
 * Replaces client-side NuVizz saving that silently dropped ~73% of stops
 * due to mobile Safari timeouts and Firebase SDK hangs during large CSV uploads.
 *
 * Two endpoints:
 *
 *   POST /.netlify/functions/marginiq-nuvizz-ingest
 *     Body: { stops: NuVizzStop[], source: string }
 *     → Fires background function, returns 202 with { run_id }
 *
 *   GET  /.netlify/functions/marginiq-nuvizz-ingest?action=status&run_id=xxx
 *     → Reads nuvizz_ingest_logs/{run_id} and returns it as JSON
 *       The UI polls this to show live progress.
 *
 * The actual Firestore writes happen in marginiq-nuvizz-ingest-background.mts
 * which has a 15-minute budget (vs 10s sync limit).
 *
 * NuVizzStop schema (what parseNuVizz() produces client-side):
 *   stop_number, pro, driver_name, delivery_date, week_ending, month,
 *   status, ship_to, city, zip, contractor_pay_base, contractor_pay_at_40
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

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

  // Payload size guard: ~200KB per stop estimate, max 60MB background payload
  if (stops.length > 100000) {
    return json({ error: "Too many stops in one request (max 100,000). Split into multiple uploads." }, 413);
  }

  const runId = `nuvizz_srv_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const source = body.source || "client_upload";

  // Write a "queued" status doc immediately so UI can show something
  const statusUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/nuvizz_ingest_logs/${runId}?key=${FIREBASE_API_KEY}`;
  await fetch(statusUrl, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      fields: {
        run_id:      { stringValue: runId },
        source:      { stringValue: source },
        state:       { stringValue: "queued" },
        queued_at:   { stringValue: new Date().toISOString() },
        stop_count:  { integerValue: String(stops.length) },
        progress_text: { stringValue: `Queued ${stops.length.toLocaleString()} stops for server-side ingest...` },
      }
    }),
  });

  // Fire the background function (non-blocking)
  const bgUrl = `${url.origin}/.netlify/functions/marginiq-nuvizz-ingest-background`;
  context.waitUntil(
    fetch(bgUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ stops, run_id: runId, source }),
    }).catch(e => console.error("Background dispatch failed:", e))
  );

  return json({ run_id: runId, stop_count: stops.length, state: "queued" }, 202);
};


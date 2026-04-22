import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — DDIS Ingest Dispatcher (v2.40.17)
 *
 * Moves DDIS persistence off the mobile browser. Client still parses the
 * CSV(s) in-browser (fast, ~1s for a 3000-row file), then POSTs the already-
 * parsed ddisFileRecords[] + ddisPayments[] payload to this dispatcher. We
 * hand the work to a background function and return 202 immediately, so the
 * phone stops blocking on 120+ sequential Firestore client writes.
 *
 * Two endpoints on one function, chosen by query string (same shape as the
 * v2.40.10 audit-rebuild dispatcher):
 *
 *   POST /.netlify/functions/marginiq-ddis-ingest
 *        body: { ddisFileRecords: [...], ddisPayments: [...] }
 *     → Fires marginiq-ddis-ingest-background with the forwarded body and
 *       returns 202. Does NOT wait for the writes to complete.
 *
 *   GET  /.netlify/functions/marginiq-ddis-ingest?action=status
 *     → Reads marginiq_config/ddis_ingest_status and returns it as JSON.
 *       The UI polls this every few seconds after dispatching.
 *
 * Why split dispatcher + worker:
 *   Chad was stalling out on 3000-row DDIS CSVs from his phone because the
 *   client SDK was batching 25 writes at a time × ~120 batches over a mobile
 *   connection. The Firestore REST batchWrite endpoint takes 500/call and
 *   runs from Netlify's egress, not the phone. Same shape as v2.40.10's
 *   audit rebuild migration.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

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

export default async (req: Request, _context: Context) => {
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

  // Default: forward the JSON body to the background worker and return 202.
  // We read the body here once so we can validate + pass it through.
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
  if (ddisFileRecords.length === 0 && ddisPayments.length === 0) {
    return new Response(JSON.stringify({ error: "nothing to ingest — both arrays empty" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const bgUrl = `${url.origin}/.netlify/functions/marginiq-ddis-ingest-background`;
  // Fire-and-forget — background functions accept POST at their public URL.
  // We do not await; immediate rejections are logged but don't fail the 202.
  fetch(bgUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ddisFileRecords, ddisPayments }),
  }).catch((e) => {
    console.error("ddis-ingest dispatch: background fetch failed", e?.message || String(e));
  });

  return new Response(
    JSON.stringify({
      ok: true,
      queued: true,
      file_records: ddisFileRecords.length,
      payment_rows: ddisPayments.length,
      message: "DDIS ingest queued. Poll ?action=status for progress.",
      dispatched_at: new Date().toISOString(),
    }),
    {
      status: 202,
      headers: { "Content-Type": "application/json" },
    },
  );
};

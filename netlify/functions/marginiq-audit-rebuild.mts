import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Audit Queue Rebuild Dispatcher (v2.40.10)
 *
 * Two endpoints on one function, chosen by query string:
 *
 *   GET/POST /.netlify/functions/marginiq-audit-rebuild
 *     → Fires off the background function marginiq-audit-rebuild-background
 *       and returns 202 immediately. Does NOT wait for the work to complete.
 *
 *   GET      /.netlify/functions/marginiq-audit-rebuild?action=status
 *     → Reads marginiq_config/audit_rebuild_status and returns it as JSON.
 *       The UI polls this every few seconds after dispatching.
 *
 * Why split dispatcher + worker:
 *   The rebuild pages 122K+ ddis_payments rows and writes thousands of
 *   audit_items. Far beyond the 10s sync-function budget — would return
 *   502 empty-body and the browser would throw "unexpected end of JSON"
 *   (same 502 class we fixed for backups in v2.40.5).
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
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/audit_rebuild_status?key=${FIREBASE_API_KEY}`;
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

  // Default: dispatch the background worker. Fire-and-forget — background
  // functions accept invocations via their public URL. We don't await.
  const bgUrl = `${url.origin}/.netlify/functions/marginiq-audit-rebuild-background`;
  // Kick off with no await, but also don't let an immediate rejection kill us:
  fetch(bgUrl, { method: "POST" }).catch((e) => {
    console.error("dispatch: background fetch failed", e?.message || String(e));
  });

  return new Response(
    JSON.stringify({
      ok: true,
      queued: true,
      message: "Audit rebuild queued. Poll ?action=status for progress.",
      dispatched_at: new Date().toISOString(),
    }),
    {
      status: 202,
      headers: { "Content-Type": "application/json" },
    },
  );
};

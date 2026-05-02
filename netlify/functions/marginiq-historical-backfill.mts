import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Historical Backfill Dispatcher (v2.52.5)
 *
 * Synchronous endpoint that dispatches to the background function and
 * returns 202 immediately. The background function does the actual Gmail
 * fetching, gzip, and chunked Firestore writes (15-min budget vs sync 26s).
 *
 * UI/CLI polls historical_backfill_logs/{run_id} for completion.
 */

const PROJECT_ID = "davismarginiq";

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default async (req: Request, _context: Context) => {
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  if (!FIREBASE_API_KEY) return json({ error: "Firebase not configured" }, 500);

  const url = new URL(req.url);

  // GET — proxy to the background function for read-only listing/dry-run.
  // Background functions typically only return 202, so for previewing we
  // need synchronous access. Forward the GET with all query params.
  if (req.method === "GET") {
    const targetUrl = `${url.protocol}//${url.host}/.netlify/functions/marginiq-historical-backfill-background${url.search}`;
    const r = await fetch(targetUrl);
    const text = await r.text();
    return new Response(text, { status: r.status, headers: { "Content-Type": "application/json" } });
  }

  if (req.method !== "POST") return new Response("method not allowed", { status: 405 });

  try {
    const body = await req.json();
    const dryRun = !!body.dry_run;

    // Dry-run? Run synchronously (it only lists; doesn't write much)
    if (dryRun) {
      const targetUrl = `${url.protocol}//${url.host}/.netlify/functions/marginiq-historical-backfill-background`;
      const r = await fetch(targetUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const text = await r.text();
      return new Response(text, { status: r.status, headers: { "Content-Type": "application/json" } });
    }

    // Real run — fire-and-forget to background, return 202 immediately
    const targetUrl = `${url.protocol}//${url.host}/.netlify/functions/marginiq-historical-backfill-background`;
    fetch(targetUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).catch(e => console.error("dispatch failed:", e));

    const expectedRunId = `historical_backfill_${body.source}_*`; // background generates exact ID
    return json({
      ok: true,
      dispatched: true,
      source: body.source,
      limit: body.limit || 10,
      message: `Background backfill started. Poll historical_backfill_logs (filter by source='${body.source}', state='running' or 'complete') in 30-300 seconds for progress.`,
      run_id_pattern: expectedRunId,
    }, 202);
  } catch (e: any) {
    return json({ ok: false, error: e?.message || String(e) }, 500);
  }
};

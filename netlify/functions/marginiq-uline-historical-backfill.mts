import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Uline DAS Historical Backfill Trigger (v2.52.8)
 *
 * Synchronous entry point that validates options and dispatches to
 * marginiq-uline-historical-backfill-background. The background worker
 * has up to 15 minutes to run vs the 10-second sync limit.
 *
 * Endpoints:
 *   POST /.netlify/functions/marginiq-uline-historical-backfill
 *     body: {
 *       newer_than: '2025-01-01',  // optional ISO date
 *       older_than: '2026-04-01',  // optional ISO date
 *       limit: 50,                 // max emails to process (1-500)
 *       dry_run: false,            // optional preview
 *       reprocess: false,          // optional - re-process emails already in uline_processed_emails
 *     }
 *
 *   GET /.netlify/functions/marginiq-uline-historical-backfill?dry_run=1
 *     Quick preview of matching emails.
 *
 * Status doc: marginiq_config/uline_historical_backfill_status
 */

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default async (req: Request, _context: Context) => {
  const url = new URL(req.url);

  // Forward to background function.
  // Netlify dispatches *-background functions when invoked by the regular
  // function runtime via fetch to the same domain. We just proxy the body
  // and query string through.
  const bgUrl = new URL(req.url);
  bgUrl.pathname = "/.netlify/functions/marginiq-uline-historical-backfill-background";

  let body: any = null;
  if (req.method === "POST") {
    try {
      body = await req.json();
    } catch (_e) {
      body = {};
    }
  }

  // Fire-and-forget. Background function returns immediately at 202.
  // We don't await its full run.
  const init: RequestInit = {
    method: req.method === "GET" ? "GET" : "POST",
    headers: { "Content-Type": "application/json" },
  };
  if (req.method === "POST") {
    init.body = JSON.stringify(body || {});
  }

  try {
    // Don't await — fire and return. The background function is invoked
    // asynchronously by Netlify when called.
    fetch(bgUrl.toString(), init).catch(e => {
      console.warn(`uline-historical-backfill: dispatch fetch failed: ${e?.message || e}`);
    });
  } catch (e: any) {
    return json({ ok: false, error: `Dispatch failed: ${e?.message || e}` }, 500);
  }

  return json({
    ok: true,
    dispatched: true,
    message: "Backfill dispatched to background worker. Watch marginiq_config/uline_historical_backfill_status for progress.",
    options: body || { dry_run: url.searchParams.get("dry_run") === "1" },
  });
};

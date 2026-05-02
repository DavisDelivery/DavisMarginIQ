import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Stop Economics Dispatcher (v2.51.4)
 *
 * Both test=true (dry-run) and test=false (full write) take >26s for one
 * month due to the 12K-stop / 16K-payment-lookup workload. We route BOTH
 * to the background function and return 202. UI polls
 * marginiq_config/stop_economics_status (or queries the rollup collections)
 * to see when results are ready.
 */

export default async (req: Request, _context: Context) => {
  if (req.method === "GET") {
    return new Response(JSON.stringify({
      ok: true, endpoint: "stop_economics_dispatcher",
      usage: "POST { month: 'YYYY-MM', test: bool }",
      note: "Both test and full modes run in background. Poll marginiq_config/stop_economics_status for completion.",
    }), { headers: { "Content-Type": "application/json" } });
  }
  if (req.method !== "POST") return new Response("method not allowed", { status: 405 });

  try {
    const body = await req.json();
    const month = body.month;
    const test = !!body.test;
    if (!month || !/^\d{4}-\d{2}$/.test(month)) {
      return new Response(JSON.stringify({ ok: false, error: "month required (YYYY-MM)" }),
        { status: 400, headers: { "Content-Type": "application/json" } });
    }

    // Forward to background function. Don't await — fire-and-forget.
    const url = new URL(req.url);
    const baseUrl = `${url.protocol}//${url.host}`;
    fetch(`${baseUrl}/.netlify/functions/marginiq-stop-economics-background`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ month, test }),
    }).catch(e => console.error("dispatch failed:", e));

    return new Response(JSON.stringify({
      ok: true,
      dispatched: true,
      month,
      test,
      message: `${test ? "Dry-run" : "Full rollup"} started in background. Check marginiq_config/stop_economics_status or query the rollup collections in 30-90 seconds.`,
    }), { status: 202, headers: { "Content-Type": "application/json" } });
  } catch (e: any) {
    return new Response(JSON.stringify({ ok: false, error: e?.message || String(e) }),
      { status: 500, headers: { "Content-Type": "application/json" } });
  }
};

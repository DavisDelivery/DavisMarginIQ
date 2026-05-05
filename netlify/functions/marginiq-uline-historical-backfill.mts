import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Uline DAS Historical Backfill Trigger (DISABLED v2.52.9)
 *
 * The historical backfill is HALTED as part of the Foundation Rebuild
 * (Phase 0). The original implementation has no cursor + self-reinvoke and
 * cannot complete within Netlify's 15-minute background-function cap.
 *
 * Phase 2 of the rebuild will reinstate this function with:
 *   - Gmail message-list cursor cached in marginiq_config
 *   - Self-reinvocation when 12 minutes elapsed
 *   - Resumable end-to-end run from Jan 2024 to present
 *
 * Until then, ALL invocations return 410 Gone. The original logic is
 * preserved in git history (tag v2.52.8-baseline).
 *
 * Forward-week Uline ingest continues via the Wednesday-night cron at
 * marginiq-uline-auto-ingest. Only the historical backfill is halted.
 */

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default async (_req: Request, _context: Context) => {
  return json({
    ok: false,
    disabled: true,
    code: "BACKFILL_HALTED_PHASE_0",
    message:
      "Uline historical backfill is halted as part of the MarginIQ Foundation Rebuild (Phase 0). Phase 2 will reinstate this with a cursor + self-reinvoke loop. See marginiq_config/uline_historical_backfill_status for the final state of the halted run.",
    rebuild_baseline_tag: "v2.52.8-baseline",
  }, 410);
};

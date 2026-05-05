import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Phase 0 Data Inventory Trigger (v2.52.9)
 *
 * Synchronous entry point that dispatches to
 * marginiq-data-inventory-background. The background worker has up to
 * 15 minutes to walk all canonical collections and write
 * marginiq_config/data_inventory_2026-05.
 *
 * Endpoints:
 *   GET /.netlify/functions/marginiq-data-inventory
 *     Fires the inventory and returns immediately. Watch the inventory
 *     doc for completion (the background sets generated_at on success).
 *
 *   GET /.netlify/functions/marginiq-data-inventory?collections=das_lines,nuvizz_stops
 *     Scope to a subset.
 *
 *   GET /.netlify/functions/marginiq-data-inventory?max=10000
 *     Override the per-collection count cap (default 50,000).
 *
 *   GET /.netlify/functions/marginiq-data-inventory?dry_run=1
 *     Run + log + return summary, skip the Firestore write.
 */

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default async (req: Request, _context: Context) => {
  const url = new URL(req.url);

  // Forward to background function with same query string.
  const bgUrl = new URL(req.url);
  bgUrl.pathname = "/.netlify/functions/marginiq-data-inventory-background";

  try {
    fetch(bgUrl.toString(), { method: "GET" }).catch(e => {
      console.warn(`data-inventory: dispatch fetch failed: ${e?.message || e}`);
    });
  } catch (e: any) {
    return json({ ok: false, error: `Dispatch failed: ${e?.message || e}` }, 500);
  }

  return json({
    ok: true,
    dispatched: true,
    message: "Inventory dispatched to background worker. Watch marginiq_config/data_inventory_2026-05 for results (typically completes in 30-120s).",
    options: {
      collections: url.searchParams.get("collections") || "all canonical (22 collections from §5)",
      max: url.searchParams.get("max") || "50000",
      dry_run: url.searchParams.get("dry_run") === "1",
    },
    target_doc: "marginiq_config/data_inventory_2026-05",
  });
};

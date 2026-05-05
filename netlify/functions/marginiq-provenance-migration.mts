import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Phase 1 Provenance Migration (v2.53.0)
 *
 * Backfills provenance fields onto pre-Phase-1 rows where reconstructable.
 * Flags rows where the chain cannot be reconstructed.
 *
 * Per-collection reconstruction strategy
 * ======================================
 *   ddis_payments       — has `source_file: <filename>`. Match against
 *                         source_files_raw where filename + source=="ddis".
 *   nuvizz_stops        — no source_file field. Flag as unprovenanced.
 *                         Real fix is the Phase 2 NuVizz historical backfill.
 *   das_lines           — already has source_file_id (written by v2.52.8).
 *                         Just adds ingested_by + schema_version defaults
 *                         to rows missing them.
 *   das_rows_raw        — already has file_id. Renames to source_file_id
 *                         and adds the other provenance fields.
 *   ddis_rows_raw       — has `source: "ddis"` but no file link. Match by
 *                         (pro + bill_date) to ddis_payments → its
 *                         reconstructed source_file_id.
 *   nuvizz_rows_raw     — same NuVizz unprovenanced caveat.
 *   unpaid_stops        — match by (pro + pu_date) to das_lines.
 *   audit_items         — match by pro to das_lines (small set, may need
 *                         manual review).
 *
 * Checkpointing
 * =============
 * The inventory function (Phase 0) demonstrated the failure mode of any
 * function that walks 50K+ docs without checkpointing — Netlify's
 * 15-minute background-function cap kills it before it writes results.
 *
 * This migration writes a checkpoint to marginiq_config every 500 rows
 * processed. On the next invocation, it resumes from the checkpoint.
 * Hit the trigger repeatedly until the report doc shows state=complete.
 *
 * Invocation
 * ==========
 *   GET /.netlify/functions/marginiq-provenance-migration
 *     Resume the migration. Processes one chunk and returns. Hit again
 *     to continue.
 *
 *   GET /.netlify/functions/marginiq-provenance-migration?reset=1
 *     Clear checkpoint and start over. Use when the migration logic
 *     itself is updated.
 *
 *   GET /.netlify/functions/marginiq-provenance-migration?collection=ddis_payments&limit=100
 *     Scope to one collection; cap rows per invocation. Used for testing.
 *
 * Report doc
 * ==========
 *   marginiq_config/provenance_migration_2026-05
 *     {
 *       state: "running" | "complete" | "failed",
 *       per_collection: {
 *         ddis_payments: { processed, enriched, unprovenanced, errored, last_doc_id }
 *         ...
 *       },
 *       started_at, last_run_at, completed_at,
 *       checkpoint: { collection, last_doc_id }
 *     }
 *
 * NOTE: This is the trigger. Heavy work happens in
 *       marginiq-provenance-migration-background.mts.
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
  bgUrl.pathname = "/.netlify/functions/marginiq-provenance-migration-background";

  try {
    fetch(bgUrl.toString(), { method: "GET" }).catch(e => {
      console.warn(`provenance-migration: dispatch failed: ${e?.message || e}`);
    });
  } catch (e: any) {
    return json({ ok: false, error: `Dispatch failed: ${e?.message || e}` }, 500);
  }

  return json({
    ok: true,
    dispatched: true,
    message: "Provenance migration dispatched. Watch marginiq_config/provenance_migration_2026-05 for progress. Hit this endpoint repeatedly until state=complete.",
    options: {
      collection: url.searchParams.get("collection") || "all",
      limit: url.searchParams.get("limit") || "default (one chunk per invocation)",
      reset: url.searchParams.get("reset") === "1",
    },
    report_doc: "marginiq_config/provenance_migration_2026-05",
  });
};

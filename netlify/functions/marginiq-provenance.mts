import type { Context } from "@netlify/functions";
import { getProvenanceChain, SOURCE_REGISTRY } from "./lib/four-layer-ingest.js";

/**
 * Davis MarginIQ — Provenance API (v2.53.0)
 *
 * Phase 1 of the Foundation Rebuild. Returns the provenance chain for any
 * row in a normalized collection — doc → source_file_id → original
 * filename → email message_id → email date → parser version.
 *
 * Used by:
 *   - Phase 4 KPI source pills (clickable lineage panels)
 *   - Manual debugging ("where did THIS row come from?")
 *   - Audit / compliance answers ("show me the original file for any
 *     line on the audited P&L")
 *
 * Endpoints
 * =========
 *   GET /.netlify/functions/marginiq-provenance?collection=das_lines&doc_id=PRO123_2026-04-25_uline_DAS-...
 *   GET /.netlify/functions/marginiq-provenance?collection=ddis_payments&doc_id=...
 *   GET /.netlify/functions/marginiq-provenance?collection=nuvizz_stops&doc_id=...
 *
 * Response
 * ========
 *   200 OK with full chain (doc + source_file).
 *   200 OK with partial chain when source_file_id is missing
 *     (legacy pre-Phase-1 row); error field explains.
 *   404 when the doc itself doesn't exist.
 *   400 when collection or doc_id is missing.
 */

const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

// Allow lookups against any registered primary collection or rows_raw collection.
function buildAllowedCollections(): Set<string> {
  const out = new Set<string>();
  for (const entry of Object.values(SOURCE_REGISTRY)) {
    out.add(entry.primary);
    out.add(entry.rowsRaw);
  }
  // Add common legacy collections for completeness — they may have docs
  // with provenance even if they were ingested via legacy paths once the
  // migration completes.
  out.add("unpaid_stops");
  out.add("audit_items");
  return out;
}

const ALLOWED_COLLECTIONS = buildAllowedCollections();

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default async (req: Request, _context: Context) => {
  if (!FIREBASE_API_KEY) {
    return json({ ok: false, error: "FIREBASE_API_KEY not configured" }, 500);
  }

  const url = new URL(req.url);
  const collection = url.searchParams.get("collection") || "";
  const docId = url.searchParams.get("doc_id") || "";

  if (!collection || !docId) {
    return json({
      ok: false,
      error: "collection and doc_id query params are both required",
      allowed_collections: Array.from(ALLOWED_COLLECTIONS).sort(),
      example: "/.netlify/functions/marginiq-provenance?collection=das_lines&doc_id=...",
    }, 400);
  }

  if (!ALLOWED_COLLECTIONS.has(collection)) {
    return json({
      ok: false,
      error: `Collection "${collection}" not in allowlist. To probe an unfamiliar collection, add it to ALLOWED_COLLECTIONS in marginiq-provenance.mts.`,
      allowed_collections: Array.from(ALLOWED_COLLECTIONS).sort(),
    }, 400);
  }

  const result = await getProvenanceChain(collection, docId, FIREBASE_API_KEY);

  if (!result.ok) {
    // Common case: doc not found. Return 404 with whatever partial info
    // we have. If the doc exists but lacks source_file_id (pre-Phase-1
    // legacy row), return 200 with a warning.
    if (result.error && result.error.includes("not found")) {
      return json({ ok: false, error: result.error, doc: result.doc || null }, 404);
    }
    return json({
      ok: false,
      partial: true,
      error: result.error,
      doc: result.doc || null,
      hint: result.doc?.source_file_id === null
        ? "This is a pre-Phase-1 row with no provenance fields. Run the marginiq-provenance-migration function to backfill what can be reconstructed."
        : undefined,
    });
  }

  return json({
    ok: true,
    chain: {
      doc: result.doc,
      source_file: result.source_file,
    },
    summary: result.source_file
      ? `${collection}/${docId} → source_files_raw/${result.source_file.file_id} (${result.source_file.filename}, from ${result.source_file.email_account} on ${result.source_file.email_date})`
      : `${collection}/${docId} → no source file`,
  });
};

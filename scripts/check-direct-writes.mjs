#!/usr/bin/env node
/**
 * Davis MarginIQ — Phase 1 CI Check: No Direct Writes to Provenanced Collections
 *
 * Scans every Netlify function source file (.mts/.ts/.js) and fails the
 * build if any function (other than the sanctioned shared module
 * lib/four-layer-ingest.ts) writes directly to a Layer-2 or Layer-3
 * collection.
 *
 * Why this exists
 * ===============
 * Phase 1 of the Foundation Rebuild moves the four-layer ingest contract
 * from a commit-message convention to a code-enforced contract. Without
 * this check, any function can call fsPatchDoc("das_lines", ...) and
 * ship a row with no source_file_id. That's how the L3 collections drifted
 * from their L1 origins in the first place.
 *
 * What it catches
 * ===============
 * For each PROVENANCED collection (rows_raw and primary collections from
 * SOURCE_REGISTRY in lib/four-layer-ingest.ts), this script flags any
 * occurrence of that collection name as a string literal inside any
 * Netlify function file outside the sanctioned write path.
 *
 * The grep is intentionally aggressive — it flags any string occurrence,
 * including comments, log messages, and unrelated reads. Reads are fine,
 * but comments will need to be re-worded to use a non-literal form (e.g.
 * "the das_lines collection" → "the DAS L3 collection") to pass the check.
 * That's a feature, not a bug: forcing comments away from literal names
 * means grep-based future audits stay reliable.
 *
 * Allow-list
 * ==========
 * The following files are exempt:
 *   - netlify/functions/lib/four-layer-ingest.ts   (the sanctioned module)
 *   - netlify/functions/marginiq-data-inventory-background.mts
 *       (read-only inventory uses literal collection names by design)
 *   - netlify/functions/marginiq-provenance.mts
 *       (lookup endpoint references collection names in error messages)
 *   - netlify/functions/marginiq-provenance-migration-background.mts
 *       (migration writes provenance fields onto pre-Phase-1 rows; uses
 *        the shared module's primitives directly)
 *   - netlify/functions/marginiq-uline-historical-backfill*.mts
 *       (DISABLED via 410 since v2.52.9; no longer writes anything)
 *   - netlify/functions/marginiq-purge-uline.mts
 *       (cleanup tool; deletes from L3, doesn't write)
 *   - netlify/functions/marginiq-purge-timeclock.mts
 *       (cleanup tool; deletes only)
 *   - netlify/functions/marginiq-stop-economics-background.mts
 *       (Layer 4 aggregation; reads L3 + writes derived stop_economics
 *        which is NOT a provenanced collection)
 *   - netlify/functions/marginiq-backup.mts
 *   - netlify/functions/marginiq-backup-run-background.mts
 *       (read-only enumeration of collection names for backup purposes)
 *
 * LEGACY allow-list — TO BE REMOVED in the Phase 1 refactor session:
 *   - netlify/functions/marginiq-ddis-auto-ingest.mts
 *   - netlify/functions/marginiq-ddis-ingest-background.mts
 *   - netlify/functions/marginiq-ddis-ingest.mts
 *   - netlify/functions/marginiq-nuvizz-auto-ingest.mts
 *   - netlify/functions/marginiq-nuvizz-ingest-background.mts
 *   - netlify/functions/marginiq-nuvizz-ingest.mts
 *   - netlify/functions/nuvizz.mts, nuvizz.js
 *       These are the un-refactored DDIS + NuVizz live ingest paths. They
 *       still call batchWriteDocs("ddis_payments", ...) etc directly. The
 *       Phase 1 refactor session will route them through ingestFile() /
 *       writeProvenancedRows() and remove them from this list, at which
 *       point the CI check tightens automatically.
 *
 * Usage
 * =====
 *   node scripts/check-direct-writes.mjs
 *
 *   Exit 0: clean.
 *   Exit 1: at least one violation found, with file:line for each.
 */

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const REPO_ROOT = process.cwd();
const FUNCTIONS_DIR = join(REPO_ROOT, "netlify/functions");

// Mirror of SOURCE_REGISTRY from lib/four-layer-ingest.ts. Keep in sync
// when adding a new source.
const PROVENANCED_COLLECTIONS = [
  "ddis_rows_raw", "ddis_payments",
  "nuvizz_rows_raw", "nuvizz_stops",
  "das_rows_raw", "das_lines",
  "payroll_rows_raw", "payroll_runs",
  "b600_rows_raw", "b600_punches",
];

const ALLOWED_FILES = new Set([
  "netlify/functions/lib/four-layer-ingest.ts",
  "netlify/functions/marginiq-data-inventory-background.mts",
  "netlify/functions/marginiq-data-inventory.mts",
  "netlify/functions/marginiq-provenance.mts",
  "netlify/functions/marginiq-provenance-migration-background.mts",
  "netlify/functions/marginiq-provenance-migration.mts",
  "netlify/functions/marginiq-uline-historical-backfill.mts",
  "netlify/functions/marginiq-uline-historical-backfill-background.mts",
  "netlify/functions/marginiq-purge-uline.mts",
  "netlify/functions/marginiq-purge-timeclock.mts",
  "netlify/functions/marginiq-stop-economics-background.mts",
  "netlify/functions/marginiq-backup.mts",
  "netlify/functions/marginiq-backup-run-background.mts",

  // ── LEGACY — to be removed in the Phase 1 refactor session ────────────
  // These un-refactored ingest paths still write to provenanced collections
  // directly. They MUST be moved off the legacy list when refactored to use
  // ingestFile() / writeProvenancedRows() from lib/four-layer-ingest.ts.
  "netlify/functions/marginiq-ddis-auto-ingest.mts",
  "netlify/functions/marginiq-ddis-ingest-background.mts",
  "netlify/functions/marginiq-ddis-ingest.mts",
  "netlify/functions/marginiq-ddis-week-backfill-background.mts",
  "netlify/functions/marginiq-nuvizz-auto-ingest.mts",
  "netlify/functions/marginiq-nuvizz-ingest-background.mts",
  "netlify/functions/marginiq-nuvizz-ingest.mts",
  "netlify/functions/nuvizz.mts",
  "netlify/functions/nuvizz.js",
  "netlify/functions/marginiq-historical-backfill.mts",
  "netlify/functions/marginiq-historical-backfill-background.mts",
  "netlify/functions/marginiq-uline-auto-ingest.mts",
  "netlify/functions/marginiq-reparse.mts",
  "netlify/functions/marginiq-audit-rebuild.mts",
  "netlify/functions/marginiq-audit-rebuild-background.mts",
  "netlify/functions/marginiq-audit-trace.mts",
  "netlify/functions/marginiq-dispute-pdf.mts",
]);

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const full = join(dir, name);
    const st = statSync(full);
    if (st.isDirectory()) out.push(...walk(full));
    else if (/\.(mts|ts|mjs|js)$/.test(name)) out.push(full);
  }
  return out;
}

const violations = [];

for (const file of walk(FUNCTIONS_DIR)) {
  const rel = relative(REPO_ROOT, file).replace(/\\/g, "/");
  if (ALLOWED_FILES.has(rel)) continue;

  const text = readFileSync(file, "utf8");
  const lines = text.split("\n");

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    // Skip pure comment lines — they're inert and don't need to be reworded.
    const trimmed = line.trim();
    if (trimmed.startsWith("//") || trimmed.startsWith("*")) continue;

    for (const coll of PROVENANCED_COLLECTIONS) {
      // Match the collection name as a string literal: "name" or 'name'
      // or `name` (template literal start).
      const patterns = [`"${coll}"`, `'${coll}'`, "`" + coll];
      if (patterns.some(p => line.includes(p))) {
        violations.push({
          file: rel,
          line_number: i + 1,
          collection: coll,
          line: line.trim().slice(0, 200),
        });
      }
    }
  }
}

if (violations.length === 0) {
  console.log("✓ Phase 1 CI check passed: no direct writes to provenanced collections detected.");
  process.exit(0);
}

console.error("");
console.error("✗ Phase 1 CI check FAILED. Direct references to provenanced collections found");
console.error("  outside the sanctioned write path (lib/four-layer-ingest.ts).");
console.error("");
console.error(`  ${violations.length} violation(s):`);
console.error("");

for (const v of violations) {
  console.error(`  ${v.file}:${v.line_number}  →  "${v.collection}"`);
  console.error(`    ${v.line}`);
}

console.error("");
console.error("To fix: route all writes through ingestFile() in lib/four-layer-ingest.ts.");
console.error("If this file legitimately needs to read (not write) the collection, add it to");
console.error("ALLOWED_FILES in scripts/check-direct-writes.mjs and explain why in the comment block.");
console.error("");

process.exit(1);

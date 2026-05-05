# MarginIQ Foundation Rebuild — Master Handoff

**Status as of 2026-05-05 (v2.53.1 recovery in progress)**

- Phase 0: signed off
- Phase 1: delivered, contains CRITICAL BUG (Firestore PATCH without updateMask = full-doc replace). Damage cataloged below.
- Phase 2: started. Uline historical backfill ran to email 362/508 under broken patch logic before being halted. Status doc deleted at 2026-05-05T15:08Z, chain confirmed dead.
- Phase 3–6: NOT STARTED. Out of scope until Phase 2 sign-off.

---

## ROLE: Recovery agent for the MarginIQ Foundation Rebuild

The orchestrator (Claude in the Claude app) is supervising. Report back after each numbered step with: what you ran, the result, and any decision points.

REPO: DavisDelivery/DavisMarginIQ (main)
SITE: davis-marginiq.netlify.app  (site ID 33e6d450-f6d6-4488-a0ae-40a63b436ea8)
FIREBASE: davismarginiq
BRAND: #1E5B92
CURRENT TAG: v2.52.8-baseline (097e0ed6c6a8)
APP_VERSION on main: 2.52.9 — bump to 2.53.1 with the fix.

SECRETS (Chad will paste in chat, do not log):
- GITHUB_PAT (write scope, repo:DavisDelivery/DavisMarginIQ)
- FIREBASE_API_KEY (davismarginiq web key)

---

## CONTEXT — WHAT HAPPENED

Phase 0 of the Foundation Rebuild is signed off. Phase 1 (provenance enforcement) was delivered but contains a CRITICAL BUG. Phase 2 (historical reingest) was started; the Uline historical backfill was actively running and propagating the bug when the prior session ended.

THE BUG: Two functions call Firestore REST PATCH without updateMask.fieldPaths. Firestore semantics: PATCH without updateMask is a full document REPLACE — every field not in the body is dropped. Every "completion patch" and every "enrich patch" was wiping the rest of the document.

CONFIRMED DAMAGE:
- audit_items: ALL 26 docs reduced to 3 fields {source_file_id, ingested_by, schema_version}. pro/paid_amount/pu_date/customer/dispute_status are gone.
- source_files_raw (Phase 1 ingests): docs missing filename/source/email_message_id/staged_at — only ~7 surviving fields.
- Uline historical backfill (running): producing the same wiped L1 metadata as it goes. Halted at processed=362 of ~508 emails.

CONFIRMED SAFE:
- source_file_chunks: gzipped originals intact. Every file recoverable from L1.
- ddis_payments rows from today: written via Firestore :commit (full doc create), not PATCH.
- Uline historical L2/L3 rows: same — written via :commit.

UNKNOWN, MUST CHECK:
- How far past audit_items the migration got. MIGRATION_ORDER:
  audit_items → source_files_raw → ddis_rows_raw → nuvizz_rows_raw →
  unpaid_stops → ddis_payments → nuvizz_stops → das_rows_raw → das_lines

---

## EXECUTION PLAN — DO NOT SKIP STEPS, REPORT AFTER EACH

### STEP 1 — STOP THE BLEEDING (DONE 2026-05-05T15:08Z)

  1a. GET status of Uline backfill:
      curl -s "https://davis-marginiq.netlify.app/.netlify/functions/marginiq-uline-historical-backfill"
  1b. Check the status doc directly:
      curl -s "https://firestore.googleapis.com/v1/projects/davismarginiq/databases/(default)/documents/marginiq_config/uline_historical_backfill_status?key=$FIREBASE_API_KEY"
  1c. DELETE the status doc to halt the self-reinvoke chain:
      curl -X DELETE "https://firestore.googleapis.com/v1/projects/davismarginiq/databases/(default)/documents/marginiq_config/uline_historical_backfill_status?key=$FIREBASE_API_KEY"
  1d. Wait 90 seconds, re-GET 1a to confirm no new chain spawned.

  STATUS: Halted at processed=362, chain=1/60. Status doc 404 after 90s wait. Chain dead.

### STEP 2 — COMMIT HANDOFF.md TO REPO (in progress)

  Write the master handoff as HANDOFF.md at repo root, main branch.
  Use Python urllib + GitHub Contents API. Commit message: "docs: master handoff for foundation rebuild recovery (v2.53.1)"

### STEP 3 — APPLY THE TWO-FILE FIX

  FILE A: netlify/functions/lib/four-layer-ingest.ts
    Locate fsPatchDoc() near line 175. Replace its body so the URL is built with URLSearchParams that appends one updateMask.fieldPaths param per key in `fields`. Bump module banner from v2.53.0 to v2.53.1.

  FILE B: netlify/functions/marginiq-provenance-migration-background.mts
    Locate patchDoc() near line 80. Same fix: append updateMask.fieldPaths per key.

  Reference implementation (apply equivalently to both):
    const params = new URLSearchParams();
    params.set("key", apiKey);
    for (const k of Object.keys(fields)) {
      params.append("updateMask.fieldPaths", k);
    }
    const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?${params.toString()}`;
    // PATCH with body { fields: toFsFields(fields) }

  Push both files in ONE commit: "fix(v2.53.1): fsPatchDoc/patchDoc use updateMask.fieldPaths to prevent full-doc replace"

  Get the deploy command from Netlify MCP and give it to Chad to run locally. Do NOT attempt to deploy from this environment.
  After Chad confirms deploy succeeded, verify by hitting the provenance API on a known-good doc and confirming response shape.

### STEP 4 — DAMAGE ASSESSMENT

  4a. Read the migration status:
      curl -s ".../documents/marginiq_config/provenance_migration_2026-05?key=$FIREBASE_API_KEY"
  4b. For every collection in MIGRATION_ORDER where stats.enriched > 0:
      curl -s ".../documents/{collection}?pageSize=3&key=$FIREBASE_API_KEY"
      For each sample, count fields. If field count == 3 and keys match
      {source_file_id, ingested_by, schema_version} → collection is wiped.
  4c. Produce a damage table: collection | total_docs | enriched_count | wiped (Y/N) | recovery_path
  REPORT before proceeding to Step 5.

### STEP 5 — RECOVERY (per damaged collection)

  audit_items:
    POST /.netlify/functions/marginiq-audit-rebuild
    Verify rebuilt count matches expected; spot-check 3 docs for full field set.

  source_files_raw (Phase 1 ingests, docId pattern {source}_{messageId}_{filename}):
    Build a one-off netlify function: marginiq-source-files-repair-background.mts
    For each doc with field count <= 7:
      - Parse docId to extract source + messageId + filename
      - For Gmail-sourced docs: re-fetch Gmail message metadata (subject, account, internalDate)
      - For non-Gmail: reconstruct from source_file_chunks header
      - PATCH the doc with the FIXED fsPatchDoc to restore metadata

  Other L3 collections (if Step 4 shows damage):
    Source_file_chunks are intact and fileId is deterministic.
    Re-run the source-specific ingest worker against L1 chunks; it will rewrite L2/L3 with provenance via the fixed writeProvenancedRows.

### STEP 6 — RESUME PHASE 2 (only after Steps 1-5 complete and Chad signs off)

  6a. Re-run Uline historical backfill:
      POST /.netlify/functions/marginiq-uline-historical-backfill
      Expect ~508 emails, ~2.4 hrs. Idempotency map will skip any already in uline_processed_emails.
  6b. Run NuVizz historical backfill (built but not yet run):
      POST /.netlify/functions/marginiq-nuvizz-historical-backfill
  6c. Build the last Phase 2 task: DDIS historical reparse
      Recompute ddis_payments from L1 source_file_chunks with provenance.
  6d. Acceptance check: das_lines, ddis_payments, nuvizz_stops have full Jan 2024+ coverage; source_files_raw count matches expected weekly cadence; 5 random rows per collection trace back to source email via provenance API.
  6e. Stop. Hand back to Chad for Phase 2 sign-off.

---

## GUARDRAILS

- Chad deploys; you do not. Netlify MCP from this environment cannot deploy alone.
- GitHub MCP is read-only here. Use bash + Python urllib + GITHUB_PAT for writes.
- Never inline long content in chat — write to files or artifacts.
- Bump APP_VERSION on every code change. v2.53.1 for the fix commit.
- Do NOT touch Phases 3-6. Phase 2 sign-off is the goal of this run.
- After every Firestore write, verify with a follow-up GET.
- If anything is ambiguous, STOP and ask the orchestrator. Do not improvise on data integrity.

REPORT FORMAT after each step:
  STEP N: [done | blocked | needs decision]
  RAN: <commands>
  RESULT: <key outputs, truncated>
  NEXT: <what you'll do next, or what you need from orchestrator>

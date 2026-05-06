import type { Context } from "@netlify/functions";
import {
  getMigrationStatus,
  setMigrationStatus,
} from "./lib/migration-status.js";
import { dispatchPhase, type PhaseRequest } from "./marginiq-migration-runner.mjs";

/**
 * Davis MarginIQ — Migration Runner Background Worker (v2.53.4 Commit 4b)
 *
 * Companion to marginiq-migration-runner.mts (sync entry). The sync entry
 * does ALL pre-validation (confirm:true, phase ordering, migration_id,
 * pending_invocation guard) and only then fires this background worker
 * via context.waitUntil(fetch(...)). This worker simply parses the body,
 * dispatches to the phase function, and writes results back to
 * marginiq_config/migration_status.
 *
 * The body shape sent by the sync entry mirrors `pending_invocation`:
 *   {
 *     phase, migration_id, options, dry_run, parallelism,
 *     continue_on_error, requested_at, sync_invocation_id
 *   }
 *
 * Idempotency: this worker reads pending_invocation from migration_status
 * before executing. If pending_invocation.sync_invocation_id doesn't match
 * the request's sync_invocation_id, the worker assumes it's a stale fire
 * (e.g. from a retry) and returns 200 without doing work.
 *
 * Notes:
 *  - Real runs only — sync handles dry-run and abort inline.
 *  - On completion (success OR failure), this worker clears pending_invocation
 *    (sets bg_completed_at) so subsequent sync POSTs are not blocked.
 *  - Errors thrown by phase functions are caught and persisted to
 *    migration_status.last_phase_error for operator visibility via the
 *    GET poll endpoint.
 */

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function nowIso(): string { return new Date().toISOString(); }

export default async (req: Request, _context: Context) => {
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  if (!FIREBASE_API_KEY) return json({ error: "FIREBASE_API_KEY not configured" }, 500);

  const url = new URL(req.url);
  const siteOrigin = `${url.protocol}//${url.host}`;

  let body: any;
  try {
    body = await req.json();
  } catch {
    return json({ ok: false, error: "invalid JSON body" }, 400);
  }

  const phaseReq: PhaseRequest = {
    phase: body.phase,
    confirm: true,
    migration_id: body.migration_id,
    options: body.options || {},
    dry_run: false,
    parallelism: body.parallelism,
    continue_on_error: body.continue_on_error === true,
  };

  // Idempotency: stale-fire detection.
  const status = await getMigrationStatus(FIREBASE_API_KEY);
  const expectedSyncId = body.sync_invocation_id;
  const actualSyncId = (status?.pending_invocation as any)?.sync_invocation_id;
  if (expectedSyncId && actualSyncId && expectedSyncId !== actualSyncId) {
    console.warn(`migration-runner-bg: stale fire (expected sync_invocation_id=${expectedSyncId}, current=${actualSyncId}); skipping`);
    return json({ ok: true, skipped: true, reason: "stale fire" });
  }

  // Mark bg start
  await setMigrationStatus(FIREBASE_API_KEY, {
    updated_at: nowIso(),
    pending_invocation: { ...(status?.pending_invocation || {}), bg_started_at: nowIso() },
  });

  let result: any;
  let errorPersisted: string | null = null;
  try {
    result = await dispatchPhase(phaseReq, siteOrigin, FIREBASE_API_KEY);
  } catch (e: any) {
    errorPersisted = e?.message || String(e);
    console.error("migration-runner-bg: dispatchPhase threw", errorPersisted);
    result = { ok: false, phase: phaseReq.phase, error: errorPersisted };
    await setMigrationStatus(FIREBASE_API_KEY, {
      updated_at: nowIso(),
      last_phase_error: { phase: phaseReq.phase, error: errorPersisted, occurred_at: nowIso() },
    });
  }

  // Clear pending_invocation regardless of outcome
  await setMigrationStatus(FIREBASE_API_KEY, {
    updated_at: nowIso(),
    pending_invocation: {
      ...(status?.pending_invocation || {}),
      bg_started_at: (status?.pending_invocation as any)?.bg_started_at || nowIso(),
      bg_completed_at: nowIso(),
      bg_outcome: errorPersisted ? "error" : (result?.ok ? "ok" : "halt"),
    },
  });

  return json({ ok: result?.ok ?? false, ...result });
};

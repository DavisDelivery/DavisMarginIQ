/**
 * Davis MarginIQ — Auto-ingest holding queue (Phase 2 Commit 4a, v2.53.4)
 *
 * When a Phase 2 migration is in progress, the three Gmail-discovery
 * auto-ingest endpoints (uline, ddis, nuvizz) must NOT process new
 * emails. Instead they queue a sentinel doc to:
 *
 *   pending_during_migration_{source}/{queueDocId}
 *
 * After M5 sign-off, Commit 4c's M6 drain phase walks each
 * pending_during_migration_* collection and re-invokes the corresponding
 * auto-ingest endpoint once per queued doc (skipping preflight sentinels).
 *
 * Why queue at all rather than simply skip:
 *   - Provides an audit trail of cron fires that occurred during the
 *     migration window (forensic value if anything looks off post-migration).
 *   - Ensures M6 drain explicitly re-invokes auto-ingest once after sign-off
 *     so the catch-up run is intentional, not implicit-on-next-cron.
 *
 * Why a synthetic doc ID rather than a real Gmail messageId:
 *   - The auto-ingests are cron-driven; they discover messages from Gmail
 *     and don't receive any inbound messageId at handler entry.
 *   - The migration gate must short-circuit BEFORE any Gmail fetch (we want
 *     zero external-API noise during migration).
 *   - cronfire_{ISO} is a per-fire identifier that drain re-invokes once.
 *     Idempotency at the message level remains the responsibility of the
 *     existing {source}_processed_emails/{messageId} pattern.
 *
 * Sentinel shape:
 *   {
 *     source: "uline" | "ddis" | "nuvizz",
 *     triggered_at: ISO,
 *     reason: string,
 *     is_preflight_sentinel: boolean,    // true only for preflight tests
 *     drain_status: "pending"            // updated by 4c M6 drain
 *   }
 */

const PROJECT_ID = "davismarginiq";
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

import { getMigrationStatus } from "./migration-status";

export type AutoIngestSource = "uline" | "ddis" | "nuvizz";

export interface HoldingQueuePayload {
  source: AutoIngestSource;
  triggered_at: string;
  reason: string;
  is_preflight_sentinel: boolean;
  drain_status?: "pending" | "drained" | "skipped" | "failed";
  // Open shape — drain logic in 4c may add drained_at, drain_response, etc.
  [k: string]: any;
}

export interface HoldingQueueEntry extends HoldingQueuePayload {
  doc_id: string;
}

/**
 * Stage a sentinel doc to pending_during_migration_{source}/{docId}.
 * Returns true on success, false on transport failure.
 */
export async function stageToHoldingQueue(
  source: AutoIngestSource,
  docId: string,
  payload: HoldingQueuePayload,
  apiKey: string,
): Promise<boolean> {
  const collection = `pending_during_migration_${source}`;
  const params = new URLSearchParams();
  params.set("key", apiKey);
  for (const k of Object.keys(payload)) {
    params.append("updateMask.fieldPaths", k);
  }
  const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?${params.toString()}`;
  const fsFields: Record<string, any> = {};
  for (const [k, v] of Object.entries(payload)) fsFields[k] = toFsValue(v);

  try {
    const r = await fetch(url, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields: fsFields }),
    });
    if (!r.ok) {
      const text = await r.text().catch(() => "");
      console.error(`stageToHoldingQueue ${source}/${docId} failed: ${r.status} ${text.slice(0, 200)}`);
      return false;
    }
    return true;
  } catch (e: any) {
    console.error(`stageToHoldingQueue ${source}/${docId} threw: ${e?.message || e}`);
    return false;
  }
}

/**
 * List all queued sentinels for a source. Used by Commit 4c's M6 drain.
 * Returns an array of entries with their doc IDs and unwrapped payloads.
 *
 * Pagination: scans up to 200 docs per request; for the migration window
 * (hours, not days) we expect <100 sentinels per source. If a holding
 * queue ever grows past 200, the caller should add cursor pagination.
 */
export async function listHoldingQueue(
  source: AutoIngestSource,
  apiKey: string,
): Promise<HoldingQueueEntry[]> {
  const collection = `pending_during_migration_${source}`;
  const url = `${FS_BASE}/${collection}?key=${apiKey}&pageSize=200`;
  const r = await fetch(url);
  if (r.status === 404) return [];
  if (!r.ok) {
    throw new Error(`listHoldingQueue ${source} failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  }
  const data: any = await r.json();
  const docs = data.documents || [];
  return docs.map((d: any) => {
    const docId = (d.name || "").split("/").pop() || "";
    const payload: any = {};
    for (const [k, v] of Object.entries(d.fields || {})) payload[k] = unwrapField(v);
    return { doc_id: docId, ...payload };
  });
}

/**
 * Delete a single holding-queue entry. Used by Commit 4c's M6 drain after
 * a sentinel is replayed (or skipped, in the case of preflight sentinels).
 */
export async function deleteHoldingQueueEntry(
  source: AutoIngestSource,
  docId: string,
  apiKey: string,
): Promise<boolean> {
  const collection = `pending_during_migration_${source}`;
  const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`;
  try {
    const r = await fetch(url, { method: "DELETE" });
    return r.ok || r.status === 404;
  } catch (e: any) {
    console.error(`deleteHoldingQueueEntry ${source}/${docId} threw: ${e?.message || e}`);
    return false;
  }
}

export interface MigrationGateResult {
  shouldShortCircuit: boolean;
  response?: Response;
}

/**
 * Shared migration-gate helper for the three auto-ingest endpoints.
 *
 * Decision matrix (after credential checks, before any Gmail fetch):
 *
 *   in_migration | preflight_test | result
 *   -------------|----------------|----------------------------------------
 *      false     |     false      | shouldShortCircuit=false (proceed)
 *      false     |     true       | short-circuit with skipped:true (no queue)
 *      true      |     false      | short-circuit with queued:true (cronfire_*)
 *      true      |     true       | short-circuit with queued:true (preflight-test-*)
 *
 * Returning shouldShortCircuit=false means the caller should continue with
 * its normal Gmail-discovery flow.
 *
 * The `response` field is the pre-built HTTP Response the caller should
 * return when shouldShortCircuit is true.
 */
export async function checkMigrationAndMaybeQueue(
  source: AutoIngestSource,
  options: { isPreflightTest: boolean },
  apiKey: string,
): Promise<MigrationGateResult> {
  let status;
  try {
    status = await getMigrationStatus(apiKey);
  } catch (e: any) {
    // Fail-open: if we can't read the migration status (transport error),
    // do NOT block the auto-ingest. Migration runner will detect the issue
    // through gate failures or concurrency-check ABORT.
    console.error(`checkMigrationAndMaybeQueue read failed (fail-open): ${e?.message || e}`);
    return { shouldShortCircuit: false };
  }

  const inMigration = status?.in_migration === true;
  const nowIso = new Date().toISOString();

  if (options.isPreflightTest) {
    if (inMigration) {
      const sentinelId = `preflight-test-${nowIso.replace(/[:.]/g, "-")}`;
      const ok = await stageToHoldingQueue(source, sentinelId, {
        source,
        triggered_at: nowIso,
        reason: "preflight test sentinel",
        is_preflight_sentinel: true,
        drain_status: "pending",
        migration_id: status?.migration_id ?? null,
        migration_phase: status?.current_phase ?? null,
      }, apiKey);
      return {
        shouldShortCircuit: true,
        response: jsonResponse({
          ok: true,
          queued: ok,
          preflight_test: true,
          sentinel_message_id: sentinelId,
          source,
          reason: "migration in progress",
          migration_id: status?.migration_id ?? null,
          migration_phase: status?.current_phase ?? null,
        }),
      };
    } else {
      return {
        shouldShortCircuit: true,
        response: jsonResponse({
          ok: true,
          skipped: true,
          preflight_test: true,
          source,
          reason: "preflight test outside migration window — no queue write",
        }),
      };
    }
  }

  if (inMigration) {
    const cronfireId = `cronfire_${nowIso.replace(/[:.]/g, "-")}`;
    const ok = await stageToHoldingQueue(source, cronfireId, {
      source,
      triggered_at: nowIso,
      reason: "cron fired during migration",
      is_preflight_sentinel: false,
      drain_status: "pending",
      migration_id: status?.migration_id ?? null,
      migration_phase: status?.current_phase ?? null,
    }, apiKey);
    return {
      shouldShortCircuit: true,
      response: jsonResponse({
        ok: true,
        queued: ok,
        queue_doc_id: cronfireId,
        source,
        reason: "migration in progress",
        migration_id: status?.migration_id ?? null,
        migration_phase: status?.current_phase ?? null,
      }),
    };
  }

  return { shouldShortCircuit: false };
}

function jsonResponse(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/**
 * Local Firestore field encoder — same convention as lib/migration-status.ts
 * and lib/health-checks.ts (no cross-module imports for this primitive).
 */
function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") {
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (typeof v === "string") return { stringValue: v };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(toFsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, x] of Object.entries(v)) fields[k] = toFsValue(x);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

function unwrapField(f: any): any {
  if (!f || typeof f !== "object") return f;
  if ("stringValue" in f) return f.stringValue;
  if ("integerValue" in f) return Number(f.integerValue);
  if ("doubleValue" in f) return f.doubleValue;
  if ("booleanValue" in f) return f.booleanValue;
  if ("nullValue" in f) return null;
  if ("timestampValue" in f) return f.timestampValue;
  if ("arrayValue" in f) return (f.arrayValue?.values || []).map(unwrapField);
  if ("mapValue" in f) {
    const out: Record<string, any> = {};
    for (const [k, v] of Object.entries(f.mapValue?.fields || {})) out[k] = unwrapField(v);
    return out;
  }
  return undefined;
}

/**
 * Davis MarginIQ — Migration status flag (Phase 2 Commit 4a, v2.53.4)
 *
 * Read/write helper for the singleton doc at:
 *   marginiq_config/migration_status
 *
 * The doc tracks whether a Phase 2 das_lines migration is in progress.
 * Standing health checks and auto-ingest endpoints consult this flag
 * before doing any data-mutating work; the migration runner (Commit 4b)
 * writes to it as it advances through phases.
 *
 * Contract: when the doc does not exist, getMigrationStatus returns null.
 * Consumers MUST treat (status === null) and (status?.in_migration !== true)
 * as the same "no migration in progress" condition.
 *
 * Storage rationale: Firestore (not Cloud Storage). The Web API key has
 * no Cloud Storage write permission, so any state that needs to be
 * inspectable + writable from a Netlify Function must live in Firestore.
 *
 * Field set:
 *   in_migration: boolean
 *     Master switch. true ⇒ all standing checks return INFO-skipped (unless
 *     ?force=1) and all auto-ingests queue rather than process.
 *
 *   migration_id: string
 *     Identifier for the current migration run, e.g. "phase2-2026-05-06".
 *     Set on M-1/M0 entry, preserved through to M5 sign-off.
 *
 *   current_phase: string
 *     One of: "preflight", "M0", "M1", "M2", "M3", "M4", "M5", "complete",
 *     "aborted", "preflight_failed". Updated by the runner at each phase
 *     transition.
 *
 *   started_at: string (ISO)
 *     When the runner first set in_migration=true for this migration_id.
 *     Used by M2/M3 concurrency checks (any new das_lines doc with
 *     ingested_at > started_at and ingested_by NOT containing migration_id
 *     is a cron race and triggers ABORT).
 *
 *   updated_at: string (ISO)
 *     Updated on every setMigrationStatus call.
 *
 *   snapshot_collection: string  (set in M5, per Q4)
 *     Name of the Firestore collection holding the M1 snapshot, e.g.
 *     "das_lines_snapshot__phase2-2026-05-06__2026-05-06T17-30-00-000Z".
 *
 *   snapshot_eligible_for_deletion_after: string (ISO, set in M5)
 *     30 days after M5 sign-off. Manual-delete only; no API to delete
 *     before this date (Q5: overrides must be deliberately uncomfortable).
 *
 *   drain_state: object (M6, populated by Commit 4c)
 *     Tracks holding-queue drain progress per source.
 *
 *   gate_results: object (optional, written by 4b)
 *     Per-gate (G0..G4, G-1) PASS/HALT records with timestamps.
 *
 * All fields except in_migration are optional from the consumer's POV.
 * Setters use a partial-merge updateMask pattern (the v2.53.2 fix) so
 * unrelated fields are never clobbered.
 */

const PROJECT_ID = "davismarginiq";
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

const STATUS_COLLECTION = "marginiq_config";
const STATUS_DOC_ID = "migration_status";

export type MigrationPhase =
  | "preflight"
  | "M0"
  | "M1"
  | "M2"
  | "M3"
  | "M4"
  | "M5"
  | "M6"
  | "complete"
  | "aborted"
  | "preflight_failed";

export interface MigrationStatus {
  in_migration: boolean;
  migration_id?: string;
  current_phase?: MigrationPhase | string;
  started_at?: string;
  updated_at?: string;
  snapshot_collection?: string;
  snapshot_eligible_for_deletion_after?: string;
  drain_state?: Record<string, any>;
  gate_results?: Record<string, any>;
  // Open shape — runner may add fields without breaking consumers.
  [k: string]: any;
}

/**
 * Read the current migration status. Returns null if the doc does not
 * exist (bootstrap state — treat as "no migration in progress").
 *
 * Throws on non-404 transport errors so callers can surface infrastructure
 * problems rather than silently fall through.
 */
export async function getMigrationStatus(apiKey: string): Promise<MigrationStatus | null> {
  const url = `${FS_BASE}/${STATUS_COLLECTION}/${STATUS_DOC_ID}?key=${apiKey}`;
  const r = await fetch(url);
  if (r.status === 404) return null;
  if (!r.ok) {
    throw new Error(`getMigrationStatus failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  }
  const doc: any = await r.json();
  return unwrapStatusDoc(doc);
}

/**
 * Partial-merge update of marginiq_config/migration_status. Uses the
 * updateMask pattern so fields outside `fields` are preserved. Returns
 * true on success, false on transport failure (with console.error).
 *
 * Caller is responsible for setting `updated_at` if they want it bumped;
 * this helper does NOT auto-add it (that would require a separate read,
 * defeating the partial-merge guarantee).
 */
export async function setMigrationStatus(
  apiKey: string,
  fields: Partial<MigrationStatus>,
): Promise<boolean> {
  const params = new URLSearchParams();
  params.set("key", apiKey);
  for (const k of Object.keys(fields)) {
    params.append("updateMask.fieldPaths", k);
  }
  const url = `${FS_BASE}/${STATUS_COLLECTION}/${STATUS_DOC_ID}?${params.toString()}`;
  const fsFields: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) fsFields[k] = toFsValue(v);

  try {
    const r = await fetch(url, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields: fsFields }),
    });
    if (!r.ok) {
      const text = await r.text().catch(() => "");
      console.error(`setMigrationStatus failed: ${r.status} ${text.slice(0, 200)}`);
      return false;
    }
    return true;
  } catch (e: any) {
    console.error(`setMigrationStatus threw: ${e?.message || e}`);
    return false;
  }
}

/**
 * Local Firestore field encoder. Duplicated here per the convention
 * established in lib/health-checks.ts (avoids cross-module imports for
 * a small primitive).
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

function unwrapStatusDoc(doc: any): MigrationStatus {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(doc?.fields || {})) out[k] = unwrapField(v);
  // Normalize the boolean — if doc exists but field is missing, treat as false.
  return {
    in_migration: out.in_migration === true,
    ...out,
  };
}

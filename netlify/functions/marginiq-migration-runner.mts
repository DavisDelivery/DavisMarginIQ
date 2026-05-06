/**
 * Davis MarginIQ — Phase 2 Migration Runner (sync entry, v2.53.4 Commit 4b)
 *
 * State machine driving the das_lines redesign migration. Phases:
 *
 *   preflight  → P1/P2/P3 sub-steps; Gate G-1
 *   M0         → backfill 10 ddis L1-but-no-L2 files; Gate G0
 *   M1         → snapshot das_lines to a dated collection; Gate G1
 *   M2         → delete v2.53.1 corrupted rows (~205K); Gate G2
 *   M3         → delete pre-Phase-1 rows (~608K); Gate G3
 *   M4         → mass reparse from L1 via batch_reparse; Gate G4
 *   M5         → sign-off bookkeeping (4c adds retention metadata)
 *   M6         → drain holding queue (4b: synthetic-only; 4c: cronfire replay)
 *   abort      → emergency cleanup, always available
 *
 * ARCHITECTURE: fire-and-poll
 * ===========================
 *  - POST (real run) ─→ validate → write pending_invocation → spawn bg → 202
 *  - POST (dry_run=1) ─→ validate → run inline → 200 (no writes)
 *  - POST phase=abort ─→ validate → flip flag → 200 (sync, no bg)
 *  - GET ?migration_id=X ─→ read migration_status → return state
 *
 * Background worker imports `dispatchPhase` from this file. Splitting
 * across two files keeps Netlify's 10s default timeout off the long
 * phase work; the sync side just validates and routes.
 *
 * GATE OVERRIDE PHILOSOPHY (Q5)
 * =============================
 * No ?force=1, no ?force_advance=1, no API for skipping a HALTed gate.
 * If a gate HALTs, the only way past is a manual Firestore edit by the
 * operator. Overrides are deliberately uncomfortable.
 *
 * CONCURRENCY-CHECK SEMANTICS (Q4 refined)
 * =========================================
 * M2 and M3 ABORT on the FIRST das_lines doc with:
 *   ingested_at > started_at AND NOT (
 *     ingested_by ENDS_WITH "@2.53.4"
 *     OR ingested_by CONTAINS migration_id
 *   )
 * Missing ingested_by counts as non-whitelisted (since neither endsWith
 * nor includes match an empty string). False-positive scenarios (manual
 * test inserts during the migration window) are recoverable via the
 * `abort` phase, then a fresh preflight + retry.
 *
 * MIGRATION ID
 * ============
 * Free-form 8-64 char string. Required on preflight + M0; subsequent
 * phases must match the in-flight migration_status.migration_id.
 */

import type { Context } from "@netlify/functions";
import {
  getMigrationStatus,
  setMigrationStatus,
  type MigrationStatus,
} from "./lib/migration-status.js";
import { listHoldingQueue, deleteHoldingQueueEntry } from "./lib/holding-queue.js";
import { gunzipSync } from "node:zlib";

// ─── Constants ──────────────────────────────────────────────────────────────

const PROJECT_ID = "davismarginiq";
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
const BG_PATH = "/.netlify/functions/marginiq-migration-runner-background";

const APP_VERSION = "2.53.7";
// v2.53.7 — Whitelist accepts current + last-stamping release.
//
// PREVIOUS_VERSION = "2.53.5" (NOT 2.53.6). Reasoning:
// - 2.53.5 is the last version that stamped `ingested_by` markers on
//   live das_lines docs — it's the M0-fix reparse version. Today's M0
//   ddis reparse writes carry `marginiq-reparse@2.53.5`. The concurrency
//   check must continue to whitelist these so M2/M3 don't treat them as
//   cron-race intruders.
// - 2.53.6 was the failed M1 :batchWrite attempt. M1 only writes to the
//   snapshot collection (das_lines_snapshot__*), not to das_lines, AND
//   every :batchWrite returned 403 so zero docs landed anywhere. There
//   are no `ingested_by: ...@2.53.6` markers anywhere in das_lines. The
//   intermediate version is a no-op for whitelist purposes.
// - 2.53.7 is the current version (this patch). marginiq-reparse.mts is
//   NOT bumped in this patch — its APP_VERSION stays at 2.53.6 since the
//   reparse logic isn't changing. New reparse writes (e.g. an M2/M3
//   reparse cycle if needed) would stamp @2.53.6, but reparse hasn't
//   re-run since the M0 fix at 2.53.5, so no @2.53.6 reparse markers
//   exist on das_lines either. (If reparse runs at @2.53.6 in the
//   future, we'd extend this list.)
//
// PREVIOUS_VERSION can be removed in the release AFTER this one — i.e.,
// when the runner next bumps past 2.53.7 and any @2.53.5 markers have
// been cleared from das_lines. M2's v2.53.1-corrupted-rows cohort and
// M3's pre-Phase-1 cohort do not target reparse markers, so this
// rolling-window stays useful as long as live ddis L1 docs may carry
// @2.53.5 ingestedBy stamps from today's M0 reparse.
const PREVIOUS_VERSION = "2.53.5";
const RUNNER_INGEST_MARKER_SUFFIXES = [`@${APP_VERSION}`, `@${PREVIOUS_VERSION}`];

// M4 per-invocation time budget. Netlify background functions cap at 15min;
// we yield at 13min to leave room for cleanup writes.
const M4_TIME_BUDGET_MS = 13 * 60 * 1000;
// Per-batch size for marginiq-reparse?action=batch_reparse.
const M4_BATCH_LIMIT_DEFAULT = 25;

// M2 thresholds (Gate G2)
const M2_DELETED_TARGET = 205_000;
const M2_DELETED_TOLERANCE = 0.05; // ±5%

// M0 hardcoded list — must be operator-supplied (Q2). No const list here.

const VALID_PHASES = [
  "preflight", "M0", "M1", "M2", "M3", "M4", "M5", "M6", "abort",
] as const;
type Phase = typeof VALID_PHASES[number];

const MIGRATION_ID_MIN = 8;
const MIGRATION_ID_MAX = 64;

// ─── Types ──────────────────────────────────────────────────────────────────

export interface PhaseRequest {
  phase: Phase;
  confirm: true;
  migration_id: string;
  options?: Record<string, any>;
  dry_run?: boolean;
  // Surfaced from URL query for M4
  parallelism?: number;
  continue_on_error?: boolean;
}

export interface GateResult {
  id: string;
  result: "PASS" | "HALT" | "ABORT";
  evaluated_at: string;
  summary: string;
  details: any;
}

export interface PhaseResult {
  ok: boolean;
  phase: Phase;
  migration_id: string;
  dry_run: boolean;
  before_state: MigrationStatus | null;
  after_state: MigrationStatus | null;
  phase_summary: string;
  phase_details: any;
  gate?: GateResult;
  // Set when phase work was skipped because something earlier already
  // resolved (e.g. abort on already-aborted state).
  noop?: boolean;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function nowIso(): string {
  return new Date().toISOString();
}

function safeId(id: string): string {
  return id.replace(/[^a-zA-Z0-9._-]/g, "_");
}

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

async function fsRunQuery(body: any, apiKey: string): Promise<any[]> {
  const r = await fetch(`${FS_BASE}:runQuery?key=${apiKey}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`fsRunQuery ${r.status}: ${(await r.text()).slice(0, 300)}`);
  return await r.json();
}

async function fsCount(collectionId: string, apiKey: string, where?: any): Promise<number> {
  const sq: any = { from: [{ collectionId }] };
  if (where) sq.where = where;
  const body = {
    structuredAggregationQuery: {
      structuredQuery: sq,
      aggregations: [{ alias: "total", count: {} }],
    },
  };
  const r = await fetch(`${FS_BASE}:runAggregationQuery?key=${apiKey}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`fsCount ${collectionId} ${r.status}: ${(await r.text()).slice(0, 300)}`);
  const data: any = await r.json();
  if (!Array.isArray(data) || !data[0]?.result?.aggregateFields?.total) return 0;
  return Number(data[0].result.aggregateFields.total.integerValue || 0);
}

async function fsGetDoc(collection: string, docId: string, apiKey: string): Promise<any | null> {
  const r = await fetch(`${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`);
  if (r.status === 404) return null;
  if (!r.ok) throw new Error(`fsGetDoc ${collection}/${docId} ${r.status}`);
  return await r.json();
}

async function fsDeleteDoc(collection: string, docId: string, apiKey: string): Promise<boolean> {
  const r = await fetch(`${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`, {
    method: "DELETE",
  });
  return r.ok || r.status === 404;
}

async function fsBatchPatchDoc(
  collection: string, docId: string, fields: Record<string, any>, apiKey: string,
): Promise<boolean> {
  const params = new URLSearchParams();
  params.set("key", apiKey);
  for (const k of Object.keys(fields)) params.append("updateMask.fieldPaths", k);
  const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?${params.toString()}`;
  const fsFields: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) fsFields[k] = toFsValue(v);
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: fsFields }),
  });
  return r.ok;
}

// ─── Batched-write helpers (v2.53.7) ────────────────────────────────────────
//
// Wraps Firestore's :commit endpoint for batched document writes. Used by
// phaseM1 to write the snapshot collection in atomic batches of up to 500
// docs per round-trip.
//
// WHY :commit, NOT :batchWrite:
//   v2.53.6 attempted :batchWrite — which is the natural fit, since it
//   returns per-write status and is non-atomic — and got HTTP 403 on every
//   call. This is a known issue in this codebase, documented twice before:
//     - lib/four-layer-ingest.ts:308 (comment: ":commit, not :batchWrite —
//       Firestore rules treat them differently")
//     - marginiq-audit-rebuild-background.mts:111 (v2.40.28 commit:
//       ":batchWrite requires full IAM auth and 403s with just an API key
//       — it bypasses security rules; :commit runs under security rules,
//       which allow API-key writes for this project")
//     - marginiq-ddis-ingest-background.mts:218–221 (v2.42.16 commit:
//       "switched from :batchWrite to :commit because the public web app's
//       Firestore API key returns 403s on :batchWrite but works fine on
//       :commit")
//   Adding OAuth/service-account auth would be a substantial scope expansion
//   (no google-auth-library / firebase-admin / service-account JSON exists
//   anywhere in this codebase). :commit is the structural fix.
//
// :commit SEMANTICS:
//   - Endpoint: POST {FS_BASE}:commit?key={apiKey}
//   - Same request body shape as :batchWrite: { writes: Write[] }
//   - Auth: API-key via ?key= works (goes through Firestore Security Rules)
//   - Atomicity: ATOMIC. The whole batch succeeds or the whole batch fails.
//     One bad doc fails all 500 in the batch.
//   - Response (2xx): { writeResults: WriteResult[], commitTime: Timestamp }
//     writeResults.length should equal ops.length on full success.
//     There is NO status[] array — failures only manifest as non-2xx HTTP.
//   - Response (non-2xx): the whole batch failed. Body has an error message
//     but no per-write detail.
//
// PER-DOC FALL-BACK:
//   Because :commit is atomic, a single bad doc in a 500-batch fails the
//   whole batch. To preserve per-doc isolation when this happens, callers
//   should use fsBatchWritePerDocFallback (defined below), which falls back
//   to per-doc :commit calls on batch failure. fsBatchWrite itself stays
//   pure: one round-trip, one outcome.
//
// LIMIT: 500 writes per request (Firestore-wide). Caller chunks > 500.
const FS_BATCHWRITE_LIMIT = 500;

interface BatchWriteOp {
  // Doc id within the target collection. Must be Firestore-safe (no '/',
  // not '.' or '..', under 1500 bytes UTF-8). Caller is responsible.
  docId: string;
  // Update payload (Firestore-encoded fields, i.e. each leaf value already
  // wrapped via toFsValue). Mutually exclusive with delete:true.
  fields?: Record<string, any>;
  // If true, this op is a delete. fields ignored.
  delete?: boolean;
}

interface BatchWriteResult {
  // Doc IDs whose write succeeded.
  written: string[];
  // Per-failure detail: { docId, code, message }.
  // For batch-level failures (no per-doc detail from :commit), every op
  // gets the same code/message describing the batch outcome.
  // For per-doc fall-back failures, each op has its own code/message.
  failed: Array<{ docId: string; code: number; message: string }>;
  // Set when the HTTP request itself failed (non-2xx, fetch threw, or
  // unparseable body). Includes the HTTP status and a body snippet for
  // diagnostic. The retry wrapper inspects this field to decide whether
  // to retry (429/5xx) or fall back (403/400/etc.).
  http_error?: { status: number; body_snippet: string };
}

/**
 * Apply up to FS_BATCHWRITE_LIMIT writes to a single collection via
 * Firestore's :commit endpoint. Single round-trip, atomic.
 *
 * Caller MUST chunk the input list to <= 500 ops (this helper does not
 * recursively chunk). Returns aggregate outcomes; never throws on
 * Firestore errors (HTTP-level or otherwise). Throws only on caller
 * misuse (ops.length > FS_BATCHWRITE_LIMIT).
 *
 * On HTTP non-2xx, the entire batch is marked failed with the HTTP status
 * surfaced via http_error. The caller (typically dispatchWithRetry → per-doc
 * fall-back) is responsible for deciding what to do next.
 */
async function fsBatchWrite(
  collection: string,
  ops: BatchWriteOp[],
  apiKey: string,
): Promise<BatchWriteResult> {
  if (ops.length === 0) return { written: [], failed: [] };
  if (ops.length > FS_BATCHWRITE_LIMIT) {
    // Caller bug — surface clearly rather than silently slicing.
    throw new Error(
      `fsBatchWrite: ops.length=${ops.length} exceeds FS_BATCHWRITE_LIMIT=${FS_BATCHWRITE_LIMIT}; caller must chunk`,
    );
  }

  const writes = ops.map(op => {
    const fullName = `projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${op.docId}`;
    if (op.delete) {
      return { delete: fullName };
    }
    return {
      update: {
        name: fullName,
        fields: op.fields || {},
      },
    };
  });

  const url = `${FS_BASE}:commit?key=${apiKey}`;
  let resp: Response;
  try {
    resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ writes }),
    });
  } catch (e: any) {
    // Network-level failure (fetch threw). Surface as http_error with
    // every op marked failed so caller's cursor-advancement logic
    // doesn't silently advance past these.
    return {
      written: [],
      failed: ops.map(op => ({ docId: op.docId, code: -1, message: `fetch threw: ${e?.message || e}` })),
      http_error: { status: 0, body_snippet: String(e?.message || e).slice(0, 300) },
    };
  }

  if (!resp.ok) {
    // Atomic batch failure — every op failed together. Body might have
    // a structured error message; capture a snippet for diagnostic but
    // don't try to parse out per-op detail (there is none).
    const bodyText = await resp.text().catch(() => "");
    return {
      written: [],
      failed: ops.map(op => ({ docId: op.docId, code: -1, message: `HTTP ${resp.status} (batch)` })),
      http_error: { status: resp.status, body_snippet: bodyText.slice(0, 300) },
    };
  }

  let data: any;
  try {
    data = await resp.json();
  } catch (e: any) {
    // 2xx but body unparseable — vanishingly rare. Treat as total
    // failure rather than guessing about partial outcomes.
    return {
      written: [],
      failed: ops.map(op => ({ docId: op.docId, code: -1, message: `body parse failed: ${e?.message || e}` })),
      http_error: { status: resp.status, body_snippet: "(unparseable)" },
    };
  }

  // CommitResponse: { writeResults: WriteResult[], commitTime: Timestamp }
  // On full success, writeResults.length === ops.length. Each WriteResult
  // is { updateTime: Timestamp, transformResults?: Value[] }; we don't
  // need to inspect contents, just count.
  const writeResults: any[] = Array.isArray(data?.writeResults) ? data.writeResults : [];
  if (writeResults.length !== ops.length) {
    // Defensive canary: :commit returning 2xx but with a writeResults
    // array of unexpected length suggests an API surface change or
    // partial-success response we don't recognize. Treat the trailing
    // gap as failed; surface as a log line.
    console.warn(`fsBatchWrite: writeResults length mismatch — got ${writeResults.length}, expected ${ops.length} (collection=${collection})`);
  }

  const written: string[] = [];
  const failed: Array<{ docId: string; code: number; message: string }> = [];
  for (let i = 0; i < ops.length; i++) {
    if (i < writeResults.length) {
      written.push(ops[i].docId);
    } else {
      // Off the end of writeResults — treat as failed.
      failed.push({
        docId: ops[i].docId,
        code: -1,
        message: `writeResults gap (got ${writeResults.length}, expected ${ops.length})`,
      });
    }
  }

  return { written, failed };
}

/**
 * v2.53.7 — Per-doc fall-back helper for fsBatchWrite. Called by the
 * dispatchWithRetry wrapper after batch-level retries are exhausted (or
 * on a non-transient error like 403/400). Issues one single-op :commit
 * per input op, collecting per-doc outcomes.
 *
 * Rationale: :commit is atomic, so one bad doc in a 500-batch fails all
 * 500. Per-doc fall-back isolates the bad doc(s) — successful docs
 * write through individually, failed docs stash with their specific
 * HTTP status, cursor-advancement invariant preserved.
 *
 * Cost: 500 round-trips per fallback batch instead of 1. Acceptable
 * because the fallback only fires on actual failure; the fast path
 * (batch :commit) handles the >99% success case in one round-trip.
 *
 * No retries inside the fallback — by the time we're here, we're already
 * past dispatchWithRetry's retry budget. Each per-doc :commit is a single
 * attempt; failures stash with their HTTP status verbatim.
 */
async function fsBatchWritePerDocFallback(
  collection: string,
  ops: BatchWriteOp[],
  apiKey: string,
): Promise<BatchWriteResult> {
  if (ops.length === 0) return { written: [], failed: [] };

  const written: string[] = [];
  const failed: Array<{ docId: string; code: number; message: string }> = [];

  for (const op of ops) {
    const fullName = `projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${op.docId}`;
    const writes = op.delete
      ? [{ delete: fullName }]
      : [{ update: { name: fullName, fields: op.fields || {} } }];
    const url = `${FS_BASE}:commit?key=${apiKey}`;

    let resp: Response;
    try {
      resp = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ writes }),
      });
    } catch (e: any) {
      failed.push({ docId: op.docId, code: -1, message: `fetch threw: ${e?.message || e}` });
      continue;
    }

    if (!resp.ok) {
      // Per-doc HTTP failure — record specific status. This is what makes
      // the fallback valuable: we can tell which specific doc(s) caused
      // the batch to fail vs which would have succeeded individually.
      const bodyText = await resp.text().catch(() => "");
      failed.push({
        docId: op.docId,
        code: resp.status,
        message: `HTTP ${resp.status} (per-doc): ${bodyText.slice(0, 100)}`,
      });
      continue;
    }

    // 2xx — count as written without parsing the body. We don't need
    // writeResults detail; success is success.
    written.push(op.docId);
  }

  return { written, failed };
}

/**
 * Fetch helper for invoking other Netlify Functions from this runner.
 * Uses the same origin as the inbound request so production / preview
 * environments hit themselves.
 */
async function invokeLocal(siteOrigin: string, path: string, init?: RequestInit): Promise<{
  ok: boolean; status: number; data: any;
}> {
  const r = await fetch(`${siteOrigin}${path}`, init);
  let data: any = null;
  try { data = await r.json(); } catch { data = await r.text(); }
  return { ok: r.ok, status: r.status, data };
}

// ─── Validation ─────────────────────────────────────────────────────────────

function validateMigrationId(id: any): { ok: true; id: string } | { ok: false; error: string } {
  if (typeof id !== "string") return { ok: false, error: "migration_id must be a string" };
  if (id.length < MIGRATION_ID_MIN || id.length > MIGRATION_ID_MAX) {
    return { ok: false, error: `migration_id length must be ${MIGRATION_ID_MIN}-${MIGRATION_ID_MAX} chars (got ${id.length})` };
  }
  return { ok: true, id };
}

interface OrderingCheck { ok: true; reason?: string; }
interface OrderingFail { ok: false; error: string; current_phase?: string; }

function validatePhaseOrdering(phase: Phase, status: MigrationStatus | null): OrderingCheck | OrderingFail {
  if (phase === "abort") return { ok: true };

  const cp = status?.current_phase;
  const inMig = status?.in_migration === true;

  // preflight: only allowed from absent / preflight_failed / aborted / complete
  if (phase === "preflight") {
    if (!status) return { ok: true, reason: "no prior migration" };
    if (cp === "preflight_failed" || cp === "aborted" || cp === "complete") {
      return { ok: true, reason: `prior state ${cp} permits new preflight` };
    }
    if (cp === "preflight" && status?.preflight_passed !== true) {
      return { ok: true, reason: "re-running incomplete preflight" };
    }
    return {
      ok: false,
      error: `cannot start preflight while migration is in flight (current_phase=${cp ?? "null"})`,
      current_phase: cp,
    };
  }

  // For M0..M6, the migration must be in flight and the phase must come in order.
  if (!inMig) {
    return {
      ok: false,
      error: `phase ${phase} requires in_migration=true (preflight must pass first)`,
      current_phase: cp,
    };
  }

  const prev = previousPhaseOf(phase);
  const gate = gateOfPhase(prev);

  if (phase === "M0") {
    if (cp === "preflight" && status?.preflight_passed === true) return { ok: true };
    if (cp === "M0") return { ok: true, reason: "re-running M0" };
    return { ok: false, error: `M0 requires current_phase=preflight with preflight_passed=true (got ${cp})`, current_phase: cp };
  }

  // M1..M6: require previous phase complete with PASS gate
  if (cp === phase) return { ok: true, reason: `re-running ${phase}` };
  if (cp !== prev) {
    return { ok: false, error: `${phase} requires current_phase=${prev} (got ${cp})`, current_phase: cp };
  }
  if (gate) {
    const gr = status?.gate_results?.[gate];
    if (gr?.result !== "PASS") {
      return { ok: false, error: `${phase} requires ${gate}=PASS (got ${gr?.result ?? "missing"})`, current_phase: cp };
    }
  }
  return { ok: true };
}

function previousPhaseOf(phase: Phase): Phase | null {
  switch (phase) {
    case "preflight": return null;
    case "M0":        return "preflight";
    case "M1":        return "M0";
    case "M2":        return "M1";
    case "M3":        return "M2";
    case "M4":        return "M3";
    case "M5":        return "M4";
    case "M6":        return "M5";
    case "abort":     return null;
    default:          return null;
  }
}

function gateOfPhase(phase: Phase | null): string | null {
  switch (phase) {
    case "preflight": return "G_minus_1";
    case "M0":        return "G0";
    case "M1":        return "G1";
    case "M2":        return "G2";
    case "M3":        return "G3";
    case "M4":        return "G4";
    default:          return null;
  }
}

// ─── Concurrency check (M2 / M3 cron-race detector) ─────────────────────────

/**
 * Returns null if no concurrency violation found. Returns the offending
 * doc summary if one is found (caller should ABORT). Pages das_lines with
 * ingested_at > started_at and applies the JS filter for ENDS_WITH /
 * CONTAINS (Firestore lacks string-suffix/contains operators).
 *
 * Pages until exhausted or a hit is found. Bounded by MAX_PAGES_TO_SCAN
 * to prevent runaway scans during catastrophic state.
 */
const MAX_PAGES_TO_SCAN = 50; // 50 × 200 = 10K rows max scanned

async function concurrencyCheck(
  startedAt: string,
  migrationId: string,
  apiKey: string,
): Promise<null | { offending_doc: string; ingested_at: string; ingested_by: string }> {
  let cursor: string | undefined;
  for (let page = 0; page < MAX_PAGES_TO_SCAN; page++) {
    const sq: any = {
      from: [{ collectionId: "das_lines" }],
      where: {
        fieldFilter: {
          field: { fieldPath: "ingested_at" },
          op: "GREATER_THAN",
          value: { stringValue: startedAt },
        },
      },
      orderBy: [{ field: { fieldPath: "ingested_at" }, direction: "ASCENDING" }],
      limit: 200,
    };
    if (cursor) sq.startAt = { values: [{ stringValue: cursor }], before: false };

    const res = await fsRunQuery({ structuredQuery: sq }, apiKey);
    const docs = res.filter((r: any) => r.document).map((r: any) => r.document);
    if (docs.length === 0) return null;

    for (const doc of docs) {
      const ingestedAt = unwrapField(doc.fields?.ingested_at) || "";
      const ingestedBy = unwrapField(doc.fields?.ingested_by) || "";
      // v2.53.7 — Accept any of RUNNER_INGEST_MARKER_SUFFIXES (current OR
      // last-stamping release version). Keeps today's M0 reparse writes
      // (@2.53.5) whitelisted alongside the current version (@2.53.7).
      // See APP_VERSION block at top of file for why @2.53.6 is skipped.
      const isWhitelisted =
        (typeof ingestedBy === "string" && RUNNER_INGEST_MARKER_SUFFIXES.some(suf => ingestedBy.endsWith(suf))) ||
        (typeof ingestedBy === "string" && migrationId && ingestedBy.includes(migrationId));
      if (!isWhitelisted) {
        return {
          offending_doc: (doc.name || "").split("/").pop() || "(unknown)",
          ingested_at: String(ingestedAt),
          ingested_by: String(ingestedBy),
        };
      }
    }

    if (docs.length < 200) return null;
    cursor = unwrapField(docs[docs.length - 1].fields?.ingested_at) || undefined;
  }
  // Scanned the cap — surface as a halt-style result rather than continuing.
  return {
    offending_doc: "(scan-cap-reached)",
    ingested_at: "(unknown)",
    ingested_by: `concurrency check exceeded ${MAX_PAGES_TO_SCAN * 200} rows scanned`,
  };
}

// ─── Phase: preflight ───────────────────────────────────────────────────────

async function phasePreflight(req: PhaseRequest, siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  const beforeState = await getMigrationStatus(apiKey);
  const startedAt = nowIso();
  const dryRun = req.dry_run === true;

  const wouldDo = {
    set_in_migration: true,
    set_current_phase: "preflight",
    set_started_at: startedAt,
    set_migration_id: req.migration_id,
    p1: "fetch /marginiq-health-check?check=1B (no force) and (force=1); assert INFO/skipped vs real run",
    p2: "fetch each auto-ingest with ?preflight_test=1; assert queued:true; verify sentinel docs present",
    p3: "invoke phaseM6 internally; assert all preflight sentinels removed",
  };
  if (dryRun) {
    return {
      ok: true, phase: "preflight", migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: "[DRY RUN] would set in_migration=true, run P1/P2/P3, evaluate Gate G-1",
      phase_details: { would_do: wouldDo },
    };
  }

  // Initialize status (or reset preflight from prior failed/aborted/complete)
  await setMigrationStatus(apiKey, {
    in_migration: true,
    migration_id: req.migration_id,
    current_phase: "preflight",
    started_at: startedAt,
    updated_at: startedAt,
    preflight_passed: false,
    pending_invocation: null,
  });

  // ── P1: standing-check gate ──────────────────────────────────────────────
  const p1: any = { sub: "P1", started_at: nowIso(), checks: [] };
  try {
    // Without force: should be INFO/skipped
    const p1a = await invokeLocal(siteOrigin, "/.netlify/functions/marginiq-health-check?check=1B", { method: "POST" });
    p1.checks.push({ url: "?check=1B", status: p1a.status, body: p1a.data });
    const a_ok = p1a.data?.status === "INFO"
      && typeof p1a.data?.summary === "string"
      && p1a.data.summary.toLowerCase().includes("skipped")
      && p1a.data?.skipped_for_migration === true;
    p1.no_force_check_ok = a_ok;

    // With force: should run real check
    const p1b = await invokeLocal(siteOrigin, "/.netlify/functions/marginiq-health-check?check=1B&force=1", { method: "POST" });
    p1.checks.push({ url: "?check=1B&force=1", status: p1b.status, body: p1b.data });
    const b_ok = p1b.data?.status !== "INFO" || p1b.data?.forced === true;
    p1.force_check_ok = b_ok;

    p1.passed = a_ok && b_ok;
  } catch (e: any) {
    p1.passed = false;
    p1.error = e?.message || String(e);
  }
  p1.completed_at = nowIso();

  // ── P2: auto-ingest queuing ──────────────────────────────────────────────
  const p2: any = { sub: "P2", started_at: nowIso(), sources: {} };
  const sources = ["uline", "ddis", "nuvizz"] as const;
  let p2Pass = true;
  for (const src of sources) {
    const r: any = { invoked_at: nowIso() };
    try {
      const resp = await invokeLocal(siteOrigin, `/.netlify/functions/marginiq-${src}-auto-ingest?preflight_test=1`, { method: "GET" });
      r.invoke_status = resp.status;
      r.invoke_body = resp.data;
      const queued = resp.data?.queued === true && resp.data?.preflight_test === true;
      r.queued = queued;
      const sentinelId = resp.data?.sentinel_message_id;
      r.sentinel_id = sentinelId;
      if (queued && sentinelId) {
        const doc = await fsGetDoc(`pending_during_migration_${src}`, sentinelId, apiKey);
        r.sentinel_present = !!doc;
        r.sentinel_is_synthetic = unwrapField(doc?.fields?.is_preflight_sentinel) === true;
      }
      r.passed = !!(queued && r.sentinel_present && r.sentinel_is_synthetic);
    } catch (e: any) {
      r.passed = false;
      r.error = e?.message || String(e);
    }
    p2.sources[src] = r;
    p2Pass = p2Pass && r.passed === true;
  }
  p2.passed = p2Pass;
  p2.completed_at = nowIso();

  // ── P3: drain logic against synthetic sentinels ──────────────────────────
  const p3: any = { sub: "P3", started_at: nowIso() };
  try {
    const drainResult = await drainSyntheticSentinels(apiKey);
    p3.drain_result = drainResult;
    // Verify no preflight sentinels remain
    let leftovers = 0;
    for (const src of sources) {
      const remaining = await listHoldingQueue(src, apiKey);
      const stillPreflight = remaining.filter(e => e.is_preflight_sentinel === true);
      leftovers += stillPreflight.length;
    }
    p3.leftover_preflight_sentinels = leftovers;
    p3.passed = leftovers === 0 && drainResult.failed === 0;
  } catch (e: any) {
    p3.passed = false;
    p3.error = e?.message || String(e);
  }
  p3.completed_at = nowIso();

  // ── Gate G-1 ─────────────────────────────────────────────────────────────
  const g1Pass = p1.passed && p2.passed && p3.passed;
  const gate: GateResult = {
    id: "G_minus_1",
    result: g1Pass ? "PASS" : "HALT",
    evaluated_at: nowIso(),
    summary: g1Pass
      ? "Preflight all sub-steps passed (P1, P2, P3)"
      : `Preflight HALT: P1=${p1.passed} P2=${p2.passed} P3=${p3.passed}`,
    details: { P1: p1, P2: p2, P3: p3 },
  };

  // Write final state
  if (g1Pass) {
    await setMigrationStatus(apiKey, {
      preflight_passed: true,
      updated_at: nowIso(),
      gate_results: { ...(beforeState?.gate_results || {}), G_minus_1: gate },
    });
  } else {
    await setMigrationStatus(apiKey, {
      in_migration: false,
      current_phase: "preflight_failed",
      preflight_passed: false,
      updated_at: nowIso(),
      gate_results: { ...(beforeState?.gate_results || {}), G_minus_1: gate },
    });
  }

  const afterState = await getMigrationStatus(apiKey);
  return {
    ok: g1Pass, phase: "preflight", migration_id: req.migration_id, dry_run: false,
    before_state: beforeState, after_state: afterState,
    phase_summary: gate.summary,
    phase_details: { P1: p1, P2: p2, P3: p3 },
    gate,
  };
}

/**
 * Drain implementation. In 4b, walks each pending_during_migration_*
 * collection. For preflight sentinels: delete + mark "skipped". For
 * cronfire entries: leave in place and return cronfire_pending_count
 * (4c will add the auto-ingest re-invocation path).
 */
async function drainSyntheticSentinels(apiKey: string): Promise<{
  drained: number; skipped_synthetic: number; cronfire_pending_count: number; failed: number;
  per_source: Record<string, any>;
}> {
  let drained = 0, skipped = 0, cronfire = 0, failed = 0;
  const perSource: Record<string, any> = {};
  for (const src of ["uline", "ddis", "nuvizz"] as const) {
    const entries = await listHoldingQueue(src, apiKey);
    let srcSkipped = 0, srcCronfire = 0, srcFailed = 0;
    for (const e of entries) {
      if (e.is_preflight_sentinel === true) {
        const ok = await deleteHoldingQueueEntry(src, e.doc_id, apiKey);
        if (ok) srcSkipped++; else srcFailed++;
      } else {
        srcCronfire++;
      }
    }
    perSource[src] = { total: entries.length, skipped_synthetic: srcSkipped, cronfire_pending: srcCronfire, failed: srcFailed };
    skipped += srcSkipped; cronfire += srcCronfire; failed += srcFailed;
  }
  return { drained, skipped_synthetic: skipped, cronfire_pending_count: cronfire, failed, per_source: perSource };
}

/**
 * Replay cronfire sentinels by invoking the auto-ingest endpoint once per
 * source that has any cronfire entries. The auto-ingest endpoint reads
 * Gmail and processes whatever's accumulated since the last successful
 * pre-migration fire; per-message idempotency lives in
 * {source}_processed_emails/{messageId}, so a single replay catches up on
 * the entire deferred window without duplicating work.
 *
 * Per-source forensic timestamps (Q1 refinement): the result records
 * cronfire_first_triggered_at and cronfire_last_triggered_at so the
 * operator can see the time range that was collapsed into the single
 * replay invocation.
 *
 * Error policy (Q4): continue across sources on per-source failure.
 * Sentinels for failed sources are left in place (Q6) — operator can
 * manually re-invoke the auto-ingest endpoint and clean up forensically.
 *
 * Runtime expectation: 3 sources × (1 list query + 1 auto-ingest call +
 * a small number of deletes per source). The auto-ingest call is the
 * dominant cost (~10–60s per source for Gmail catch-up). Even at the
 * 60s upper bound × 3 sources = ~3 minutes — well under the 13-min
 * background-function budget. M6 is sequential by design and does not
 * need its own time budgeting at expected volumes.
 */
async function replayCronfireSentinels(siteOrigin: string, apiKey: string): Promise<{
  replays_attempted: number;
  replays_ok: number;
  replays_failed: number;
  sentinels_deleted: number;
  sentinels_left_in_place: number;
  per_source: Record<string, any>;
}> {
  const result = {
    replays_attempted: 0,
    replays_ok: 0,
    replays_failed: 0,
    sentinels_deleted: 0,
    sentinels_left_in_place: 0,
    per_source: {} as Record<string, any>,
  };

  for (const src of ["uline", "ddis", "nuvizz"] as const) {
    const entries = await listHoldingQueue(src, apiKey);
    const cronfireEntries = entries.filter(e => e.is_preflight_sentinel !== true);
    if (cronfireEntries.length === 0) {
      result.per_source[src] = { cronfire_count: 0, replayed: false, skipped: true, reason: "no cronfire entries" };
      continue;
    }

    // Forensic timestamps (Q1): min/max triggered_at across the cronfire batch
    const triggeredTimes = cronfireEntries
      .map(e => (typeof e.triggered_at === "string" ? e.triggered_at : ""))
      .filter(t => t.length > 0)
      .sort();
    const firstTriggeredAt = triggeredTimes[0] || null;
    const lastTriggeredAt = triggeredTimes[triggeredTimes.length - 1] || null;

    result.replays_attempted++;
    try {
      // Single GET — auto-ingest's gate (checkMigrationAndMaybeQueue) will see
      // in_migration=false (since M5 ran before M6) and proceed normally.
      // If queued:true comes back, in_migration is unexpectedly still set —
      // treat as a replay failure and leave sentinels in place.
      const resp = await invokeLocal(siteOrigin, `/.netlify/functions/marginiq-${src}-auto-ingest`, { method: "GET" });
      const queued = resp.data?.queued === true;
      const replaySucceeded = resp.ok && !queued;

      if (replaySucceeded) {
        result.replays_ok++;
        let deleted = 0;
        let deleteFailures = 0;
        for (const e of cronfireEntries) {
          const ok = await deleteHoldingQueueEntry(src, e.doc_id, apiKey);
          if (ok) deleted++; else deleteFailures++;
        }
        result.sentinels_deleted += deleted;
        result.sentinels_left_in_place += deleteFailures;
        result.per_source[src] = {
          cronfire_count: cronfireEntries.length,
          cronfire_first_triggered_at: firstTriggeredAt,
          cronfire_last_triggered_at: lastTriggeredAt,
          replayed: true,
          auto_ingest_status: resp.status,
          auto_ingest_summary: resp.data?.summary ?? resp.data?.message ?? null,
          sentinels_deleted: deleted,
          sentinel_delete_failures: deleteFailures,
        };
      } else {
        result.replays_failed++;
        result.sentinels_left_in_place += cronfireEntries.length;
        result.per_source[src] = {
          cronfire_count: cronfireEntries.length,
          cronfire_first_triggered_at: firstTriggeredAt,
          cronfire_last_triggered_at: lastTriggeredAt,
          replayed: false,
          error: queued
            ? `auto-ingest returned queued:true — in_migration is unexpectedly still set`
            : `auto-ingest replay failed (status=${resp.status})`,
          response: resp.data,
        };
      }
    } catch (e: any) {
      result.replays_failed++;
      result.sentinels_left_in_place += cronfireEntries.length;
      result.per_source[src] = {
        cronfire_count: cronfireEntries.length,
        cronfire_first_triggered_at: firstTriggeredAt,
        cronfire_last_triggered_at: lastTriggeredAt,
        replayed: false,
        error: e?.message || String(e),
      };
    }
  }
  return result;
}

// ─── Phase: M0 (ddis backfill, Gate G0) ─────────────────────────────────────

async function phaseM0(req: PhaseRequest, siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  const beforeState = await getMigrationStatus(apiKey);
  const dryRun = req.dry_run === true;
  const hardcoded: string[] = Array.isArray(req.options?.hardcoded_ddis_files)
    ? req.options!.hardcoded_ddis_files : [];
  const acknowledged = req.options?.discovery_acknowledged === true;

  // ── Discovery: ddis files with chunks but no parsed rows ─────────────────
  const sq: any = {
    from: [{ collectionId: "source_files_raw" }],
    where: {
      compositeFilter: {
        op: "AND",
        filters: [
          { fieldFilter: { field: { fieldPath: "source" }, op: "EQUAL", value: { stringValue: "ddis" } } },
          { fieldFilter: { field: { fieldPath: "chunk_count" }, op: "GREATER_THAN", value: { integerValue: "0" } } },
        ],
      },
    },
    limit: 500,
  };
  let discoveredAll: any[] = [];
  try {
    const res = await fsRunQuery({ structuredQuery: sq }, apiKey);
    discoveredAll = res.filter((r: any) => r.document).map((r: any) => r.document);
  } catch (e: any) {
    return {
      ok: false, phase: "M0", migration_id: req.migration_id, dry_run: dryRun,
      before_state: beforeState, after_state: beforeState,
      phase_summary: `M0 discovery query failed: ${e?.message || e}`,
      phase_details: { error: e?.message || String(e) },
    };
  }

  // Filter to those lacking parsed_row_count or with parsed_row_count===0
  const discovered = discoveredAll.filter(d => {
    const prc = unwrapField(d.fields?.parsed_row_count);
    return prc === undefined || prc === null || Number(prc) === 0;
  }).map(d => (d.name || "").split("/").pop());

  const inHardcodedOnly = hardcoded.filter(f => !discovered.includes(f));
  const inDiscoveredOnly = discovered.filter(f => !hardcoded.includes(f));
  const divergence = inHardcodedOnly.length > 0 || inDiscoveredOnly.length > 0;

  // ── Q2: side-by-side INFO-halt on divergence unless acknowledged ─────────
  if (divergence && !acknowledged) {
    const summary = `M0 INFO-halt: hardcoded=${hardcoded.length} discovered=${discovered.length} in_hardcoded_only=${inHardcodedOnly.length} in_discovered_only=${inDiscoveredOnly.length}`;
    const details = {
      hardcoded_list: hardcoded,
      discovered_list: discovered,
      in_hardcoded_only: inHardcodedOnly,
      in_discovered_only: inDiscoveredOnly,
      operator_action: "Review divergence; re-invoke M0 with options.discovery_acknowledged=true to proceed using the (intentional) target list",
    };
    if (!dryRun) {
      await setMigrationStatus(apiKey, {
        updated_at: nowIso(),
        M0_state: { ...(beforeState?.M0_state || {}), discovery_info_halt: details, last_attempt_at: nowIso() },
      });
    }
    return {
      ok: true, phase: "M0", migration_id: req.migration_id, dry_run: dryRun,
      before_state: beforeState, after_state: dryRun ? beforeState : await getMigrationStatus(apiKey),
      phase_summary: summary,
      phase_details: details,
      gate: { id: "G0", result: "HALT", evaluated_at: nowIso(),
        summary: "Discovery mismatch; awaiting operator acknowledgment", details },
    };
  }

  // Target list: when acknowledged, take operator's hardcoded list as canonical.
  // When no divergence, hardcoded === discovered, so either works.
  const targets = (acknowledged ? hardcoded : discovered).slice();

  if (dryRun) {
    return {
      ok: true, phase: "M0", migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: `[DRY RUN] would invoke marginiq-reparse for ${targets.length} ddis file(s)`,
      phase_details: {
        would_do: { invoke_count: targets.length, targets, divergence_acknowledged: acknowledged },
      },
    };
  }

  // ── Real run: invoke marginiq-reparse for each target ────────────────────
  const reparseResults: any[] = [];
  let reparsedCount = 0;
  for (const fileId of targets) {
    try {
      const resp = await invokeLocal(siteOrigin, "/.netlify/functions/marginiq-reparse", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_id: fileId, action: "reparse" }),
      });
      reparseResults.push({ file_id: fileId, status: resp.status, ok: resp.data?.ok === true });
      if (resp.data?.ok === true) reparsedCount++;
    } catch (e: any) {
      reparseResults.push({ file_id: fileId, ok: false, error: e?.message || String(e) });
    }
  }

  // ── Gate G0: verify parsed_row_count set on all targets ──────────────────
  let prcSet = 0;
  let totalRowsAcrossTargets = 0;
  for (const fileId of targets) {
    const doc = await fsGetDoc("source_files_raw", fileId, apiKey);
    const prc = doc ? Number(unwrapField(doc.fields?.parsed_row_count) || 0) : 0;
    if (prc > 0) prcSet++;
    totalRowsAcrossTargets += prc;
  }
  const g0Pass = prcSet === targets.length && totalRowsAcrossTargets > 0;
  const gate: GateResult = {
    id: "G0",
    result: g0Pass ? "PASS" : "HALT",
    evaluated_at: nowIso(),
    summary: g0Pass
      ? `M0 reparsed ${reparsedCount}/${targets.length}; parsed_row_count set on all ${prcSet}`
      : `G0 HALT: ${prcSet}/${targets.length} have parsed_row_count > 0`,
    details: { targets, reparsed: reparsedCount, parsed_row_count_set: prcSet, total_rows: totalRowsAcrossTargets },
  };

  await setMigrationStatus(apiKey, {
    current_phase: "M0",
    updated_at: nowIso(),
    gate_results: { ...(beforeState?.gate_results || {}), G0: gate },
    M0_state: {
      targets, reparse_results: reparseResults, completed_at: nowIso(),
    },
  });

  const afterState = await getMigrationStatus(apiKey);
  return {
    ok: g0Pass, phase: "M0", migration_id: req.migration_id, dry_run: false,
    before_state: beforeState, after_state: afterState,
    phase_summary: gate.summary,
    phase_details: { targets, reparse_results: reparseResults },
    gate,
  };
}

// ─── Phase: M1 (snapshot, Gate G1) ──────────────────────────────────────────

// v2.53.6 — M1 rewrite: batched writes, time-budget yield, resumable cursor.
//
// Three-case resume state machine (entered before any work):
//   (1) M1_state.completed_at SET + M1_state.last_processed_doc_id SET
//       → previous M1 fully completed; treat as fresh re-run, generate NEW
//       snapshot name (don't append to a finished snapshot).
//   (2) M1_state.completed_at UNSET + M1_state.last_processed_doc_id SET
//       → previous M1 was time-yielded or crashed mid-flight; resume into
//       the persisted M1_state.snapshot_collection from that cursor.
//   (3) Both UNSET → fresh start.
//   (anything else, e.g. cursor set but snapshot_collection absent) →
//       corrupted state; HALT with a clear error rather than guessing.
//
// Batched-write architecture:
//   - Each page = 200 das_lines docs read via fsRunQuery (existing pattern).
//   - Each page becomes one fsBatchWrite call (200 ops, well under 500 cap).
//   - 3 pages dispatched concurrently via Promise.all (the M1_CONCURRENCY
//     constant). Mirrors M4's parallelism shape, simpler than sliding-window.
//   - Per-doc result merged into written[] / failed[] lists per group.
//   - Cursor advances to the highest docId seen in this group (whether
//     written or failed-stashed); failed docs are tracked separately in
//     M1_state.failed_doc_ids[] (bounded to last 1000) plus a scalar
//     failed_count for the true total.
//
// Cursor-advancement invariant (Q4b from the brief):
//   The persisted M1_state.last_processed_doc_id reflects the last docId
//   we have EITHER written-confirmed OR failure-stashed — both outcomes
//   constitute "processed" because failed docs are not retried from the
//   cursor, they're stashed in failed_doc_ids[] for separate inspection.
//   On crash mid-group (before checkpoint), the cursor stays at its
//   previous value and resume re-processes the in-flight group; writes
//   are idempotent (same docId, same fields) so re-processing is safe.
//
// Mid-loop HALT (Q4a from the brief):
//   If failed_count > preCount * 0.05 at any checkpoint, abort the loop
//   immediately and HALT G1. Continuing to write a snapshot we already
//   know is too corrupt to be a usable recovery position is wasted work
//   AND obscures the failure mode behind a wall of "wrote 800K with 5%
//   missing" that operators can ignore.
//
// Time budget: 13min (mirror M4_TIME_BUDGET_MS) to leave 2min for cleanup.
// On time-yield, return ok:true with a "re-invoke phase=M1 to resume"
// summary — the migration runner's existing dispatch logic re-allows
// the same phase when current_phase matches.
const M1_CONCURRENCY = 3;
const M1_PAGE_SIZE = 200;
const M1_FAILED_DOC_IDS_BOUND = 1000;
const M1_FAILED_RATIO_HALT = 0.05;

async function phaseM1(req: PhaseRequest, _siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  const beforeState = await getMigrationStatus(apiKey);
  const dryRun = req.dry_run === true;
  const prevState = (beforeState?.M1_state || {}) as Record<string, any>;

  // Pre-count for Gate G1 (always — used in dry-run summary too)
  const preCount = await fsCount("das_lines", apiKey);

  // ── Resume state machine ─────────────────────────────────────────────────
  const prevSnapshotCollection: string | undefined = prevState.snapshot_collection;
  const prevCursor: string | undefined = prevState.last_processed_doc_id;
  const prevCompletedAt: string | undefined = prevState.completed_at;

  let snapshotName: string;
  let resuming = false;
  let isFreshRerun = false;

  if (prevCompletedAt && prevCursor) {
    // Case (1): previous M1 completed; this is a fresh re-run. Generate new name.
    isFreshRerun = true;
    const tsTag = nowIso().replace(/[:.]/g, "-");
    snapshotName = `das_lines_snapshot__${safeId(req.migration_id)}__${tsTag}`;
  } else if (!prevCompletedAt && prevCursor) {
    // Case (2): previous M1 interrupted; resume into existing snapshot.
    if (!prevSnapshotCollection) {
      // Corrupted state guard: cursor without a snapshot collection.
      const summary = `M1 HALT: corrupted M1_state — last_processed_doc_id=${prevCursor} but snapshot_collection is missing. Operator must clear M1_state in marginiq_config/migration_status before retry.`;
      const gate: GateResult = {
        id: "G1", result: "HALT", evaluated_at: nowIso(),
        summary,
        details: { reason: "corrupted_m1_state", prev_cursor: prevCursor, prev_snapshot_collection: null },
      };
      if (!dryRun) {
        await setMigrationStatus(apiKey, {
          updated_at: nowIso(),
          gate_results: { ...(beforeState?.gate_results || {}), G1: gate },
        });
      }
      return {
        ok: false, phase: "M1", migration_id: req.migration_id, dry_run: dryRun,
        before_state: beforeState, after_state: dryRun ? beforeState : await getMigrationStatus(apiKey),
        phase_summary: summary, phase_details: gate.details, gate,
      };
    }
    snapshotName = prevSnapshotCollection;
    resuming = true;
  } else {
    // Case (3): fresh start.
    const tsTag = nowIso().replace(/[:.]/g, "-");
    snapshotName = `das_lines_snapshot__${safeId(req.migration_id)}__${tsTag}`;
  }

  if (dryRun) {
    return {
      ok: true, phase: "M1", migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: resuming
        ? `[DRY RUN] would resume snapshot to ${snapshotName} from cursor=${prevCursor} (target ${preCount} docs)`
        : `[DRY RUN] would snapshot ${preCount} das_lines docs to ${snapshotName}${isFreshRerun ? " (fresh re-run; previous snapshot left intact)" : ""}`,
      phase_details: {
        would_do: {
          snapshot_collection: snapshotName,
          pre_count: preCount,
          resuming,
          is_fresh_rerun: isFreshRerun,
          resume_cursor: resuming ? prevCursor : null,
        },
      },
    };
  }

  // Initialize counters: on resume, restore prior counts; otherwise zero.
  let processed = resuming ? Number(prevState.processed || 0) : 0;
  let failedCount = resuming ? Number(prevState.failed_count || 0) : 0;
  const failedDocIds: Array<{ docId: string; code: number; message: string }> = resuming
    ? (Array.isArray(prevState.failed_doc_ids) ? prevState.failed_doc_ids.slice() : [])
    : [];
  let cursor: string | undefined = resuming ? prevCursor : undefined;
  let groups = 0;

  // Persist snapshot collection name immediately so 4c's M5 retention
  // metadata write has a stable name to reference. On resume this is a
  // no-op but harmless. Reset failure stash on fresh-start / fresh-rerun;
  // preserve on resume.
  await setMigrationStatus(apiKey, {
    current_phase: "M1",
    snapshot_collection: snapshotName,
    updated_at: nowIso(),
    M1_state: {
      snapshot_collection: snapshotName,
      pre_count: preCount,
      started_at: resuming ? (prevState.started_at || nowIso()) : nowIso(),
      processed,
      failed_count: failedCount,
      failed_doc_ids: failedDocIds.slice(-M1_FAILED_DOC_IDS_BOUND),
      last_processed_doc_id: cursor || null,
      resumed_from: resuming ? prevCursor : null,
      last_progress_at: nowIso(),
    },
  });

  // ── Per-batch retry + per-doc fall-back helper ──────────────────────────
  // Wraps fsBatchWrite with two layers:
  //   1. Retry-on-transient-error at the batch level. HTTP 429 / 5xx.
  //      2 retries with fixed backoff 1000ms then 3000ms.
  //   2. Per-doc fall-back when retries are exhausted OR on first
  //      non-transient error (e.g., 403, 400, INVALID_ARGUMENT for a
  //      single bad doc that fails the whole atomic batch). Falls back to
  //      individual single-op :commit calls, isolating the bad doc(s).
  //      See fsBatchWritePerDocFallback for the per-doc mechanics.
  //
  // Co-located with M1 by intent — M2/M3 may want different retry policy
  // (e.g., longer backoff for destructive ops). Don't generalize until then.
  const M1_RETRY_DELAYS_MS = [1000, 3000];
  const dispatchWithRetry = async (
    collection: string,
    ops: BatchWriteOp[],
  ): Promise<BatchWriteResult> => {
    let lastResult: BatchWriteResult | undefined;
    for (let attempt = 0; attempt <= M1_RETRY_DELAYS_MS.length; attempt++) {
      const result = await fsBatchWrite(collection, ops, apiKey);
      // Success: no http_error means the batch :commit returned 2xx with
      // a writeResults array. Return immediately.
      if (!result.http_error) return result;

      const transient = result.http_error.status === 429 ||
        (result.http_error.status >= 500 && result.http_error.status < 600);

      if (!transient) {
        // Non-transient batch failure (e.g. 403, 400, INVALID_ARGUMENT).
        // No point retrying; fall back to per-doc :commit immediately.
        console.warn(`M1 batch non-transient error (HTTP ${result.http_error.status}); falling back to per-doc :commit for ${ops.length} ops`);
        return await fsBatchWritePerDocFallback(collection, ops, apiKey);
      }

      // Transient: retry the batch.
      lastResult = result;
      if (attempt < M1_RETRY_DELAYS_MS.length) {
        console.warn(`M1 batch transient error (HTTP ${result.http_error.status}); retry ${attempt + 1}/${M1_RETRY_DELAYS_MS.length} in ${M1_RETRY_DELAYS_MS[attempt]}ms`);
        await new Promise(r => setTimeout(r, M1_RETRY_DELAYS_MS[attempt]));
      }
    }
    // Exhausted retries on transient errors. Fall back to per-doc :commit
    // — the per-doc requests are independent, so a temporary throttle that
    // blocked a 500-doc atomic batch may let many of the 500 individual
    // requests through.
    console.warn(`M1 batch transient error: exhausted ${M1_RETRY_DELAYS_MS.length} retries (last status ${lastResult?.http_error?.status}); falling back to per-doc :commit for ${ops.length} ops`);
    return await fsBatchWritePerDocFallback(collection, ops, apiKey);
  };

  // ── Time-budget loop: read+write groups of M1_CONCURRENCY pages ──────────
  const startMs = Date.now();
  let halted = false;
  let haltReason: string | null = null;

  while (true) {
    if (Date.now() - startMs > M4_TIME_BUDGET_MS) {
      haltReason = "time_budget_exceeded";
      halted = true;
      break;
    }

    // Fetch up to M1_CONCURRENCY pages SEQUENTIALLY (the read side has to
    // advance the cursor between pages), then dispatch their writes in
    // parallel. This is the "groups of 3" form per the brief — simpler
    // than sliding-window, matches M4's idiom.
    const pagesData: Array<{ docs: any[]; lastDocId: string }> = [];
    let groupExhausted = false;
    for (let p = 0; p < M1_CONCURRENCY; p++) {
      const sq: any = {
        from: [{ collectionId: "das_lines" }],
        orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
        limit: M1_PAGE_SIZE,
      };
      if (cursor) sq.startAt = { values: [{ referenceValue: cursor }], before: false };

      const res = await fsRunQuery({ structuredQuery: sq }, apiKey);
      const docs = res.filter((r: any) => r.document).map((r: any) => r.document);
      if (docs.length === 0) {
        groupExhausted = true;
        break;
      }
      const lastDocId = (docs[docs.length - 1].name || "").split("/").pop()!;
      pagesData.push({ docs, lastDocId });
      // Advance read cursor for next page in this group. The full document
      // name is the value the existing cursor pattern uses (line 890 of
      // pre-patch original).
      cursor = doc_to_full_name(docs[docs.length - 1]);
      if (docs.length < M1_PAGE_SIZE) {
        // Last page in das_lines collection — no more pages exist.
        groupExhausted = true;
        break;
      }
    }

    if (pagesData.length === 0) {
      break;
    }

    // Build batch ops per page, dispatch in parallel.
    const groupResults = await Promise.all(pagesData.map(async (pd) => {
      const ops: BatchWriteOp[] = pd.docs.map((doc: any) => {
        const docId = (doc.name || "").split("/").pop()!;
        return {
          docId,
          fields: toFsFieldsForSnapshot(doc),
        };
      });
      return await dispatchWithRetry(snapshotName, ops);
    }));

    // Merge results
    let groupProcessed = 0;
    for (const r of groupResults) {
      groupProcessed += r.written.length + r.failed.length;
      for (const f of r.failed) {
        failedCount++;
        // Bound the persisted list at last M1_FAILED_DOC_IDS_BOUND entries.
        // failedCount tracks the true total separately.
        failedDocIds.push(f);
      }
    }
    processed += groupProcessed;
    groups++;

    // Mid-loop HALT: failed_count > pre_count * 0.05 ⇒ snapshot too corrupt
    // to be a usable recovery position. Stop wasting work, surface clearly.
    if (failedCount > preCount * M1_FAILED_RATIO_HALT) {
      halted = true;
      haltReason = `failure_ratio_exceeded (failed=${failedCount}, pre_count=${preCount}, threshold=${M1_FAILED_RATIO_HALT * 100}%)`;
      break;
    }

    // Per-group checkpoint. Cursor is already advanced to the last docId
    // in the last page of this group (set during the read phase above).
    // Persist it so a crash before next checkpoint can resume here.
    const lastDocIdThisGroup = pagesData[pagesData.length - 1].lastDocId;
    await setMigrationStatus(apiKey, {
      updated_at: nowIso(),
      M1_state: {
        snapshot_collection: snapshotName,
        pre_count: preCount,
        started_at: resuming ? (prevState.started_at || nowIso()) : (prevState.started_at || nowIso()),
        processed,
        failed_count: failedCount,
        failed_doc_ids: failedDocIds.slice(-M1_FAILED_DOC_IDS_BOUND),
        last_processed_doc_id: lastDocIdThisGroup,
        resumed_from: resuming ? prevCursor : null,
        groups,
        last_progress_at: nowIso(),
      },
    });

    if (groupExhausted) {
      break;
    }
  }

  // ── Yield path: time budget exceeded, but no completion-state writes.
  // Re-invoking phase=M1 will resume from the persisted cursor.
  if (halted && haltReason === "time_budget_exceeded") {
    const afterState = await getMigrationStatus(apiKey);
    return {
      ok: true, // ok:true so the runner records the yield as not-an-error
      phase: "M1", migration_id: req.migration_id, dry_run: false,
      before_state: beforeState, after_state: afterState,
      phase_summary: `M1 yielded after ${groups} group(s), processed=${processed}/${preCount}, failed=${failedCount}. Re-invoke phase=M1 to resume.`,
      phase_details: {
        snapshot_collection: snapshotName, processed, failed_count: failedCount,
        groups, last_processed_doc_id: cursor, halt_reason: haltReason,
      },
    };
  }

  // ── Failure-ratio HALT: write completion state with explicit halt_reason
  // and emit a HALT gate. Caller (operator) decides whether to retry.
  if (halted && haltReason && haltReason.startsWith("failure_ratio_exceeded")) {
    const gate: GateResult = {
      id: "G1", result: "HALT", evaluated_at: nowIso(),
      summary: `G1 HALT: ${haltReason}. Snapshot at ${snapshotName} is too corrupt to be a usable recovery position; do not advance to M2.`,
      details: {
        snapshot_collection: snapshotName, processed, failed_count: failedCount,
        pre_count: preCount, threshold_ratio: M1_FAILED_RATIO_HALT,
        sample_failures: failedDocIds.slice(-20), // last 20 for diagnostic
      },
    };
    await setMigrationStatus(apiKey, {
      updated_at: nowIso(),
      gate_results: { ...(beforeState?.gate_results || {}), G1: gate },
      M1_state: {
        snapshot_collection: snapshotName, pre_count: preCount,
        started_at: prevState.started_at || nowIso(),
        processed, failed_count: failedCount,
        failed_doc_ids: failedDocIds.slice(-M1_FAILED_DOC_IDS_BOUND),
        last_processed_doc_id: cursor || null,
        halted_at: nowIso(), halt_reason: haltReason,
      },
    });
    const afterState = await getMigrationStatus(apiKey);
    return {
      ok: false, phase: "M1", migration_id: req.migration_id, dry_run: false,
      before_state: beforeState, after_state: afterState,
      phase_summary: gate.summary, phase_details: gate.details, gate,
    };
  }

  // ── Reached end: evaluate G1.
  // G1 has two checks:
  //   (1) Count + write-failure parity: snapshotCount === preCount && failedCount === 0
  //   (2) Field-level sample validation: 200 docs from snapshot collection
  //       compared field-by-field against das_lines source. Mirrors G2's
  //       sampleSurvivingChunkDecompress pattern. See sampleSnapshotIntegrity below.
  //
  // Both must pass for G1=PASS. (1) failures cover whole-doc loss; (2)
  // failures cover silent field corruption (e.g., :batchWrite reported
  // success on a doc that ended up with a malformed field map).
  const snapshotCount = await fsCount(snapshotName, apiKey);
  const countCheckPassed = snapshotCount === preCount && failedCount === 0;

  // Run field-level sample validation regardless of count check — even on
  // count-failure, the sample diagnostic helps the operator understand
  // whether the failure mode is "missing rows" vs "corrupt rows" vs both.
  // Sample size = 200 (4× G2's 50; G1 is the gate that authorizes destructive
  // M2/M3 phases, so higher confidence is warranted; cost is 2 GETs × 200 = ~400
  // round-trips, trivial against M1's ~25min total).
  const sampleResult = await sampleSnapshotIntegrity(snapshotName, apiKey, 200);
  const samplePassed = sampleResult.all_match;

  const g1Pass = countCheckPassed && samplePassed;

  let summary: string;
  if (g1Pass) {
    summary = `G1 PASS: snapshot count matches (${snapshotCount} == ${preCount}); sample integrity verified (${sampleResult.matched}/${sampleResult.sampled} docs field-equal)`;
  } else if (!countCheckPassed && !samplePassed) {
    summary = `G1 HALT: BOTH count check failed (snapshot=${snapshotCount} pre_count=${preCount} delta=${snapshotCount - preCount} failed_count=${failedCount}) AND sample integrity failed (${sampleResult.matched}/${sampleResult.sampled} matched, ${sampleResult.field_mismatches.length} mismatches)`;
  } else if (!countCheckPassed) {
    summary = `G1 HALT (count): snapshot=${snapshotCount} pre_count=${preCount} delta=${snapshotCount - preCount} failed_count=${failedCount}`;
  } else {
    summary = `G1 HALT (sample integrity): ${sampleResult.matched}/${sampleResult.sampled} matched, ${sampleResult.field_mismatches.length} field-level mismatches detected`;
  }

  const gate: GateResult = {
    id: "G1",
    result: g1Pass ? "PASS" : "HALT",
    evaluated_at: nowIso(),
    summary,
    details: {
      snapshot_count: snapshotCount, das_lines_count: preCount,
      delta: snapshotCount - preCount, failed_count: failedCount,
      processed, groups,
      count_check_passed: countCheckPassed,
      sample_check_passed: samplePassed,
      sample_size: sampleResult.sampled,
      sample_matched: sampleResult.matched,
      // field_mismatches is bounded by sample size (200) so safe to inline.
      // Each entry: { doc_id, reason, expected_field?, actual_field? }
      field_mismatches: sampleResult.field_mismatches,
      // Last 20 batch-write failures, separate from field mismatches.
      // Batch-write failures = doc never made it into the snapshot at all;
      // field mismatches = doc made it but with corrupt fields.
      sample_batch_write_failures: failedDocIds.slice(-20),
    },
  };

  await setMigrationStatus(apiKey, {
    updated_at: nowIso(),
    gate_results: { ...(beforeState?.gate_results || {}), G1: gate },
    M1_state: {
      snapshot_collection: snapshotName, pre_count: preCount,
      started_at: prevState.started_at || nowIso(),
      snapshot_count: snapshotCount, processed,
      failed_count: failedCount,
      failed_doc_ids: failedDocIds.slice(-M1_FAILED_DOC_IDS_BOUND),
      last_processed_doc_id: cursor || null,
      groups, completed_at: nowIso(),
    },
  });
  const afterState = await getMigrationStatus(apiKey);

  return {
    ok: g1Pass, phase: "M1", migration_id: req.migration_id, dry_run: false,
    before_state: beforeState, after_state: afterState,
    phase_summary: gate.summary,
    phase_details: { snapshot_collection: snapshotName, processed, failed_count: failedCount, groups },
    gate,
  };
}

/**
 * v2.53.6 — Encode a das_lines doc's fields for snapshot write, preserving
 * the original Firestore-typed field map verbatim and adding the two
 * snapshot markers (snapshot_source, snapshot_at). Used by the M1
 * batched-write path; equivalent to the pre-patch fsBatchPatchDoc body's
 * `{...unwrapDocFields(doc), snapshot_source, snapshot_at}` followed by
 * per-leaf toFsValue, but skips the unwrap+rewrap roundtrip — original
 * field encoding is reused directly.
 */
function toFsFieldsForSnapshot(doc: any): Record<string, any> {
  const out: Record<string, any> = {};
  // Copy original Firestore-encoded fields verbatim — no unwrap/rewrap.
  // Preserves timestampValue, doubleValue precision, integerValue string
  // form, mapValue/arrayValue nesting, exactly as Firestore returned them.
  for (const [k, v] of Object.entries(doc?.fields || {})) {
    out[k] = v;
  }
  out.snapshot_source = toFsValue("das_lines");
  out.snapshot_at = toFsValue(nowIso());
  return out;
}

function unwrapDocFields(doc: any): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(doc?.fields || {})) out[k] = unwrapField(v);
  return out;
}

function doc_to_full_name(doc: any): string {
  // doc.name is e.g. "projects/davismarginiq/databases/(default)/documents/das_lines/abc"
  return doc.name;
}

// ─── Phase: M2 (delete v2.53.1 corrupted, Gate G2) ──────────────────────────

async function phaseM2(req: PhaseRequest, siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  return await phaseDeleteCohort({
    phaseId: "M2",
    gateId: "G2",
    req, siteOrigin, apiKey,
    description: "v2.53.1 corrupted rows",
    buildFilter: () => ({
      fieldFilter: {
        field: { fieldPath: "ingested_at" },
        op: "GREATER_THAN_OR_EQUAL",
        value: { stringValue: "2026-05-05T17:25:00Z" },
      },
    }),
    extraJsFilter: (doc) => {
      const ib = unwrapField(doc.fields?.ingested_by) || "";
      return typeof ib === "string" && ib.includes("@2.53.1");
    },
    expectedDeletedTarget: M2_DELETED_TARGET,
    expectedDeletedTolerance: M2_DELETED_TOLERANCE,
    extraDeletes: async (apiKey, dry, opts) => {
      // 146 entries from uline_processed_emails (operator-supplied)
      const list: string[] = Array.isArray(opts?.uline_processed_emails_to_delete)
        ? opts.uline_processed_emails_to_delete : [];
      const wouldFetchList = opts?.would_fetch_list === true;
      // Q3: dry-run "would-fetch-list" mode — query and return candidates
      if (wouldFetchList) {
        const sqe: any = {
          from: [{ collectionId: "uline_processed_emails" }],
          where: {
            fieldFilter: {
              field: { fieldPath: "auto_ingest_run_id" },
              op: "GREATER_THAN_OR_EQUAL",
              value: { stringValue: "uline_auto_2026-05-05" },
            },
          },
          limit: 500,
        };
        const res = await fsRunQuery({ structuredQuery: sqe }, apiKey);
        const ids = res.filter((r: any) => r.document).map((r: any) => (r.document.name || "").split("/").pop());
        return { would_fetch_list: true, candidates: ids, count: ids.length };
      }
      let deleted = 0, failed = 0;
      if (!dry) {
        for (const id of list) {
          const ok = await fsDeleteDoc("uline_processed_emails", id, apiKey);
          if (ok) deleted++; else failed++;
        }
        // marginiq_config/uline_historical_backfill_status
        await fsDeleteDoc("marginiq_config", "uline_historical_backfill_status", apiKey);
      }
      return { uline_processed_emails: { provided: list.length, deleted, failed }, dry };
    },
  });
}

// ─── Phase: M3 (delete pre-Phase-1 rows, Gate G3) ───────────────────────────

async function phaseM3(req: PhaseRequest, siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  return await phaseDeleteCohort({
    phaseId: "M3",
    gateId: "G3",
    req, siteOrigin, apiKey,
    description: "pre-Phase-1 long-docId rows",
    buildFilter: () => null, // Full scan needed; filter in JS
    extraJsFilter: (doc) => {
      const docId: string = (doc.name || "").split("/").pop() || "";
      // Pre-Phase-1 long-docId pattern: starts with "uline__" and has 3 underscore segments
      if (docId.startsWith("uline__")) return true;
      // Or rows lacking ingested_by but having source_file_id resolving to Phase-1-era source_files_raw
      const ib = unwrapField(doc.fields?.ingested_by);
      if (!ib) return true; // missing ingested_by → very likely pre-Phase-1
      return false;
    },
    // M3 has no fixed deleted-count target; gate is smoke-reparse-based instead.
    expectedDeletedTarget: 0,
    expectedDeletedTolerance: 1,
    skipCountGate: true,
    extraDeletes: async () => ({}),
    finalGateOverride: async (req, siteOrigin, apiKey, deletedCount) => {
      // G3: smoke-reparse one file via marginiq-reparse; verify Model B docId
      const sq: any = {
        from: [{ collectionId: "source_files_raw" }],
        where: {
          compositeFilter: {
            op: "AND",
            filters: [
              { fieldFilter: { field: { fieldPath: "source" }, op: "EQUAL", value: { stringValue: "uline" } } },
              { fieldFilter: { field: { fieldPath: "state" }, op: "EQUAL", value: { stringValue: "ingested" } } },
            ],
          },
        },
        limit: 1,
      };
      const res = await fsRunQuery({ structuredQuery: sq }, apiKey);
      const doc = res.find((r: any) => r.document)?.document;
      const fileId = doc ? (doc.name || "").split("/").pop() : null;
      if (!fileId) {
        return {
          id: "G3", result: "HALT" as const, evaluated_at: nowIso(),
          summary: "G3 HALT: no uline source_files_raw with state=ingested available for smoke reparse",
          details: { deleted_count: deletedCount },
        };
      }
      const resp = await invokeLocal(siteOrigin, "/.netlify/functions/marginiq-reparse", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_id: fileId, action: "reparse" }),
      });
      const ok = resp.data?.ok === true;
      // Verify Model B docId scheme — sample one of the resulting das_lines
      let modelBVerified = false;
      if (ok) {
        const sample: any = {
          from: [{ collectionId: "das_lines" }],
          where: { fieldFilter: { field: { fieldPath: "source_file_id" }, op: "EQUAL", value: { stringValue: fileId } } },
          limit: 1,
        };
        const r2 = await fsRunQuery({ structuredQuery: sample }, apiKey);
        const sampleDoc = r2.find((r: any) => r.document)?.document;
        const sampleId: string = sampleDoc ? (sampleDoc.name || "").split("/").pop() : "";
        // Model B docId: {pro}_{pu_date||"nodate"}_{service_type}
        modelBVerified = /^[A-Za-z0-9-]+_[\d-]+_[A-Z0-9]+$/i.test(sampleId);
      }
      const passed = ok && modelBVerified;
      return {
        id: "G3", result: passed ? "PASS" : "HALT", evaluated_at: nowIso(),
        summary: passed
          ? `G3 PASS: smoke-reparse ${fileId} ok, Model B docId verified`
          : `G3 HALT: smoke-reparse ok=${ok} model_b_verified=${modelBVerified}`,
        details: { smoke_reparse_file_id: fileId, reparse_response: resp.data, model_b_verified: modelBVerified, deleted_count: deletedCount },
      };
    },
  });
}

// ─── Shared deletion-cohort engine for M2/M3 ────────────────────────────────

interface CohortPhaseSpec {
  phaseId: "M2" | "M3";
  gateId: "G2" | "G3";
  req: PhaseRequest;
  siteOrigin: string;
  apiKey: string;
  description: string;
  buildFilter: () => any | null;
  extraJsFilter: (doc: any) => boolean;
  expectedDeletedTarget: number;
  expectedDeletedTolerance: number;
  skipCountGate?: boolean;
  extraDeletes: (apiKey: string, dryRun: boolean, opts: any) => Promise<any>;
  finalGateOverride?: (req: PhaseRequest, siteOrigin: string, apiKey: string, deletedCount: number) => Promise<GateResult>;
}

async function phaseDeleteCohort(spec: CohortPhaseSpec): Promise<PhaseResult> {
  const { phaseId, gateId, req, siteOrigin, apiKey } = spec;
  const beforeState = await getMigrationStatus(apiKey);
  const dryRun = req.dry_run === true;
  const startedAt = beforeState?.started_at || nowIso();

  // Concurrency check (runs in dry-run too — read-only)
  const offender = await concurrencyCheck(startedAt, req.migration_id, apiKey);
  if (offender) {
    if (!dryRun) {
      const gate: GateResult = {
        id: gateId, result: "ABORT", evaluated_at: nowIso(),
        summary: `${phaseId} ABORT: cron race detected (das_lines/${offender.offending_doc} ingested_at=${offender.ingested_at} ingested_by=${offender.ingested_by})`,
        details: offender,
      };
      await setMigrationStatus(apiKey, {
        in_migration: false,
        current_phase: "aborted",
        aborted_at: nowIso(),
        abort_reason: gate.summary,
        updated_at: nowIso(),
        gate_results: { ...(beforeState?.gate_results || {}), [gateId]: gate },
      });
      return {
        ok: false, phase: phaseId, migration_id: req.migration_id, dry_run: false,
        before_state: beforeState, after_state: await getMigrationStatus(apiKey),
        phase_summary: gate.summary, phase_details: offender, gate,
      };
    }
    // dry-run: surface as warning but don't abort state
    return {
      ok: false, phase: phaseId, migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: `[DRY RUN] ${phaseId} concurrency-check found cron race: would ABORT`,
      phase_details: { would_abort_due_to: offender },
    };
  }

  // Walk and delete (or count for dry-run)
  const PAGE = 200;
  let cursor: string | undefined;
  let scanned = 0, matched = 0, deleted = 0, deleteFailures = 0;
  const filter = spec.buildFilter();

  // For dry-run with would_fetch_list, short-circuit to extraDeletes
  if (dryRun && req.options?.would_fetch_list === true) {
    const extra = await spec.extraDeletes(apiKey, true, req.options);
    return {
      ok: true, phase: phaseId, migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: `[DRY RUN] ${phaseId} would-fetch-list mode`,
      phase_details: { extra },
    };
  }

  while (true) {
    const sq: any = {
      from: [{ collectionId: "das_lines" }],
      orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
      limit: PAGE,
    };
    if (filter) sq.where = filter;
    if (cursor) sq.startAt = { values: [{ referenceValue: cursor }], before: false };

    const res = await fsRunQuery({ structuredQuery: sq }, apiKey);
    const docs = res.filter((r: any) => r.document).map((r: any) => r.document);
    if (docs.length === 0) break;
    scanned += docs.length;

    for (const doc of docs) {
      if (!spec.extraJsFilter(doc)) continue;
      matched++;
      if (!dryRun) {
        const docId = (doc.name || "").split("/").pop()!;
        const ok = await fsDeleteDoc("das_lines", docId, apiKey);
        if (ok) deleted++; else deleteFailures++;
      }
    }

    if (docs.length < PAGE) break;
    cursor = docs[docs.length - 1].name;
  }

  // Extra deletes (e.g. uline_processed_emails for M2)
  const extra = await spec.extraDeletes(apiKey, dryRun, req.options || {});

  // Sample 50 surviving rows + verify chunks decompress (M2 only — M3 uses smoke-reparse gate)
  let sampleResult: any = null;
  if (!spec.skipCountGate && !dryRun) {
    sampleResult = await sampleSurvivingChunkDecompress(apiKey, 50);
  }

  // Build gate result
  let gate: GateResult;
  if (spec.finalGateOverride) {
    gate = await spec.finalGateOverride(req, siteOrigin, apiKey, deleted);
  } else if (spec.skipCountGate) {
    gate = {
      id: gateId, result: deleteFailures === 0 ? "PASS" : "HALT",
      evaluated_at: nowIso(),
      summary: `${gateId}: deleted=${deleted} failures=${deleteFailures}`,
      details: { scanned, matched, deleted, deleteFailures },
    };
  } else {
    const target = spec.expectedDeletedTarget;
    const tol = spec.expectedDeletedTolerance;
    const lower = target * (1 - tol);
    const upper = target * (1 + tol);
    const inRange = deleted >= lower && deleted <= upper;
    const sampleOk = sampleResult ? sampleResult.all_decompressed === true : false;
    const passed = inRange && sampleOk && deleteFailures === 0;
    gate = {
      id: gateId, result: passed ? "PASS" : "HALT",
      evaluated_at: nowIso(),
      summary: passed
        ? `${gateId} PASS: deleted=${deleted} (target ±${tol*100}%) sample-50 chunks decompressed`
        : `${gateId} HALT: deleted=${deleted} target=${target} ±${tol*100}% sample_ok=${sampleOk} failures=${deleteFailures}`,
      details: { deleted, target, lower, upper, sample: sampleResult, deleteFailures, scanned, matched },
    };
  }

  if (dryRun) {
    return {
      ok: true, phase: phaseId, migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: `[DRY RUN] ${phaseId} would delete ${matched} ${spec.description} (scanned ${scanned})`,
      phase_details: { would_do: { scanned, matched, target: spec.expectedDeletedTarget }, extra },
    };
  }

  await setMigrationStatus(apiKey, {
    current_phase: phaseId,
    updated_at: nowIso(),
    gate_results: { ...(beforeState?.gate_results || {}), [gateId]: gate },
    [`${phaseId}_state`]: { scanned, matched, deleted, deleteFailures, sample: sampleResult, extra, completed_at: nowIso() },
  });

  const afterState = await getMigrationStatus(apiKey);
  return {
    ok: gate.result === "PASS", phase: phaseId, migration_id: req.migration_id, dry_run: false,
    before_state: beforeState, after_state: afterState,
    phase_summary: gate.summary,
    phase_details: { scanned, matched, deleted, deleteFailures, extra, sample: sampleResult },
    gate,
  };
}

/**
 * v2.53.6 — Deep equality with order-insensitive comparison on object keys.
 * Used by sampleSnapshotIntegrity to compare unwrapped Firestore field
 * values where nested mapValue field ordering may differ between separate
 * reads (top-level field insertion order is preserved by Firestore's REST
 * surface, but nested map sub-key ordering across separate reads is not
 * a guarantee we want to bet G1 on).
 *
 * Arrays compared order-sensitively (they're semantically ordered).
 * Objects compared by key set + per-key recursive equality.
 * Primitives via ===.
 *
 * Not exported; G1 sampler is the only caller. Keep co-located.
 */
function deepEqualOrderInsensitive(a: any, b: any): boolean {
  if (a === b) return true;
  if (a === null || b === null) return a === b;
  if (typeof a !== typeof b) return false;
  if (typeof a !== "object") return a === b;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a)) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!deepEqualOrderInsensitive(a[i], b[i])) return false;
    }
    return true;
  }
  const aKeys = Object.keys(a);
  const bKeys = Object.keys(b);
  if (aKeys.length !== bKeys.length) return false;
  for (const k of aKeys) {
    if (!Object.prototype.hasOwnProperty.call(b, k)) return false;
    if (!deepEqualOrderInsensitive(a[k], b[k])) return false;
  }
  return true;
}

/**
 * v2.53.6 — G1 sample-integrity validator. Mirrors sampleSurvivingChunkDecompress
 * (G2's sampler) in shape, but compares snapshot docs to their das_lines
 * source counterparts field-by-field rather than verifying chunk decompression.
 *
 * Pattern: pull first n docs from the snapshot collection ordered by docId
 * ASC. For each, GET the same docId from das_lines, compare unwrapped field
 * maps. Mismatches (other than the two snapshot markers `snapshot_source`
 * and `snapshot_at`, which are expected) push to field_mismatches[] with
 * a short reason string and `continue`.
 *
 * Strict binary contract: any mismatch → all_match:false → G1 HALT.
 * Sample size n=200 (4× G2's 50) chosen because G1 authorizes destructive
 * M2/M3 phases — higher confidence warranted; cost is 2 GETs × n = 400
 * round-trips, trivial relative to M1's wall-clock.
 *
 * NOTE on "first n by docId": matches G2's precedent. Detects systematic
 * field drift well; doesn't catch sub-1% sparse corruption reliably even
 * at n=200 (which is fine — sparse corruption that bypasses :batchWrite's
 * status reporting is a Firestore-internal pathology that we can't
 * meaningfully defend against here without doing a full collection scan).
 */
async function sampleSnapshotIntegrity(
  snapshotCollection: string,
  apiKey: string,
  n: number,
): Promise<{
  sampled: number;
  matched: number;
  all_match: boolean;
  field_mismatches: Array<{ doc_id: string; reason: string }>;
}> {
  // Pull first n docs from snapshot collection ordered ASC.
  const sq: any = {
    from: [{ collectionId: snapshotCollection }],
    orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
    limit: n,
  };
  const res = await fsRunQuery({ structuredQuery: sq }, apiKey);
  const snapshotDocs = res.filter((r: any) => r.document).map((r: any) => r.document);

  const fieldMismatches: Array<{ doc_id: string; reason: string }> = [];
  let matched = 0;

  // Snapshot markers added by toFsFieldsForSnapshot — expected to differ
  // from das_lines source (das_lines doesn't have them) and excluded
  // from comparison.
  const SNAPSHOT_MARKER_FIELDS = new Set(["snapshot_source", "snapshot_at"]);

  for (const snapDoc of snapshotDocs) {
    const docId = (snapDoc.name || "").split("/").pop()!;
    const sourceDoc = await fsGetDoc("das_lines", docId, apiKey);
    if (!sourceDoc) {
      fieldMismatches.push({ doc_id: docId, reason: "source das_lines/{docId} not found (snapshot has orphan)" });
      continue;
    }

    const snapFields = (snapDoc.fields || {}) as Record<string, any>;
    const sourceFields = (sourceDoc.fields || {}) as Record<string, any>;

    // Build the set of source field names; snapshot must contain all of them
    // verbatim (since toFsFieldsForSnapshot copies fields-by-reference) plus
    // the two snapshot markers and nothing else.
    const sourceKeys = new Set(Object.keys(sourceFields));
    const snapKeys = new Set(Object.keys(snapFields));

    // Check no source field is missing from snapshot.
    let mismatchReason: string | null = null;
    for (const k of sourceKeys) {
      if (!snapKeys.has(k)) {
        mismatchReason = `field "${k}" missing from snapshot`;
        break;
      }
      // Compare unwrapped values for equality. unwrapField handles
      // every Firestore type used in das_lines (string/integer/double/
      // boolean/null/timestamp/array/map). Use order-insensitive deep
      // equality for objects (avoids false-positive HALTs if Firestore
      // ever re-orders nested mapValue sub-keys across separate reads —
      // top-level field order is preserved verbatim, sub-key order isn't
      // a guarantee we want to bet G1 on). Arrays compared order-sensitively.
      // Diagnostic message still uses JSON.stringify so operator sees the
      // actual divergent values.
      const srcVal = unwrapField(sourceFields[k]);
      const snapVal = unwrapField(snapFields[k]);
      if (!deepEqualOrderInsensitive(srcVal, snapVal)) {
        mismatchReason = `field "${k}" value differs (source=${JSON.stringify(srcVal).slice(0, 80)}, snap=${JSON.stringify(snapVal).slice(0, 80)})`;
        break;
      }
    }
    if (mismatchReason) {
      fieldMismatches.push({ doc_id: docId, reason: mismatchReason });
      continue;
    }

    // Check snapshot has no extra fields beyond source + the two markers.
    for (const k of snapKeys) {
      if (sourceKeys.has(k)) continue;
      if (SNAPSHOT_MARKER_FIELDS.has(k)) continue;
      mismatchReason = `snapshot has unexpected extra field "${k}"`;
      break;
    }
    if (mismatchReason) {
      fieldMismatches.push({ doc_id: docId, reason: mismatchReason });
      continue;
    }

    // Verify the two snapshot markers are present and well-formed.
    if (!snapKeys.has("snapshot_source") || unwrapField(snapFields.snapshot_source) !== "das_lines") {
      fieldMismatches.push({ doc_id: docId, reason: `snapshot_source missing or != "das_lines"` });
      continue;
    }
    if (!snapKeys.has("snapshot_at") || typeof unwrapField(snapFields.snapshot_at) !== "string") {
      fieldMismatches.push({ doc_id: docId, reason: `snapshot_at missing or not a string` });
      continue;
    }

    matched++;
  }

  return {
    sampled: snapshotDocs.length,
    matched,
    all_match: fieldMismatches.length === 0 && snapshotDocs.length > 0,
    field_mismatches: fieldMismatches,
  };
}

async function sampleSurvivingChunkDecompress(apiKey: string, n: number): Promise<{
  sampled: number;
  source_files_raw_resolved: number;
  chunks_decompressed: number;
  all_decompressed: boolean;
  failures: any[];
}> {
  // Sample n surviving das_lines docs. For each, verify:
  //   1. source_file_id is set
  //   2. source_files_raw/{source_file_id} resolves
  //   3. source_file_chunks/{source_file_id}__000 exists
  //   4. Its data_b64 field is present and base64-decodes
  //   5. Decoded bytes start with gzip magic 0x1f 0x8b
  //   6. Full gunzipSync succeeds (catches CRC and length-mismatch corruption)
  //   7. Decompressed bytes are non-empty
  //
  // Strict binary contract (Q3): any failure → all_decompressed:false → G2 HALT.
  // Failure list is bounded by the sample size (n=50) so the HALT response
  // can include the full diagnostic for operator inspection.
  const sq: any = {
    from: [{ collectionId: "das_lines" }],
    orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
    limit: n,
  };
  const res = await fsRunQuery({ structuredQuery: sq }, apiKey);
  const docs = res.filter((r: any) => r.document).map((r: any) => r.document);
  const failures: any[] = [];
  let resolved = 0;
  let decompressed = 0;

  for (const d of docs) {
    const docId = (d.name || "").split("/").pop()!;
    const sfid = unwrapField(d.fields?.source_file_id);
    if (!sfid) {
      failures.push({ doc: docId, reason: "no source_file_id" });
      continue;
    }
    const src = await fsGetDoc("source_files_raw", String(sfid), apiKey);
    if (!src) {
      failures.push({ doc: docId, reason: `source_files_raw/${sfid} not found` });
      continue;
    }
    resolved++;

    const firstChunkId = `${sfid}__000`;
    const chunk = await fsGetDoc("source_file_chunks", firstChunkId, apiKey);
    if (!chunk) {
      failures.push({ doc: docId, reason: `source_file_chunks/${firstChunkId} missing` });
      continue;
    }
    const dataB64 = unwrapField(chunk.fields?.data_b64);
    if (typeof dataB64 !== "string" || dataB64.length === 0) {
      failures.push({ doc: docId, reason: `chunk ${firstChunkId} has no data_b64` });
      continue;
    }
    let buf: Buffer;
    try {
      buf = Buffer.from(dataB64, "base64");
    } catch (e: any) {
      failures.push({ doc: docId, reason: `chunk ${firstChunkId} base64 decode failed: ${e?.message || e}` });
      continue;
    }
    if (buf.length < 2 || buf[0] !== 0x1f || buf[1] !== 0x8b) {
      const m0 = buf[0]?.toString(16) || "??";
      const m1 = buf[1]?.toString(16) || "??";
      failures.push({ doc: docId, reason: `chunk ${firstChunkId} bad gzip magic 0x${m0} 0x${m1}` });
      continue;
    }
    try {
      const out = gunzipSync(buf);
      if (out.length === 0) {
        failures.push({ doc: docId, reason: `chunk ${firstChunkId} decompressed to 0 bytes` });
        continue;
      }
      decompressed++;
    } catch (e: any) {
      failures.push({ doc: docId, reason: `chunk ${firstChunkId} gunzip error: ${e?.message || e}` });
    }
  }

  return {
    sampled: docs.length,
    source_files_raw_resolved: resolved,
    chunks_decompressed: decompressed,
    all_decompressed: failures.length === 0,
    failures,
  };
}

// ─── Phase: M4 (mass reparse, Gate G4) ──────────────────────────────────────

async function phaseM4(req: PhaseRequest, siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  const beforeState = await getMigrationStatus(apiKey);
  const dryRun = req.dry_run === true;
  const parallelism = clampInt(req.parallelism, 1, 4, 1);
  const continueOnError = req.continue_on_error === true;

  // Build target list (idempotent — same query each invocation)
  const sq: any = {
    from: [{ collectionId: "source_files_raw" }],
    where: {
      compositeFilter: {
        op: "AND",
        filters: [
          { fieldFilter: { field: { fieldPath: "source" }, op: "EQUAL", value: { stringValue: "uline" } } },
          { fieldFilter: { field: { fieldPath: "state" }, op: "EQUAL", value: { stringValue: "ingested" } } },
        ],
      },
    },
    orderBy: [{ field: { fieldPath: "__name__" }, direction: "ASCENDING" }],
    limit: 1500,
  };
  const res = await fsRunQuery({ structuredQuery: sq }, apiKey);
  const allFiles: string[] = res
    .filter((r: any) => r.document)
    .map((r: any) => (r.document.name || "").split("/").pop()!);

  const prevState = beforeState?.M4_state || {};
  const cursorIndex = Number(prevState.cursor_index || 0);
  const filesCompleted = Array.isArray(prevState.files_completed_list) ? prevState.files_completed_list : [];
  const filesFailed = Array.isArray(prevState.failed_file_ids) ? prevState.failed_file_ids : [];

  if (dryRun) {
    return {
      ok: true, phase: "M4", migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: `[DRY RUN] M4 would reparse ${allFiles.length} uline files (resume from index ${cursorIndex}; parallelism=${parallelism})`,
      phase_details: {
        would_do: { total_files: allFiles.length, cursor_index: cursorIndex, files_remaining: allFiles.length - cursorIndex, parallelism, continue_on_error: continueOnError },
      },
    };
  }

  // Time-budget loop
  const startMs = Date.now();
  let i = cursorIndex;
  let halted = false;
  let haltReason: string | null = null;
  const newCompleted = [...filesCompleted];
  const newFailed = [...filesFailed];

  while (i < allFiles.length) {
    if (Date.now() - startMs > M4_TIME_BUDGET_MS) {
      haltReason = "time_budget_exceeded";
      break;
    }

    // Process one batch of `parallelism` files
    const batch = allFiles.slice(i, i + parallelism);
    const results = await Promise.all(batch.map(async (fileId) => {
      try {
        const resp = await invokeLocal(siteOrigin, "/.netlify/functions/marginiq-reparse", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ file_id: fileId, action: "reparse" }),
        });
        return { fileId, ok: resp.data?.ok === true, status: resp.status, transient: isTransientError(resp.status) };
      } catch (e: any) {
        return { fileId, ok: false, status: 0, transient: true, error: e?.message || String(e) };
      }
    }));

    for (const r of results) {
      if (r.ok) {
        newCompleted.push(r.fileId);
      } else {
        newFailed.push(r.fileId);
        if (!continueOnError) {
          halted = true;
          haltReason = `single-file-failure (${r.fileId} status=${r.status} transient=${r.transient})`;
          break;
        }
        if (continueOnError && !r.transient) {
          halted = true;
          haltReason = `non-transient failure (${r.fileId} status=${r.status})`;
          break;
        }
      }
    }
    if (halted) break;
    i += batch.length;

    // Per-batch checkpoint
    await setMigrationStatus(apiKey, {
      updated_at: nowIso(),
      M4_state: {
        total_files: allFiles.length,
        cursor_index: i,
        files_completed_count: newCompleted.length,
        files_failed_count: newFailed.length,
        files_completed_list: newCompleted.slice(-100), // last 100 only to bound size
        failed_file_ids: newFailed,
        parallelism, continue_on_error: continueOnError,
        last_progress_at: nowIso(),
      },
    });
  }

  const reachedEnd = i >= allFiles.length;
  // ── Gate G4: only evaluate if M4 reached the end ──────────────────────────
  let gate: GateResult | undefined;
  if (reachedEnd && !halted) {
    gate = await evaluateG4(siteOrigin);
  }

  const m4State = {
    total_files: allFiles.length,
    cursor_index: i,
    files_completed_count: newCompleted.length,
    files_failed_count: newFailed.length,
    failed_file_ids: newFailed,
    parallelism, continue_on_error: continueOnError,
    halted, halt_reason: haltReason,
    completed_at: reachedEnd ? nowIso() : null,
    elapsed_ms: Date.now() - startMs,
  };

  await setMigrationStatus(apiKey, {
    current_phase: "M4",
    updated_at: nowIso(),
    M4_state: m4State,
    ...(gate ? { gate_results: { ...(beforeState?.gate_results || {}), G4: gate } } : {}),
  });

  const afterState = await getMigrationStatus(apiKey);
  return {
    ok: gate ? gate.result === "PASS" : !halted,
    phase: "M4", migration_id: req.migration_id, dry_run: false,
    before_state: beforeState, after_state: afterState,
    phase_summary: reachedEnd
      ? (gate ? gate.summary : `M4 finished but gate evaluation failed`)
      : `M4 yielded after ${i - cursorIndex} files (cursor ${i}/${allFiles.length}, halted=${halted}, reason=${haltReason}). Re-invoke phase=M4 to resume.`,
    phase_details: m4State,
    gate,
  };
}

function clampInt(v: any, min: number, max: number, fallback: number): number {
  const n = parseInt(String(v ?? ""), 10);
  if (Number.isNaN(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function isTransientError(status: number): boolean {
  return status === 0 || status === 408 || status === 429 || status === 502 || status === 503 || status === 504;
}

async function evaluateG4(siteOrigin: string): Promise<GateResult> {
  const checks = ["1A", "2", "2B", "6"];
  const results: any[] = [];
  for (const c of checks) {
    const resp = await invokeLocal(siteOrigin, `/.netlify/functions/marginiq-health-check?check=${c}&full=1&force=1`, { method: "POST" });
    results.push({ check: c, status: resp.data?.status, summary: resp.data?.summary });
  }
  // Regression checks
  const c3 = await invokeLocal(siteOrigin, "/.netlify/functions/marginiq-health-check?check=3&force=1", { method: "POST" });
  results.push({ check: "3", status: c3.data?.status, summary: c3.data?.summary });
  const c5 = await invokeLocal(siteOrigin, "/.netlify/functions/marginiq-health-check?check=5&full=1&force=1", { method: "POST" });
  results.push({ check: "5", status: c5.data?.status, summary: c5.data?.summary });

  const allPass = results.every(r => r.status === "PASS");
  return {
    id: "G4",
    result: allPass ? "PASS" : "HALT",
    evaluated_at: nowIso(),
    summary: allPass
      ? "G4 PASS: 1A, 2, 2B, 6 (full) and 3, 5 regression all PASS"
      : `G4 HALT: ${results.filter(r => r.status !== "PASS").map(r => `${r.check}=${r.status}`).join(", ")}`,
    details: { results },
  };
}

// ─── Phase: M5 (sign-off + retention metadata) ──────────────────────────────

const M5_RETENTION_DAYS = 30;
const M5_RETENTION_MS = M5_RETENTION_DAYS * 24 * 60 * 60 * 1000;

async function phaseM5(req: PhaseRequest, _siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  const beforeState = await getMigrationStatus(apiKey);
  const dryRun = req.dry_run === true;

  // Defensive: M5 should only be reachable after M1 wrote snapshot_collection
  // and Gate G1 PASSed. If either is missing, something is genuinely off (M1
  // bug, manual Firestore edit, or out-of-band field deletion). HALT with a
  // diagnostic that includes the G1 gate result so the operator can see
  // whether G1 actually passed (per Q5 refinement).
  const snapshotCollection = beforeState?.snapshot_collection;
  if (!snapshotCollection || typeof snapshotCollection !== "string") {
    const g1 = beforeState?.gate_results?.G1;
    const summary = `M5 HALT: migration_status.snapshot_collection is missing/invalid. ` +
      `Cannot write retention metadata without a target collection name. ` +
      `G1 gate state: ${g1?.result ?? "(no record)"}. ` +
      (g1?.result === "PASS"
        ? `G1 reported PASS so M1 should have written this field — investigate M1 logic or check for manual edits.`
        : `G1 was not PASS, so M5 should not have been reachable — investigate phase ordering enforcement.`);
    return {
      ok: false, phase: "M5", migration_id: req.migration_id, dry_run: dryRun,
      before_state: beforeState, after_state: beforeState,
      phase_summary: summary,
      phase_details: {
        snapshot_collection: snapshotCollection ?? null,
        G1_gate_result: g1 ?? null,
        diagnostic: "snapshot_collection missing despite phase ordering gating M5 on G1=PASS",
      },
    };
  }

  // Compute retention timestamp (Q2: simple 30 calendar days, ms arithmetic)
  const completedAt = nowIso();
  const retentionAfter = new Date(Date.parse(completedAt) + M5_RETENTION_MS).toISOString();

  if (dryRun) {
    return {
      ok: true, phase: "M5", migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: `[DRY RUN] M5 would sign off: snapshot=${snapshotCollection}, eligible_for_deletion_after=${retentionAfter}`,
      phase_details: {
        would_do: {
          in_migration: false,
          current_phase: "complete",
          completed_at: completedAt,
          snapshot_collection: snapshotCollection,
          snapshot_eligible_for_deletion_after: retentionAfter,
          retention_days: M5_RETENTION_DAYS,
        },
      },
    };
  }

  await setMigrationStatus(apiKey, {
    in_migration: false,
    current_phase: "complete",
    completed_at: completedAt,
    snapshot_collection: snapshotCollection,                 // re-confirm
    snapshot_eligible_for_deletion_after: retentionAfter,
    retention_days: M5_RETENTION_DAYS,
    updated_at: completedAt,
  });
  const afterState = await getMigrationStatus(apiKey);
  return {
    ok: true, phase: "M5", migration_id: req.migration_id, dry_run: false,
    before_state: beforeState, after_state: afterState,
    phase_summary: `M5 sign-off complete at ${completedAt}. Snapshot ${snapshotCollection} eligible for deletion after ${retentionAfter} (${M5_RETENTION_DAYS} days).`,
    phase_details: {
      completed_at: completedAt,
      snapshot_collection: snapshotCollection,
      snapshot_eligible_for_deletion_after: retentionAfter,
      retention_days: M5_RETENTION_DAYS,
    },
  };
}

// ─── Phase: M6 (drain — synthetic + cronfire replay) ────────────────────────

async function phaseM6(req: PhaseRequest, siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  const beforeState = await getMigrationStatus(apiKey);
  const dryRun = req.dry_run === true;

  if (dryRun) {
    // Walk queues per source to count what would happen, with forensic
    // timestamps so the operator can preview the cronfire window that
    // would be replayed.
    const perSource: Record<string, any> = {};
    let totalSynthetic = 0, totalCronfire = 0;
    for (const src of ["uline", "ddis", "nuvizz"] as const) {
      const entries = await listHoldingQueue(src, apiKey);
      const synthetic = entries.filter(e => e.is_preflight_sentinel === true);
      const cronfire = entries.filter(e => e.is_preflight_sentinel !== true);
      const triggered = cronfire
        .map(e => (typeof e.triggered_at === "string" ? e.triggered_at : ""))
        .filter(t => t.length > 0)
        .sort();
      perSource[src] = {
        synthetic_count: synthetic.length,
        cronfire_count: cronfire.length,
        cronfire_first_triggered_at: triggered[0] || null,
        cronfire_last_triggered_at: triggered[triggered.length - 1] || null,
      };
      totalSynthetic += synthetic.length;
      totalCronfire += cronfire.length;
    }
    return {
      ok: true, phase: "M6", migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: `[DRY RUN] M6 would drain ${totalSynthetic} synthetic sentinels and replay ${totalCronfire} cronfire entries via auto-ingest invocations`,
      phase_details: {
        would_do: {
          synthetic_total: totalSynthetic,
          cronfire_total: totalCronfire,
          replay_invocations: Object.values(perSource).filter((s: any) => s.cronfire_count > 0).length,
          per_source: perSource,
        },
      },
    };
  }

  // Sequential: synthetic drain first (cheap, deterministic), then cronfire
  // replay (involves auto-ingest HTTP calls). See replayCronfireSentinels
  // docstring for runtime expectations under the 13-min background budget.
  const synthetic = await drainSyntheticSentinels(apiKey);
  const cronfire = await replayCronfireSentinels(siteOrigin, apiKey);

  await setMigrationStatus(apiKey, {
    updated_at: nowIso(),
    drain_state: {
      synthetic,
      cronfire,
      completed_at: nowIso(),
      drained_by: "4c-full",
    },
  });
  const afterState = await getMigrationStatus(apiKey);

  // ok=true if everything cleaned up successfully. Synthetic failures or
  // cronfire replay failures both lower this; the per_source breakdown in
  // drain_state.cronfire.per_source tells the operator which sources to
  // manually retry.
  const ok = synthetic.failed === 0
    && cronfire.replays_failed === 0
    && cronfire.sentinels_left_in_place === 0;

  const summary =
    `M6 drained ${synthetic.skipped_synthetic} synthetic sentinels; ` +
    `replayed ${cronfire.replays_ok}/${cronfire.replays_attempted} cronfire sources ` +
    `(${cronfire.sentinels_deleted} sentinels deleted, ${cronfire.sentinels_left_in_place} left in place)`;

  return {
    ok, phase: "M6", migration_id: req.migration_id, dry_run: false,
    before_state: beforeState, after_state: afterState,
    phase_summary: summary,
    phase_details: { synthetic, cronfire },
  };
}

// ─── Phase: abort ───────────────────────────────────────────────────────────

async function phaseAbort(req: PhaseRequest, _siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  const beforeState = await getMigrationStatus(apiKey);
  const dryRun = req.dry_run === true;
  if (dryRun) {
    return {
      ok: true, phase: "abort", migration_id: req.migration_id, dry_run: true,
      before_state: beforeState, after_state: beforeState,
      phase_summary: "[DRY RUN] abort would set in_migration=false, current_phase=aborted",
      phase_details: { would_do: { in_migration: false, current_phase: "aborted" } },
    };
  }
  if (beforeState?.current_phase === "aborted") {
    return {
      ok: true, phase: "abort", migration_id: req.migration_id, dry_run: false,
      before_state: beforeState, after_state: beforeState,
      phase_summary: "abort no-op: already aborted",
      phase_details: { already_aborted_at: beforeState.aborted_at },
      noop: true,
    };
  }
  const abortedAt = nowIso();
  const reason = String(req.options?.reason || "operator-initiated abort");
  await setMigrationStatus(apiKey, {
    in_migration: false,
    current_phase: "aborted",
    aborted_at: abortedAt,
    abort_reason: reason,
    updated_at: abortedAt,
  });
  const afterState = await getMigrationStatus(apiKey);
  return {
    ok: true, phase: "abort", migration_id: req.migration_id, dry_run: false,
    before_state: beforeState, after_state: afterState,
    phase_summary: `aborted at ${abortedAt}: ${reason}`,
    phase_details: { aborted_at: abortedAt, reason },
  };
}

// ─── Dispatcher (used by background) ────────────────────────────────────────

/**
 * The dispatcher is what the background worker imports and calls. It does NO
 * pre-validation (the sync entry already did all of that) — it just routes
 * to the right phase function.
 */
export async function dispatchPhase(req: PhaseRequest, siteOrigin: string, apiKey: string): Promise<PhaseResult> {
  switch (req.phase) {
    case "preflight": return await phasePreflight(req, siteOrigin, apiKey);
    case "M0":        return await phaseM0(req, siteOrigin, apiKey);
    case "M1":        return await phaseM1(req, siteOrigin, apiKey);
    case "M2":        return await phaseM2(req, siteOrigin, apiKey);
    case "M3":        return await phaseM3(req, siteOrigin, apiKey);
    case "M4":        return await phaseM4(req, siteOrigin, apiKey);
    case "M5":        return await phaseM5(req, siteOrigin, apiKey);
    case "M6":        return await phaseM6(req, siteOrigin, apiKey);
    case "abort":     return await phaseAbort(req, siteOrigin, apiKey);
    default:
      throw new Error(`unknown phase: ${(req as any).phase}`);
  }
}

// ─── HTTP entry point (sync) ────────────────────────────────────────────────

export default async (req: Request, context: Context) => {
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  if (!FIREBASE_API_KEY) return json({ error: "FIREBASE_API_KEY not configured" }, 500);

  const url = new URL(req.url);
  const siteOrigin = `${url.protocol}//${url.host}`;

  // ── GET poll endpoint ────────────────────────────────────────────────────
  if (req.method === "GET") {
    const status = await getMigrationStatus(FIREBASE_API_KEY);
    const filterMid = url.searchParams.get("migration_id");
    if (filterMid && status && status.migration_id !== filterMid) {
      return json({
        ok: true,
        warning: `requested migration_id=${filterMid} does not match in-flight migration_id=${status.migration_id}`,
        status,
      });
    }
    return json({ ok: true, status, allowed_phases: VALID_PHASES });
  }

  if (req.method !== "POST") return json({ ok: false, error: "method not allowed" }, 405);

  // ── Parse + validate request ─────────────────────────────────────────────
  let body: any = {};
  try { body = await req.json(); } catch { return json({ ok: false, error: "invalid JSON body" }, 400); }

  if (body.confirm !== true) {
    return json({ ok: false, error: "confirm:true required on every invocation (Q5 — overrides must be deliberately uncomfortable)" }, 400);
  }

  const phase = body.phase;
  if (!VALID_PHASES.includes(phase)) {
    return json({ ok: false, error: `invalid phase '${phase}'. Valid: ${VALID_PHASES.join(", ")}` }, 400);
  }

  const midCheck = validateMigrationId(body.migration_id);
  if (!midCheck.ok) return json({ ok: false, error: midCheck.error }, 400);

  // Surface query-param knobs into the body for M4
  const dryRun = url.searchParams.get("dry_run") === "1";
  const parallelism = url.searchParams.has("parallelism")
    ? clampInt(url.searchParams.get("parallelism"), 1, 4, 1) : 1;
  const continueOnError = url.searchParams.get("continue_on_error") === "1";

  if (url.searchParams.has("parallelism")) {
    const raw = parseInt(url.searchParams.get("parallelism") || "", 10);
    if (Number.isNaN(raw) || raw < 1 || raw > 4) {
      return json({ ok: false, error: "parallelism must be integer 1-4" }, 400);
    }
  }

  const phaseReq: PhaseRequest = {
    phase, confirm: true, migration_id: midCheck.id,
    options: body.options || {}, dry_run: dryRun, parallelism, continue_on_error: continueOnError,
  };

  // ── Phase ordering / cross-migration / pending-invocation guard ──────────
  const status = await getMigrationStatus(FIREBASE_API_KEY);

  // Two-migration collision: if a migration is in flight with a different ID,
  // reject all phases (except abort, which always proceeds).
  if (phase !== "abort" && status?.migration_id && status.migration_id !== midCheck.id) {
    return json({
      ok: false,
      error: `migration_id mismatch: status doc has '${status.migration_id}', request has '${midCheck.id}'`,
      current_status: status,
    }, 409);
  }

  // Pending-invocation guard: if a phase is mid-flight (background hasn't
  // cleared pending_invocation yet), reject new POSTs to keep work serialized.
  if (status?.pending_invocation && (status.pending_invocation as any).bg_completed_at == null) {
    return json({
      ok: false,
      error: "another phase invocation is pending or in flight",
      pending_invocation: status.pending_invocation,
    }, 409);
  }

  const ordering = validatePhaseOrdering(phase, status);
  if (!ordering.ok) {
    return json({ ok: false, error: (ordering as OrderingFail).error, current_phase: (ordering as OrderingFail).current_phase, current_status: status }, 409);
  }

  // ── Dry-run: execute inline and return ───────────────────────────────────
  if (dryRun) {
    try {
      const result = await dispatchPhase(phaseReq, siteOrigin, FIREBASE_API_KEY);
      return json({ ...result, confirm_received: true });
    } catch (e: any) {
      return json({ ok: false, error: `dry-run failed: ${e?.message || e}` }, 500);
    }
  }

  // ── abort: execute inline (sync — just a flag flip) ──────────────────────
  if (phase === "abort") {
    try {
      const result = await dispatchPhase(phaseReq, siteOrigin, FIREBASE_API_KEY);
      return json({ ...result, confirm_received: true });
    } catch (e: any) {
      return json({ ok: false, error: `abort failed: ${e?.message || e}` }, 500);
    }
  }

  // ── Real run: write pending_invocation, fire background, return 202 ──────
  const syncInvocationId = `sync_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const pendingInvocation = {
    phase, migration_id: midCheck.id, options: phaseReq.options,
    dry_run: false, parallelism, continue_on_error: continueOnError,
    requested_at: nowIso(), sync_invocation_id: syncInvocationId,
  };
  await setMigrationStatus(FIREBASE_API_KEY, {
    pending_invocation: pendingInvocation,
    updated_at: nowIso(),
  });

  const bgUrl = `${siteOrigin}${BG_PATH}`;
  context.waitUntil(
    fetch(bgUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(pendingInvocation),
    }).catch(e => {
      console.error("migration-runner: bg fire failed", e?.message || e);
    })
  );

  return json({
    ok: true,
    queued: true,
    phase,
    migration_id: midCheck.id,
    sync_invocation_id: syncInvocationId,
    poll_url: `${siteOrigin}/.netlify/functions/marginiq-migration-runner?migration_id=${encodeURIComponent(midCheck.id)}`,
    message: "phase queued for background execution. Poll status_doc or poll_url for progress.",
  }, 202);
};

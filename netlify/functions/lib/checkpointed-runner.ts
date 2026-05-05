/**
 * Davis MarginIQ — Checkpointed Runner (v2.53.0, Phase 2 task 1)
 *
 * Generic helper for long-running background functions that need to
 * survive Netlify's 15-minute background-function cap.
 *
 * THE PROBLEM
 * ===========
 * Netlify background functions die at 15 minutes wall time. Backfills
 * walking 50K+ rows or 500+ Gmail messages can't finish in one shot.
 * Pre-Phase 2, our backfills had two failure modes:
 *
 *   1. Single-shot: function dies mid-walk, status doc never updates,
 *      user has no idea what happened (the 24-hour Uline backfill).
 *
 *   2. Manual re-trigger: function processes a chunk, writes checkpoint,
 *      relies on user to hit the trigger again (the Phase 0 inventory
 *      function and the Phase 1 migration). Works but tedious — 32
 *      re-triggers per run for a meaningful sized backfill.
 *
 * THE SOLUTION
 * ============
 * runCheckpointed() wraps a worker function, manages cursor state in
 * Firestore, runs for up to WALL_BUDGET_MS, then if there's more work
 * left, fires another invocation of the SAME function URL with the
 * cursor in tow. Up to MAX_CHAIN_LENGTH self-reinvocations per chain;
 * after that the chain stops and the user must manually re-trigger
 * (safety net against runaway loops).
 *
 * USAGE
 * =====
 *   import { runCheckpointed, type CheckpointState } from "./lib/checkpointed-runner.js";
 *
 *   export default async (req: Request, _ctx: Context) => {
 *     await runCheckpointed({
 *       runId: "uline_historical_2026-05",
 *       runnerUrl: req.url,                     // for self-reinvoke
 *       statusCollection: "marginiq_config",
 *       statusDocId: "uline_historical_status",
 *       apiKey: FIREBASE_API_KEY!,
 *       work: async (state, ctx) => {
 *         // Do up to ctx.timeBudgetMs of work.
 *         // Return updated state (with cursor) and 'complete' flag.
 *         // ctx.shouldYield() returns true when budget is exhausted.
 *
 *         while (state.cursor < someTotal && !ctx.shouldYield()) {
 *           const item = await fetchNext(state.cursor);
 *           await processItem(item);
 *           state.cursor++;
 *           state.processed++;
 *         }
 *
 *         return { state, complete: state.cursor >= someTotal };
 *       },
 *     });
 *   };
 *
 * The shape of `state.cursor` is up to the caller — could be a number,
 * a Gmail message ID, a Firestore page token, anything serializable.
 *
 * STATUS DOC SCHEMA
 * =================
 *   marginiq_config/{statusDocId}:
 *     run_id: string
 *     state: "running" | "complete" | "failed" | "stopped_chain_limit"
 *     started_at: ISO
 *     last_run_at: ISO
 *     completed_at: ISO | null
 *     chain_length: integer  (how many self-reinvocations so far)
 *     total_invocations: integer
 *     elapsed_ms_total: integer (sum across all invocations)
 *     cursor: caller-defined (any JSON)
 *     payload: caller-defined (any JSON, for arbitrary state)
 *     processed: integer (caller's "items handled" counter)
 *     errors: integer
 *     last_error: string | null
 *
 * FIRESTORE WRITE BUDGET
 * ======================
 * Each invocation writes the status doc twice: at start (state=running,
 * chain_length++) and at end (state=running with new cursor, OR
 * state=complete). Caller can write checkpoints inside `work()` via
 * ctx.checkpoint() if they want finer granularity.
 */

const PROJECT_ID = "davismarginiq";
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

const WALL_BUDGET_MS = 12 * 60 * 1000;  // 12 minutes (3 min safety margin under 15-min cap)
const MAX_CHAIN_LENGTH = 100;            // hard cap on self-reinvocations per chain
const REINVOKE_DELAY_MS = 2_000;         // small delay before self-reinvoke to let logs flush

// ─── Types ────────────────────────────────────────────────────────────────────

export interface CheckpointState {
  run_id: string;
  cursor: any;                  // caller-defined; serialized as JSON in Firestore
  payload: any;                 // caller-defined arbitrary state
  processed: number;
  errors: number;
}

export interface WorkContext {
  /** True once WALL_BUDGET_MS has elapsed within the current invocation. */
  shouldYield: () => boolean;
  /** Time remaining in this invocation's budget, in ms. */
  timeBudgetMs: () => number;
  /** Write an interim checkpoint mid-run. Optional — runner does this at end too. */
  checkpoint: (state: CheckpointState, note?: string) => Promise<void>;
}

export type WorkFn = (
  state: CheckpointState,
  ctx: WorkContext,
) => Promise<{ state: CheckpointState; complete: boolean; error?: string }>;

export interface RunCheckpointedParams {
  runId: string;
  runnerUrl: string;             // typically req.url; used for self-reinvoke
  statusCollection: string;      // typically "marginiq_config"
  statusDocId: string;
  apiKey: string;
  work: WorkFn;
  initialCursor?: any;           // used on the very first invocation
  initialPayload?: any;
  /** Override WALL_BUDGET_MS for this run. Useful for testing. */
  wallBudgetMs?: number;
  /** Override MAX_CHAIN_LENGTH for this run. Useful for safety on noisy backfills. */
  maxChainLength?: number;
}

// ─── Firestore primitives ─────────────────────────────────────────────────────

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") {
    if (!isFinite(v)) return { nullValue: null };
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (typeof v === "string") return { stringValue: v };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(toFsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

function unwrap(v: any): any {
  if (!v || typeof v !== "object") return v;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return parseInt(v.integerValue);
  if ("doubleValue" in v) return v.doubleValue;
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("timestampValue" in v) return v.timestampValue;
  if ("arrayValue" in v) return (v.arrayValue?.values || []).map(unwrap);
  if ("mapValue" in v) {
    const out: Record<string, any> = {};
    for (const [k, val] of Object.entries(v.mapValue?.fields || {})) out[k] = unwrap(val);
    return out;
  }
  return v;
}

async function patchStatus(
  collection: string,
  docId: string,
  fields: Record<string, any>,
  apiKey: string,
): Promise<boolean> {
  const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const fsFields: Record<string, any> = {};
  // Special handling for cursor + payload: serialize as JSON strings to avoid
  // schema constraints when callers stash arbitrary nested data.
  for (const [k, v] of Object.entries(fields)) {
    if (k === "cursor" || k === "payload") {
      fsFields[k] = { stringValue: typeof v === "string" ? v : JSON.stringify(v ?? null) };
    } else {
      fsFields[k] = toFsValue(v);
    }
  }
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: fsFields }),
  });
  if (!r.ok) {
    console.error(`checkpointed-runner: patchStatus ${docId} failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  }
  return r.ok;
}

async function loadStatus(
  collection: string,
  docId: string,
  apiKey: string,
): Promise<any | null> {
  const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  const data: any = await r.json();
  const f = data.fields || {};
  const out: any = {};
  for (const [k, v] of Object.entries(f)) {
    out[k] = unwrap(v);
    // Re-deserialize JSON-stashed cursor + payload
    if ((k === "cursor" || k === "payload") && typeof out[k] === "string") {
      try { out[k] = JSON.parse(out[k]); } catch { /* leave as string */ }
    }
  }
  return out;
}

// ─── Self-reinvoke ────────────────────────────────────────────────────────────

async function selfReinvoke(runnerUrl: string): Promise<void> {
  // Sleep briefly so the current invocation can flush logs cleanly.
  await new Promise(r => setTimeout(r, REINVOKE_DELAY_MS));

  // Fire-and-forget POST to ourselves. Same URL — the runner will load
  // checkpoint state from Firestore on entry.
  try {
    fetch(runnerUrl, { method: "POST", body: "" }).catch(e => {
      console.warn(`checkpointed-runner: self-reinvoke fetch failed: ${e?.message || e}`);
    });
  } catch (e: any) {
    console.error(`checkpointed-runner: self-reinvoke threw: ${e?.message || e}`);
  }
}

// ─── Public API ───────────────────────────────────────────────────────────────

/**
 * Run a checkpointed background worker. Handles cursor state, wall-time
 * budgeting, and self-reinvocation. The caller's `work` function is
 * called once per invocation with up to WALL_BUDGET_MS to do its thing.
 *
 * Returns when the work is done (state=complete), the chain limit hits,
 * or the caller's work function throws.
 */
export async function runCheckpointed(params: RunCheckpointedParams): Promise<{
  ok: boolean;
  state: "complete" | "running" | "stopped_chain_limit" | "failed";
  reinvoked: boolean;
  duration_ms: number;
  processed_this_run: number;
  error?: string;
}> {
  const { runId, runnerUrl, statusCollection, statusDocId, apiKey, work, initialCursor, initialPayload } = params;
  const wallBudgetMs = params.wallBudgetMs ?? WALL_BUDGET_MS;
  const maxChainLength = params.maxChainLength ?? MAX_CHAIN_LENGTH;
  const t0 = Date.now();

  // Load existing state (or initialize)
  const existing = await loadStatus(statusCollection, statusDocId, apiKey);

  let state: CheckpointState;
  let chainLength = 0;
  let totalInvocations = 0;
  let elapsedMsTotal = 0;
  let processedTotalBefore = 0;

  if (existing && existing.run_id === runId && existing.state === "running") {
    // Resume
    state = {
      run_id: runId,
      cursor: existing.cursor ?? initialCursor ?? null,
      payload: existing.payload ?? initialPayload ?? null,
      processed: existing.processed || 0,
      errors: existing.errors || 0,
    };
    chainLength = (existing.chain_length || 0);
    totalInvocations = (existing.total_invocations || 0);
    elapsedMsTotal = (existing.elapsed_ms_total || 0);
    processedTotalBefore = state.processed;
    console.log(`checkpointed-runner: resuming ${runId} (chain=${chainLength}, processed=${state.processed})`);
  } else {
    // Fresh start
    state = {
      run_id: runId,
      cursor: initialCursor ?? null,
      payload: initialPayload ?? null,
      processed: 0,
      errors: 0,
    };
    chainLength = 0;
    totalInvocations = 0;
    elapsedMsTotal = 0;
    console.log(`checkpointed-runner: starting fresh ${runId}`);
  }

  // Mark running
  totalInvocations++;
  chainLength++;
  await patchStatus(statusCollection, statusDocId, {
    run_id: runId,
    state: "running",
    started_at: existing?.started_at || new Date().toISOString(),
    last_run_at: new Date().toISOString(),
    chain_length: chainLength,
    total_invocations: totalInvocations,
    elapsed_ms_total: elapsedMsTotal,
    completed_at: null,
    last_error: null,
    cursor: state.cursor,
    payload: state.payload,
    processed: state.processed,
    errors: state.errors,
  }, apiKey);

  // Build the work context
  const yieldAt = t0 + wallBudgetMs;
  const ctx: WorkContext = {
    shouldYield: () => Date.now() >= yieldAt,
    timeBudgetMs: () => Math.max(0, yieldAt - Date.now()),
    checkpoint: async (s: CheckpointState, note?: string) => {
      await patchStatus(statusCollection, statusDocId, {
        run_id: runId,
        state: "running",
        last_run_at: new Date().toISOString(),
        cursor: s.cursor,
        payload: s.payload,
        processed: s.processed,
        errors: s.errors,
        progress_text: note || `Working… ${s.processed} processed`,
      }, apiKey);
    },
  };

  // Run the worker
  let workResult: { state: CheckpointState; complete: boolean; error?: string };
  try {
    workResult = await work(state, ctx);
  } catch (e: any) {
    const errMsg = e?.message || String(e);
    console.error(`checkpointed-runner: work() threw: ${errMsg}`);
    elapsedMsTotal += Date.now() - t0;
    await patchStatus(statusCollection, statusDocId, {
      run_id: runId,
      state: "failed",
      completed_at: new Date().toISOString(),
      last_run_at: new Date().toISOString(),
      elapsed_ms_total: elapsedMsTotal,
      last_error: errMsg,
      progress_text: `✗ Failed: ${errMsg.slice(0, 200)}`,
    }, apiKey);
    return {
      ok: false,
      state: "failed",
      reinvoked: false,
      duration_ms: Date.now() - t0,
      processed_this_run: state.processed - processedTotalBefore,
      error: errMsg,
    };
  }

  state = workResult.state;
  elapsedMsTotal += Date.now() - t0;
  const processedThisRun = state.processed - processedTotalBefore;

  if (workResult.complete) {
    await patchStatus(statusCollection, statusDocId, {
      run_id: runId,
      state: "complete",
      completed_at: new Date().toISOString(),
      last_run_at: new Date().toISOString(),
      elapsed_ms_total: elapsedMsTotal,
      cursor: state.cursor,
      payload: state.payload,
      processed: state.processed,
      errors: state.errors,
      progress_text: `✓ Complete: ${state.processed} processed across ${totalInvocations} invocation(s) in ${Math.round(elapsedMsTotal / 1000)}s${state.errors ? ` (${state.errors} errors)` : ""}`,
    }, apiKey);
    return {
      ok: state.errors === 0,
      state: "complete",
      reinvoked: false,
      duration_ms: Date.now() - t0,
      processed_this_run: processedThisRun,
    };
  }

  // Not complete — decide whether to self-reinvoke
  if (chainLength >= maxChainLength) {
    await patchStatus(statusCollection, statusDocId, {
      run_id: runId,
      state: "stopped_chain_limit",
      completed_at: new Date().toISOString(),
      last_run_at: new Date().toISOString(),
      elapsed_ms_total: elapsedMsTotal,
      cursor: state.cursor,
      payload: state.payload,
      processed: state.processed,
      errors: state.errors,
      progress_text: `⊘ Stopped at chain limit (${maxChainLength} self-reinvocations). ${state.processed} processed. Re-trigger manually to continue.`,
    }, apiKey);
    console.warn(`checkpointed-runner: ${runId} hit chain limit ${maxChainLength}, stopping`);
    return {
      ok: true,
      state: "stopped_chain_limit",
      reinvoked: false,
      duration_ms: Date.now() - t0,
      processed_this_run: processedThisRun,
    };
  }

  // Save state and self-reinvoke
  await patchStatus(statusCollection, statusDocId, {
    run_id: runId,
    state: "running",
    last_run_at: new Date().toISOString(),
    elapsed_ms_total: elapsedMsTotal,
    cursor: state.cursor,
    payload: state.payload,
    processed: state.processed,
    errors: state.errors,
    progress_text: `Yielded after ${Math.round((Date.now() - t0) / 1000)}s — chain=${chainLength}/${maxChainLength}, processed=${state.processed}. Self-reinvoking…`,
  }, apiKey);

  await selfReinvoke(runnerUrl);

  return {
    ok: true,
    state: "running",
    reinvoked: true,
    duration_ms: Date.now() - t0,
    processed_this_run: processedThisRun,
  };
}

/**
 * Read the current state of a checkpointed run. Useful for status endpoints.
 */
export async function getRunStatus(
  statusCollection: string,
  statusDocId: string,
  apiKey: string,
): Promise<any | null> {
  return await loadStatus(statusCollection, statusDocId, apiKey);
}

/**
 * Reset a run's state (deletes checkpoint). Use before manually re-triggering
 * a fresh start. Caller is responsible for confirming the user actually
 * wants to lose progress.
 */
export async function resetRun(
  statusCollection: string,
  statusDocId: string,
  apiKey: string,
): Promise<boolean> {
  const url = `${FS_BASE}/${statusCollection}/${encodeURIComponent(statusDocId)}?key=${apiKey}`;
  const r = await fetch(url, { method: "DELETE" });
  return r.ok;
}

import type { Context } from "@netlify/functions";
import { runCheckpointed, type CheckpointState } from "./lib/checkpointed-runner.js";

/**
 * Davis MarginIQ — Phase 2 task 1: smoke test for checkpointed runner.
 *
 * Counts from 0 to TARGET, intentionally slow (writes a Firestore doc
 * each iteration to simulate real I/O). Designed to NOT finish in one
 * 12-minute invocation, so the self-reinvoke path is exercised.
 *
 * Endpoints
 * =========
 *   GET /.netlify/functions/marginiq-runner-smoketest
 *     Continue/start the test. Self-reinvokes until done.
 *
 *   GET /.netlify/functions/marginiq-runner-smoketest?reset=1
 *     Reset cursor and start over.
 *
 *   GET /.netlify/functions/marginiq-runner-smoketest?target=500
 *     Override target (default 5000).
 *
 * Status doc: marginiq_config/runner_smoketest_status
 *
 * After the test passes (state=complete), this function can stay in the
 * tree as a regression check or be deleted in a future cleanup PR.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

const STATUS_DOC = "runner_smoketest_status";
const DEFAULT_TARGET = 5000;
const ITER_DELAY_MS = 50; // ~50ms/iter * 5000 = 250s, well under one invocation budget

async function fakeWork(i: number): Promise<void> {
  // Write a small status field — exercises real I/O latency.
  const url = `${FS_BASE}/marginiq_config/runner_smoketest_progress?key=${FIREBASE_API_KEY}`;
  await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      fields: {
        last_i: { integerValue: String(i) },
        last_at: { stringValue: new Date().toISOString() },
      },
    }),
  });
  await new Promise(r => setTimeout(r, ITER_DELAY_MS));
}

export default async (req: Request, _ctx: Context) => {
  if (!FIREBASE_API_KEY) {
    return new Response(JSON.stringify({ error: "FIREBASE_API_KEY missing" }), { status: 500 });
  }

  const url = new URL(req.url);
  const target = Math.max(10, Math.min(50000, parseInt(url.searchParams.get("target") || String(DEFAULT_TARGET))));
  const runId = `smoketest_${target}`;

  if (url.searchParams.get("reset") === "1") {
    const resetUrl = `${FS_BASE}/marginiq_config/${STATUS_DOC}?key=${FIREBASE_API_KEY}`;
    await fetch(resetUrl, { method: "DELETE" });
    console.log(`smoketest: reset ${runId}`);
  }

  // Strip any query string before passing as runner URL — query state lives
  // in the status doc, not the URL.
  const cleanUrl = `${url.origin}${url.pathname}`;

  const result = await runCheckpointed({
    runId,
    runnerUrl: cleanUrl,
    statusCollection: "marginiq_config",
    statusDocId: STATUS_DOC,
    apiKey: FIREBASE_API_KEY,
    initialCursor: 0,
    initialPayload: { target },
    // Force self-reinvoke with a short budget. Real backfills use the
    // default 12-min budget. Keep small here so we exercise the path
    // without waiting forever.
    wallBudgetMs: 20_000,
    maxChainLength: 30,
    work: async (state, ctx) => {
      let cursor: number = (typeof state.cursor === "number") ? state.cursor : 0;
      const tgt: number = state.payload?.target || target;

      while (cursor < tgt && !ctx.shouldYield()) {
        try {
          await fakeWork(cursor);
          cursor++;
          state.processed = cursor;
        } catch (e: any) {
          state.errors++;
          console.warn(`smoketest iter ${cursor} error: ${e?.message || e}`);
          cursor++; // skip past the error
        }

        // Mid-run checkpoint every 100 iters
        if (cursor % 100 === 0) {
          state.cursor = cursor;
          await ctx.checkpoint(state, `Smoketest at ${cursor}/${tgt}`);
        }
      }

      state.cursor = cursor;
      return { state, complete: cursor >= tgt };
    },
  });

  return new Response(JSON.stringify({
    ok: result.ok,
    state: result.state,
    reinvoked: result.reinvoked,
    processed_this_run: result.processed_this_run,
    duration_ms: result.duration_ms,
    target,
    status_doc: `marginiq_config/${STATUS_DOC}`,
  }, null, 2), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
};

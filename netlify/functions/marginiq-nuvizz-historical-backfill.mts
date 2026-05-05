import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — NuVizz Historical Backfill TRIGGER (v2.53.1, Phase 2 task 3)
 *
 * Lightweight sync trigger. Validates options, resets/initializes the
 * status doc, and fires the background worker. The actual work happens
 * in marginiq-nuvizz-historical-backfill-background, wrapped in
 * lib/checkpointed-runner.ts so it can survive Netlify's 15-min cap
 * via cursor + self-reinvoke.
 *
 * ENDPOINTS
 * =========
 *   GET  /.netlify/functions/marginiq-nuvizz-historical-backfill
 *     Returns current status doc.
 *
 *   GET  /.netlify/functions/marginiq-nuvizz-historical-backfill?dry_run=1
 *     Lists matching emails without ingesting.
 *
 *   POST /.netlify/functions/marginiq-nuvizz-historical-backfill
 *     body: {
 *       newer_than: '2024-01-01',  // ISO date, default 2024-01-01
 *       older_than: '2026-12-31',  // ISO date, default today
 *       limit: 1000,                // max emails to process, default 1000
 *       dry_run: false,            // optional preview
 *       reset: false,              // wipe checkpoint and start fresh
 *       reprocess: false,          // bypass nuvizz_processed_emails idempotency
 *     }
 *
 * STATUS DOC
 * ==========
 * marginiq_config/nuvizz_historical_backfill_status — schema documented
 * in checkpointed-runner.ts plus these worker-specific fields:
 *   newer_than, older_than, limit, dry_run, reprocess
 *   accounts: string[]
 *   emails_found, emails_processed, emails_skipped
 *   files_staged, files_processed
 *   lines_written, raw_rows_written
 *   errors
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
const STATUS_DOC = "nuvizz_historical_backfill_status";
const BG_PATH = "/.netlify/functions/marginiq-nuvizz-historical-backfill-background";

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

interface Options {
  newer_than: string;
  older_than: string | null;
  limit: number;
  dry_run: boolean;
  reset: boolean;
  reprocess: boolean;
}

function parseOptions(req: Request, body: any): Options {
  const url = new URL(req.url);
  const fromQuery = (k: string) => url.searchParams.get(k);
  const fromBody = (k: string) => body?.[k];
  const get = (k: string) => fromBody(k) ?? fromQuery(k);

  const today = new Date().toISOString().slice(0, 10);
  return {
    newer_than: String(get("newer_than") || "2024-01-01"),
    older_than: get("older_than") ? String(get("older_than")) : null,
    limit: Math.max(1, Math.min(2000, parseInt(String(get("limit") || "1000"))) || 1000),
    dry_run: get("dry_run") === "1" || get("dry_run") === "true" || get("dry_run") === true,
    reset: get("reset") === "1" || get("reset") === "true" || get("reset") === true,
    reprocess: get("reprocess") === "1" || get("reprocess") === "true" || get("reprocess") === true,
  };
}

async function readStatus(): Promise<any | null> {
  if (!FIREBASE_API_KEY) return null;
  const url = `${FS_BASE}/marginiq_config/${STATUS_DOC}?key=${FIREBASE_API_KEY}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
}

async function deleteStatus(): Promise<void> {
  if (!FIREBASE_API_KEY) return;
  const url = `${FS_BASE}/marginiq_config/${STATUS_DOC}?key=${FIREBASE_API_KEY}`;
  await fetch(url, { method: "DELETE" });
}

export default async (req: Request, context: Context) => {
  if (!FIREBASE_API_KEY) {
    return json({ error: "FIREBASE_API_KEY not configured" }, 500);
  }

  const url = new URL(req.url);

  // GET with no params = read status
  if (req.method === "GET" && !url.searchParams.has("dry_run") && !url.searchParams.has("trigger")) {
    const status = await readStatus();
    return json({ ok: true, status_doc_path: `marginiq_config/${STATUS_DOC}`, status });
  }

  let body: any = {};
  if (req.method === "POST") {
    try { body = await req.json(); } catch {}
  }

  const opts = parseOptions(req, body);

  // If a run is currently in flight, reject unless reset=true
  const existing = await readStatus();
  if (!opts.reset && existing) {
    const f = existing.fields || {};
    const state = f.state?.stringValue;
    if (state === "running") {
      return json({
        ok: false,
        error: "A backfill is already running. Pass reset=true to wipe and start over.",
        status_doc_path: `marginiq_config/${STATUS_DOC}`,
        current_state: state,
        last_progress: f.progress_text?.stringValue,
      }, 409);
    }
  }

  if (opts.reset) {
    await deleteStatus();
  }

  // Fire the background. Pass options as JSON body. The background reads
  // them and stores into payload for the duration of the chain.
  const bgUrl = `${url.origin}${BG_PATH}`;
  context.waitUntil(
    fetch(bgUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        newer_than: opts.newer_than,
        older_than: opts.older_than,
        limit: opts.limit,
        dry_run: opts.dry_run,
        reprocess: opts.reprocess,
      }),
    }).catch(e => {
      console.error("nuvizz-historical-backfill TRIGGER: bg fire failed", e?.message || e);
    })
  );

  return json({
    ok: true,
    queued: true,
    options: opts,
    status_doc_path: `marginiq_config/${STATUS_DOC}`,
    poll: `${url.origin}/.netlify/functions/marginiq-nuvizz-historical-backfill`,
    message: "Backfill queued. Poll the status_doc_path or this URL for progress.",
  }, 202);
};

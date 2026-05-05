import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Uline DAS Historical Backfill Background Worker (DISABLED v2.52.9)
 *
 * Halted as part of the Foundation Rebuild (Phase 0). The pre-v2.52.9
 * implementation had no cursor + self-reinvoke pattern, so it timed out
 * at Netlify's 15-minute background-function cap and required manual
 * re-triggers. After ~95 of 508 historical Uline DAS emails were
 * processed (~285K das_lines rows), the run was halted on May 5, 2026.
 *
 * Phase 2 of the rebuild will reinstate this with:
 *   - Gmail message-list cursor cached in marginiq_config
 *   - Self-reinvocation when ~12 minutes elapsed
 *   - End-to-end resumable run from Jan 2024 to present
 *
 * Until then, ALL invocations return 410 Gone. On the FIRST call after
 * deploy, the worker stamps marginiq_config/uline_historical_backfill_status
 * with state="halted" so the status doc reflects reality.
 *
 * Forward-week Uline ingest continues via marginiq-uline-auto-ingest.
 *
 * The original logic (912 lines) is preserved in git history.
 * Tag: v2.52.8-baseline.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
const STATUS_DOC = "uline_historical_backfill_status";

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

async function fsPatchDoc(coll: string, docId: string, fields: Record<string, any>, apiKey: string): Promise<boolean> {
  const url = `${FS_BASE}/${coll}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const body: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) body[k] = toFsValue(v);
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: body }),
  });
  return r.ok;
}

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export default async (_req: Request, _context: Context) => {
  console.log("uline-historical-backfill-background: DISABLED — Phase 0 halt. Returning 410.");

  // Best-effort status stamp so the doc reflects the halted state.
  if (FIREBASE_API_KEY) {
    try {
      await fsPatchDoc("marginiq_config", STATUS_DOC, {
        state: "halted",
        halted_at: new Date().toISOString(),
        halted_reason: "Phase 0 of Foundation Rebuild. Function ran >24h via manual re-triggers without completing. Cursor + self-reinvoke loop deferred to Phase 2.",
        halted_baseline_tag: "v2.52.8-baseline",
        phase: "halted",
        progress_text: "✗ Halted — Foundation Rebuild Phase 0. See halted_reason.",
      }, FIREBASE_API_KEY);
    } catch (e: any) {
      console.warn(`uline-historical-backfill-background: status stamp failed: ${e?.message || e}`);
    }
  }

  return json({
    ok: false,
    disabled: true,
    code: "BACKFILL_HALTED_PHASE_0",
    message:
      "Uline historical backfill is halted as part of the MarginIQ Foundation Rebuild (Phase 0). Phase 2 will reinstate this with a cursor + self-reinvoke loop. See marginiq_config/uline_historical_backfill_status for state.",
    rebuild_baseline_tag: "v2.52.8-baseline",
  }, 410);
};

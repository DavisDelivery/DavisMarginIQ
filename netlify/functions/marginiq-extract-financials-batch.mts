import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — Financial extraction BATCH DISPATCHER (v2.50.0).
 *
 * Modes (selected by HTTP method + ?action=):
 *
 *   POST  → discover PDFs in Storage under audited_financials_v2/, create
 *           a batch, queue extract_jobs/{period} per PDF, fire workers
 *           with concurrency=3, return {batch_id} 202.
 *
 *   GET ?action=status&batch_id=X → returns the batch doc + per-job summary
 *
 *   GET ?action=health  → smoke test
 *
 * Why this design:
 *   - Single trigger from the browser; everything else is server-side
 *   - Concurrency=3: Tier 2 has 1K RPM, plenty of room. Sequential = ~25min;
 *     concurrent-3 = ~10min for 31 PDFs.
 *   - Per-PDF job docs let the UI surface granular progress and retry just
 *     the failures.
 *   - Batch doc is the single source of truth for "is the import done yet"
 *     so the browser can poll it intermittently or just look at it later.
 */

const PROJECT_ID = "davismarginiq";
const STORAGE_BUCKET = "davismarginiq.firebasestorage.app";
const CONCURRENCY = 3;

function fsBase(): string {
  return `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
}

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// ── Firestore helpers ───────────────────────────────────────────────────────
function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (typeof v === "string") return { stringValue: v };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(toFsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}
function toFsFields(obj: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(obj)) out[k] = toFsValue(v);
  return out;
}
function fsValueToJs(v: any): any {
  if (!v) return null;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return Number(v.doubleValue);
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("arrayValue" in v) return (v.arrayValue.values || []).map(fsValueToJs);
  if ("mapValue" in v) {
    const out: Record<string, any> = {};
    for (const [k, val] of Object.entries(v.mapValue.fields || {})) out[k] = fsValueToJs(val);
    return out;
  }
  return null;
}

async function readDoc(path: string): Promise<any | null> {
  const apiKey = Netlify.env.get("FIREBASE_API_KEY");
  const resp = await fetch(`${fsBase()}/${path}?key=${apiKey}`);
  if (!resp.ok) return null;
  const data: any = await resp.json();
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(data.fields || {})) out[k] = fsValueToJs(v);
  return out;
}

async function writeDoc(path: string, fields: Record<string, any>): Promise<boolean> {
  const apiKey = Netlify.env.get("FIREBASE_API_KEY");
  const resp = await fetch(`${fsBase()}/${path}?key=${apiKey}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: toFsFields(fields) }),
  });
  return resp.ok;
}

async function listCollection(collectionId: string, pageSize = 100): Promise<any[]> {
  const apiKey = Netlify.env.get("FIREBASE_API_KEY");
  const out: any[] = [];
  let pageToken: string | undefined;
  for (let safety = 0; safety < 50; safety++) {
    const params = new URLSearchParams({ key: apiKey || "", pageSize: String(pageSize) });
    if (pageToken) params.set("pageToken", pageToken);
    const resp = await fetch(`${fsBase()}/${collectionId}?${params}`);
    if (!resp.ok) break;
    const data: any = await resp.json();
    for (const doc of data.documents || []) out.push(doc);
    if (!data.nextPageToken) break;
    pageToken = data.nextPageToken;
  }
  return out;
}

// ── Storage listing ─────────────────────────────────────────────────────────
async function listFinancialPDFs(): Promise<{ name: string; period: string }[]> {
  const url = `https://firebasestorage.googleapis.com/v0/b/${STORAGE_BUCKET}/o?prefix=audited_financials_v2/&maxResults=1000`;
  const resp = await fetch(url);
  if (!resp.ok) return [];
  const data: any = await resp.json();
  const out: { name: string; period: string }[] = [];
  for (const item of data.items || []) {
    const name: string = item.name;
    if (!name.endsWith(".pdf")) continue;
    // Path is audited_financials_v2/{period}.pdf
    const m = name.match(/audited_financials_v2\/(\d{4}-\d{2})\.pdf$/);
    if (!m) continue;
    out.push({ name, period: m[1] });
  }
  // Also fall back to the legacy path in case PDFs were uploaded under the
  // old prefix during the v2.48.x era.
  const legacyUrl = `https://firebasestorage.googleapis.com/v0/b/${STORAGE_BUCKET}/o?prefix=audited_financials/&maxResults=1000`;
  const legacyResp = await fetch(legacyUrl);
  if (legacyResp.ok) {
    const legacyData: any = await legacyResp.json();
    for (const item of legacyData.items || []) {
      const name: string = item.name;
      if (!name.endsWith(".pdf")) continue;
      const m = name.match(/audited_financials\/(\d{4}-\d{2})\.pdf$/);
      if (!m) continue;
      // Don't overwrite v2 entries
      if (out.find(x => x.period === m[1])) continue;
      out.push({ name, period: m[1] });
    }
  }
  return out;
}

// ── Worker dispatch with bounded concurrency ────────────────────────────────
async function dispatchWorkers(origin: string, batchId: string, periods: string[]): Promise<void> {
  const workerUrl = `${origin}/.netlify/functions/marginiq-extract-financial-background`;
  const queue = [...periods];
  const inFlight: Promise<any>[] = [];

  const fireOne = async (period: string) => {
    try {
      // Fire and forget: background worker writes its own status to Firestore
      await fetch(workerUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ period, batch_id: batchId }),
      });
    } catch (e) {
      // Background functions return 202 immediately; if even the dispatch
      // fails, mark the job failed so the batch progresses.
      console.error(`Dispatch failed for ${period}:`, e);
      await writeDoc(`extract_jobs/${period}`, {
        state: "failed",
        error: `Dispatch to background function failed: ${(e as Error).message}`,
        completed_at: new Date().toISOString(),
      });
    }
  };

  // For background functions, the dispatch itself returns 202 instantly,
  // so we can fire all of them — the runtime concurrency is enforced by
  // Netlify's own scaling, not by us. But to keep Anthropic rate limits
  // happy we throttle dispatch with a short delay between firings.
  for (const period of queue) {
    inFlight.push(fireOne(period));
    // Stagger by 500ms so the first batch hits Anthropic at slightly different
    // moments, avoiding any per-second rate spike.
    await new Promise(r => setTimeout(r, 500));
  }
  await Promise.all(inFlight);
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  const action = url.searchParams.get("action");

  // ── Health ──────────────────────────────────────────────────────────────
  if (req.method === "GET" && action === "health") {
    return json({
      ok: true,
      version: "2.50.0",
      firebase_configured: !!Netlify.env.get("FIREBASE_API_KEY"),
      anthropic_configured: !!Netlify.env.get("ANTHROPIC_API_KEY"),
    });
  }

  // ── Status ─────────────────────────────────────────────────────────────
  if (req.method === "GET" && action === "status") {
    const batchId = url.searchParams.get("batch_id") || "";
    if (!batchId) return json({ error: "batch_id required" }, 400);
    const batch = await readDoc(`extract_batches/${batchId}`);
    if (!batch) return json({ error: "Batch not found" }, 404);

    // Pull all extract_jobs (cheap — usually 30 docs) and filter to this batch
    const allJobs = await listCollection("extract_jobs");
    const jobs = allJobs
      .map(d => {
        const f: Record<string, any> = {};
        for (const [k, v] of Object.entries(d.fields || {})) f[k] = fsValueToJs(v);
        return { id: d.name.split("/").pop(), ...f };
      })
      .filter(j => j.batch_id === batchId);

    return json({ batch, jobs });
  }

  // ── Dispatch new batch ─────────────────────────────────────────────────
  if (req.method !== "POST") return json({ error: "POST or GET ?action= required" }, 405);
  if (!Netlify.env.get("FIREBASE_API_KEY")) return json({ error: "FIREBASE_API_KEY not configured" }, 500);

  const body: any = await req.json().catch(() => ({}));
  const onlyPeriod: string | null = body.period || null; // optional: extract just one PDF
  const skipExisting: boolean = body.skip_existing !== false; // default true

  // 1. List PDFs in Storage
  let pdfs = await listFinancialPDFs();
  if (onlyPeriod) {
    pdfs = pdfs.filter(p => p.period === onlyPeriod);
    if (!pdfs.length) {
      return json({ error: `No PDF found for period ${onlyPeriod}` }, 404);
    }
  }
  if (!pdfs.length) {
    return json({ error: "No financial PDFs found in Storage. Import via Gmail Sync first." }, 404);
  }

  // 2. Optionally skip already-extracted periods
  let toProcess = pdfs;
  if (skipExisting && !onlyPeriod) {
    const existing = await listCollection("audited_financials_v2");
    const existingPeriods = new Set(
      existing.map(d => {
        const id = d.name.split("/").pop();
        return id;
      })
    );
    toProcess = pdfs.filter(p => !existingPeriods.has(p.period));
  }

  if (!toProcess.length) {
    return json({ ok: true, message: "All periods already extracted", pdfs_found: pdfs.length });
  }

  // 3. Create batch doc
  const batchId = `batch_${new Date().toISOString().replace(/[:.]/g, "-")}_${Math.random().toString(36).slice(2, 6)}`;
  await writeDoc(`extract_batches/${batchId}`, {
    batch_id: batchId,
    state: "running",
    total_count: toProcess.length,
    completed_count: 0,
    failed_count: 0,
    periods: toProcess.map(p => p.period),
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  });

  // 4. Queue per-PDF jobs
  for (const pdf of toProcess) {
    await writeDoc(`extract_jobs/${pdf.period}`, {
      period: pdf.period,
      pdf_storage_path: pdf.name,
      batch_id: batchId,
      state: "queued",
      queued_at: new Date().toISOString(),
    });
  }

  // 5. Fire all background workers (context.waitUntil so the dispatcher
  //    response returns instantly while the background fan-out happens).
  context.waitUntil(
    dispatchWorkers(url.origin, batchId, toProcess.map(p => p.period))
  );

  return json({
    batch_id: batchId,
    total_count: toProcess.length,
    state: "running",
    skipped_existing: pdfs.length - toProcess.length,
  }, 202);
};

export const config: Config = {
  timeout: 26,
};

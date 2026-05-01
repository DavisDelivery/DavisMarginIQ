import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — Financial extraction BATCH DISPATCHER (v2.50.1).
 *
 * Modes (selected by HTTP method + ?action=):
 *
 *   POST  → discover AMP CPAs financial-statement PDFs across all
 *           connected Gmail accounts, create a batch, queue
 *           extract_jobs/{period} per email (carrying gmail coordinates
 *           — message_id, attachment_id, account_doc_id), fire workers,
 *           return {batch_id} 202.
 *
 *   GET ?action=status&batch_id=X → returns the batch doc + per-job summary
 *
 *   GET ?action=health  → smoke test
 *
 * Why Gmail-direct (not Firebase Storage):
 *   - The original Gmail Sync flow processed PDFs in-memory and never
 *     persisted them, so Storage at audited_financials_v2/ is empty.
 *   - Gmail is the source of truth anyway; pulling at extraction time
 *     keeps the pipeline stateless and avoids a duplicate-storage step.
 *   - Each job doc carries enough info (message_id + attachment_id +
 *     account_doc_id) for the worker to refetch independently.
 *
 * Why this design:
 *   - Single trigger from the browser; everything else is server-side
 *   - Background functions run 15min, so even at ~120s per PDF the
 *     entire 31-PDF batch finishes well within budget when fanned out.
 *   - Per-PDF job docs let the UI surface granular progress and retry
 *     just the failures.
 *   - Batch doc is the single source of truth for "is the import done yet"
 *     so the browser can poll it intermittently or just look at it later.
 */

const PROJECT_ID = "davismarginiq";

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

// Writes (PATCHes) a Firestore doc. Defaults to MERGE semantics — only the
// fields you pass in are touched; everything else is preserved. Set
// merge=false to replace the entire document.
//
// Why this matters: Firestore REST's PATCH replaces the whole document
// unless `updateMask.fieldPaths` is specified.
async function writeDoc(
  path: string,
  fields: Record<string, any>,
  merge: boolean = true,
): Promise<boolean> {
  const apiKey = Netlify.env.get("FIREBASE_API_KEY");
  const params = new URLSearchParams();
  params.set("key", apiKey || "");
  if (merge) {
    for (const k of Object.keys(fields)) params.append("updateMask.fieldPaths", k);
  }
  const resp = await fetch(`${fsBase()}/${path}?${params.toString()}`, {
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

// ── Gmail discovery: list AMP CPAs financial-statement PDFs ────────────────
// Each email yields one period (YYYY-MM) parsed from filename or subject.
// Returned objects carry the Gmail coordinates the worker needs.

type GmailPdfRef = {
  period: string;             // e.g. "2025-12"
  filename: string;
  message_id: string;
  attachment_id: string;
  account_doc_id: string;     // marginiq_config doc holding the refresh token
  account_email: string;
  email_date_iso: string | null;
  size_bytes: number;
};

const AMP_CPAS_QUERY = 'from:@ampcpas.com filename:"Financial Statements" filename:pdf';

const MONTH_TOKEN_TO_NUM: Record<string, string> = {
  jan: "01", january: "01",
  feb: "02", february: "02",
  mar: "03", march: "03",
  apr: "04", april: "04",
  may: "05",
  jun: "06", june: "06",
  jul: "07", july: "07",
  aug: "08", august: "08",
  sep: "09", sept: "09", september: "09",
  oct: "10", october: "10",
  nov: "11", november: "11",
  dec: "12", december: "12",
};

function parsePeriod(filename: string, subject: string): string | null {
  // Look for "<Mon> <YYYY>" anywhere in either filename or subject.
  for (const s of [filename || "", subject || ""]) {
    const lower = s.toLowerCase();
    const m = lower.match(
      /(jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)[a-z]*\s+(\d{4})/
    );
    if (m) {
      const mo = MONTH_TOKEN_TO_NUM[m[1]];
      const yr = m[2];
      if (mo && yr) return `${yr}-${mo}`;
    }
  }
  return null;
}

type ConnectedAccount = { docId: string; email: string; refresh_token: string };

async function listConnectedGmailAccounts(apiKey: string): Promise<ConnectedAccount[]> {
  const accounts: Record<string, ConnectedAccount> = {};
  const r = await fetch(
    `${fsBase()}/marginiq_config?key=${apiKey}&pageSize=100`
  );
  if (!r.ok) return [];
  const data: any = await r.json();
  for (const d of (data.documents || [])) {
    const docId = (d.name || "").split("/").pop() || "";
    if (docId !== "gmail_tokens" && !docId.startsWith("gmail_tokens_")) continue;
    const fields = d.fields || {};
    const refreshToken = fields.refresh_token?.stringValue;
    const email = fields.email?.stringValue || "unknown";
    if (!refreshToken) continue;
    const existing = accounts[email];
    if (!existing || (existing.docId === "gmail_tokens" && docId !== "gmail_tokens")) {
      accounts[email] = { docId, email, refresh_token: refreshToken };
    }
  }
  return Object.values(accounts);
}

async function getAccessToken(
  clientId: string, clientSecret: string, refreshToken: string
): Promise<string | null> {
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
      grant_type: "refresh_token",
    }),
  });
  const d: any = await r.json();
  if (!r.ok || !d.access_token) return null;
  return d.access_token;
}

async function listAmpCpaPdfs(): Promise<GmailPdfRef[]> {
  const apiKey = Netlify.env.get("FIREBASE_API_KEY");
  const clientId = Netlify.env.get("GOOGLE_CLIENT_ID");
  const clientSecret = Netlify.env.get("GOOGLE_CLIENT_SECRET");
  if (!apiKey || !clientId || !clientSecret) {
    throw new Error("Required env vars missing (FIREBASE_API_KEY, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET)");
  }

  const accounts = await listConnectedGmailAccounts(apiKey);
  if (accounts.length === 0) return [];

  // Per-period dedup. The same monthly statement could in theory hit
  // multiple connected inboxes; first one wins.
  const byPeriod: Record<string, GmailPdfRef> = {};

  for (const acct of accounts) {
    const accessToken = await getAccessToken(clientId, clientSecret, acct.refresh_token);
    if (!accessToken) continue;

    // Page through Gmail messages
    const messages: Array<{ id: string }> = [];
    let pageToken: string | undefined;
    let pages = 0;
    while (pages < 5) {
      const url = new URL("https://gmail.googleapis.com/gmail/v1/users/me/messages");
      url.searchParams.set("q", AMP_CPAS_QUERY);
      url.searchParams.set("maxResults", "200");
      if (pageToken) url.searchParams.set("pageToken", pageToken);
      const r = await fetch(url.toString(), {
        headers: { Authorization: `Bearer ${accessToken}` },
      });
      if (!r.ok) break;
      const d: any = await r.json();
      const batch = d.messages || [];
      if (batch.length === 0) break;
      messages.push(...batch);
      pageToken = d.nextPageToken;
      pages += 1;
      if (!pageToken) break;
    }

    // Fetch each message's headers + attachment list. Run in parallel —
    // Gmail handles ~250 RPS for this user.
    await Promise.all(messages.map(async (msg: any) => {
      try {
        const fr = await fetch(
          `https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}`,
          { headers: { Authorization: `Bearer ${accessToken}` } }
        );
        const full: any = await fr.json();
        if (!fr.ok) return;

        const headers: any[] = full.payload?.headers || [];
        const getHeader = (name: string) =>
          headers.find((h: any) => h.name.toLowerCase() === name.toLowerCase())?.value || "";

        const subject = getHeader("Subject");
        const dateStr = getHeader("Date");
        const dateObj = dateStr ? new Date(dateStr) : null;
        const dateISO = dateObj && !isNaN(dateObj.getTime()) ? dateObj.toISOString() : null;

        // Walk MIME tree; pick the FIRST .pdf attachment named like
        // "Financial Statements*.pdf" — AMP emails sometimes include
        // an invoice PDF as a second attachment, which we skip.
        let chosen: any = null;
        const walkParts = (part: any) => {
          if (chosen) return;
          if (part.filename && part.body?.attachmentId) {
            const fn = String(part.filename);
            if (fn.toLowerCase().endsWith(".pdf") &&
                /financial\s+statements/i.test(fn)) {
              chosen = {
                filename: fn,
                size: part.body?.size || 0,
                attachmentId: part.body.attachmentId,
              };
            }
          }
          if (part.parts) part.parts.forEach(walkParts);
        };
        if (full.payload) walkParts(full.payload);
        if (!chosen) return;

        const period = parsePeriod(chosen.filename, subject);
        if (!period) return;

        // Dedup: keep newest email per period (in case AMP resends a
        // corrected statement, the latest one wins).
        const existing = byPeriod[period];
        if (existing) {
          const existingDate = existing.email_date_iso ? Date.parse(existing.email_date_iso) : 0;
          const candidateDate = dateISO ? Date.parse(dateISO) : 0;
          if (candidateDate <= existingDate) return;
        }
        byPeriod[period] = {
          period,
          filename: chosen.filename,
          message_id: msg.id,
          attachment_id: chosen.attachmentId,
          account_doc_id: acct.docId,
          account_email: acct.email,
          email_date_iso: dateISO,
          size_bytes: chosen.size,
        };
      } catch (_e) {
        /* skip on per-message errors; the batch continues */
      }
    }));
  }

  return Object.values(byPeriod).sort((a, b) => a.period.localeCompare(b.period));
}

// ── Hand off to background dispatch coordinator ─────────────────────────────
// The coordinator (marginiq-extract-financials-dispatch-background) runs
// up to 15 minutes and paces worker firings to respect Anthropic ITPM.
async function handoffToCoordinator(origin: string, batchId: string, periods: string[]): Promise<void> {
  const coordinatorUrl = `${origin}/.netlify/functions/marginiq-extract-financials-dispatch-background`;
  try {
    await fetch(coordinatorUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ batch_id: batchId, periods }),
    });
    // Coordinator returns 202 instantly. We don't await the actual fanout.
  } catch (e: any) {
    console.error(`Coordinator handoff failed for ${batchId}:`, e.message || e);
    // Best-effort: mark all jobs as failed so the batch state reflects reality.
    for (const period of periods) {
      await writeDoc(`extract_jobs/${period}`, {
        state: "failed",
        error: `Coordinator handoff failed: ${e.message || e}`,
        completed_at: new Date().toISOString(),
      });
    }
  }
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  const action = url.searchParams.get("action");

  // ── Health ──────────────────────────────────────────────────────────────
  if (req.method === "GET" && action === "health") {
    return json({
      ok: true,
      version: "2.50.1-gmail-direct",
      firebase_configured: !!Netlify.env.get("FIREBASE_API_KEY"),
      anthropic_configured: !!Netlify.env.get("ANTHROPIC_API_KEY"),
      gmail_oauth_configured: !!Netlify.env.get("GOOGLE_CLIENT_ID") && !!Netlify.env.get("GOOGLE_CLIENT_SECRET"),
    });
  }

  // ── Discover (dry-run): list AMP CPAs PDFs + which would be processed ──
  if (req.method === "GET" && action === "discover") {
    try {
      const pdfs = await listAmpCpaPdfs();
      const existing = await listCollection("audited_financials_v2");
      const existingPeriods = new Set(
        existing.map(d => (d.name.split("/").pop() as string))
      );
      const annotated = pdfs.map(p => ({
        ...p,
        already_extracted: existingPeriods.has(p.period),
      }));
      return json({
        ok: true,
        pdfs_found: pdfs.length,
        already_extracted_count: annotated.filter(p => p.already_extracted).length,
        would_process_count: annotated.filter(p => !p.already_extracted).length,
        pdfs: annotated,
      });
    } catch (e: any) {
      return json({ error: e.message || "discovery failed" }, 500);
    }
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
  const onlyPeriod: string | null = body.period || null;          // optional: extract just one period
  const skipExisting: boolean = body.skip_existing !== false;      // default true
  const force: boolean = body.force === true;                       // override skip_existing

  // 1. Discover AMP CPAs financial PDFs across connected Gmail accounts
  let pdfs: GmailPdfRef[];
  try {
    pdfs = await listAmpCpaPdfs();
  } catch (e: any) {
    return json({ error: e.message || "Gmail discovery failed" }, 500);
  }

  if (onlyPeriod) {
    pdfs = pdfs.filter(p => p.period === onlyPeriod);
    if (!pdfs.length) {
      return json({ error: `No AMP CPAs email found for period ${onlyPeriod}` }, 404);
    }
  }
  if (!pdfs.length) {
    return json({ error: "No AMP CPAs financial PDFs found in Gmail. Verify Gmail is connected and the search query 'from:@ampcpas.com filename:\"Financial Statements\" filename:pdf' returns results." }, 404);
  }

  // 2. Optionally skip already-extracted periods
  let toProcess = pdfs;
  let skippedExistingCount = 0;
  if (skipExisting && !onlyPeriod && !force) {
    const existing = await listCollection("audited_financials_v2");
    const existingPeriods = new Set(
      existing.map(d => (d.name.split("/").pop() as string))
    );
    toProcess = pdfs.filter(p => !existingPeriods.has(p.period));
    skippedExistingCount = pdfs.length - toProcess.length;
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
    source: "gmail",
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  });

  // 4. Queue per-period jobs carrying Gmail coordinates
  for (const pdf of toProcess) {
    await writeDoc(`extract_jobs/${pdf.period}`, {
      period: pdf.period,
      filename: pdf.filename,
      gmail_message_id: pdf.message_id,
      gmail_attachment_id: pdf.attachment_id,
      gmail_account_doc_id: pdf.account_doc_id,
      gmail_account_email: pdf.account_email,
      email_date_iso: pdf.email_date_iso,
      pdf_size_bytes: pdf.size_bytes,
      batch_id: batchId,
      state: "queued",
      queued_at: new Date().toISOString(),
    });
  }

  // 5. Hand off to the background coordinator (it paces the worker firings
  //    to stay under Anthropic's per-minute token limit).
  context.waitUntil(
    handoffToCoordinator(url.origin, batchId, toProcess.map(p => p.period))
  );

  return json({
    batch_id: batchId,
    total_count: toProcess.length,
    state: "running",
    skipped_existing: skippedExistingCount,
    pdfs_found: pdfs.length,
  }, 202);
};

export const config: Config = {
  timeout: 26,
};

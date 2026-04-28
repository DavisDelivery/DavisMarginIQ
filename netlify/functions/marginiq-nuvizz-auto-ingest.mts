import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — NuVizz Auto-Ingest Scheduled Function (v2.42.6)
 *
 * Runs Sunday morning 8am ET to pull the most recent NuVizz weekly
 * driver-stops CSV from any connected Gmail account and ingest it through
 * the now-fixed dispatcher (v2.41.17).
 *
 * Schedule: cron `0 12,13 * * 0` (Sun 12:00 UTC + 13:00 UTC).
 *   - 12:00 UTC = 8am EDT (Mar–Nov, daylight time)
 *   - 13:00 UTC = 8am EST (Nov–Mar, standard time)
 * Same DST-handling pattern as the B600 scraper (v2.41.14): cron has no
 * concept of DST so we fire at both UTC offsets and rely on idempotency
 * to make the duplicate run a harmless no-op.
 *
 * Idempotency: every successfully-processed Gmail messageId gets recorded
 * in nuvizz_processed_emails/{messageId}. Subsequent runs skip messages
 * already there. The dispatcher itself is also idempotent (PRO-keyed
 * doc IDs in nuvizz_stops, PATCH overwrites with identical values).
 *
 * Process:
 *   1. List connected Gmail accounts (legacy gmail_tokens + per-account
 *      gmail_tokens_* docs in marginiq_config).
 *   2. Search each account for `from:nuvizzapps@nuvizzapps.com has:attachment`,
 *      most recent 5 messages.
 *   3. Pick the newest unprocessed messageId across all accounts.
 *   4. Download its CSV attachment, parse to NuVizz stop shape.
 *   5. POST parsed stops to marginiq-nuvizz-ingest (chunked dispatcher).
 *   6. Mark messageId processed; write run summary to
 *      nuvizz_auto_ingest_logs/{run_id}.
 *
 * Manual trigger: GET /.netlify/functions/marginiq-nuvizz-auto-ingest
 *   (also POST). Useful for catching up after an outage or testing.
 *
 * NuVizz email pattern (from observed data): irregular send times
 * (Tue 1pm, Tue 3pm, Fri 11am, Fri 9am, Sun morning) — they're triggered
 * manually by Chad running the report. Sunday 8am window catches any
 * report generated during the prior week.
 *
 * Env vars: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, FIREBASE_API_KEY
 */

const PROJECT_ID = "davismarginiq";
// Filter on subject:Driver_stops (without quotes, no colon, no spaces) to
// distinguish the weekly driver_stops CSV from older "Stop Delivery
// Notification" event emails (DAVIS####### subjects from 2019-2021).
// Avoiding quoted phrases because Gmail's URL-encoded search has trouble
// with embedded colons and underscores even when quoted. The single term
// `Driver_stops` is unique enough — that exact substring only appears in
// the weekly report subject line.
const VENDOR_QUERY = "from:nuvizzapps@nuvizzapps.com subject:Driver_stops has:attachment";
const CONTRACTOR_PAY_PCT = 0.40;

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// ─── Firestore helpers ───────────────────────────────────────────────────────

async function fsGetDoc(coll: string, docId: string, apiKey: string): Promise<any | null> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${coll}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
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

async function fsPatchDoc(coll: string, docId: string, fields: Record<string, any>, apiKey: string): Promise<boolean> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${coll}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const body: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) body[k] = toFsValue(v);
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: body }),
  });
  return r.ok;
}

// ─── Gmail account discovery + token refresh ────────────────────────────────

type TokenDoc = { docId: string; email: string; refresh_token: string };

async function listConnectedAccounts(apiKey: string): Promise<TokenDoc[]> {
  const accounts: Record<string, TokenDoc> = {};
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config?key=${apiKey}&pageSize=100`;
  const r = await fetch(url);
  if (!r.ok) return [];
  const data: any = await r.json();
  for (const d of (data.documents || [])) {
    const docId = (d.name || "").split("/").pop() || "";
    if (docId !== "gmail_tokens" && !docId.startsWith("gmail_tokens_")) continue;
    const f = d.fields || {};
    const refreshToken = f.refresh_token?.stringValue;
    const email = f.email?.stringValue || "unknown";
    if (!refreshToken) continue;
    const existing = accounts[email];
    // Prefer per-account doc over legacy singleton when both exist for same email.
    if (!existing || (existing.docId === "gmail_tokens" && docId !== "gmail_tokens")) {
      accounts[email] = { docId, email, refresh_token: refreshToken };
    }
  }
  return Object.values(accounts);
}

async function getFreshAccessToken(clientId: string, clientSecret: string, refreshToken: string): Promise<string | null> {
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
  const data: any = await r.json();
  return r.ok ? data.access_token : null;
}

// ─── Gmail message search + attachment download ────────────────────────────

async function searchNuvizzMessages(accessToken: string, maxResults: number = 5): Promise<{ id: string }[]> {
  const url = `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${encodeURIComponent(VENDOR_QUERY)}&maxResults=${maxResults}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!r.ok) return [];
  const data: any = await r.json();
  return data.messages || [];
}

async function getMessageDetails(accessToken: string, messageId: string): Promise<any | null> {
  const url = `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  return r.ok ? await r.json() : null;
}

// Recursively walk MIME parts to find the first CSV attachment.
function findCsvAttachment(payload: any): { filename: string; attachmentId: string; mimeType: string; size: number } | null {
  if (!payload) return null;
  const filename: string = payload.filename || "";
  const fnLower = filename.toLowerCase();
  if (filename && payload.body?.attachmentId &&
      (fnLower.endsWith(".csv") || fnLower.includes("driver_stops") || fnLower.includes("nuvizz"))) {
    return {
      filename,
      attachmentId: payload.body.attachmentId,
      mimeType: payload.mimeType || "",
      size: payload.body.size || 0,
    };
  }
  for (const part of (payload.parts || [])) {
    const found = findCsvAttachment(part);
    if (found) return found;
  }
  return null;
}

async function downloadAttachment(accessToken: string, messageId: string, attachmentId: string): Promise<string | null> {
  const url = `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}/attachments/${attachmentId}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!r.ok) return null;
  const data: any = await r.json();
  // Gmail returns URL-safe base64; convert to standard base64
  const b64 = (data.data || "").replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(b64, "base64").toString("utf8");
}

// ─── NuVizz CSV parser ───────────────────────────────────────────────────────
// Replicates parseNuVizz from public/MarginIQ.jsx exactly. Producing the same
// shape as the client-side parser so the dispatcher's PRO-keyed dedup works.

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (c === "," && !inQuotes) {
      out.push(cur);
      cur = "";
    } else {
      cur += c;
    }
  }
  out.push(cur);
  return out;
}

function parseMoney(s: string): number {
  if (!s) return 0;
  const cleaned = String(s).trim().replace(/[$,"]/g, "");
  const n = parseFloat(cleaned);
  return isNaN(n) ? 0 : n;
}

function parseDateMDY(s: string): string | null {
  if (!s) return null;
  const datePart = String(s).trim().split(" ")[0];
  const m = datePart.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (!m) return null;
  const mo = parseInt(m[1], 10);
  const d = parseInt(m[2], 10);
  let y = parseInt(m[3], 10);
  if (y < 100) y += 2000;
  return `${y}-${String(mo).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

function weekEndingFriday(isoDate: string): string {
  // Roll forward to next Friday (or same day if already Friday).
  const d = new Date(isoDate + "T12:00:00Z");
  const dow = d.getUTCDay(); // 0=Sun..6=Sat
  const daysToFri = (5 - dow + 7) % 7;
  const fri = new Date(d.getTime() + daysToFri * 86400000);
  return fri.toISOString().slice(0, 10);
}

function normalizePro(v: any): string | null {
  if (v == null) return null;
  let s = String(v).trim();
  if (s === "") return null;
  s = s.replace(/\.0+$/, "");
  const stripped = s.replace(/^0+/, "");
  return stripped || s;
}

interface ParsedStop {
  stop_number: string | null;
  pro: string | null;
  driver_name: string;
  delivery_date: string | null;
  week_ending: string | null;
  month: string | null;
  status: string | null;
  ship_to: string | null;
  city: string | null;
  zip: string | null;
  contractor_pay_base: number;
  contractor_pay_at_40: number;
}

function parseNuvizzCsv(csvText: string): ParsedStop[] {
  const lines = csvText.replace(/\r\n/g, "\n").split("\n").filter(l => l.length > 0);
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]).map(h => h.toLowerCase().trim());
  const idx = (name: string) => headers.indexOf(name);
  const iDeliv = idx("delivery end");
  const iStopNum = idx("stop number");
  const iDriver = idx("driver name");
  const iStatus = idx("stop status");
  const iShipTo = idx("ship to name");
  const iCity = idx("ship to - city");
  const iZip = idx("ship to - zip code");
  const iSeal = idx("stop sealnbr");

  const stops: ParsedStop[] = [];
  for (let li = 1; li < lines.length; li++) {
    const cols = parseCsvLine(lines[li]);
    const stopNum = (iStopNum >= 0 ? (cols[iStopNum] || "") : "").trim();
    const driver = (iDriver >= 0 ? (cols[iDriver] || "") : "").trim();
    if (!stopNum && !driver) continue;
    const deliveryDate = iDeliv >= 0 ? parseDateMDY(cols[iDeliv] || "") : null;
    if (!deliveryDate) continue;
    const payBase = iSeal >= 0 ? parseMoney(cols[iSeal] || "") : 0;
    stops.push({
      stop_number: stopNum || null,
      pro: normalizePro(stopNum),
      driver_name: driver.replace(/\s+/g, " ").trim(),
      delivery_date: deliveryDate,
      week_ending: weekEndingFriday(deliveryDate),
      month: deliveryDate.slice(0, 7),
      status: (iStatus >= 0 ? (cols[iStatus] || "") : "").trim() || null,
      ship_to: (iShipTo >= 0 ? (cols[iShipTo] || "") : "").trim() || null,
      city: (iCity >= 0 ? (cols[iCity] || "") : "").trim() || null,
      zip: (iZip >= 0 ? (cols[iZip] || "") : "").trim() || null,
      contractor_pay_base: payBase,
      contractor_pay_at_40: payBase * CONTRACTOR_PAY_PCT,
    });
  }
  return stops;
}

// ─── Dispatcher invocation (multi-batch) ────────────────────────────────────
//
// Splits the parsed stops into chunks small enough to fit under Netlify's
// ~4 MB sync POST limit, then dispatches each chunk separately. Each chunk
// becomes its own dispatcher run_id; we track all of them. Even though
// each individual dispatcher call still goes through the chunked-payload
// fix from v2.41.17 (which works fine for any size), the limiting factor
// here is the function-to-function sync POST size.
//
// Empirically (during the 2025 recovery): 10K stops fits in ~3 MB JSON,
// so we target 10K stops per batch as a safe ceiling. The 2025 weekly CSV
// was ~13K stops at 2 MB, so most weekly emails will go in one batch.
// Edge case: the Apr 21 email had 7.5 MB of attachment → ~50K stops →
// 5 batches.

const BATCH_STOP_COUNT = 10000;

async function dispatchToIngest(siteOrigin: string, stops: ParsedStop[], source: string): Promise<{
  ok: boolean;
  runIds: string[];
  totalChunks: number;
  batchesDispatched: number;
  batchesFailed: number;
  error?: string;
}> {
  const runIds: string[] = [];
  let totalChunks = 0;
  let batchesFailed = 0;
  const errors: string[] = [];

  // Split stops into batches of BATCH_STOP_COUNT
  const batches: ParsedStop[][] = [];
  for (let i = 0; i < stops.length; i += BATCH_STOP_COUNT) {
    batches.push(stops.slice(i, i + BATCH_STOP_COUNT));
  }

  for (let b = 0; b < batches.length; b++) {
    const batch = batches[b];
    const batchSource = batches.length > 1 ? `${source}_b${b + 1}of${batches.length}` : source;
    const url = `${siteOrigin}/.netlify/functions/marginiq-nuvizz-ingest`;
    try {
      const r = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ stops: batch, source: batchSource }),
      });
      let data: any = null;
      try { data = await r.json(); } catch {}
      if (!r.ok) {
        batchesFailed++;
        errors.push(`Batch ${b + 1}: ${data?.error || `HTTP ${r.status}`}`);
        continue;
      }
      if (data?.run_id) runIds.push(data.run_id);
      if (data?.chunk_count) totalChunks += data.chunk_count;
    } catch (e: any) {
      batchesFailed++;
      errors.push(`Batch ${b + 1}: ${e.message || String(e)}`);
    }
  }

  return {
    ok: batchesFailed === 0,
    runIds,
    totalChunks,
    batchesDispatched: batches.length - batchesFailed,
    batchesFailed,
    error: errors.length > 0 ? errors.join("; ") : undefined,
  };
}

// ─── Main handler ──────────────────────────────────────────────────────────

export default async (req: Request, _context: Context) => {
  const CLIENT_ID = process.env["GOOGLE_CLIENT_ID"];
  const CLIENT_SECRET = process.env["GOOGLE_CLIENT_SECRET"];
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  if (!CLIENT_ID || !CLIENT_SECRET || !FIREBASE_API_KEY) {
    return json({ error: "OAuth/Firebase not configured" }, 500);
  }

  const url = new URL(req.url);
  const dryRun = url.searchParams.get("dry_run") === "1";
  const runId = `nuvizz_auto_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const startedAt = new Date().toISOString();

  await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
    run_id: runId,
    state: "running",
    started_at: startedAt,
    dry_run: dryRun,
    progress: "Listing connected Gmail accounts...",
  }, FIREBASE_API_KEY);

  try {
    const accounts = await listConnectedAccounts(FIREBASE_API_KEY);
    if (accounts.length === 0) {
      const err = "No connected Gmail accounts found";
      await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    // Find newest unprocessed message across all accounts.
    let bestMsg: { messageId: string; account: TokenDoc; accessToken: string; details: any; internalDate: number } | null = null;
    let totalChecked = 0;

    for (const acct of accounts) {
      const accessToken = await getFreshAccessToken(CLIENT_ID, CLIENT_SECRET, acct.refresh_token);
      if (!accessToken) continue;
      const msgs = await searchNuvizzMessages(accessToken, 5);
      totalChecked += msgs.length;
      for (const m of msgs) {
        const processed = await fsGetDoc("nuvizz_processed_emails", m.id, FIREBASE_API_KEY);
        if (processed) continue;
        const details = await getMessageDetails(accessToken, m.id);
        if (!details) continue;
        const internalDate = parseInt(details.internalDate || "0", 10);
        if (!bestMsg || internalDate > bestMsg.internalDate) {
          bestMsg = { messageId: m.id, account: acct, accessToken, details, internalDate };
        }
      }
    }

    if (!bestMsg) {
      const msg = `No new unprocessed NuVizz emails found (checked ${totalChecked} across ${accounts.length} account(s))`;
      await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
        state: "complete",
        completed_at: new Date().toISOString(),
        progress: msg,
        accounts_checked: accounts.length,
        new_emails_found: 0,
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "complete", progress: msg }, 200);
    }

    const subject = bestMsg.details.payload?.headers?.find((h: any) => h.name === "Subject")?.value || "";
    const att = findCsvAttachment(bestMsg.details.payload);
    if (!att) {
      const err = `Email ${bestMsg.messageId} has no CSV attachment`;
      await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
        message_id: bestMsg.messageId, subject,
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
      progress: `Found ${att.filename} (${att.size} bytes) in ${bestMsg.account.email}; downloading...`,
      message_id: bestMsg.messageId, subject,
      attachment_filename: att.filename,
      attachment_size: att.size,
      account_email: bestMsg.account.email,
      email_date: new Date(bestMsg.internalDate).toISOString(),
    }, FIREBASE_API_KEY);

    if (dryRun) {
      await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
        state: "complete",
        completed_at: new Date().toISOString(),
        progress: `[DRY RUN] Would dispatch ${att.filename} from ${bestMsg.account.email}`,
      }, FIREBASE_API_KEY);
      return json({
        run_id: runId,
        state: "complete",
        dry_run: true,
        message_id: bestMsg.messageId,
        subject,
        attachment: att,
        account: bestMsg.account.email,
      }, 200);
    }

    const csvText = await downloadAttachment(bestMsg.accessToken, bestMsg.messageId, att.attachmentId);
    if (!csvText) {
      const err = "Attachment download failed";
      await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
      progress: "Parsing CSV...",
      csv_bytes: csvText.length,
    }, FIREBASE_API_KEY);
    const stops = parseNuvizzCsv(csvText);
    if (stops.length === 0) {
      const err = "CSV produced 0 stops — parser failure or empty file";
      await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    const siteOrigin = `${url.protocol}//${url.host}`;
    const source = `auto_gmail_${bestMsg.account.email}_${bestMsg.messageId.slice(0, 12)}`;
    await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
      progress: `Dispatching ${stops.length.toLocaleString()} stops to ingest endpoint...`,
      stops_parsed: stops.length,
    }, FIREBASE_API_KEY);

    const dispatch = await dispatchToIngest(siteOrigin, stops, source);
    if (!dispatch.ok && dispatch.batchesDispatched === 0) {
      // Total failure — no batches went through
      const err = `Dispatch failed: ${dispatch.error}`;
      await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    // Mark messageId as processed (idempotency record). Even if some batches
    // failed, we mark it processed — the failed batches' stops were never
    // saved, but PRO-keyed dedup means a future re-run wouldn't help anyway
    // (would just re-fail the same way). The status doc records partial
    // failure for visibility.
    await fsPatchDoc("nuvizz_processed_emails", bestMsg.messageId, {
      message_id: bestMsg.messageId,
      subject,
      account_email: bestMsg.account.email,
      email_date: new Date(bestMsg.internalDate).toISOString(),
      processed_at: new Date().toISOString(),
      auto_ingest_run_id: runId,
      dispatcher_run_ids: dispatch.runIds,
      stops_parsed: stops.length,
      attachment_filename: att.filename,
      batches_dispatched: dispatch.batchesDispatched,
      batches_failed: dispatch.batchesFailed,
    }, FIREBASE_API_KEY);

    const finalState = dispatch.batchesFailed === 0 ? "complete" : "complete_with_errors";
    const summary = dispatch.batchesFailed === 0
      ? `✓ Dispatched ${stops.length.toLocaleString()} stops in ${dispatch.batchesDispatched} batch(es); ${dispatch.totalChunks} chunks total`
      : `⚠ Dispatched ${dispatch.batchesDispatched}/${dispatch.batchesDispatched + dispatch.batchesFailed} batches; ${dispatch.batchesFailed} failed: ${dispatch.error}`;

    await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
      state: finalState,
      completed_at: new Date().toISOString(),
      progress: summary,
      dispatcher_run_ids: dispatch.runIds,
      dispatcher_total_chunks: dispatch.totalChunks,
      batches_dispatched: dispatch.batchesDispatched,
      batches_failed: dispatch.batchesFailed,
      stops_parsed: stops.length,
    }, FIREBASE_API_KEY);

    return json({
      run_id: runId,
      state: finalState,
      message_id: bestMsg.messageId,
      account: bestMsg.account.email,
      subject,
      stops_parsed: stops.length,
      dispatcher_run_ids: dispatch.runIds,
      dispatcher_total_chunks: dispatch.totalChunks,
      batches_dispatched: dispatch.batchesDispatched,
      batches_failed: dispatch.batchesFailed,
    }, 200);
  } catch (err: any) {
    await fsPatchDoc("nuvizz_auto_ingest_logs", runId, {
      state: "failed",
      error: err?.message || String(err),
      completed_at: new Date().toISOString(),
    }, FIREBASE_API_KEY);
    return json({ run_id: runId, state: "failed", error: err?.message || String(err) }, 500);
  }
};

// Schedule: Sunday at 12:00 UTC AND 13:00 UTC. Same DST-aware pattern as
// the B600 scraper (v2.41.14). Runs at 8am EDT (Mar–Nov) and 8am EST
// (Nov–Mar). The duplicate run is idempotent via nuvizz_processed_emails
// tracking — once a messageId has been processed, the second run picks
// the next-newest unprocessed (or no-op if there isn't one).
export const config: Config = {
  schedule: "0 12,13 * * 0",
};

import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — DDIS Email Auto-Ingest (v2.42.11)
 *
 * Scheduled function that runs Monday morning, looks for the most recent
 * unprocessed Uline DDIS820 payment-remittance CSV email in any connected
 * Gmail account, downloads the attachment, parses it, and dispatches
 * through the existing marginiq-ddis-ingest endpoint.
 *
 * Why Monday morning: Uline DDIS remittances arrive throughout the week
 * (typically Mon/Wed/Fri) as APFreight settlements clear. Running Monday
 * 8am ET catches anything posted Fri-Sun, then subsequent runs catch the
 * rest as they arrive. Idempotency means daily runs are also safe.
 *
 * Schedule: cron `0 12,13 * * 1` (Mon 12:00 UTC + 13:00 UTC).
 *   - 12:00 UTC = 8am EDT (Mar–Nov)
 *   - 13:00 UTC = 8am EST (Nov–Mar)
 * Same DST-aware double-fire pattern as the B600 + NuVizz schedules.
 *
 * Process (parallels marginiq-nuvizz-auto-ingest.mts v2.42.10):
 *   1. listConnectedAccounts() - chad@ + billing@
 *   2. searchDdisMessages() - 'from:APFreight@uline.com filename:csv newer_than:30d'
 *   3. Skip messageIds already in ddis_processed_emails/{messageId}
 *   4. Pick newest unprocessed across all accounts
 *   5. Download CSV attachment, parse into row-objects (lowercased headers)
 *   6. parseDdisRows() → payments[] (port of parseDDIS in MarginIQ.jsx)
 *   7. computeBillWeekEnding() → bill_week_ending + covers_weeks
 *   8. Build ddisFileRecords[] (single entry per email) + ddisPayments[]
 *   9. POST to marginiq-ddis-ingest dispatcher
 *  10. Mark messageId processed; write ddis_auto_ingest_logs/{run_id}
 *
 * DDIS files are typically much smaller than NuVizz weekly exports (~3K
 * rows / a few hundred KB), so single-batch dispatch fits comfortably
 * under Netlify's sync POST limit. No multi-batch logic needed here.
 *
 * Env vars: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, FIREBASE_API_KEY
 */

const PROJECT_ID = "davismarginiq";
const VENDOR_QUERY = "from:APFreight@uline.com filename:csv newer_than:30d";

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

async function searchDdisMessages(accessToken: string, maxResults: number = 5): Promise<{ id: string }[]> {
  const url = new URL("https://gmail.googleapis.com/gmail/v1/users/me/messages");
  url.searchParams.set("q", VENDOR_QUERY);
  url.searchParams.set("maxResults", String(maxResults));
  const r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
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
      (fnLower.endsWith(".csv") || fnLower.includes("ddis"))) {
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
  const b64 = (data.data || "").replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(b64, "base64").toString("utf8");
}

// ─── CSV parser (mirrors readCSV + parseCSVStream from MarginIQ.jsx) ─────────

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

// Returns array of objects keyed by lowercased headers — matches what
// readCSV() produces in the client and what parseDDIS expects as input.
function csvToRowObjects(csvText: string): Record<string, any>[] {
  // Strip UTF-8 BOM
  if (csvText.charCodeAt(0) === 0xFEFF) csvText = csvText.slice(1);
  const lines = csvText.replace(/\r\n/g, "\n").split("\n");
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0]).map(h => String(h || "").toLowerCase().trim());
  const out: Record<string, any>[] = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (line.length === 0) continue;
    const cols = parseCsvLine(line);
    // Skip completely empty lines
    if (cols.length === 1 && cols[0] === "") continue;
    const obj: Record<string, any> = {};
    for (let j = 0; j < headers.length; j++) {
      const v = cols[j];
      obj[headers[j]] = (v === undefined || v === "") ? null : v;
    }
    out.push(obj);
  }
  return out;
}

// ─── DDIS parser (port of parseDDIS in MarginIQ.jsx) ────────────────────────

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

function normalizePro(v: any): string | null {
  if (v == null) return null;
  let s = String(v).trim();
  if (s === "") return null;
  s = s.replace(/\.0+$/, "");
  const stripped = s.replace(/^0+/, "");
  return stripped || s;
}

interface DdisPayment {
  pro: string;
  voucher: string | null;
  check: string | null;
  bill_date: string | null;
  paid: number;
}

function parseDdisRows(rows: Record<string, any>[]): DdisPayment[] {
  const payments: DdisPayment[] = [];
  for (const r of rows) {
    const proRaw = r["pro#"];
    if (!proRaw) continue;
    const pro = normalizePro(proRaw);
    if (!pro) continue;
    const paidAmount = parseFloat(String(r["paid amount"] || "")) || 0;
    const billDate = parseDateMDY(String(r["bill date"] || ""));
    payments.push({
      pro,
      voucher: r["voucher#"] ? String(r["voucher#"]) : null,
      check: r["check#"] ? String(r["check#"]) : null,
      bill_date: billDate,
      paid: paidAmount,
    });
  }
  return payments;
}

// ─── Bill-week-ending derivation (port of computeBillWeekEnding) ────────────

function fridayEndOf(iso: string): string | null {
  const d = new Date(iso + "T12:00:00Z");
  if (isNaN(d.getTime())) return null;
  const dow = d.getUTCDay(); // 0=Sun..6=Sat
  const daysToFri = (5 - dow + 7) % 7;
  const fri = new Date(d.getTime() + daysToFri * 86400000);
  return fri.toISOString().slice(0, 10);
}

const COVERS_PCT = 0.20;

interface BillWeekResult {
  bill_week_ending: string | null;
  week_ambiguous: boolean;
  ambiguous_candidates: { friday: string; rows: number }[];
  top5: { date: string; count: number }[];
  covers_weeks: string[];
}

function computeBillWeekEnding(billDates: (string | null)[]): BillWeekResult {
  const result: BillWeekResult = {
    bill_week_ending: null,
    week_ambiguous: false,
    ambiguous_candidates: [],
    top5: [],
    covers_weeks: [],
  };
  if (billDates.length === 0) return result;
  const dateCounts = new Map<string, number>();
  for (const bd of billDates) {
    if (!bd) continue;
    dateCounts.set(bd, (dateCounts.get(bd) || 0) + 1);
  }
  if (dateCounts.size === 0) return result;
  const totalRows = billDates.filter(d => d != null).length;
  const sorted = [...dateCounts.entries()].sort((a, b) => b[1] - a[1]);
  const top5 = sorted.slice(0, 5).map(([date, count]) => ({ date, count }));
  result.top5 = top5;
  // Bucket each top-5 date to its Fri-ending envelope, sum row counts per envelope
  const envelopeCounts = new Map<string, number>();
  for (const { date, count } of top5) {
    const fri = fridayEndOf(date);
    if (!fri) continue;
    envelopeCounts.set(fri, (envelopeCounts.get(fri) || 0) + count);
  }
  if (envelopeCounts.size === 0) return result;
  const envs = [...envelopeCounts.entries()].sort((a, b) => b[1] - a[1]);
  if (envs.length > 1 && envs[0][1] === envs[1][1]) {
    result.week_ambiguous = true;
    const tieCount = envs[0][1];
    result.ambiguous_candidates = envs.filter(([, n]) => n === tieCount).map(([friday, rows]) => ({ friday, rows }));
  } else {
    result.bill_week_ending = envs[0][0];
  }
  // covers_weeks: ALL bill_dates whose Fri envelope has ≥20% of total rows
  const allEnvelopeCounts = new Map<string, number>();
  for (const [bd, count] of dateCounts.entries()) {
    const fri = fridayEndOf(bd);
    if (!fri) continue;
    allEnvelopeCounts.set(fri, (allEnvelopeCounts.get(fri) || 0) + count);
  }
  const threshold = Math.max(1, Math.ceil(totalRows * COVERS_PCT));
  const coversSet = new Set<string>();
  for (const [fri, count] of allEnvelopeCounts.entries()) {
    if (count >= threshold) coversSet.add(fri);
  }
  if (result.bill_week_ending) coversSet.add(result.bill_week_ending);
  for (const c of result.ambiguous_candidates) coversSet.add(c.friday);
  result.covers_weeks = [...coversSet].sort();
  return result;
}

// ─── Build ddisFileRecords + ddisPayments (mirrors ingestFiles loop) ────────

interface DdisIngestPayload {
  ddisFileRecords: any[];
  ddisPayments: any[];
}

function buildIngestPayload(
  filename: string,
  payments: DdisPayment[],
  emailFrom: string,
): DdisIngestPayload {
  const fileId = filename.replace(/[^a-z0-9._-]/gi, "_").slice(0, 140);
  const billDates = payments.map(p => p.bill_date).filter((d): d is string => !!d).sort();
  const totalPaid = payments.reduce((s, p) => s + p.paid, 0);
  const wk = computeBillWeekEnding(payments.map(p => p.bill_date));
  const uploadedAt = new Date().toISOString();

  const fileRecord = {
    file_id: fileId,
    filename,
    record_count: payments.length,
    total_paid: totalPaid,
    earliest_bill_date: billDates[0] || null,
    latest_bill_date: billDates[billDates.length - 1] || null,
    bill_week_ending: wk.bill_week_ending,
    week_ambiguous: wk.week_ambiguous,
    ambiguous_candidates: wk.ambiguous_candidates,
    top5_bill_dates: wk.top5,
    covers_weeks: wk.covers_weeks,
    checks: [...new Set(payments.map(p => p.check).filter(Boolean))],
    uploaded_at: uploadedAt,
    source: "auto_gmail_ddis",
    email_from: emailFrom,
  };

  const paymentDocs = [];
  for (const p of payments) {
    if (!p.pro || p.paid <= 0) continue;
    const payId = `${p.pro}_${p.bill_date || "nodate"}_${p.check || "nocheck"}`;
    paymentDocs.push({
      id: payId,
      pro: p.pro,
      paid_amount: p.paid,
      bill_date: p.bill_date || null,
      check: p.check || null,
      voucher: p.voucher || null,
      source_file: filename,
      uploaded_at: uploadedAt,
    });
  }

  return {
    ddisFileRecords: [fileRecord],
    ddisPayments: paymentDocs,
  };
}

// ─── Dispatcher invocation ──────────────────────────────────────────────────

async function dispatchDdisIngest(
  siteOrigin: string,
  payload: DdisIngestPayload,
): Promise<{ ok: boolean; runId?: string; error?: string }> {
  const url = `${siteOrigin}/.netlify/functions/marginiq-ddis-ingest`;
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  let data: any = null;
  try { data = await r.json(); } catch {}
  if (!r.ok) return { ok: false, error: data?.error || `HTTP ${r.status}` };
  return { ok: true, runId: data?.run_id || data?.status_id };
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
  const runId = `ddis_auto_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const startedAt = new Date().toISOString();

  await fsPatchDoc("ddis_auto_ingest_logs", runId, {
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
      await fsPatchDoc("ddis_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    let bestMsg: { messageId: string; account: TokenDoc; accessToken: string; details: any; internalDate: number } | null = null;
    let totalChecked = 0;

    for (const acct of accounts) {
      const accessToken = await getFreshAccessToken(CLIENT_ID, CLIENT_SECRET, acct.refresh_token);
      if (!accessToken) continue;
      const msgs = await searchDdisMessages(accessToken, 5);
      totalChecked += msgs.length;
      for (const m of msgs) {
        const processed = await fsGetDoc("ddis_processed_emails", m.id, FIREBASE_API_KEY);
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
      const msg = `No new unprocessed DDIS emails found (checked ${totalChecked} across ${accounts.length} account(s))`;
      await fsPatchDoc("ddis_auto_ingest_logs", runId, {
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
      await fsPatchDoc("ddis_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
        message_id: bestMsg.messageId, subject,
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    await fsPatchDoc("ddis_auto_ingest_logs", runId, {
      progress: `Found ${att.filename} (${att.size} bytes) in ${bestMsg.account.email}`,
      message_id: bestMsg.messageId, subject,
      attachment_filename: att.filename,
      attachment_size: att.size,
      account_email: bestMsg.account.email,
      email_date: new Date(bestMsg.internalDate).toISOString(),
    }, FIREBASE_API_KEY);

    if (dryRun) {
      await fsPatchDoc("ddis_auto_ingest_logs", runId, {
        state: "complete",
        completed_at: new Date().toISOString(),
        progress: `[DRY RUN] Would dispatch ${att.filename} from ${bestMsg.account.email}`,
      }, FIREBASE_API_KEY);
      return json({
        run_id: runId, state: "complete", dry_run: true,
        message_id: bestMsg.messageId, subject, attachment: att,
        account: bestMsg.account.email,
      }, 200);
    }

    const csvText = await downloadAttachment(bestMsg.accessToken, bestMsg.messageId, att.attachmentId);
    if (!csvText) {
      const err = "Attachment download failed";
      await fsPatchDoc("ddis_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    await fsPatchDoc("ddis_auto_ingest_logs", runId, {
      progress: "Parsing CSV...",
      csv_bytes: csvText.length,
    }, FIREBASE_API_KEY);

    const rows = csvToRowObjects(csvText);
    const payments = parseDdisRows(rows);

    // Sanity check: a real DDIS820 file has hundreds to thousands of rows.
    // If we get fewer than 10, something is off (wrong file format, wrong
    // sender, etc.). Mark processed-and-skipped to avoid re-trying.
    if (payments.length < 10) {
      const note = `Parsed only ${payments.length} payments from ${rows.length} rows — below threshold; likely non-DDIS format`;
      await fsPatchDoc("ddis_processed_emails", bestMsg.messageId, {
        message_id: bestMsg.messageId, subject,
        account_email: bestMsg.account.email,
        email_date: new Date(bestMsg.internalDate).toISOString(),
        processed_at: new Date().toISOString(),
        auto_ingest_run_id: runId,
        skipped: true, skip_reason: note,
        attachment_filename: att.filename,
        rows_parsed: rows.length,
        payments_parsed: payments.length,
      }, FIREBASE_API_KEY);
      await fsPatchDoc("ddis_auto_ingest_logs", runId, {
        state: "complete",
        completed_at: new Date().toISOString(),
        progress: `⊘ Skipped: ${note}`,
        skipped: true,
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "complete", skipped: true, skip_reason: note }, 200);
    }

    const ingestPayload = buildIngestPayload(att.filename, payments, bestMsg.account.email);

    await fsPatchDoc("ddis_auto_ingest_logs", runId, {
      progress: `Dispatching ${payments.length.toLocaleString()} payments to ingest endpoint...`,
      payments_parsed: payments.length,
      ddis_payments_dispatched: ingestPayload.ddisPayments.length,
      bill_week_ending: ingestPayload.ddisFileRecords[0].bill_week_ending,
      week_ambiguous: ingestPayload.ddisFileRecords[0].week_ambiguous,
      total_paid: ingestPayload.ddisFileRecords[0].total_paid,
    }, FIREBASE_API_KEY);

    const siteOrigin = `${url.protocol}//${url.host}`;
    const dispatch = await dispatchDdisIngest(siteOrigin, ingestPayload);
    if (!dispatch.ok) {
      const err = `Dispatch failed: ${dispatch.error}`;
      await fsPatchDoc("ddis_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    await fsPatchDoc("ddis_processed_emails", bestMsg.messageId, {
      message_id: bestMsg.messageId, subject,
      account_email: bestMsg.account.email,
      email_date: new Date(bestMsg.internalDate).toISOString(),
      processed_at: new Date().toISOString(),
      auto_ingest_run_id: runId,
      dispatcher_run_id: dispatch.runId || "",
      payments_parsed: payments.length,
      attachment_filename: att.filename,
      bill_week_ending: ingestPayload.ddisFileRecords[0].bill_week_ending,
      total_paid: ingestPayload.ddisFileRecords[0].total_paid,
    }, FIREBASE_API_KEY);

    await fsPatchDoc("ddis_auto_ingest_logs", runId, {
      state: "complete",
      completed_at: new Date().toISOString(),
      progress: `✓ Dispatched ${payments.length.toLocaleString()} payments (bill_week=${ingestPayload.ddisFileRecords[0].bill_week_ending}, total_paid=$${ingestPayload.ddisFileRecords[0].total_paid.toFixed(2)})`,
      dispatcher_run_id: dispatch.runId || "",
    }, FIREBASE_API_KEY);

    return json({
      run_id: runId,
      state: "complete",
      message_id: bestMsg.messageId,
      account: bestMsg.account.email,
      subject,
      payments_parsed: payments.length,
      bill_week_ending: ingestPayload.ddisFileRecords[0].bill_week_ending,
      total_paid: ingestPayload.ddisFileRecords[0].total_paid,
      dispatcher_run_id: dispatch.runId,
    }, 200);
  } catch (err: any) {
    await fsPatchDoc("ddis_auto_ingest_logs", runId, {
      state: "failed",
      error: err?.message || String(err),
      completed_at: new Date().toISOString(),
    }, FIREBASE_API_KEY);
    return json({ run_id: runId, state: "failed", error: err?.message || String(err) }, 500);
  }
};

// Schedule: Monday at 12:00 UTC AND 13:00 UTC. Same DST-aware double-fire
// pattern as B600 (Sun) and NuVizz (Sun) schedules. Idempotency via the
// ddis_processed_emails collection makes the duplicate run a no-op.
export const config: Config = {
  schedule: "0 12,13 * * 1",
};

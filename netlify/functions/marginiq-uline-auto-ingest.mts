import type { Context, Config } from "@netlify/functions";
import { gzipSync } from "node:zlib";

/**
 * Davis MarginIQ — Uline DAS Auto-Ingest (Staging) (v2.42.18)
 *
 * Runs Monday morning. Searches connected Gmail accounts for the weekly
 * Uline DAS xlsx batch (3-4 files per email — delivery, truckload,
 * accessorials, sometimes corrections). Downloads every CSV/XLSX
 * attachment and stages it as a doc in pending_uline_files/{file_id}
 * with the file content gzipped+base64 (chunked under 1 MiB Firestore
 * doc cap when needed).
 *
 * This is a STAGING-ONLY pipeline — does NOT parse the files, does NOT
 * dispatch anything to a Firestore-write worker. The existing UI in
 * MarginIQ.jsx Data Ingest tab will list pending files with a "Load"
 * button. Load pulls the file content back, reconstructs a File-like
 * object, and feeds it into the existing client-side ingestFiles()
 * pipeline unchanged. This keeps the complex DAS audit math
 * (buildWeeklyRollups, recon variance, audit items) on the proven
 * client-side path.
 *
 * Why this design: DAS ingest involves 8+ steps with cross-references
 * to DDIS payment data. Building a server-side dispatcher with parity
 * to the client-side path would be a multi-day project. Staging
 * captures most of the value (no more manual Gmail-search-and-download
 * every Monday) without risking parity bugs.
 *
 * Idempotency: gmail messageId in uline_processed_emails/{messageId}
 * after attachments are successfully staged. Subsequent runs skip
 * already-processed messages.
 *
 * Schedule: cron `0 13,14 * * 1` (Mon 13:00 UTC + 14:00 UTC).
 *   - 13:00 UTC = 9am EDT (Mar–Nov)
 *   - 14:00 UTC = 9am EST (Nov–Mar)
 * Runs an hour after DDIS auto-ingest (which is at 12,13 * * 1) so DDIS
 * payment data is in Firestore first — the manual DAS upload's audit
 * math depends on it.
 *
 * Manual trigger: GET /.netlify/functions/marginiq-uline-auto-ingest
 *   ?dry_run=1 — preview without staging
 *
 * Env vars: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, FIREBASE_API_KEY
 */

const PROJECT_ID = "davismarginiq";
// Same query the existing manual gmail-search uses for the "uline" vendor.
// Includes billing@davisdelivery.com because corrected/follow-up files
// sometimes get re-sent from there. Excludes APFreight (DDIS).
const VENDOR_QUERY = '(from:@uline.com OR from:billing@davisdelivery.com) filename:das filename:xlsx -from:APFreight@uline.com newer_than:30d';

// Target raw chunk size — same as DDIS dispatcher. Keeps each Firestore
// doc well under the 1 MiB doc cap once gzipped+base64-encoded.
const RAW_CHUNK_BUDGET = 700_000;

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

// ─── Gmail search + attachment download ─────────────────────────────────────

async function searchUlineMessages(accessToken: string, maxResults: number = 5): Promise<{ id: string }[]> {
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

interface DasAttachment {
  filename: string;
  attachmentId: string;
  mimeType: string;
  size: number;
}

// Walk MIME parts to collect ALL DAS attachments (CSV or XLSX). Unlike the
// NuVizz/DDIS auto-ingests which pick one file per email, DAS emails carry
// 3-4 files (delivery + truckload + accessorials, sometimes corrections).
function findAllDasAttachments(payload: any): DasAttachment[] {
  const out: DasAttachment[] = [];
  function walk(node: any) {
    if (!node) return;
    const filename: string = node.filename || "";
    const fnLower = filename.toLowerCase();
    if (filename && node.body?.attachmentId) {
      // Match the same "starts with das" filter the manual UI uses.
      if (fnLower.startsWith("das ") || fnLower.startsWith("das_") || fnLower.startsWith("das-") || fnLower.includes(" das ")) {
        if (fnLower.endsWith(".xlsx") || fnLower.endsWith(".xls") || fnLower.endsWith(".csv")) {
          out.push({
            filename,
            attachmentId: node.body.attachmentId,
            mimeType: node.mimeType || "",
            size: node.body.size || 0,
          });
        }
      }
    }
    for (const part of (node.parts || [])) walk(part);
  }
  walk(payload);
  return out;
}

async function downloadAttachmentBinary(accessToken: string, messageId: string, attachmentId: string): Promise<Buffer | null> {
  const url = `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}/attachments/${attachmentId}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!r.ok) return null;
  const data: any = await r.json();
  // Gmail returns URL-safe base64. Convert to standard base64 then decode
  // to a Buffer (binary, not utf-8 string — XLSX is a zip archive).
  const b64 = (data.data || "").replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(b64, "base64");
}

// ─── Stage attachment to Firestore as gzipped chunks ────────────────────────

function sanitizeFileId(filename: string): string {
  return filename.replace(/[^a-z0-9._-]/gi, "_").slice(0, 140);
}

async function stageAttachment(
  apiKey: string,
  fileId: string,
  filename: string,
  mimeType: string,
  binary: Buffer,
  emailMeta: { messageId: string; subject: string; account: string; emailDate: string },
): Promise<{ ok: boolean; chunkCount: number; rawBytes: number; gzBytes: number; error?: string }> {
  // Gzip the binary content first, then base64-encode for storage as
  // stringValue. Most XLSX files (which are zip-compressed already) won't
  // compress much further, but the gzip wrapper is cheap and consistent
  // with the dispatcher pattern.
  const gz = gzipSync(binary);
  const b64 = gz.toString("base64");

  // Each Firestore doc has a 1 MiB cap. Base64 encoding inflates by 4/3,
  // so we size chunks of the base64 string to ~700 KB each.
  const chunks: string[] = [];
  for (let i = 0; i < b64.length; i += RAW_CHUNK_BUDGET) {
    chunks.push(b64.substring(i, i + RAW_CHUNK_BUDGET));
  }

  // Write the parent file doc first
  const headerOk = await fsPatchDoc("pending_uline_files", fileId, {
    file_id: fileId,
    filename,
    mime_type: mimeType,
    size_bytes: binary.length,
    gz_bytes: gz.length,
    chunk_count: chunks.length,
    state: "staged",
    staged_at: new Date().toISOString(),
    email_message_id: emailMeta.messageId,
    email_subject: emailMeta.subject,
    email_account: emailMeta.account,
    email_date: emailMeta.emailDate,
  }, apiKey);
  if (!headerOk) return { ok: false, chunkCount: 0, rawBytes: binary.length, gzBytes: gz.length, error: "Failed to write parent file doc" };

  // Write each chunk
  let chunkErrors = 0;
  for (let i = 0; i < chunks.length; i++) {
    const chunkId = `${fileId}__${String(i).padStart(3, "0")}`;
    const ok = await fsPatchDoc("pending_uline_file_chunks", chunkId, {
      file_id: fileId,
      chunk_index: i,
      chunk_count: chunks.length,
      data_b64: chunks[i],
      created_at: new Date().toISOString(),
    }, apiKey);
    if (!ok) chunkErrors++;
  }

  if (chunkErrors > 0) {
    return { ok: false, chunkCount: chunks.length, rawBytes: binary.length, gzBytes: gz.length, error: `${chunkErrors}/${chunks.length} chunks failed to write` };
  }
  return { ok: true, chunkCount: chunks.length, rawBytes: binary.length, gzBytes: gz.length };
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
  const runId = `uline_auto_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const startedAt = new Date().toISOString();

  await fsPatchDoc("uline_auto_ingest_logs", runId, {
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
      await fsPatchDoc("uline_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    // Find newest unprocessed Uline DAS email across all accounts
    let bestMsg: { messageId: string; account: TokenDoc; accessToken: string; details: any; internalDate: number } | null = null;
    let totalChecked = 0;

    for (const acct of accounts) {
      const accessToken = await getFreshAccessToken(CLIENT_ID, CLIENT_SECRET, acct.refresh_token);
      if (!accessToken) continue;
      const msgs = await searchUlineMessages(accessToken, 5);
      totalChecked += msgs.length;
      for (const m of msgs) {
        const processed = await fsGetDoc("uline_processed_emails", m.id, FIREBASE_API_KEY);
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
      const msg = `No new unprocessed Uline DAS emails (checked ${totalChecked} across ${accounts.length} account(s))`;
      await fsPatchDoc("uline_auto_ingest_logs", runId, {
        state: "complete",
        completed_at: new Date().toISOString(),
        progress: msg,
        accounts_checked: accounts.length,
        new_emails_found: 0,
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "complete", progress: msg }, 200);
    }

    const subject = bestMsg.details.payload?.headers?.find((h: any) => h.name === "Subject")?.value || "";
    const attachments = findAllDasAttachments(bestMsg.details.payload);
    if (attachments.length === 0) {
      const err = `Email ${bestMsg.messageId} has no DAS attachments`;
      await fsPatchDoc("uline_auto_ingest_logs", runId, {
        state: "failed", error: err, completed_at: new Date().toISOString(),
        message_id: bestMsg.messageId, subject,
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, state: "failed", error: err }, 200);
    }

    await fsPatchDoc("uline_auto_ingest_logs", runId, {
      progress: `Found ${attachments.length} DAS attachment(s) in ${bestMsg.account.email}`,
      message_id: bestMsg.messageId, subject,
      attachment_count: attachments.length,
      attachment_names: attachments.map(a => a.filename),
      total_size_bytes: attachments.reduce((s, a) => s + a.size, 0),
      account_email: bestMsg.account.email,
      email_date: new Date(bestMsg.internalDate).toISOString(),
    }, FIREBASE_API_KEY);

    if (dryRun) {
      await fsPatchDoc("uline_auto_ingest_logs", runId, {
        state: "complete",
        completed_at: new Date().toISOString(),
        progress: `[DRY RUN] Would stage ${attachments.length} attachment(s) from ${bestMsg.account.email}`,
      }, FIREBASE_API_KEY);
      return json({
        run_id: runId, state: "complete", dry_run: true,
        message_id: bestMsg.messageId, subject,
        attachments,
        account: bestMsg.account.email,
      }, 200);
    }

    // Skip already-staged files and skip files already in file_log (already
    // ingested). The UI's existing "skip if filename in file_log" check
    // happens client-side; we mirror it here so we don't waste time staging.
    const stageResults: any[] = [];
    let totalStaged = 0;
    let totalSkipped = 0;
    let totalFailed = 0;

    for (const att of attachments) {
      const fileId = sanitizeFileId(att.filename);
      // Skip if already staged
      const existing = await fsGetDoc("pending_uline_files", fileId, FIREBASE_API_KEY);
      if (existing && existing.fields?.state?.stringValue === "staged") {
        stageResults.push({ filename: att.filename, file_id: fileId, status: "already_staged" });
        totalSkipped++;
        continue;
      }
      // Skip if already imported (in file_log). file_log is keyed by file_id.
      const inLog = await fsGetDoc("file_log", fileId, FIREBASE_API_KEY);
      if (inLog) {
        stageResults.push({ filename: att.filename, file_id: fileId, status: "already_imported" });
        totalSkipped++;
        continue;
      }

      const binary = await downloadAttachmentBinary(bestMsg.accessToken, bestMsg.messageId, att.attachmentId);
      if (!binary) {
        stageResults.push({ filename: att.filename, file_id: fileId, status: "download_failed" });
        totalFailed++;
        continue;
      }

      const result = await stageAttachment(FIREBASE_API_KEY, fileId, att.filename, att.mimeType, binary, {
        messageId: bestMsg.messageId,
        subject,
        account: bestMsg.account.email,
        emailDate: new Date(bestMsg.internalDate).toISOString(),
      });
      stageResults.push({
        filename: att.filename,
        file_id: fileId,
        status: result.ok ? "staged" : "stage_failed",
        chunk_count: result.chunkCount,
        size_bytes: result.rawBytes,
        gz_bytes: result.gzBytes,
        error: result.error,
      });
      if (result.ok) totalStaged++; else totalFailed++;
    }

    // Mark message processed only if at least one attachment staged successfully
    // OR every attachment was already-staged/already-imported (idempotent).
    if (totalStaged > 0 || (totalFailed === 0 && totalSkipped > 0)) {
      await fsPatchDoc("uline_processed_emails", bestMsg.messageId, {
        message_id: bestMsg.messageId, subject,
        account_email: bestMsg.account.email,
        email_date: new Date(bestMsg.internalDate).toISOString(),
        processed_at: new Date().toISOString(),
        auto_ingest_run_id: runId,
        attachments_total: attachments.length,
        attachments_staged: totalStaged,
        attachments_skipped: totalSkipped,
        attachments_failed: totalFailed,
        attachment_names: attachments.map(a => a.filename),
      }, FIREBASE_API_KEY);
    }

    const finalState = totalFailed === 0 ? "complete" : (totalStaged > 0 ? "complete_with_errors" : "failed");
    await fsPatchDoc("uline_auto_ingest_logs", runId, {
      state: finalState,
      completed_at: new Date().toISOString(),
      progress: `${totalStaged} staged, ${totalSkipped} skipped, ${totalFailed} failed (out of ${attachments.length})`,
      stage_results: stageResults,
      attachments_staged: totalStaged,
      attachments_skipped: totalSkipped,
      attachments_failed: totalFailed,
    }, FIREBASE_API_KEY);

    return json({
      run_id: runId,
      state: finalState,
      message_id: bestMsg.messageId,
      account: bestMsg.account.email,
      subject,
      attachments_total: attachments.length,
      attachments_staged: totalStaged,
      attachments_skipped: totalSkipped,
      attachments_failed: totalFailed,
      stage_results: stageResults,
    }, 200);
  } catch (err: any) {
    await fsPatchDoc("uline_auto_ingest_logs", runId, {
      state: "failed",
      error: err?.message || String(err),
      completed_at: new Date().toISOString(),
    }, FIREBASE_API_KEY);
    return json({ run_id: runId, state: "failed", error: err?.message || String(err) }, 500);
  }
};

// Schedule: Monday 13:00 UTC + 14:00 UTC. DST-aware double-fire pattern.
// Sequenced to run AFTER DDIS (12,13 UTC) so payment data is in Firestore
// before any DAS file gets manually loaded — the audit math depends on it.
export const config: Config = {
  schedule: "0 13,14 * * 1",
};

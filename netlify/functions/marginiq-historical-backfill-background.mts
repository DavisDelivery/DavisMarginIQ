import type { Context } from "@netlify/functions";
import { gzipSync } from "node:zlib";

/**
 * Davis MarginIQ — Historical Gmail Backfill (v2.52.4)
 *
 * Pulls historical NuVizz / DDIS / Uline files from Gmail and stages
 * them through Layer 1 (source_files_raw). Once staged, marginiq-reparse
 * can re-run the current parsers (with v2.52.0+ raw-row preservation)
 * over them.
 *
 * The problem this solves: User has been ingesting NuVizz + DDIS files
 * for months/years, but every parser was trimming columns. v2.52.0 fixed
 * the parsers going forward, but historical data (200K+ stops) still has
 * the trimmed schema. v2.52.2 added Layer 1 staging for new files, but
 * old files were never staged. This backfill function bridges that gap.
 *
 * Endpoints:
 *   GET  /.netlify/functions/marginiq-historical-backfill?source=nuvizz&limit=10&dry_run=1
 *        Lists matching emails from Gmail, returns metadata only
 *   POST /.netlify/functions/marginiq-historical-backfill
 *        body: {
 *          source: 'nuvizz' | 'ddis',
 *          limit: 10,                 // max emails to process this run
 *          older_than: '2026-01-01',  // optional date filter (process only emails before)
 *          newer_than: '2024-01-01',  // optional date filter
 *          stage_only: true,          // if true, just stage Layer 1; don't trigger reparse
 *        }
 *
 * For each unstaged email found:
 *   1. Find the attachment
 *   2. Download bytes
 *   3. Stage to source_files_raw + source_file_chunks (Layer 1)
 *   4. Optionally trigger reparse via marginiq-reparse
 *
 * Idempotency: file_id format is `{source}__{messageId}__{filename}`,
 * matching the auto-ingest path. Re-running this function on the same
 * email is a no-op (overwrite of identical data).
 *
 * Env vars: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, FIREBASE_API_KEY
 */

const PROJECT_ID = "davismarginiq";
const RAW_CHUNK_BUDGET = 700_000;

const SEARCH_QUERIES: Record<string, string> = {
  nuvizz: "from:nuvizzapps@nuvizzapps.com has:attachment",
  ddis:   "from:APFreight@uline.com filename:csv",
  // Uline DAS is more complex (multiple senders + multiple attachments per email)
  // Excluded from this backfill — Uline already has its own staging at pending_uline_files.
};

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
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
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

async function fsGetDoc(coll: string, docId: string, apiKey: string): Promise<any | null> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${coll}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
}

async function fsPatchDoc(coll: string, docId: string, fields: Record<string, any>, apiKey: string): Promise<boolean> {
  // v2.53.1 — append updateMask.fieldPaths per key so PATCH is partial-merge,
  // not full-doc replace. Without this, every field outside `fields` is dropped.
  const params = new URLSearchParams();
  params.set("key", apiKey);
  for (const k of Object.keys(fields)) {
    params.append("updateMask.fieldPaths", k);
  }
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${coll}/${encodeURIComponent(docId)}?${params.toString()}`;
  const body: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) body[k] = toFsValue(v);
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: body }),
  });
  return r.ok;
}

// Reuse the same Gmail account discovery pattern as auto-ingest
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
  if (!r.ok) return null;
  const data: any = await r.json();
  return data.access_token || null;
}

async function gmailSearch(accessToken: string, q: string, maxResults = 100): Promise<Array<{ id: string; threadId: string }>> {
  const url = `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${encodeURIComponent(q)}&maxResults=${maxResults}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!r.ok) return [];
  const data: any = await r.json();
  return data.messages || [];
}

async function gmailGetMessage(accessToken: string, messageId: string): Promise<any | null> {
  const url = `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}?format=full`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!r.ok) return null;
  return await r.json();
}

function findCsvAttachment(payload: any): { filename: string; attachmentId: string; mimeType: string; size: number } | null {
  if (!payload) return null;
  const filename: string = payload.filename || "";
  if (filename && payload.body?.attachmentId) {
    const fnLower = filename.toLowerCase();
    if (fnLower.endsWith(".csv") || fnLower.endsWith(".xlsx") || fnLower.endsWith(".xls")) {
      return {
        filename,
        attachmentId: payload.body.attachmentId,
        mimeType: payload.mimeType || "",
        size: payload.body.size || 0,
      };
    }
  }
  for (const part of (payload.parts || [])) {
    const found = findCsvAttachment(part);
    if (found) return found;
  }
  return null;
}

async function downloadAttachment(accessToken: string, messageId: string, attachmentId: string): Promise<Buffer | null> {
  const url = `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}/attachments/${attachmentId}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!r.ok) return null;
  const data: any = await r.json();
  const b64 = (data.data || "").replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(b64, "base64");
}

function sanitizeFileId(filename: string): string {
  return filename.replace(/[^a-z0-9._-]/gi, "_").slice(0, 140);
}

async function stageRawFile(
  apiKey: string,
  source: string,
  fileId: string,
  filename: string,
  bytes: Buffer,
  emailMeta: { messageId: string; subject: string; account: string; emailDate: string },
): Promise<{ ok: boolean; chunkCount: number; rawBytes: number; gzBytes: number; error?: string }> {
  const gz = gzipSync(bytes);
  const b64 = gz.toString("base64");
  const chunks: string[] = [];
  for (let i = 0; i < b64.length; i += RAW_CHUNK_BUDGET) {
    chunks.push(b64.substring(i, i + RAW_CHUNK_BUDGET));
  }
  const headerOk = await fsPatchDoc("source_files_raw", fileId, {
    file_id: fileId,
    source,
    filename,
    size_bytes: bytes.length,
    gz_bytes: gz.length,
    chunk_count: chunks.length,
    state: "staged",
    staged_at: new Date().toISOString(),
    staged_by: "historical_backfill",
    email_message_id: emailMeta.messageId,
    email_subject: emailMeta.subject,
    email_account: emailMeta.account,
    email_date: emailMeta.emailDate,
  }, apiKey);
  if (!headerOk) return { ok: false, chunkCount: 0, rawBytes: bytes.length, gzBytes: gz.length, error: "Failed to write parent file doc" };
  let chunkErrors = 0;
  for (let i = 0; i < chunks.length; i++) {
    const chunkId = `${fileId}__${String(i).padStart(3, "0")}`;
    const ok = await fsPatchDoc("source_file_chunks", chunkId, {
      file_id: fileId, chunk_index: i, chunk_count: chunks.length,
      data_b64: chunks[i], created_at: new Date().toISOString(),
    }, apiKey);
    if (!ok) chunkErrors++;
  }
  if (chunkErrors > 0) return { ok: false, chunkCount: chunks.length, rawBytes: bytes.length, gzBytes: gz.length, error: `${chunkErrors}/${chunks.length} chunks failed` };
  return { ok: true, chunkCount: chunks.length, rawBytes: bytes.length, gzBytes: gz.length };
}

function getHeader(payload: any, name: string): string {
  const headers = payload?.headers || [];
  const h = headers.find((x: any) => (x.name || "").toLowerCase() === name.toLowerCase());
  return h?.value || "";
}

export default async (req: Request, _context: Context) => {
  const CLIENT_ID = process.env["GOOGLE_CLIENT_ID"];
  const CLIENT_SECRET = process.env["GOOGLE_CLIENT_SECRET"];
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  if (!CLIENT_ID || !CLIENT_SECRET || !FIREBASE_API_KEY) {
    return json({ error: "OAuth/Firebase not configured" }, 500);
  }

  const url = new URL(req.url);

  // Determine source + bounds
  let source: string, limit: number, olderThan: string | undefined, newerThan: string | undefined, stageOnly: boolean;
  let dryRun: boolean;
  if (req.method === "GET") {
    source = url.searchParams.get("source") || "";
    limit = Math.min(Number(url.searchParams.get("limit") || "10"), 50);
    olderThan = url.searchParams.get("older_than") || undefined;
    newerThan = url.searchParams.get("newer_than") || undefined;
    stageOnly = true; // GET is always preview
    dryRun = url.searchParams.get("dry_run") === "1";
  } else if (req.method === "POST") {
    const body = await req.json().catch(() => ({}));
    source = body.source || "";
    limit = Math.min(Number(body.limit || 10), 50);
    olderThan = body.older_than;
    newerThan = body.newer_than;
    stageOnly = body.stage_only !== false; // default true (only stage; don't reparse)
    dryRun = !!body.dry_run;
  } else {
    return new Response("method not allowed", { status: 405 });
  }

  if (!source || !SEARCH_QUERIES[source]) {
    return json({ ok: false, error: `source must be one of: ${Object.keys(SEARCH_QUERIES).join(", ")}` }, 400);
  }

  const runId = `historical_backfill_${source}_${new Date().toISOString().replace(/[:.]/g, "-")}`;
  const startedAt = new Date().toISOString();

  await fsPatchDoc("historical_backfill_logs", runId, {
    run_id: runId,
    source,
    state: "running",
    started_at: startedAt,
    limit,
    older_than: olderThan || null,
    newer_than: newerThan || null,
    stage_only: stageOnly,
    dry_run: dryRun,
  }, FIREBASE_API_KEY);

  try {
    // Build search query with optional date bounds
    let query = SEARCH_QUERIES[source];
    if (newerThan) query += ` after:${newerThan.replace(/-/g, "/")}`;
    if (olderThan) query += ` before:${olderThan.replace(/-/g, "/")}`;

    const accounts = await listConnectedAccounts(FIREBASE_API_KEY);
    if (accounts.length === 0) {
      await fsPatchDoc("historical_backfill_logs", runId, {
        state: "failed", error: "No connected Gmail accounts", completed_at: new Date().toISOString(),
      }, FIREBASE_API_KEY);
      return json({ run_id: runId, error: "No connected Gmail accounts" }, 200);
    }

    // Collect ALL matching messages across ALL accounts up to limit
    const candidates: Array<{
      messageId: string; account: TokenDoc; accessToken: string;
      details: any; subject: string; internalDate: number;
    }> = [];

    for (const acct of accounts) {
      if (candidates.length >= limit * 2) break; // collect 2x to filter unstaged
      const accessToken = await getFreshAccessToken(CLIENT_ID, CLIENT_SECRET, acct.refresh_token);
      if (!accessToken) continue;
      const msgs = await gmailSearch(accessToken, query, Math.min(limit * 2, 100));
      for (const m of msgs) {
        if (candidates.length >= limit * 2) break;
        const details = await gmailGetMessage(accessToken, m.id);
        if (!details) continue;
        const subject = getHeader(details.payload, "Subject");
        const internalDate = Number(details.internalDate || 0);
        candidates.push({ messageId: m.id, account: acct, accessToken, details, subject, internalDate });
      }
    }

    // Filter to unstaged (or already-staged from another path)
    const toProcess: typeof candidates = [];
    const alreadyStaged: any[] = [];
    for (const c of candidates) {
      const att = findCsvAttachment(c.details.payload);
      if (!att) continue;
      const fileId = `${source}__${c.messageId}__${sanitizeFileId(att.filename)}`;
      const existing = await fsGetDoc("source_files_raw", fileId, FIREBASE_API_KEY);
      if (existing?.fields?.state?.stringValue === "staged") {
        alreadyStaged.push({ message_id: c.messageId, file_id: fileId, filename: att.filename });
      } else {
        toProcess.push(c);
        if (toProcess.length >= limit) break;
      }
    }

    if (dryRun || (req.method === "GET" && !url.searchParams.get("execute"))) {
      const preview = toProcess.map(c => {
        const att = findCsvAttachment(c.details.payload);
        return {
          message_id: c.messageId,
          subject: c.subject,
          email_date: new Date(c.internalDate).toISOString(),
          attachment: att ? { filename: att.filename, size: att.size } : null,
          would_stage_as: att ? `${source}__${c.messageId}__${sanitizeFileId(att.filename)}` : null,
        };
      });
      await fsPatchDoc("historical_backfill_logs", runId, {
        state: "complete",
        completed_at: new Date().toISOString(),
        dry_run: true,
        messages_found: candidates.length,
        already_staged: alreadyStaged.length,
        would_process: toProcess.length,
      }, FIREBASE_API_KEY);
      return json({
        ok: true,
        run_id: runId,
        dry_run: true,
        source, query,
        messages_found: candidates.length,
        already_staged: alreadyStaged.length,
        would_process: toProcess.length,
        preview,
      });
    }

    // Actually stage each unstaged file
    const results: any[] = [];
    let stagedCount = 0;
    let failedCount = 0;
    for (let i = 0; i < toProcess.length; i++) {
      const c = toProcess[i];
      const att = findCsvAttachment(c.details.payload);
      if (!att) continue;
      const fileId = `${source}__${c.messageId}__${sanitizeFileId(att.filename)}`;

      await fsPatchDoc("historical_backfill_logs", runId, {
        progress: `Processing ${i + 1}/${toProcess.length}: ${att.filename}...`,
      }, FIREBASE_API_KEY);

      const bytes = await downloadAttachment(c.accessToken, c.messageId, att.attachmentId);
      if (!bytes) {
        results.push({ file_id: fileId, ok: false, error: "Download failed" });
        failedCount++;
        continue;
      }

      const stageRes = await stageRawFile(
        FIREBASE_API_KEY,
        source,
        fileId,
        att.filename,
        bytes,
        {
          messageId: c.messageId,
          subject: c.subject,
          account: c.account.email,
          emailDate: new Date(c.internalDate).toISOString(),
        },
      );
      results.push({
        file_id: fileId,
        filename: att.filename,
        ok: stageRes.ok,
        size_bytes: stageRes.rawBytes,
        gz_bytes: stageRes.gzBytes,
        chunks: stageRes.chunkCount,
        error: stageRes.error,
      });
      if (stageRes.ok) stagedCount++; else failedCount++;
    }

    // Optionally trigger reparse on the just-staged files
    if (!stageOnly && stagedCount > 0) {
      const baseUrl = `${url.protocol}//${url.host}`;
      for (const r of results) {
        if (!r.ok) continue;
        const reparseRes = await fetch(`${baseUrl}/.netlify/functions/marginiq-reparse`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ file_id: r.file_id, action: "reparse" }),
        }).catch(() => null);
        r.reparse_dispatched = !!reparseRes && reparseRes.ok;
      }
    }

    await fsPatchDoc("historical_backfill_logs", runId, {
      state: "complete",
      completed_at: new Date().toISOString(),
      messages_found: candidates.length,
      already_staged: alreadyStaged.length,
      processed: toProcess.length,
      staged_ok: stagedCount,
      staged_failed: failedCount,
      stage_only: stageOnly,
    }, FIREBASE_API_KEY);

    return json({
      ok: failedCount === 0,
      run_id: runId,
      source,
      messages_found: candidates.length,
      already_staged: alreadyStaged.length,
      processed: toProcess.length,
      staged_ok: stagedCount,
      staged_failed: failedCount,
      reparse_dispatched: !stageOnly,
      results,
    });
  } catch (e: any) {
    console.error("historical-backfill failed:", e);
    await fsPatchDoc("historical_backfill_logs", runId, {
      state: "failed", error: e?.message || String(e), completed_at: new Date().toISOString(),
    }, FIREBASE_API_KEY);
    return json({ run_id: runId, error: e?.message || String(e) }, 500);
  }
};

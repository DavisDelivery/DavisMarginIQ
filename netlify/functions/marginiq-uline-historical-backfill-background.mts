import type { Context } from "@netlify/functions";
import { gzipSync } from "node:zlib";
import * as XLSX from "xlsx";

/**
 * Davis MarginIQ — Uline DAS Historical Backfill Background Worker (v2.52.8)
 *
 * THE PROBLEM THIS SOLVES
 * -----------------------
 * For DDIS and NuVizz, the v2.52.0+ pipeline preserves every column from
 * every file forever (Layer 1 source_files_raw + Layer 2 *_rows_raw +
 * Layer 3 normalized collections).
 *
 * For Uline DAS, the pipeline was different and broken:
 *   - Auto-ingest staged files to pending_uline_files (a queue)
 *   - User had to click "Load" in the UI to invoke the client-side parser
 *   - Client parser computed every DAS line in browser memory
 *   - But it ONLY persisted the unpaid/variance subset to unpaid_stops +
 *     audit_items. Fully-paid DAS lines were COMPUTED AND DISCARDED.
 *
 * Result: there was no complete DAS ledger anywhere. Every analysis that
 * needed total billed-$, weight, or paid-line data hit a wall.
 *
 * v2.52.8 fixes this by adding a SERVER-SIDE DAS ingestor that:
 *   1. Pulls DAS xlsx files directly from Gmail (no UI click needed)
 *   2. Stages each file to source_files_raw (Layer 1, gzipped originals)
 *   3. Parses each row and persists EVERY parsed row to das_lines (Layer 3)
 *      — paid OR unpaid, delivery, accessorial, OR truckload — every line.
 *   4. Persists every column verbatim to das_rows_raw (Layer 2) so future
 *      analyses can use columns the current parser doesn't know about
 *      (notes, invoice numbers, customer codes, etc.)
 *
 * Going forward, both the historical backfill and any future weekly
 * file ingest write to the same das_lines collection. The DAS ledger
 * is finally complete.
 *
 * ENDPOINTS
 * ---------
 *   POST /.netlify/functions/marginiq-uline-historical-backfill
 *     body: {
 *       newer_than: '2025-01-01',  // ISO date
 *       older_than: '2026-04-01',  // ISO date
 *       limit: 50,                 // max emails to process
 *       dry_run: true,             // optional preview
 *     }
 *
 *   GET /.netlify/functions/marginiq-uline-historical-backfill?dry_run=1
 *     Lists matching emails (default last 90 days)
 *
 * STATUS DOC
 * ----------
 *   marginiq_config/uline_historical_backfill_status:
 *     state, started_at, completed_at, phase, progress_text,
 *     emails_found, emails_processed, files_staged, lines_written,
 *     raw_rows_written, errors, error
 *
 * SCHEMA: das_lines/{docId}
 *   docId = sanitize(`${pro}_${pu_date or 'nodate'}_${file_id}`)
 *   Fields:
 *     pro:           string (normalized, no leading zeros)
 *     order:         string | null
 *     customer:      string | null
 *     city:          string | null
 *     state:         string | null
 *     zip:           string | null
 *     pu:            integer | null  (raw pickup-date column from Uline)
 *     pu_date:       string YYYY-MM-DD | null
 *     month:         string YYYY-MM | null
 *     week_ending:   string YYYY-MM-DD | null  (Friday-of-week)
 *     cost:          number  (base rate from "cost" column)
 *     new_cost:      number  (final rate from "new cost" column; falls back to cost)
 *     extra_cost:    number  (accessorial $)
 *     warehouse:     string | null  (wh column)
 *     skid:          integer
 *     loose:         integer
 *     weight:        number  (lbs, from wgt column)
 *     via:           string | null
 *     code:          string | null  (accessorial code: "DET", "INS", "LIF", etc)
 *     is_accessorial: boolean
 *     service_type:  "delivery" | "truckload" | "accessorial"
 *     source_file_id: string  (file_id this row came from — points back to source_files_raw)
 *     ingested_at:   ISO timestamp
 *
 * SCHEMA: das_rows_raw/{docId}_{row_index}
 *   Fields:
 *     pro, source="uline", file_id, row_index, ingested_at
 *     raw: { ...every original column from the xlsx, lowercased keys }
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const GOOGLE_CLIENT_ID = process.env["GOOGLE_CLIENT_ID"];
const GOOGLE_CLIENT_SECRET = process.env["GOOGLE_CLIENT_SECRET"];

const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
const RAW_CHUNK_BUDGET = 700_000;

// Same query pattern as the auto-ingest, but with date filters injected.
const VENDOR_BASE_QUERY = '(from:@uline.com OR from:billing@davisdelivery.com) filename:das filename:xlsx -from:APFreight@uline.com';

// ─── Firestore helpers ────────────────────────────────────────────────────────

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

async function fsGetDoc(coll: string, docId: string, apiKey: string): Promise<any | null> {
  const url = `${FS_BASE}/${coll}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
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

async function batchWriteDocs(
  collection: string,
  docs: Array<{ docId: string; fields: any }>,
  apiKey: string,
): Promise<{ ok: number; failed: number }> {
  if (docs.length === 0) return { ok: 0, failed: 0 };
  if (docs.length > 500) {
    let ok = 0, failed = 0;
    for (let i = 0; i < docs.length; i += 500) {
      const r = await batchWriteDocs(collection, docs.slice(i, i + 500), apiKey);
      ok += r.ok; failed += r.failed;
    }
    return { ok, failed };
  }
  // :commit, not :batchWrite — see DDIS ingest for rationale.
  const url = `${FS_BASE}:commit?key=${apiKey}`;
  const writes = docs.map(d => ({
    update: {
      name: `projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${d.docId}`,
      fields: d.fields,
    },
  }));
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ writes }),
  });
  if (!resp.ok) {
    console.error(`commit ${collection} failed: ${resp.status} ${(await resp.text()).slice(0, 300)}`);
    return { ok: 0, failed: docs.length };
  }
  const data: any = await resp.json();
  const writeResults = data.writeResults || [];
  return { ok: writeResults.length, failed: docs.length - writeResults.length };
}

// ─── Status writer ────────────────────────────────────────────────────────────

const STATUS_DOC = "uline_historical_backfill_status";

async function writeStatus(patch: Record<string, any>): Promise<void> {
  if (!FIREBASE_API_KEY) return;
  await fsPatchDoc("marginiq_config", STATUS_DOC, patch, FIREBASE_API_KEY);
}

// ─── Gmail account discovery + token refresh ─────────────────────────────────

type TokenDoc = { docId: string; email: string; refresh_token: string };

async function listConnectedAccounts(apiKey: string): Promise<TokenDoc[]> {
  const accounts: Record<string, TokenDoc> = {};
  const url = `${FS_BASE}/marginiq_config?key=${apiKey}&pageSize=100`;
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

async function getFreshAccessToken(refreshToken: string): Promise<string | null> {
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) return null;
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: GOOGLE_CLIENT_ID,
      client_secret: GOOGLE_CLIENT_SECRET,
      refresh_token: refreshToken,
      grant_type: "refresh_token",
    }),
  });
  const data: any = await r.json();
  return r.ok ? data.access_token : null;
}

// ─── Gmail search + attachment download ──────────────────────────────────────

async function searchUlineMessages(
  accessToken: string,
  newerThan: string | null,
  olderThan: string | null,
  maxResults: number,
): Promise<{ id: string }[]> {
  let query = VENDOR_BASE_QUERY;
  // Gmail's date operators expect YYYY/MM/DD format
  if (newerThan) query += ` after:${newerThan.replace(/-/g, "/")}`;
  if (olderThan) query += ` before:${olderThan.replace(/-/g, "/")}`;

  const allMsgs: { id: string }[] = [];
  let pageToken: string | undefined;
  let safety = 0;
  do {
    const url = new URL("https://gmail.googleapis.com/gmail/v1/users/me/messages");
    url.searchParams.set("q", query);
    url.searchParams.set("maxResults", String(Math.min(100, maxResults - allMsgs.length)));
    if (pageToken) url.searchParams.set("pageToken", pageToken);
    const r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
    if (!r.ok) break;
    const data: any = await r.json();
    for (const m of (data.messages || [])) allMsgs.push(m);
    if (allMsgs.length >= maxResults) break;
    pageToken = data.nextPageToken;
    safety++;
  } while (pageToken && safety < 10);
  return allMsgs.slice(0, maxResults);
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

function findAllDasAttachments(payload: any): DasAttachment[] {
  const out: DasAttachment[] = [];
  const dasPattern = /^das[\s\d_\-]/i;
  function walk(node: any) {
    if (!node) return;
    const filename: string = node.filename || "";
    const fnLower = filename.toLowerCase();
    if (filename && node.body?.attachmentId) {
      if (dasPattern.test(fnLower)) {
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
  const b64 = (data.data || "").replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(b64, "base64");
}

// ─── DAS parser (mirrors public/MarginIQ.jsx parseOriginalOrAccessorial) ─────

function normalizePro(raw: any): string | null {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  // Strip leading zeros — Uline pads PROs to 7 digits with leading zero
  const digits = s.replace(/^0+/, "");
  return digits || s;
}

function puToDate(pu: number | null): string | null {
  // Uline pu column is YYYYMMDD as integer
  if (!pu) return null;
  const s = String(pu);
  if (s.length !== 8) return null;
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
}

function puToMonth(pu: number | null): string | null {
  if (!pu) return null;
  const s = String(pu);
  if (s.length !== 8) return null;
  return `${s.slice(0, 4)}-${s.slice(4, 6)}`;
}

function weekEndingFriday(dateStr: string | null): string | null {
  // Returns the Friday-of-week (week-ending) for a YYYY-MM-DD date.
  // Mirrors the client's weekEndingFriday helper.
  if (!dateStr) return null;
  const [y, m, d] = dateStr.split("-").map(Number);
  if (!y || !m || !d) return null;
  const dt = new Date(Date.UTC(y, m - 1, d));
  // dow: 0=Sun, 1=Mon, ..., 5=Fri, 6=Sat
  const dow = dt.getUTCDay();
  // Days to add to reach Friday: (5 - dow + 7) % 7
  const daysToFri = (5 - dow + 7) % 7;
  dt.setUTCDate(dt.getUTCDate() + daysToFri);
  const yy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

function detectServiceType(filename: string): "delivery" | "truckload" | "accessorial" {
  const fn = filename.toLowerCase();
  // " TK" or "-TK" suffix in filename = truckload file
  if (/(?:^|[\s\-_])tk(?=\.[a-z]+$|[\s\-_])/i.test(fn)) return "truckload";
  // "accessorials" in filename = accessorial file
  if (/accessorial/i.test(fn)) return "accessorial";
  return "delivery";
}

interface ParsedDasRow {
  pro: string;
  order: string | null;
  customer: string | null;
  city: string | null;
  state: string | null;
  zip: string | null;
  pu: number | null;
  pu_date: string | null;
  month: string | null;
  week_ending: string | null;
  cost: number;
  new_cost: number;
  extra_cost: number;
  warehouse: string | null;
  skid: number;
  loose: number;
  weight: number;
  via: string | null;
  code: string | null;
  is_accessorial: boolean;
  service_type: "delivery" | "truckload" | "accessorial";
  raw: Record<string, string>;
}

function parseDasWorkbook(buffer: Buffer, filename: string): ParsedDasRow[] {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const ws = wb.Sheets[wb.SheetNames[0]];
  if (!ws) return [];

  // Check for Uline-style meta row on row 0 (cells contain patterns like
  // "Num,0 (No Blanks)", "CAPS (COD,$$$)"). If detected, skip one row.
  const raw0 = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null }) as any[][];
  let skipRows = 0;
  if (raw0.length >= 2) {
    const r0 = (raw0[0] || []).filter(v => v != null).map(v => String(v).toLowerCase());
    const metaHints = r0.filter(v =>
      /^num\s*,/.test(v) || /no blanks/.test(v) || /^caps\s/.test(v) || v.startsWith("not blank")
    ).length;
    if (metaHints >= 3 && r0.length >= 5) skipRows = 1;
  }

  const rawRows = XLSX.utils.sheet_to_json(ws, { defval: null, range: skipRows }) as any[];
  // Lowercase + trim all keys to match client parser
  const rows = rawRows.map(r => {
    const o: Record<string, any> = {};
    Object.keys(r).forEach(k => { o[String(k).toLowerCase().trim()] = r[k]; });
    return o;
  });

  const fileST = detectServiceType(filename);
  const out: ParsedDasRow[] = [];

  for (const r of rows) {
    const proRaw = r.pro ?? r["pro#"];
    if (!proRaw) continue;
    const proStr = String(proRaw).toLowerCase();
    if (proStr === "pro" || proStr === "pro#") continue;
    const pro = normalizePro(proRaw);
    if (!pro) continue;

    const cost = parseFloat(r.cost) || 0;
    const newCost = parseFloat(r["new cost"]) || 0;
    const extraCost = parseFloat(r["extra cost"]) || 0;
    const wgt = parseFloat(r.wgt) || 0;
    const skid = parseInt(r.skid) || 0;
    const loose = parseInt(r.loose) || 0;
    const pu = r.pu ? parseInt(r.pu) : null;
    const codeStr = r.code ? String(r.code).trim() : null;
    const hasCode = !!(codeStr && codeStr.length > 0);

    let rowST: "delivery" | "truckload" | "accessorial";
    if (fileST === "truckload") rowST = "truckload";
    else if (hasCode) rowST = "accessorial";
    else rowST = "delivery";

    // Sanitize raw row keys for Firestore (no . [ ] / leading __)
    const safeRaw: Record<string, string> = {};
    for (const [k, v] of Object.entries(r || {})) {
      if (!k) continue;
      const sk = String(k).replace(/[.\[\]\/]/g, "_").replace(/^__/, "_");
      safeRaw[sk] = v == null ? "" : (typeof v === "string" ? v : String(v));
    }

    const puDate = pu ? puToDate(pu) : null;
    out.push({
      pro,
      order: r.order ? String(r.order) : null,
      customer: r.customer ? String(r.customer).trim() : null,
      city: r.city ? String(r.city).trim() : null,
      state: r.st ? String(r.st).trim() : null,
      zip: r.zip ? String(r.zip).trim() : null,
      pu,
      pu_date: puDate,
      month: pu ? puToMonth(pu) : null,
      week_ending: puDate ? weekEndingFriday(puDate) : null,
      cost,
      new_cost: newCost || cost,
      extra_cost: extraCost,
      warehouse: r.wh ? String(r.wh).trim() : null,
      skid,
      loose,
      weight: wgt,
      via: r.via ? String(r.via).trim() : null,
      code: codeStr,
      is_accessorial: hasCode,
      service_type: rowST,
      raw: safeRaw,
    });
  }
  return out;
}

// ─── Layer 1 staging — gzipped original bytes ────────────────────────────────

function sanitizeFileId(filename: string): string {
  return filename.replace(/[^a-z0-9._-]/gi, "_").slice(0, 140);
}

function sanitizeDocId(id: string): string {
  return String(id).replace(/[/]/g, "_").replace(/^\.+$/, "_").slice(0, 1400);
}

async function stageFileLayer1(
  fileId: string,
  filename: string,
  mimeType: string,
  binary: Buffer,
  emailMeta: { messageId: string; subject: string; account: string; emailDate: string },
  apiKey: string,
): Promise<{ ok: boolean; chunkCount: number; rawBytes: number; gzBytes: number; error?: string }> {
  const gz = gzipSync(binary);
  const b64 = gz.toString("base64");

  const chunks: string[] = [];
  for (let i = 0; i < b64.length; i += RAW_CHUNK_BUDGET) {
    chunks.push(b64.substring(i, i + RAW_CHUNK_BUDGET));
  }

  const headerOk = await fsPatchDoc("source_files_raw", fileId, {
    file_id: fileId,
    filename,
    source: "uline",
    mime_type: mimeType,
    raw_bytes: binary.length,
    gz_bytes: gz.length,
    chunk_count: chunks.length,
    email_message_id: emailMeta.messageId,
    email_subject: emailMeta.subject,
    email_account: emailMeta.account,
    email_date: emailMeta.emailDate,
    state: "staged",
    staged_at: new Date().toISOString(),
    staged_by: "uline-historical-backfill",
  }, apiKey);
  if (!headerOk) return { ok: false, chunkCount: 0, rawBytes: binary.length, gzBytes: gz.length, error: "Header doc write failed" };

  // Write chunks
  const chunkDocs = chunks.map((data, i) => ({
    docId: `${fileId}__${String(i).padStart(3, "0")}`,
    fields: toFsValue({
      file_id: fileId,
      chunk_index: i,
      chunk_count: chunks.length,
      data_b64: data,
      created_at: new Date().toISOString(),
    }).mapValue.fields,
  }));
  const chunkResult = await batchWriteDocs("source_file_chunks", chunkDocs, apiKey);
  if (chunkResult.failed > 0) {
    return { ok: false, chunkCount: chunks.length, rawBytes: binary.length, gzBytes: gz.length, error: `${chunkResult.failed} of ${chunks.length} chunks failed` };
  }
  return { ok: true, chunkCount: chunks.length, rawBytes: binary.length, gzBytes: gz.length };
}

// ─── Layer 2 + Layer 3 writes ────────────────────────────────────────────────

async function persistParsedRows(
  fileId: string,
  filename: string,
  rows: ParsedDasRow[],
  apiKey: string,
): Promise<{ linesOk: number; rawOk: number; failed: number }> {
  const ingestedAt = new Date().toISOString();

  // Layer 3: das_lines — every parsed row, paid or unpaid
  const lineDocs = rows.map((r, idx) => {
    const docId = sanitizeDocId(`${r.pro}_${r.pu_date || "nodate"}_${fileId}_${idx}`);
    return {
      docId,
      fields: toFsValue({
        ...r,
        // raw is already present; keep it but don't double-encode
        source_file_id: fileId,
        source_filename: filename,
        ingested_at: ingestedAt,
      }).mapValue.fields,
    };
  });

  // Layer 2: das_rows_raw — every column verbatim, indexed for future queries
  const rawDocs = rows.map((r, idx) => {
    const docId = sanitizeDocId(`${r.pro}_${r.pu_date || "nodate"}_${fileId}_${idx}`);
    return {
      docId,
      fields: toFsValue({
        pro: r.pro,
        source: "uline",
        file_id: fileId,
        row_index: idx,
        pu_date: r.pu_date,
        month: r.month,
        ingested_at: ingestedAt,
        raw: r.raw,
      }).mapValue.fields,
    };
  });

  let linesOk = 0, rawOk = 0, failed = 0;
  if (lineDocs.length > 0) {
    const lr = await batchWriteDocs("das_lines", lineDocs, apiKey);
    linesOk = lr.ok; failed += lr.failed;
  }
  if (rawDocs.length > 0) {
    const rr = await batchWriteDocs("das_rows_raw", rawDocs, apiKey);
    rawOk = rr.ok; failed += rr.failed;
  }
  return { linesOk, rawOk, failed };
}

async function markFileProcessed(fileId: string, summary: any, apiKey: string): Promise<void> {
  await fsPatchDoc("source_files_raw", fileId, {
    state: "processed",
    processed_at: new Date().toISOString(),
    parse_summary: summary,
  }, apiKey);
}

// ─── Idempotency ─────────────────────────────────────────────────────────────

async function isMessageProcessed(messageId: string, apiKey: string): Promise<boolean> {
  const doc = await fsGetDoc("uline_processed_emails", messageId, apiKey);
  return doc !== null && (doc as any).fields !== undefined;
}

async function markMessageProcessed(messageId: string, summary: any, apiKey: string): Promise<void> {
  await fsPatchDoc("uline_processed_emails", messageId, {
    processed_at: new Date().toISOString(),
    processed_by: "uline-historical-backfill",
    summary,
  }, apiKey);
}

// ─── Main backfill loop ──────────────────────────────────────────────────────

interface BackfillOptions {
  newerThan: string | null;
  olderThan: string | null;
  limit: number;
  dryRun: boolean;
  reprocess: boolean;
}

async function runBackfill(opts: BackfillOptions): Promise<any> {
  if (!FIREBASE_API_KEY) throw new Error("FIREBASE_API_KEY not configured");
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) throw new Error("GOOGLE_CLIENT_ID/SECRET not configured");

  const startedAt = new Date().toISOString();
  const accounts = await listConnectedAccounts(FIREBASE_API_KEY);
  if (accounts.length === 0) {
    await writeStatus({
      state: "failed",
      started_at: startedAt,
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: "✗ No Gmail accounts connected",
      error: "No connected Gmail accounts",
    });
    return { ok: false, error: "No connected Gmail accounts" };
  }

  await writeStatus({
    state: "running",
    started_at: startedAt,
    completed_at: null,
    phase: "searching_gmail",
    progress_text: `Searching Gmail (${accounts.length} accounts) for DAS files between ${opts.newerThan || "any"} and ${opts.olderThan || "now"}…`,
    accounts: accounts.map(a => a.email),
    newer_than: opts.newerThan,
    older_than: opts.olderThan,
    limit: opts.limit,
    dry_run: opts.dryRun,
    emails_found: 0,
    emails_processed: 0,
    emails_skipped: 0,
    files_staged: 0,
    files_processed: 0,
    lines_written: 0,
    raw_rows_written: 0,
    errors: 0,
    error: null,
  });

  // Search across all connected accounts
  let allMessages: Array<{ id: string; account: string; accessToken: string }> = [];
  for (const acct of accounts) {
    const accessToken = await getFreshAccessToken(acct.refresh_token);
    if (!accessToken) {
      console.warn(`uline-backfill: failed to refresh token for ${acct.email}`);
      continue;
    }
    const msgs = await searchUlineMessages(accessToken, opts.newerThan, opts.olderThan, opts.limit);
    for (const m of msgs) allMessages.push({ id: m.id, account: acct.email, accessToken });
  }

  // De-dupe by message id (same email might appear in multiple accounts)
  const seen = new Set<string>();
  allMessages = allMessages.filter(m => {
    if (seen.has(m.id)) return false;
    seen.add(m.id);
    return true;
  });

  await writeStatus({
    phase: "searching_gmail",
    progress_text: `Found ${allMessages.length} matching emails. ${opts.dryRun ? "(dry run — not processing)" : "Processing…"}`,
    emails_found: allMessages.length,
  });

  if (opts.dryRun) {
    // Return preview only
    const previews: any[] = [];
    for (const m of allMessages.slice(0, 50)) {
      const details = await getMessageDetails(m.accessToken, m.id);
      if (!details) continue;
      const headers: any[] = details.payload?.headers || [];
      const subject = headers.find(h => h.name === "Subject")?.value || "";
      const from = headers.find(h => h.name === "From")?.value || "";
      const date = headers.find(h => h.name === "Date")?.value || "";
      const internalDate = details.internalDate ? new Date(parseInt(details.internalDate)).toISOString() : "";
      const attachments = findAllDasAttachments(details.payload);
      previews.push({
        message_id: m.id,
        account: m.account,
        from,
        subject,
        date,
        internal_date: internalDate,
        attachment_count: attachments.length,
        attachments: attachments.map(a => ({ filename: a.filename, size: a.size })),
      });
    }
    await writeStatus({
      state: "complete",
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: `✓ Dry run complete: ${allMessages.length} emails, ${previews.reduce((s, p) => s + p.attachment_count, 0)} attachments`,
    });
    return {
      ok: true,
      dry_run: true,
      emails_found: allMessages.length,
      previews,
    };
  }

  // Actually process
  let emailsProcessed = 0;
  let emailsSkipped = 0;
  let filesStaged = 0;
  let filesProcessed = 0;
  let linesWritten = 0;
  let rawRowsWritten = 0;
  let errors = 0;

  for (const m of allMessages) {
    try {
      // Idempotency
      if (!opts.reprocess && await isMessageProcessed(m.id, FIREBASE_API_KEY)) {
        emailsSkipped++;
        continue;
      }

      const details = await getMessageDetails(m.accessToken, m.id);
      if (!details) {
        errors++;
        continue;
      }
      const headers: any[] = details.payload?.headers || [];
      const subject = headers.find(h => h.name === "Subject")?.value || "";
      const internalDate = details.internalDate ? new Date(parseInt(details.internalDate)).toISOString() : "";
      const attachments = findAllDasAttachments(details.payload);
      if (attachments.length === 0) {
        await markMessageProcessed(m.id, { reason: "no_das_attachments" }, FIREBASE_API_KEY);
        emailsSkipped++;
        continue;
      }

      const fileSummaries: any[] = [];
      for (const att of attachments) {
        const binary = await downloadAttachmentBinary(m.accessToken, m.id, att.attachmentId);
        if (!binary) {
          errors++;
          fileSummaries.push({ filename: att.filename, error: "download_failed" });
          continue;
        }

        const fileId = `uline__${m.id}__${sanitizeFileId(att.filename)}`;

        // Layer 1: stage gzipped original bytes
        const stageResult = await stageFileLayer1(
          fileId,
          att.filename,
          att.mimeType,
          binary,
          { messageId: m.id, subject, account: m.account, emailDate: internalDate },
          FIREBASE_API_KEY,
        );
        if (!stageResult.ok) {
          errors++;
          fileSummaries.push({ filename: att.filename, error: `stage_failed: ${stageResult.error}` });
          continue;
        }
        filesStaged++;

        // Parse + Layer 2 + Layer 3
        let rows: ParsedDasRow[];
        try {
          rows = parseDasWorkbook(binary, att.filename);
        } catch (e: any) {
          errors++;
          fileSummaries.push({ filename: att.filename, error: `parse_failed: ${e?.message || e}` });
          continue;
        }

        const persistResult = await persistParsedRows(fileId, att.filename, rows, FIREBASE_API_KEY);
        linesWritten += persistResult.linesOk;
        rawRowsWritten += persistResult.rawOk;
        if (persistResult.failed > 0) errors += persistResult.failed;

        await markFileProcessed(fileId, {
          rows_parsed: rows.length,
          lines_written: persistResult.linesOk,
          raw_rows_written: persistResult.rawOk,
          failed_writes: persistResult.failed,
        }, FIREBASE_API_KEY);

        filesProcessed++;
        fileSummaries.push({
          filename: att.filename,
          rows: rows.length,
          lines_written: persistResult.linesOk,
        });
      }

      await markMessageProcessed(m.id, { files: fileSummaries, subject, processed_at: new Date().toISOString() }, FIREBASE_API_KEY);
      emailsProcessed++;

      // Progress update every 5 emails
      if (emailsProcessed % 5 === 0) {
        await writeStatus({
          phase: "ingesting",
          progress_text: `Processed ${emailsProcessed}/${allMessages.length} emails · ${filesProcessed} files · ${linesWritten.toLocaleString()} DAS lines persisted`,
          emails_processed: emailsProcessed,
          emails_skipped: emailsSkipped,
          files_staged: filesStaged,
          files_processed: filesProcessed,
          lines_written: linesWritten,
          raw_rows_written: rawRowsWritten,
          errors,
        });
      }
    } catch (e: any) {
      console.error(`uline-backfill: error processing ${m.id}: ${e?.message || e}`);
      errors++;
    }
  }

  const finalStatus = errors === 0 ? "complete" : (filesProcessed > 0 ? "complete_with_errors" : "failed");
  const completedAt = new Date().toISOString();
  await writeStatus({
    state: finalStatus,
    completed_at: completedAt,
    phase: "done",
    progress_text: `✓ Backfill done · ${emailsProcessed}/${allMessages.length} emails · ${filesProcessed} files · ${linesWritten.toLocaleString()} DAS lines (${rawRowsWritten.toLocaleString()} raw rows) · ${errors} errors`,
    emails_processed: emailsProcessed,
    emails_skipped: emailsSkipped,
    files_staged: filesStaged,
    files_processed: filesProcessed,
    lines_written: linesWritten,
    raw_rows_written: rawRowsWritten,
    errors,
  });

  return {
    ok: errors === 0 || filesProcessed > 0,
    emails_found: allMessages.length,
    emails_processed: emailsProcessed,
    emails_skipped: emailsSkipped,
    files_staged: filesStaged,
    files_processed: filesProcessed,
    lines_written: linesWritten,
    raw_rows_written: rawRowsWritten,
    errors,
    started_at: startedAt,
    completed_at: completedAt,
  };
}

// ─── Entry point ─────────────────────────────────────────────────────────────

export default async (req: Request, _context: Context) => {
  const url = new URL(req.url);

  let opts: BackfillOptions = {
    newerThan: null,
    olderThan: null,
    limit: 50,
    dryRun: false,
    reprocess: false,
  };

  if (req.method === "GET") {
    opts.dryRun = url.searchParams.get("dry_run") === "1" || url.searchParams.get("dry_run") === "true";
    opts.newerThan = url.searchParams.get("newer_than");
    opts.olderThan = url.searchParams.get("older_than");
    const limitStr = url.searchParams.get("limit");
    if (limitStr) opts.limit = Math.max(1, Math.min(500, parseInt(limitStr) || 50));
  } else if (req.method === "POST") {
    try {
      const body: any = await req.json();
      opts.newerThan = body.newer_than || null;
      opts.olderThan = body.older_than || null;
      opts.limit = Math.max(1, Math.min(500, parseInt(body.limit) || 50));
      opts.dryRun = !!body.dry_run;
      opts.reprocess = !!body.reprocess;
    } catch (e: any) {
      console.error("uline-historical-backfill: bad JSON body", e?.message || e);
    }
  }

  console.log(`uline-historical-backfill-background: start opts=${JSON.stringify(opts)}`);
  try {
    const result = await runBackfill(opts);
    console.log(`uline-historical-backfill-background: done ${JSON.stringify(result).slice(0, 300)}`);
  } catch (e: any) {
    console.error(`uline-historical-backfill-background: FAILED ${e?.message || e}`);
    await writeStatus({
      state: "failed",
      completed_at: new Date().toISOString(),
      phase: "done",
      progress_text: `✗ ${e?.message || String(e)}`,
      error: String(e?.message || e),
    }).catch(() => {});
  }
};

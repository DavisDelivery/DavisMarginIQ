import type { Context } from "@netlify/functions";
import * as XLSX from "xlsx";
import { runCheckpointed, type CheckpointState } from "./lib/checkpointed-runner.js";
import { ingestFile, deriveFileId, type ParsedRow } from "./lib/four-layer-ingest.js";

/**
 * Davis MarginIQ — Uline DAS Historical Backfill BACKGROUND WORKER (v2.53.3, Phase 2 task 2)
 *
 * Walks Gmail for Uline DAS attachments and ingests each one through
 * lib/four-layer-ingest.ts. Wrapped in lib/checkpointed-runner.ts so the
 * walk can span many invocations without losing state.
 *
 * v2.53.3 — REBUILT FOR MODEL B
 * =============================
 *   - das_lines L3 docId is now `{pro}_{pu_date||"nodate"}_{service_type}`
 *     (collision-safe canonical merge key, replacing v2.53.1's
 *     `{pro}_{pu_date}_{idx}` which silently overwrote ~50% of rows).
 *   - das_rows_raw L2 docId is now `{file_id}__{row_index}` (lossless;
 *     same delivery in N files = N L2 rows, one per source row).
 *   - Five file kinds classified: delivery, truckload, accessorial,
 *     correction, remittance. REMITTANCE files write L2-only (no L3).
 *     CORRECTION files use a different parser branch.
 *   - L2 raw_cells captured as Array<{header,header_index,value}> —
 *     truly lossless (no key sanitization, preserves duplicate headers).
 *   - source_files_raw gains column_headers, sheet_names, file_kind for
 *     schema-drift detection.
 *   - L3 writes go through l3WriteMode='merge' which calls mergeDasLineRow
 *     with stagedAt=L1 staged_at. Latest-wins per (pro, pu_date, service_type).
 *
 * STATE LIFECYCLE
 * ===============
 *   1. First invocation: receives options in body. Loads Gmail accounts,
 *      runs the search across all of them, dedupes by message id, stores
 *      the full message list in payload.message_queue. Sets cursor=0.
 *   2. Each invocation: pops messages off the queue starting at `cursor`,
 *      ingests them one at a time, increments cursor.
 *   3. When cursor === message_queue.length: marks complete.
 *
 * IDEMPOTENCY
 * ===========
 *   - file_id is deterministic (sha256 of source|filename|messageId).
 *     Re-ingesting the same file overwrites the same docs at all layers.
 *   - L3 merge is deterministic when stagedAt is the L1 doc's staged_at
 *     (preserved across reparse runs). See DESIGN.md §4 idempotency proof.
 *   - uline_processed_emails/{messageId} is an idempotency record. If
 *     present, the message is skipped unless reprocess=true.
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const GOOGLE_CLIENT_ID = process.env["GOOGLE_CLIENT_ID"];
const GOOGLE_CLIENT_SECRET = process.env["GOOGLE_CLIENT_SECRET"];

const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
const STATUS_DOC = "uline_historical_backfill_status";

// Gmail search query — same as the auto-ingest, with date filters injected.
const VENDOR_BASE_QUERY = '(from:@uline.com OR from:billing@davisdelivery.com) filename:das filename:xlsx -from:APFreight@uline.com';

// Schema version for parsed DAS rows. Bump on parser changes.
// v2.53.3 — bumped from 1.0.0 because the L3 docId scheme and field
// shape both changed under Model B.
const DAS_SCHEMA_VERSION = "2.0.0";
const INGESTED_BY = "marginiq-uline-historical-backfill-background@2.53.3";

// Per-invocation work budget. 10 minutes leaves headroom for the final
// status write and the self-reinvoke fetch handoff.
const WALL_BUDGET_MS = 10 * 60 * 1000;
// Hard cap on chain length. 60 invocations × 10 min = 10hrs of effective work.
// Plenty for ~500 emails (each takes maybe 5–15s including Gmail download
// + parse + Firestore writes), with a wide safety margin.
const MAX_CHAIN = 60;

// ─── Firestore primitives ────────────────────────────────────────────────────

function fsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") {
    if (!isFinite(v)) return { nullValue: null };
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (typeof v === "string") return { stringValue: v };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(fsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = fsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

async function fsPatch(coll: string, docId: string, fields: Record<string, any>): Promise<boolean> {
  // v2.53.1 — append updateMask.fieldPaths per key so PATCH is partial-merge,
  // not full-doc replace. Without this, every field outside `fields` is dropped.
  const params = new URLSearchParams();
  params.set("key", FIREBASE_API_KEY || "");
  for (const k of Object.keys(fields)) {
    params.append("updateMask.fieldPaths", k);
  }
  const url = `${FS_BASE}/${coll}/${encodeURIComponent(docId)}?${params.toString()}`;
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(fields)) out[k] = fsValue(v);
  const r = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: out }),
  });
  return r.ok;
}

async function fsGet(coll: string, docId: string): Promise<any | null> {
  const url = `${FS_BASE}/${coll}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
}

// ─── Gmail helpers ───────────────────────────────────────────────────────────

interface TokenDoc { docId: string; email: string; refresh_token: string }

async function listConnectedAccounts(): Promise<TokenDoc[]> {
  const accounts: Record<string, TokenDoc> = {};
  const url = `${FS_BASE}/marginiq_config?key=${FIREBASE_API_KEY}&pageSize=100`;
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

async function getAccessToken(refreshToken: string): Promise<string | null> {
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
  if (!r.ok) return null;
  const data: any = await r.json();
  return data.access_token || null;
}

async function searchUlineMessages(
  accessToken: string,
  newerThan: string | null,
  olderThan: string | null,
  maxResults: number,
): Promise<{ id: string }[]> {
  let query = VENDOR_BASE_QUERY;
  if (newerThan) query += ` after:${newerThan.replace(/-/g, "/")}`;
  if (olderThan) query += ` before:${olderThan.replace(/-/g, "/")}`;

  const all: { id: string }[] = [];
  let pageToken: string | undefined;
  let safety = 0;
  do {
    const u = new URL("https://gmail.googleapis.com/gmail/v1/users/me/messages");
    u.searchParams.set("q", query);
    u.searchParams.set("maxResults", String(Math.min(100, maxResults - all.length)));
    if (pageToken) u.searchParams.set("pageToken", pageToken);
    const r = await fetch(u.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
    if (!r.ok) break;
    const data: any = await r.json();
    for (const m of (data.messages || [])) all.push({ id: m.id });
    if (all.length >= maxResults) break;
    pageToken = data.nextPageToken;
    safety++;
  } while (pageToken && safety < 30);
  return all.slice(0, maxResults);
}

async function getMessageDetails(accessToken: string, messageId: string): Promise<any | null> {
  const r = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}`,
    { headers: { Authorization: `Bearer ${accessToken}` } },
  );
  return r.ok ? await r.json() : null;
}

interface DasAttachment { filename: string; attachmentId: string; mimeType: string; size: number }

function findDasAttachments(payload: any): DasAttachment[] {
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

async function downloadAttachment(accessToken: string, messageId: string, attachmentId: string): Promise<Buffer | null> {
  const r = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}/attachments/${attachmentId}`,
    { headers: { Authorization: `Bearer ${accessToken}` } },
  );
  if (!r.ok) return null;
  const data: any = await r.json();
  const b64 = (data.data || "").replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(b64, "base64");
}

// ─── DAS parser ──────────────────────────────────────────────────────────────
// v2.53.3 — five file kinds (delivery, truckload, accessorial, correction,
// remittance) with filename-first classification. Helper functions
// (normalizePro, puToDate, puToMonth, weekEndingFriday) are defined below
// alongside the parser.

// ─── File classification (v2.53.3) ───────────────────────────────────────────
// Five distinct uline file kinds discovered in the May 5 design audit
// (see DESIGN.md §3). Filename-first; column-headers fallback for
// ambiguous cases; subject as tiebreaker for unknowns.
//
// REMITTANCE files write L2 only (no das_lines rows). They are paid-status
// confirmations, not new billing data.
//
// CORRECTION files have a different schema entirely
// (Pro#, SCAC, Invoice Date, Transmit Date, Charge) and are parsed by a
// separate branch in the main parser.

export type FileKind = "delivery" | "truckload" | "accessorial" | "correction" | "remittance" | "unknown";

function classifyUlineFile(filename: string, headers: string[], subject?: string): FileKind {
  const fn = (filename || "").toLowerCase();

  // Strongest filename signals first.
  // Truckload: "TK" or "TL" suffix surrounded by separators or at end.
  if (/(?:^|[\s\-_])t[kl](?=\.[a-z]+$|[\s\-_])/i.test(fn)) return "truckload";
  // Accessorial — includes the typo "acceessorials" (3 e's) seen in the wild.
  if (/acc[e]?essorial/i.test(fn)) return "accessorial";
  if (/remit/i.test(fn)) return "remittance";
  if (/process|correct|late|dispute|update/i.test(fn)) return "correction";

  // Filename ambiguous — fall back to header set.
  const hdrs = (headers || []).map(h => (h || "").toString().toLowerCase().trim());
  if (hdrs.includes("scac") && hdrs.includes("transmit date")) return "correction";
  if (hdrs.includes("status") && hdrs.length <= 6 && hdrs.includes("pro")) return "remittance";

  // Standard delivery file: starts with "das" and has the canonical
  // pro/cost/new cost trio.
  if (fn.startsWith("das") && hdrs.includes("pro") && hdrs.includes("cost") && hdrs.includes("new cost")) {
    return "delivery";
  }

  // Subject tiebreaker (last resort).
  const subj = (subject || "").toLowerCase();
  if (subj.includes("aging") || subj.includes("remittance")) return "remittance";
  if (subj.includes("dispute") || subj.includes("update")) return "correction";

  return "unknown";
}

// ─── Parser output bundle (v2.53.3) ──────────────────────────────────────────
// The parser now returns the parsed rows AND per-file metadata (column_headers,
// sheet_names, file_kind) so the worker can pass them into ingestFile via
// metadata.l1Extras for source_files_raw.

interface ParseBundle {
  rows: ParsedRow[];
  l1Extras: {
    column_headers: string[];
    sheet_names: string[];
    file_kind: FileKind;
  };
}

// ─── Raw cell capture ────────────────────────────────────────────────────────
// L2 raw_cells is an Array<{header, header_index, value}>. Using an array
// (not a sanitized-key map) preserves duplicate / unsanitizable headers,
// preserves column order, and makes schema-drift detection trivial.
//
// SheetJS returns rows as objects keyed by header strings. We rebuild the
// header-indexed array by aligning to a canonical header order captured
// once per sheet.

interface RawCell {
  header: string;
  header_index: number;
  value: string;
}

function captureRawCells(row: any, headers: string[]): RawCell[] {
  const out: RawCell[] = [];
  for (let i = 0; i < headers.length; i++) {
    const h = headers[i];
    const v = row[h];
    out.push({
      header: h,
      header_index: i,
      value: v == null ? "" : (typeof v === "string" ? v : String(v)),
    });
  }
  return out;
}

// ─── DAS parser ──────────────────────────────────────────────────────────────
// v2.53.3 rebuild for Model B. Same field-extraction logic as v2.52.8 baseline
// for delivery/truckload/accessorial; new branch for correction files;
// new L2-only path for remittance files.

function normalizePro(raw: any): string | null {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  // Preserve ULI-NNNNNNN truckload PROs as-is (not all digits).
  // For numeric PROs, strip leading zeros for join consistency.
  if (/^\d+$/.test(s)) {
    const digits = s.replace(/^0+/, "");
    return digits || s;
  }
  return s;
}

function puToDate(pu: number | null): string | null {
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
  if (!dateStr) return null;
  const [y, m, d] = dateStr.split("-").map(Number);
  if (!y || !m || !d) return null;
  const dt = new Date(Date.UTC(y, m - 1, d));
  const dow = dt.getUTCDay();
  const daysToFri = (5 - dow + 7) % 7;
  dt.setUTCDate(dt.getUTCDate() + daysToFri);
  const yy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

/**
 * Parse a DAS workbook into a ParseBundle.
 *
 * fileId is required because L2 docIds embed it ({file_id}__{row_index})
 * and L3 docIds need it for provenance threading. Pre-computed by the
 * caller via deriveFileId(); this parser must use the SAME file_id that
 * ingestFile() will use internally so L1 + L2 + L3 references stay aligned.
 */
function parseDasWorkbookToRows(buffer: Buffer, filename: string, fileId: string, subject?: string): ParseBundle {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheetNames = wb.SheetNames.slice();

  // Branch by file kind. We need at least one sheet's headers to classify
  // ambiguous filenames, so peek at the first sheet's row 0 first.
  const firstSheet = wb.Sheets[sheetNames[0]];
  if (!firstSheet) {
    return {
      rows: [],
      l1Extras: { column_headers: [], sheet_names: sheetNames, file_kind: "unknown" },
    };
  }

  // Capture row 0 for header inspection. Some DAS files have a Uline-style
  // metadata row 0 (e.g. "Num,0 (No Blanks)") and the real headers are row 1.
  const raw0 = XLSX.utils.sheet_to_json(firstSheet, { header: 1, defval: null }) as any[][];
  let skipRows = 0;
  if (raw0.length >= 2) {
    const r0 = (raw0[0] || []).filter(v => v != null).map(v => String(v).toLowerCase());
    const metaHints = r0.filter(v =>
      /^num\s*,/.test(v) || /no blanks/.test(v) || /^caps\s/.test(v) || v.startsWith("not blank")
    ).length;
    if (metaHints >= 3 && r0.length >= 5) skipRows = 1;
  }

  // Headers row (after skipping meta if present).
  const headerRow: any[] = (raw0[skipRows] || []) as any[];
  const headers: string[] = headerRow.map(h => (h == null ? "" : String(h)).trim());

  const fileKind = classifyUlineFile(filename, headers, subject);

  // Dispatch
  if (fileKind === "remittance") {
    return parseRemittance(wb, sheetNames, fileId, headers, fileKind);
  }
  if (fileKind === "correction") {
    return parseCorrection(wb, sheetNames, fileId, headers, fileKind);
  }
  // delivery, truckload, accessorial, unknown — all parse with the
  // standard DAS branch. Unknown files we attempt-parse and the resulting
  // rows will land in das_rows_raw + das_lines if they contain a valid PRO.
  return parseDeliveryStyleWorkbook(wb, sheetNames, fileId, filename, headers, skipRows, fileKind);
}

/**
 * The dominant case: delivery, truckload, accessorial files share the
 * canonical DAS schema [pro, order, customer, city, st, zip, pu, cost,
 * wh, skid, loose, wgt, via, extra cost, code, new cost, notes].
 */
function parseDeliveryStyleWorkbook(
  wb: any,
  sheetNames: string[],
  fileId: string,
  filename: string,
  headers: string[],
  skipRows: number,
  fileKind: FileKind,
): ParseBundle {
  const ws = wb.Sheets[sheetNames[0]];
  // Re-read with normalized object keys (lowercase). We retain the
  // ORIGINAL headers in `headers` for L2 raw_cells; the lowercase keys
  // are only used for L3 normalized field extraction.
  const rawObjs = XLSX.utils.sheet_to_json(ws, { defval: null, range: skipRows }) as any[];
  const lowerKeyRows: any[] = rawObjs.map(r => {
    const o: Record<string, any> = {};
    Object.keys(r).forEach(k => { o[String(k).toLowerCase().trim()] = r[k]; });
    return o;
  });

  const out: ParsedRow[] = [];

  for (let idx = 0; idx < rawObjs.length; idx++) {
    const rOrig = rawObjs[idx];
    const r = lowerKeyRows[idx];
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

    // Service-type per row, with row-level override.
    let rowST: "delivery" | "truckload" | "accessorial";
    if (fileKind === "truckload") rowST = "truckload";
    else if (fileKind === "accessorial") rowST = "accessorial";
    else if (fileKind === "delivery" || fileKind === "unknown") {
      // Within a delivery file (or unknown), a populated `code` cell flips
      // that row to accessorial. Matches v2.52.8 line 430 semantics.
      rowST = hasCode ? "accessorial" : "delivery";
    } else {
      rowST = "delivery";
    }

    const puDate = pu ? puToDate(pu) : null;

    // Lossless raw_cells capture (Array<{header, header_index, value}>).
    const rawCells = captureRawCells(rOrig, headers);

    const normalized: Record<string, any> = {
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
    };

    // L3 docId: canonical merge key (pro, pu_date, service_type).
    const l3DocId = `${pro}_${puDate || "nodate"}_${rowST}`;
    // L2 docId: lossless ({file_id}__{row_index}). Includes the original
    // 0-based row index (not normalized index) so reparse is deterministic.
    const l2DocId = `${fileId}__${String(idx).padStart(6, "0")}`;

    out.push({
      docId: l3DocId,
      l2DocId,
      rawFields: {
        // Lossless: every column from the source row, byte-for-byte, plus
        // a few denormalized fields for query convenience.
        raw_cells: rawCells,
        sheet_name: sheetNames[0],
        file_kind: fileKind,
        row_index: idx,
        // Denormalized PRO + pu_date for L2 indexed queries.
        parsed_pro: pro,
        parsed_pu_date: puDate,
      },
      normalizedFields: normalized,
    });
  }

  return {
    rows: out,
    l1Extras: { column_headers: headers, sheet_names: sheetNames, file_kind: fileKind },
  };
}

/**
 * Correction / processed_late files. Schema:
 *   [Pro#, SCAC, Invoice Date, Transmit Date, Charge]
 * These are late-billed delivery charges. Write to das_lines as
 * service_type='delivery'.
 */
function parseCorrection(
  wb: any,
  sheetNames: string[],
  fileId: string,
  headers: string[],
  fileKind: FileKind,
): ParseBundle {
  const ws = wb.Sheets[sheetNames[0]];
  const rawObjs = XLSX.utils.sheet_to_json(ws, { defval: null }) as any[];

  const out: ParsedRow[] = [];
  for (let idx = 0; idx < rawObjs.length; idx++) {
    const rOrig = rawObjs[idx];
    const r: Record<string, any> = {};
    Object.keys(rOrig).forEach(k => { r[String(k).toLowerCase().trim()] = rOrig[k]; });

    const proRaw = r["pro#"] ?? r.pro;
    if (!proRaw) continue;
    const pro = normalizePro(proRaw);
    if (!pro) continue;

    const charge = parseFloat(r.charge) || 0;
    // Invoice Date is the closest analog to pu_date for these files.
    const invoiceDate = r["invoice date"];
    let puDate: string | null = null;
    if (invoiceDate) {
      // SheetJS returns Excel date cells as Date objects when cellDates:true
      // is set; otherwise as numeric serials. The defaults give us strings
      // like "2025-02-03 00:00:00" — extract the date portion.
      const s = String(invoiceDate).trim();
      const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
      if (m) puDate = m[1];
    }

    const rawCells = captureRawCells(rOrig, headers);

    const normalized: Record<string, any> = {
      pro,
      scac: r.scac ? String(r.scac).trim() : null,
      invoice_date: puDate,
      transmit_date: r["transmit date"] ? String(r["transmit date"]).trim() : null,
      charge,
      // Map onto the canonical das_lines fields so the read path doesn't
      // need to special-case correction rows.
      pu_date: puDate,
      month: puDate ? puDate.slice(0, 7) : null,
      week_ending: weekEndingFriday(puDate),
      cost: charge,
      new_cost: charge,
      extra_cost: 0,
      code: null,
      is_accessorial: false,
      service_type: "delivery",
    };

    const l3DocId = `${pro}_${puDate || "nodate"}_delivery`;
    const l2DocId = `${fileId}__${String(idx).padStart(6, "0")}`;

    out.push({
      docId: l3DocId,
      l2DocId,
      rawFields: {
        raw_cells: rawCells,
        sheet_name: sheetNames[0],
        file_kind: fileKind,
        row_index: idx,
        parsed_pro: pro,
        parsed_pu_date: puDate,
      },
      normalizedFields: normalized,
    });
  }

  return {
    rows: out,
    l1Extras: { column_headers: headers, sheet_names: sheetNames, file_kind: fileKind },
  };
}

/**
 * Remittance files. L2 ONLY — these are paid-status confirmations and
 * don't add das_lines billing rows. Each row gets `skipL3: true` so
 * ingestFile writes only L2.
 *
 * Remittance files often have multiple sheets (one per "paid on" date);
 * we walk every sheet so the L2 capture is complete.
 */
function parseRemittance(
  wb: any,
  sheetNames: string[],
  fileId: string,
  _firstSheetHeaders: string[],
  fileKind: FileKind,
): ParseBundle {
  const out: ParsedRow[] = [];
  // Capture per-sheet headers; the cross-sheet superset goes into l1Extras.
  const allHeaders = new Set<string>();
  let globalIdx = 0;

  for (const sn of sheetNames) {
    const ws = wb.Sheets[sn];
    if (!ws) continue;
    const sheetRaw = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null }) as any[][];
    if (sheetRaw.length < 2) continue;
    const sheetHeaders: string[] = (sheetRaw[0] || []).map(h => (h == null ? "" : String(h)).trim());
    sheetHeaders.forEach(h => allHeaders.add(h));
    const sheetObjs = XLSX.utils.sheet_to_json(ws, { defval: null }) as any[];

    for (let idx = 0; idx < sheetObjs.length; idx++) {
      const rOrig = sheetObjs[idx];
      const rawCells = captureRawCells(rOrig, sheetHeaders);
      // Pull pro for indexing, but DON'T require it — we want every row
      // captured even if PRO is null/non-numeric (lossless guarantee).
      const lower: Record<string, any> = {};
      Object.keys(rOrig).forEach(k => { lower[String(k).toLowerCase().trim()] = rOrig[k]; });
      const proRaw = lower.pro ?? lower["pro#"];
      const pro = proRaw ? normalizePro(proRaw) : null;

      // Some sheets have a "Status" cell carrying free text like
      // "Paid 63.2 for pro 6914473 on check 1346766" — preserve verbatim
      // in raw_cells; downstream tooling can parse it.

      const l2DocId = `${fileId}__${String(globalIdx).padStart(6, "0")}`;
      out.push({
        // No real L3; provide a stable docId so type contract is satisfied.
        docId: `${fileId}__remit__${globalIdx}`,
        l2DocId,
        skipL3: true,
        rawFields: {
          raw_cells: rawCells,
          sheet_name: sn,
          file_kind: fileKind,
          row_index: idx,
          global_row_index: globalIdx,
          parsed_pro: pro,
        },
        // Sentinel — never written.
        normalizedFields: {},
      });
      globalIdx++;
    }
  }

  return {
    rows: out,
    l1Extras: {
      column_headers: Array.from(allHeaders),
      sheet_names: sheetNames,
      file_kind: fileKind,
    },
  };
}

// ─── Idempotency on processed emails ─────────────────────────────────────────

async function isMessageProcessed(messageId: string): Promise<boolean> {
  const doc = await fsGet("uline_processed_emails", messageId);
  if (!doc) return false;
  return Object.keys(doc.fields || {}).length > 0;
}

async function markMessageProcessed(messageId: string, summary: any): Promise<void> {
  await fsPatch("uline_processed_emails", messageId, {
    processed_at: new Date().toISOString(),
    processed_by: INGESTED_BY,
    summary,
  });
}

// ─── Worker payload type ─────────────────────────────────────────────────────

interface QueueItem {
  message_id: string;
  account: string;       // email address
  // NOTE: refresh_token intentionally NOT stored here. Each invocation
  // re-lists connected accounts and re-derives access tokens. This keeps
  // the payload doc small (~50 bytes/item × 500 items = ~25KB) instead
  // of bloating it with full refresh tokens (~170 bytes/item × 500 =
  // ~85KB written 100 times during the chain = 8.5MB of waste).
}

interface BackfillPayload {
  newer_than: string;
  older_than: string | null;
  limit: number;
  dry_run: boolean;
  reprocess: boolean;
  queue?: QueueItem[];     // populated on first invocation
  emails_found: number;
  emails_processed: number;
  emails_skipped: number;
  files_staged: number;
  files_processed: number;
  lines_written: number;
  raw_rows_written: number;
  errors: number;
  accounts: string[];
}

// ─── Per-message processing ──────────────────────────────────────────────────

async function processOneMessage(
  item: QueueItem,
  accessTokenByAccount: Map<string, string>,
  reprocess: boolean,
  payload: BackfillPayload,
): Promise<{ ok: boolean; skipped?: string; files?: number; rows?: number; error?: string }> {
  // Idempotency
  if (!reprocess && await isMessageProcessed(item.message_id)) {
    return { ok: true, skipped: "already_processed" };
  }

  const accessToken = accessTokenByAccount.get(item.account);
  if (!accessToken) {
    return { ok: false, error: `no_access_token_for_account: ${item.account}` };
  }

  const details = await getMessageDetails(accessToken, item.message_id);
  if (!details) {
    return { ok: false, error: "message_details_fetch_failed" };
  }

  const headers: any[] = details.payload?.headers || [];
  const subject = headers.find(h => h.name === "Subject")?.value || "";
  const internalDate = details.internalDate
    ? new Date(parseInt(details.internalDate)).toISOString()
    : "";

  const attachments = findDasAttachments(details.payload);
  if (attachments.length === 0) {
    await markMessageProcessed(item.message_id, { reason: "no_das_attachments", subject });
    return { ok: true, skipped: "no_attachments" };
  }

  const fileSummaries: any[] = [];
  let totalLines = 0;
  let filesOk = 0;

  for (const att of attachments) {
    const binary = await downloadAttachment(accessToken, item.message_id, att.attachmentId);
    if (!binary) {
      payload.errors++;
      fileSummaries.push({ filename: att.filename, error: "download_failed" });
      continue;
    }

    // v2.53.3 — Pre-parse to capture l1Extras (column_headers, sheet_names,
    // file_kind) BEFORE calling ingestFile, which needs them for the
    // source_files_raw header doc. Pre-compute fileId via the same helper
    // ingestFile uses internally so L1, L2, and L3 references stay aligned.
    //
    // Live-ingest path uses stagedAt = NOW (the L1 doc is being created
    // right now). The reparse path will pass the L1 doc's existing
    // staged_at instead — see marginiq-reparse for that branch.
    const fileIdForRows = deriveFileId("uline", att.filename, item.message_id);
    let bundle;
    try {
      bundle = parseDasWorkbookToRows(binary, att.filename, fileIdForRows, subject);
    } catch (e: any) {
      payload.errors++;
      fileSummaries.push({ filename: att.filename, error: `parse_threw: ${e?.message || e}` });
      continue;
    }
    const liveStagedAt = new Date().toISOString();

    // ingestFile() does L1 + L2 + L3 atomically. Parser closure returns the
    // pre-parsed rows; ingestFile won't re-parse.
    const result = await ingestFile({
      source: "uline",
      filename: att.filename,
      binary,
      parser: () => bundle.rows,
      metadata: {
        messageId: item.message_id,
        emailDate: internalDate,
        account: item.account,
        subject,
        schemaVersion: DAS_SCHEMA_VERSION,
        ingestedBy: INGESTED_BY,
        stagedAt: liveStagedAt,
        l1Extras: bundle.l1Extras,
      },
      apiKey: FIREBASE_API_KEY!,
      l3WriteMode: "merge",
    });

    if (!result.ok) {
      payload.errors++;
      fileSummaries.push({
        filename: att.filename,
        error: result.error,
        l1: result.layer1.ok,
        l2_failed: result.layer2.failed,
        l3_failed: result.layer3.failed,
      });
      continue;
    }

    payload.files_staged++;
    payload.files_processed++;
    payload.lines_written += result.layer3.written;
    payload.raw_rows_written += result.layer2.written;
    totalLines += result.layer3.written;
    filesOk++;
    fileSummaries.push({
      filename: att.filename,
      file_id: result.fileId,
      file_kind: bundle.l1Extras.file_kind,
      l3_written: result.layer3.written,
      l2_written: result.layer2.written,
    });
  }

  await markMessageProcessed(item.message_id, {
    subject,
    files: fileSummaries,
    processed_at: new Date().toISOString(),
  });

  return { ok: true, files: filesOk, rows: totalLines };
}

// ─── Dry-run preview (single-shot, doesn't use checkpointed-runner) ──────────

async function dryRunPreview(opts: { newerThan: string; olderThan: string | null; limit: number }): Promise<any> {
  const accounts = await listConnectedAccounts();
  if (accounts.length === 0) {
    return { ok: false, error: "no Gmail accounts connected" };
  }

  const previews: any[] = [];
  const seen = new Set<string>();
  for (const acct of accounts) {
    const tok = await getAccessToken(acct.refresh_token);
    if (!tok) continue;
    const msgs = await searchUlineMessages(tok, opts.newerThan, opts.olderThan, opts.limit);
    for (const m of msgs) {
      if (seen.has(m.id)) continue;
      seen.add(m.id);
      const details = await getMessageDetails(tok, m.id);
      if (!details) continue;
      const headers: any[] = details.payload?.headers || [];
      const subject = headers.find(h => h.name === "Subject")?.value || "";
      const from = headers.find(h => h.name === "From")?.value || "";
      const internalDate = details.internalDate
        ? new Date(parseInt(details.internalDate)).toISOString() : "";
      const attachments = findDasAttachments(details.payload);
      previews.push({
        message_id: m.id,
        account: acct.email,
        from,
        subject,
        date: internalDate,
        attachment_count: attachments.length,
        attachments: attachments.map(a => ({ filename: a.filename, size: a.size })),
      });
      if (previews.length >= 100) break; // cap preview size
    }
    if (previews.length >= 100) break;
  }

  await fsPatch("marginiq_config", STATUS_DOC, {
    state: "complete",
    completed_at: new Date().toISOString(),
    progress_text: `✓ Dry run: ${previews.length} matching emails (showing first ${Math.min(previews.length, 100)})`,
    dry_run: true,
    emails_found: previews.length,
    accounts: accounts.map(a => a.email),
  });

  return {
    ok: true,
    dry_run: true,
    emails_found: previews.length,
    accounts: accounts.map(a => a.email),
    previews,
  };
}

// ─── Main entry ──────────────────────────────────────────────────────────────

export default async (req: Request, ctx: Context) => {
  if (!FIREBASE_API_KEY || !GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) {
    console.error("uline-historical-backfill-bg: missing env vars");
    await fsPatch("marginiq_config", STATUS_DOC, {
      state: "failed",
      completed_at: new Date().toISOString(),
      progress_text: "✗ Missing FIREBASE_API_KEY / GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET",
      error: "missing_env",
    });
    return;
  }

  // Parse body for first-invocation options. Self-reinvocations have empty body.
  let body: any = {};
  try { body = await req.json(); } catch {}

  const newerThan: string = body.newer_than || "2024-01-01";
  const olderThan: string | null = body.older_than || null;
  const limit: number = parseInt(body.limit) || 1000;
  const dryRun: boolean = !!body.dry_run;
  const reprocess: boolean = !!body.reprocess;

  // Dry runs are single-shot; they don't need the checkpointed runner.
  // Self-reinvocations always have empty body, so dryRun will be false
  // for them and they fall through to the runCheckpointed path.
  if (dryRun) {
    try {
      const result = await dryRunPreview({ newerThan, olderThan, limit });
      console.log(`uline-historical-bg: dry run done — ${JSON.stringify(result).slice(0, 300)}`);
    } catch (e: any) {
      console.error(`uline-historical-bg: dry run threw: ${e?.message || e}`);
      await fsPatch("marginiq_config", STATUS_DOC, {
        state: "failed",
        progress_text: `✗ Dry run failed: ${e?.message || e}`,
        completed_at: new Date().toISOString(),
        error: String(e?.message || e),
      });
    }
    return;
  }

  const cleanUrl = `${new URL(req.url).origin}${new URL(req.url).pathname}`;
  const runId = `uline_historical_${newerThan}_to_${olderThan || "now"}_l${limit}`;

  await runCheckpointed({
    runId,
    runnerUrl: cleanUrl,
    statusCollection: "marginiq_config",
    statusDocId: STATUS_DOC,
    apiKey: FIREBASE_API_KEY,
    wallBudgetMs: WALL_BUDGET_MS,
    maxChainLength: MAX_CHAIN,
    context: ctx,
    initialCursor: 0,
    initialPayload: {
      newer_than: newerThan,
      older_than: olderThan,
      limit,
      dry_run: false,
      reprocess,
      queue: undefined,
      emails_found: 0,
      emails_processed: 0,
      emails_skipped: 0,
      files_staged: 0,
      files_processed: 0,
      lines_written: 0,
      raw_rows_written: 0,
      errors: 0,
      accounts: [],
    } as BackfillPayload,
    work: async (state: CheckpointState, ctxw) => {
      const payload = state.payload as BackfillPayload;
      let cursor: number = (typeof state.cursor === "number") ? state.cursor : 0;

      // Per-invocation: refresh access tokens for all connected Gmail
      // accounts. Access tokens last ~1hr; the chain may run hours, so
      // we re-mint at every invocation rather than store them.
      const accounts = await listConnectedAccounts();
      const accessTokenByAccount = new Map<string, string>();
      for (const acct of accounts) {
        const tok = await getAccessToken(acct.refresh_token);
        if (tok) accessTokenByAccount.set(acct.email, tok);
        else console.warn(`uline-historical-bg: token refresh failed for ${acct.email}`);
      }
      if (accessTokenByAccount.size === 0) {
        throw new Error("No working Gmail tokens — all accounts failed refresh");
      }

      // Step 1 (only on first invocation): build the message queue
      if (!payload.queue || payload.queue.length === 0) {
        const allItems: QueueItem[] = [];
        const seen = new Set<string>();
        for (const acct of accounts) {
          const tok = accessTokenByAccount.get(acct.email);
          if (!tok) continue;
          const msgs = await searchUlineMessages(tok, payload.newer_than, payload.older_than, payload.limit);
          for (const m of msgs) {
            if (seen.has(m.id)) continue;
            seen.add(m.id);
            allItems.push({
              message_id: m.id,
              account: acct.email,
            });
            if (allItems.length >= payload.limit) break;
          }
          if (allItems.length >= payload.limit) break;
        }

        payload.queue = allItems;
        payload.emails_found = allItems.length;
        payload.accounts = accounts.map(a => a.email);
        state.payload = payload;

        await ctxw.checkpoint(state, `Discovered ${allItems.length} Uline emails across ${accounts.length} accounts. Beginning ingest…`);

        if (allItems.length === 0) {
          // Nothing to do — complete immediately
          return { state, complete: true };
        }
      }

      const queue = payload.queue!;

      // Step 2: process messages in cursor order until budget exhausted
      while (cursor < queue.length && !ctxw.shouldYield()) {
        const item = queue[cursor];
        try {
          const result = await processOneMessage(item, accessTokenByAccount, payload.reprocess, payload);
          if (result.skipped) {
            payload.emails_skipped++;
          } else if (result.ok) {
            payload.emails_processed++;
          } else {
            payload.errors++;
            console.warn(`uline-historical-bg: ${item.message_id} error: ${result.error}`);
          }
        } catch (e: any) {
          payload.errors++;
          console.error(`uline-historical-bg: ${item.message_id} threw: ${e?.message || e}`);
        }

        cursor++;
        state.processed = cursor;

        // Mid-run checkpoint every 5 messages so the user can watch progress.
        if (cursor % 5 === 0) {
          state.cursor = cursor;
          state.payload = payload;
          await ctxw.checkpoint(
            state,
            `Processed ${cursor}/${queue.length} emails · ${payload.files_processed} files · ${payload.lines_written.toLocaleString()} lines · ${payload.errors} errors`,
          );
        }
      }

      state.cursor = cursor;
      state.payload = payload;

      const complete = cursor >= queue.length;
      return { state, complete };
    },
  });
};

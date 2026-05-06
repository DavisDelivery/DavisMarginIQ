import * as XLSX from "xlsx";
import type { ParsedRow } from "./four-layer-ingest.js";

/**
 * Davis MarginIQ — Uline DAS workbook parser (v2.53.3, lib extraction)
 *
 * Extracted from marginiq-uline-historical-backfill-background.mts in
 * Commit 2 so that:
 *   - The Gmail-walking worker uses it (live ingest path)
 *   - The marginiq-reparse endpoint uses it (reparse-from-L1 path)
 *   - The migration runner (Commit 4) uses it
 *
 * One canonical parser — no drift risk between the live and reparse
 * paths.
 *
 * Five file kinds (delivery, truckload, accessorial, correction,
 * remittance) with filename-first classification. Helper functions
 * (normalizePro, puToDate, puToMonth, weekEndingFriday) live alongside
 * the parser.
 *
 * IDEMPOTENCY CONTRACT
 * ====================
 * The parser is pure: same (buffer, filename, fileId, subject) inputs
 * produce identical ParseBundle output. NO calls to Date.now() or any
 * other source of non-determinism. Reparse safety depends on this.
 *
 * The caller controls the merge semantics (live vs reparse) via the
 * stagedAt value passed to ingestFile() — not via this parser.
 */

// ─── File classification ─────────────────────────────────────────────────────
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

export function classifyUlineFile(filename: string, headers: string[], subject?: string): FileKind {
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

// ─── Parser output bundle ────────────────────────────────────────────────────
// The parser returns the parsed rows AND per-file metadata (column_headers,
// sheet_names, file_kind) so the caller can pass them into ingestFile via
// metadata.l1Extras for source_files_raw.

export interface ParseBundle {
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

// ─── DAS parser helpers ──────────────────────────────────────────────────────

export function normalizePro(raw: any): string | null {
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

// ─── Main parser entrypoint ──────────────────────────────────────────────────

/**
 * Parse a DAS workbook into a ParseBundle.
 *
 * fileId is required because L2 docIds embed it ({file_id}__{row_index})
 * and L3 docIds need it for provenance threading. Pre-computed by the
 * caller via deriveFileId(); this parser must use the SAME file_id that
 * ingestFile() will use internally so L1 + L2 + L3 references stay aligned.
 *
 * Pure function: same inputs always produce identical output. No
 * Date.now() or other non-determinism.
 */
export function parseDasWorkbookToRows(buffer: Buffer, filename: string, fileId: string, subject?: string): ParseBundle {
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
  _filename: string,
  headers: string[],
  skipRows: number,
  fileKind: FileKind,
): ParseBundle {
  const ws = wb.Sheets[sheetNames[0]];
  // Re-read with normalized object keys (lowercase). We retain the
  // ORIGINAL headers in `headers` for L2 raw_cells; the lowercase keys
  // are only used for L3 normalized field extraction.
  const rawObjs = XLSX.utils.sheet_to_json(ws, { defval: null, range: skipRows }) as any[];
  const lowerKeyRows: any[] = rawObjs.map((r: any) => {
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
    const sheetHeaders: string[] = (sheetRaw[0] || []).map((h: any) => (h == null ? "" : String(h)).trim());
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

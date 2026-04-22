import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — DDIS Bill-Week Backfill (v2.40.9)
 *
 * For every document in `ddis_files`, compute a canonical `bill_week_ending`
 * (YYYY-MM-DD Friday) by looking at which bill_dates appear most often in
 * the file's payments:
 *
 *   1. Query ddis_payments where source_file == {filename}.
 *   2. Count rows per bill_date.
 *   3. Take the top 5 most-common dates (Chad's rule — these represent the
 *      week the file is primarily settling; the rest are straggler PROs from
 *      older invoices being paid in the same settlement).
 *   4. Bucket each of the top 5 dates to its Sat→Fri envelope (Friday-ending).
 *   5. The envelope holding the most top-5 rows wins → bill_week_ending.
 *   6. If the top envelope TIES with another envelope on row count, we set
 *      week_ambiguous = true and surface candidates for manual resolution
 *      in Settings rather than guess. Expected to be rare.
 *
 * Background function because for the full 42-file set this is 42 × up to
 * ~5000 rows of Firestore reads = potentially 200K+ reads, which would
 * easily blow past the 10s sync-function budget and return 502 empty-body.
 *
 * Invoke:
 *   POST /.netlify/functions/marginiq-ddis-week-backfill-background
 *   (Netlify routes `-background` suffixed functions to the 15-min budget
 *    tier and returns 202 immediately to the caller.)
 *
 * Env vars required:
 *   FIREBASE_API_KEY   — for Firestore REST reads + writes
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

// ─── Firestore REST helpers ─────────────────────────────────────────

type FsDoc = { name: string; fields?: Record<string, any> };

/** List all docs in a collection (paged). */
async function listAllDocs(collection: string): Promise<FsDoc[]> {
  const out: FsDoc[] = [];
  let pageToken: string | undefined = undefined;
  for (let i = 0; i < 200; i++) {
    // Safety ceiling: 200 pages × 300 = 60K docs. ddis_files has ~42.
    const params = new URLSearchParams({ key: FIREBASE_API_KEY!, pageSize: "300" });
    if (pageToken) params.set("pageToken", pageToken);
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}?${params.toString()}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`listAllDocs ${collection} HTTP ${resp.status}: ${await resp.text()}`);
    const data: any = await resp.json();
    if (Array.isArray(data.documents)) out.push(...data.documents);
    if (!data.nextPageToken) break;
    pageToken = data.nextPageToken;
  }
  return out;
}

/**
 * Run a structured query to pull bill_date rows for one source_file.
 * Returns only the bill_date strings, not the full payment docs, to save
 * memory. Firestore's :runQuery endpoint pages differently from document
 * listing — we loop until a page returns fewer than pageSize rows.
 */
async function queryBillDatesForFile(sourceFile: string): Promise<string[]> {
  // Firestore REST :runQuery doesn't support nextPageToken — pagination is
  // cursor-based via startAfter. But ddis_payments per file is at most a
  // few thousand rows, well under the 5000-doc single-query cap, so a
  // single call with a high limit is fine in practice.
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:runQuery?key=${FIREBASE_API_KEY}`;
  const body = {
    structuredQuery: {
      from: [{ collectionId: "ddis_payments" }],
      where: {
        fieldFilter: {
          field: { fieldPath: "source_file" },
          op: "EQUAL",
          value: { stringValue: sourceFile },
        },
      },
      // Only pull the bill_date field — smaller response.
      select: { fields: [{ fieldPath: "bill_date" }] },
      limit: 10000,
    },
  };
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`queryBillDates ${sourceFile} HTTP ${resp.status}: ${await resp.text()}`);
  const rows: any[] = await resp.json();
  const dates: string[] = [];
  for (const r of rows) {
    const doc = r?.document;
    if (!doc) continue;
    const bd = doc.fields?.bill_date?.stringValue;
    if (bd) dates.push(bd);
  }
  return dates;
}

/** PATCH (merge) a ddis_files doc with computed fields. */
async function patchDdisFile(fileId: string, patch: Record<string, any>): Promise<void> {
  // Build fieldPaths update mask so we only touch the fields we're setting.
  const fieldPaths = Object.keys(patch);
  const params = new URLSearchParams({ key: FIREBASE_API_KEY! });
  for (const fp of fieldPaths) params.append("updateMask.fieldPaths", fp);
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/ddis_files/${encodeURIComponent(fileId)}?${params.toString()}`;
  // Encode JS values to Firestore REST Value shape.
  const fields: Record<string, any> = {};
  for (const [k, v] of Object.entries(patch)) fields[k] = toFsValue(v);
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  if (!resp.ok) throw new Error(`patchDdisFile ${fileId} HTTP ${resp.status}: ${await resp.text()}`);
}

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "string") return { stringValue: v };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") {
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (Array.isArray(v)) {
    return { arrayValue: { values: v.map(toFsValue) } };
  }
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

/** Decode a Firestore REST Value back to a JS value (shallow). */
function fromFsValue(v: any): any {
  if (!v) return null;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return v.doubleValue;
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("arrayValue" in v) return (v.arrayValue?.values || []).map(fromFsValue);
  if ("mapValue" in v) {
    const out: Record<string, any> = {};
    for (const [k, val] of Object.entries(v.mapValue?.fields || {})) out[k] = fromFsValue(val);
    return out;
  }
  return null;
}

// ─── Bill-week algorithm (mirrors MarginIQ.jsx computeBillWeekEnding) ───

function fridayEndOf(iso: string): string | null {
  const d = new Date(iso + "T00:00:00");
  if (isNaN(d.getTime())) return null;
  const day = d.getDay(); // 0=Sun..6=Sat
  const daysTo = (5 - day + 7) % 7;
  d.setDate(d.getDate() + daysTo);
  return d.toISOString().slice(0, 10);
}

type WeekResult = {
  bill_week_ending: string | null;
  week_ambiguous: boolean;
  ambiguous_candidates: { friday: string; rows: number }[];
  top5_bill_dates: { date: string; count: number }[];
};

function computeBillWeekEnding(billDates: string[]): WeekResult {
  const result: WeekResult = {
    bill_week_ending: null,
    week_ambiguous: false,
    ambiguous_candidates: [],
    top5_bill_dates: [],
  };
  if (!Array.isArray(billDates) || billDates.length === 0) return result;
  const dateCounts = new Map<string, number>();
  for (const bd of billDates) {
    if (!bd) continue;
    dateCounts.set(bd, (dateCounts.get(bd) || 0) + 1);
  }
  if (dateCounts.size === 0) return result;
  const sorted = [...dateCounts.entries()].sort((a, b) => b[1] - a[1]);
  const top5 = sorted.slice(0, 5).map(([date, count]) => ({ date, count }));
  result.top5_bill_dates = top5;

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
    result.ambiguous_candidates = envs
      .filter(([, n]) => n === tieCount)
      .map(([friday, rows]) => ({ friday, rows }));
  } else {
    result.bill_week_ending = envs[0][0];
  }
  return result;
}

// ─── Main backfill ──────────────────────────────────────────────────

type BackfillFileResult = {
  file_id: string;
  filename: string | null;
  row_count: number;
  bill_week_ending: string | null;
  week_ambiguous: boolean;
  ambiguous_candidates: { friday: string; rows: number }[];
  already_set: boolean;
  resolved_manually: boolean;
  updated: boolean;
  error: string | null;
};

async function runBackfill(): Promise<{ processed: number; updated: number; skipped: number; errors: number; files: BackfillFileResult[] }> {
  const files = await listAllDocs("ddis_files");
  console.log(`ddis-week-backfill: found ${files.length} ddis_files docs`);
  const fileResults: BackfillFileResult[] = [];
  let updated = 0;
  let skipped = 0;
  let errors = 0;

  for (const doc of files) {
    const docId = doc.name.split("/").pop()!;
    const fields = doc.fields || {};
    const filename = fields.filename?.stringValue || fields.file_id?.stringValue || docId;

    // Skip files that were already manually resolved — don't clobber a
    // user's choice on re-run.
    const resolvedManually = !!fields.week_resolved_manually?.booleanValue;
    const currentWeek = fields.bill_week_ending?.stringValue || null;
    const currentlyAmbiguous = !!fields.week_ambiguous?.booleanValue;

    if (resolvedManually) {
      fileResults.push({
        file_id: docId,
        filename,
        row_count: Number(fields.record_count?.integerValue || 0),
        bill_week_ending: currentWeek,
        week_ambiguous: false,
        ambiguous_candidates: [],
        already_set: true,
        resolved_manually: true,
        updated: false,
        error: null,
      });
      skipped += 1;
      continue;
    }

    try {
      const billDates = await queryBillDatesForFile(filename);
      const wk = computeBillWeekEnding(billDates);

      // If the file already has the exact same computed state, skip the write.
      const sameAsCurrent =
        currentWeek === wk.bill_week_ending &&
        currentlyAmbiguous === wk.week_ambiguous;

      if (sameAsCurrent && !wk.week_ambiguous && currentWeek) {
        fileResults.push({
          file_id: docId,
          filename,
          row_count: billDates.length,
          bill_week_ending: wk.bill_week_ending,
          week_ambiguous: false,
          ambiguous_candidates: [],
          already_set: true,
          resolved_manually: false,
          updated: false,
          error: null,
        });
        skipped += 1;
        continue;
      }

      await patchDdisFile(docId, {
        bill_week_ending: wk.bill_week_ending,
        week_ambiguous: wk.week_ambiguous,
        ambiguous_candidates: wk.ambiguous_candidates,
        top5_bill_dates: wk.top5_bill_dates,
        bill_week_computed_at: new Date().toISOString(),
      });

      fileResults.push({
        file_id: docId,
        filename,
        row_count: billDates.length,
        bill_week_ending: wk.bill_week_ending,
        week_ambiguous: wk.week_ambiguous,
        ambiguous_candidates: wk.ambiguous_candidates,
        already_set: false,
        resolved_manually: false,
        updated: true,
        error: null,
      });
      updated += 1;
      console.log(
        `  ✓ ${docId}: ${billDates.length} rows → ` +
        (wk.bill_week_ending
          ? `week ending ${wk.bill_week_ending}`
          : `AMBIGUOUS (${wk.ambiguous_candidates.map((c) => c.friday).join(", ")})`)
      );
    } catch (e: any) {
      errors += 1;
      const msg = e?.message || String(e);
      fileResults.push({
        file_id: docId,
        filename,
        row_count: 0,
        bill_week_ending: null,
        week_ambiguous: false,
        ambiguous_candidates: [],
        already_set: false,
        resolved_manually: false,
        updated: false,
        error: msg,
      });
      console.error(`  ✗ ${docId}: ${msg}`);
    }
  }

  // Write a summary doc so Settings can show "last backfill ran at …" later.
  // Non-critical — ignore failure.
  try {
    const summaryUrl = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/ddis_backfill_summary?key=${FIREBASE_API_KEY}`;
    await fetch(summaryUrl, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        fields: toFsValue({
          last_run_at: new Date().toISOString(),
          processed: files.length,
          updated,
          skipped,
          errors,
        }).mapValue.fields,
      }),
    });
  } catch {
    /* non-fatal */
  }

  return { processed: files.length, updated, skipped, errors, files: fileResults };
}

// ─── Entry point ────────────────────────────────────────────────────

export default async (_req: Request, _context: Context) => {
  if (!FIREBASE_API_KEY) {
    console.error("ddis-week-backfill: missing FIREBASE_API_KEY env var");
    return;
  }
  const t0 = Date.now();
  console.log("ddis-week-backfill: start");
  try {
    const r = await runBackfill();
    console.log(
      `ddis-week-backfill: done — processed ${r.processed}, updated ${r.updated}, ` +
      `skipped ${r.skipped}, errors ${r.errors} (${Math.round((Date.now() - t0) / 1000)}s)`
    );
  } catch (e: any) {
    console.error("ddis-week-backfill: FAILED", e?.message || String(e));
  }
};

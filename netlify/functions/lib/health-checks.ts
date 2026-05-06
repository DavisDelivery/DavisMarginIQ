/**
 * Davis MarginIQ — Standing health-check infrastructure (v2.53.3)
 *
 * Centralized result-writing for the 9 standing checks introduced in
 * Phase 2 Commit 3. Each check endpoint imports `runHealthCheck` and
 * passes a closure that produces the {status, summary, details} payload;
 * the wrapper handles timing, Firestore writes, and HTTP response shape.
 *
 * Storage:
 *   marginiq_health_checks/{check_id}__{ran_at}   — time-series record
 *   marginiq_health_checks/{check_id}__latest     — quick-lookup mirror
 *
 * Both docs share the same payload. The `__latest` mirror is overwritten
 * on every run; the timestamped doc is append-only history.
 *
 * See DESIGN.md §5 for the full check spec.
 */

const PROJECT_ID = "davismarginiq";
const FS_BASE = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;

export type CheckId = "1A" | "1B" | "2" | "2B" | "2C" | "3" | "4" | "5" | "6";
export type CheckStatus = "PASS" | "FAIL" | "INFO";

export interface HealthCheckResult {
  check_id: CheckId;
  ran_at: string;       // ISO timestamp
  status: CheckStatus;
  summary: string;      // one-line human-readable
  details: any;         // check-specific payload
  duration_ms: number;
}

export interface CheckClosureResult {
  status: CheckStatus;
  summary: string;
  details: any;
}

/**
 * Run a standing check and persist its result.
 *
 * The closure should perform the check's read-only work and return
 * {status, summary, details}. This wrapper:
 *   1. Records start time
 *   2. Invokes the closure (catches throws, reports as FAIL)
 *   3. Writes both the timestamped doc and the __latest mirror
 *   4. Returns an HTTP Response containing the full result
 *
 * If the Firestore writes themselves fail, the response still includes
 * the result body (since the check itself completed). The caller can
 * inspect `wrote_to_firestore` to know whether persistence succeeded.
 */
export async function runHealthCheck(
  checkId: CheckId,
  apiKey: string,
  closure: () => Promise<CheckClosureResult>,
): Promise<Response> {
  const startMs = Date.now();
  const ranAt = new Date().toISOString();

  let result: CheckClosureResult;
  try {
    result = await closure();
  } catch (e: any) {
    result = {
      status: "FAIL",
      summary: `Check ${checkId} threw: ${e?.message || String(e)}`,
      details: { error: e?.message || String(e), stack: e?.stack || null },
    };
  }

  const durationMs = Date.now() - startMs;
  const fullResult: HealthCheckResult = {
    check_id: checkId,
    ran_at: ranAt,
    status: result.status,
    summary: result.summary,
    details: result.details,
    duration_ms: durationMs,
  };

  // Persist to Firestore. Two writes — both can independently fail.
  // We don't fail the whole check if persistence fails; the caller will
  // see wrote_to_firestore: false and can investigate.
  const tsId = `${checkId}__${ranAt.replace(/[:.]/g, "-")}`;
  const latestId = `${checkId}__latest`;

  const tsOk = await writeHealthDoc(tsId, fullResult, apiKey);
  const latestOk = await writeHealthDoc(latestId, fullResult, apiKey);

  return new Response(JSON.stringify({
    ok: result.status !== "FAIL",
    ...fullResult,
    wrote_to_firestore: tsOk && latestOk,
    timestamped_doc_id: tsId,
  }), {
    status: result.status === "FAIL" ? 200 : 200,  // both still HTTP 200; caller reads .status
    headers: { "Content-Type": "application/json" },
  });
}

/**
 * Write a HealthCheckResult to marginiq_health_checks. Uses PATCH with
 * an explicit updateMask (the v2.53.2 fix pattern) so updates land
 * cleanly on the __latest mirror.
 */
async function writeHealthDoc(docId: string, result: HealthCheckResult, apiKey: string): Promise<boolean> {
  const fields = toFsFields({
    check_id: result.check_id,
    ran_at: result.ran_at,
    status: result.status,
    summary: result.summary,
    // Stringify details to a JSON blob — the result shapes differ per
    // check and we don't want to invent a Firestore schema for each.
    // Stays inspectable in the console and trivially parsed by readers.
    details_json: JSON.stringify(result.details ?? {}),
    duration_ms: result.duration_ms,
  });

  const updateMask = Object.keys(fields).map(f => `updateMask.fieldPaths=${f}`).join("&");
  const url = `${FS_BASE}/marginiq_health_checks/${encodeURIComponent(docId)}?${updateMask}&key=${apiKey}`;

  try {
    const r = await fetch(url, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fields }),
    });
    if (!r.ok) {
      const text = await r.text().catch(() => "");
      console.error(`writeHealthDoc ${docId} failed: ${r.status} ${text.slice(0, 200)}`);
      return false;
    }
    return true;
  } catch (e: any) {
    console.error(`writeHealthDoc ${docId} threw: ${e?.message || e}`);
    return false;
  }
}

/**
 * Local copy of the toFsFields helper (lib/four-layer-ingest.ts has its
 * own and we don't want a cross-module import for this small primitive).
 * Maps JS values to Firestore typed-field representations.
 */
function toFsFields(obj: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(obj)) {
    out[k] = toFsValue(v);
  }
  return out;
}

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "string") return { stringValue: v };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") {
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  }
  if (Array.isArray(v)) return { arrayValue: { values: v.map(toFsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

/**
 * GET helper used by checks that need to read existing Firestore docs.
 * Returns parsed JSON on success, null on 404, throws on other errors.
 */
export async function fsGetDoc(collection: string, docId: string, apiKey: string): Promise<any | null> {
  const url = `${FS_BASE}/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const r = await fetch(url);
  if (r.status === 404) return null;
  if (!r.ok) {
    throw new Error(`fsGetDoc ${collection}/${docId} failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  }
  return await r.json();
}

/**
 * Run a Firestore structuredQuery. Returns the raw RunQueryResponse array.
 */
export async function fsRunQuery(body: any, apiKey: string): Promise<any[]> {
  const url = `${FS_BASE}:runQuery?key=${apiKey}`;
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    throw new Error(`fsRunQuery failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  }
  return await r.json();
}

/**
 * Run a Firestore aggregation query (count, sum, avg).
 * Returns the parsed result.
 */
export async function fsRunAggregation(body: any, apiKey: string): Promise<any[]> {
  const url = `${FS_BASE}:runAggregationQuery?key=${apiKey}`;
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    throw new Error(`fsRunAggregation failed: ${r.status} ${(await r.text()).slice(0, 300)}`);
  }
  return await r.json();
}

/**
 * Unwrap a Firestore field object to native JS. Handles primitives,
 * arrays, and maps. Used by check closures to read query results.
 */
export function unwrapField(f: any): any {
  if (!f || typeof f !== "object") return f;
  if ("stringValue" in f) return f.stringValue;
  if ("integerValue" in f) return Number(f.integerValue);
  if ("doubleValue" in f) return f.doubleValue;
  if ("booleanValue" in f) return f.booleanValue;
  if ("nullValue" in f) return null;
  if ("timestampValue" in f) return f.timestampValue;
  if ("arrayValue" in f) {
    return (f.arrayValue?.values || []).map(unwrapField);
  }
  if ("mapValue" in f) {
    const out: Record<string, any> = {};
    const fields = f.mapValue?.fields || {};
    for (const [k, v] of Object.entries(fields)) out[k] = unwrapField(v);
    return out;
  }
  return undefined;
}

export function unwrapDoc(doc: any): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(doc?.fields || {})) out[k] = unwrapField(v);
  return out;
}

/**
 * Page a structuredQuery in chunks. Used by checks that need to scan
 * collections larger than the 200-row Firestore default. Calls the
 * provided callback for each page; callback returns false to stop.
 */
export async function fsPaginate(
  baseQuery: any,
  pageSize: number,
  apiKey: string,
  onPage: (docs: any[]) => Promise<boolean | void>,
): Promise<{ pages: number; total: number }> {
  let pages = 0;
  let total = 0;
  let cursor: string | undefined;
  while (true) {
    const q = JSON.parse(JSON.stringify(baseQuery));
    q.structuredQuery.limit = pageSize;
    if (cursor) {
      q.structuredQuery.startAt = { values: [{ referenceValue: cursor }], before: false };
    }
    const res = await fsRunQuery(q, apiKey);
    const docs = res.filter(r => r.document).map(r => r.document);
    if (docs.length === 0) break;
    pages++;
    total += docs.length;
    const cont = await onPage(docs);
    if (cont === false) break;
    if (docs.length < pageSize) break;
    cursor = docs[docs.length - 1].name;
  }
  return { pages, total };
}

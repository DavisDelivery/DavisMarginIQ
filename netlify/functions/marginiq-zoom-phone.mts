import type { Context } from "@netlify/functions";

// ═══════════════════════════════════════════════════════════════════════════
// Zoom Phone Reporting — v3
// ═══════════════════════════════════════════════════════════════════════════
//
// CRITICAL FIX: Split storage by USER + DAY to stay under Firestore's 1MB
// document limit. Previous design hit limit for high-volume users (Jessica
// had 2598 records, Brandi had 2083 — both over the 1MB doc limit).
//
// Storage layout:
//   zoom_user_index/{userId}      — small doc with user metadata + sync status
//   zoom_calls/{YYYY-MM-DD}_{userId}  — one doc per user per day (~50-150 records each)
//
// Architecture:
//   - sync-user: paginates through Zoom, groups records by date, writes
//     ONE Firestore doc per (date, user) combo
//   - history: lists all docs in zoom_calls collection, applies date/dir
//     filter, deduplicates by callId, returns aggregated set

const TOKEN_URL = "https://zoom.us/oauth/token";
const API       = "https://api.zoom.us/v2";
const CORS      = { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" };
const sleep     = (ms: number) => new Promise(r => setTimeout(r, ms));

async function getToken(acct: string, cid: string, csec: string): Promise<string> {
  const res = await fetch(
    `${TOKEN_URL}?grant_type=account_credentials&account_id=${encodeURIComponent(acct)}`,
    { method: "POST", headers: { "Authorization": "Basic " + btoa(`${cid}:${csec}`), "Content-Type": "application/x-www-form-urlencoded" } }
  );
  const d: any = await res.json();
  if (!res.ok) throw new Error(d.reason || d.message || `Auth failed (${res.status})`);
  return d.access_token;
}

async function getPhoneUsers(token: string) {
  const users: any[] = [];
  let npt = "", pages = 0;
  do {
    const url = `${API}/phone/users?page_size=100${npt ? "&next_page_token=" + encodeURIComponent(npt) : ""}`;
    const res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
    const d: any = await res.json();
    if (!res.ok) throw new Error(d.message || `Users API ${res.status}`);
    users.push(...(d.users || []));
    npt = d.next_page_token || "";
    if (++pages >= 5) break;
  } while (npt);
  return users.map(u => ({
    id:    u.id || "",
    name:  u.display_name || u.name || u.email || "Unknown",
    email: u.email || "",
    ext:   String(u.extension_number || ""),
  }));
}

async function getUserCallLogs(
  token: string, userId: string, from: string, to: string
): Promise<{ records: any[]; truncated: boolean; error?: string }> {
  const records: any[] = [];
  let npt = "", pages = 0;
  const MAX_PAGES = 30;

  while (true) {
    const url = `${API}/phone/users/${userId}/call_logs?from=${from}&to=${to}&type=all&page_size=300${npt ? "&next_page_token=" + encodeURIComponent(npt) : ""}`;
    let res: Response | null = null;
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
        if (res.status === 429) { await sleep(1500); continue; }
        break;
      } catch (e) { await sleep(500); }
    }
    if (!res) return { records, truncated: true, error: "fetch failed" };
    const d: any = await res.json().catch(() => ({}));
    if (!res.ok) {
      if (res.status === 404 || res.status === 400) return { records, truncated: false };
      return { records, truncated: true, error: d.message || `HTTP ${res.status}` };
    }
    records.push(...(d.call_logs || []));
    npt = d.next_page_token || "";
    pages++;
    if (!npt) return { records, truncated: false };
    if (pages >= MAX_PAGES) return { records, truncated: true };
  }
}

function normalizeRecord(raw: any, owner: { name: string; email: string; ext: string }) {
  const rawResult = String(raw.result || "");
  let result: string;
  let isAnsweredCall: boolean;
  let isAttributedToUser: boolean;

  if (rawResult === "Call connected") {
    result = "answered"; isAnsweredCall = true; isAttributedToUser = true;
  } else if (rawResult === "Answered by Other Member") {
    result = "answered"; isAnsweredCall = true; isAttributedToUser = false;
  } else if (rawResult === "No Answer" || rawResult === "Call Cancel" || rawResult === "Rejected") {
    result = "missed"; isAnsweredCall = false; isAttributedToUser = false;
  } else if (rawResult.toLowerCase().includes("voicemail")) {
    result = "voicemail"; isAnsweredCall = false; isAttributedToUser = false;
  } else {
    result = "other"; isAnsweredCall = false; isAttributedToUser = false;
  }

  return {
    callId:        raw.call_id || raw.id || "",
    date:          raw.date_time || "",
    answerTime:    raw.answer_start_time || "",
    endTime:       raw.call_end_time || "",
    direction:     String(raw.direction || "").toLowerCase(),
    answeredBy:    isAttributedToUser ? owner.name : "",
    answeredEmail: owner.email,
    answeredExt:   owner.ext,
    cacheOwner:    owner.name,
    callerName:    raw.caller_name || "",
    callerNum:     raw.caller_number || raw.caller_did_number || "",
    calleeName:    raw.callee_name || "",
    calleeNum:     raw.callee_number || raw.callee_did_number || "",
    path:          raw.path || "",
    department:    raw.department || "",
    result,
    rawResult,
    isAnsweredCall,
    isAttributedToUser,
    duration:      Number(raw.duration) || 0,
    talkTime:      Number(raw.duration) || 0,
  };
}

function dateKey(iso: string): string {
  // Extract YYYY-MM-DD from ISO timestamp; return empty string if invalid
  if (!iso || typeof iso !== "string") return "";
  const m = iso.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : "";
}

function aggregateForReport(allRecords: any[], from: string, to: string, dir: string): any[] {
  const fromMs = new Date(from + "T00:00:00Z").getTime();
  const toMs   = new Date(to   + "T23:59:59.999Z").getTime();
  const inRange = allRecords.filter(r => {
    if (!r || !r.date) return false;
    const ms = new Date(r.date).getTime();
    if (isNaN(ms) || ms < fromMs || ms > toMs) return false;
    if (dir && r.direction !== dir) return false;
    return true;
  });

  const byCallId = new Map<string, any[]>();
  for (const r of inRange) {
    if (!r.callId) continue;
    if (!byCallId.has(r.callId)) byCallId.set(r.callId, []);
    byCallId.get(r.callId)!.push(r);
  }

  const result: any[] = [];
  for (const [callId, recs] of byCallId.entries()) {
    const answerer = recs.find(r => r.isAttributedToUser);
    if (answerer) {
      result.push({ ...answerer, answeredBy: answerer.cacheOwner });
    } else {
      const sample = recs[0];
      result.push({ ...sample, answeredBy: sample.isAnsweredCall ? "" : sample.cacheOwner });
    }
  }
  return result.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
}

// ── Firebase ────────────────────────────────────────────────────────────────
function toFV(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean")        return { booleanValue: v };
  if (typeof v === "number")         return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (typeof v === "string")         return { stringValue: v };
  if (Array.isArray(v))              return { arrayValue: { values: v.map(toFV) } };
  if (typeof v === "object")         return { mapValue: { fields: Object.fromEntries(Object.entries(v).map(([k,val]) => [k, toFV(val)])) } };
  return { stringValue: String(v) };
}
function fromFVVal(v: any): any {
  if (v.nullValue   !== undefined) return null;
  if (v.booleanValue !== undefined) return v.booleanValue;
  if (v.integerValue !== undefined) return Number(v.integerValue);
  if (v.doubleValue  !== undefined) return v.doubleValue;
  if (v.stringValue  !== undefined) return v.stringValue;
  if (v.arrayValue)  return (v.arrayValue.values || []).map(fromFVVal);
  if (v.mapValue)    return Object.fromEntries(Object.entries(v.mapValue.fields || {}).map(([k,val]) => [k, fromFVVal(val)]));
  return null;
}
function fromFV(fields: any): any {
  return Object.fromEntries(Object.entries(fields || {}).map(([k,v]) => [k, fromFVVal(v)]));
}

async function fsSet(proj: string, key: string, col: string, doc: string, data: any) {
  const fields = Object.fromEntries(Object.entries(data).map(([k,v]) => [k, toFV(v)]));
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}/${encodeURIComponent(doc)}?key=${key}`;
  const res = await fetch(url, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fields }) });
  if (!res.ok) {
    const err = await res.text().catch(()=>"");
    throw new Error(`PATCH ${col}/${doc} ${res.status}: ${err.slice(0,150)}`);
  }
}

async function fsListDocs(proj: string, key: string, col: string, pageToken?: string): Promise<{docs: any[], nextPageToken?: string}> {
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}?key=${key}&pageSize=300${pageToken ? "&pageToken=" + encodeURIComponent(pageToken) : ""}`;
  const res = await fetch(url);
  if (!res.ok) return { docs: [] };
  const d: any = await res.json();
  return {
    docs: (d.documents || []).map((doc: any) => ({ _name: doc.name, ...fromFV(doc.fields || {}) })),
    nextPageToken: d.nextPageToken,
  };
}

async function fsListAllDocs(proj: string, key: string, col: string): Promise<any[]> {
  const all: any[] = [];
  let pt: string | undefined;
  let pages = 0;
  do {
    const { docs, nextPageToken } = await fsListDocs(proj, key, col, pt);
    all.push(...docs);
    pt = nextPageToken;
    if (++pages > 20) break;  // safety: max 6000 docs
  } while (pt);
  return all;
}

async function fsDelete(proj: string, key: string, col: string, doc: string) {
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}/${encodeURIComponent(doc)}?key=${key}`;
  await fetch(url, { method: "DELETE" }).catch(() => {});
}

// Save user's records split by date to avoid 1MB doc limit
async function saveUserRecordsByDay(
  proj: string, key: string, userId: string, owner: any, records: any[]
): Promise<{ daysWritten: number; totalWritten: number; failures: string[] }> {
  // Group records by date (YYYY-MM-DD)
  const byDate = new Map<string, any[]>();
  for (const r of records) {
    const dk = dateKey(r.date);
    if (!dk) continue;
    if (!byDate.has(dk)) byDate.set(dk, []);
    byDate.get(dk)!.push(r);
  }

  const failures: string[] = [];
  let totalWritten = 0;
  let daysWritten = 0;

  // Write one doc per (date, user)
  for (const [dk, dayRecs] of byDate.entries()) {
    const docId = `${dk}_${userId}`;
    try {
      await fsSet(proj, key, "zoom_history_days", docId, {
        date: dk,
        userId, name: owner.name, ext: owner.ext,
        records: dayRecs,
        count: dayRecs.length,
      });
      daysWritten++;
      totalWritten += dayRecs.length;
    } catch (e: any) {
      failures.push(`${dk}: ${e?.message?.slice(0,100)}`);
    }
  }

  return { daysWritten, totalWritten, failures };
}

// Wipe all of a user's day-docs
async function wipeUserDays(proj: string, key: string, userId: string) {
  const all = await fsListAllDocs(proj, key, "zoom_history_days");
  for (const doc of all) {
    if (doc.userId === userId && doc._name) {
      const docId = doc._name.split("/").pop();
      await fsDelete(proj, key, "zoom_history_days", docId);
    }
  }
}

// ── Main handler ─────────────────────────────────────────────────────────────
export default async (req: Request, _ctx: Context) => {
  const ACCT    = process.env["ZOOM_ACCOUNT_ID"];
  const CID     = process.env["ZOOM_CLIENT_ID"];
  const CSEC    = process.env["ZOOM_CLIENT_SECRET"];
  const FB_KEY  = process.env["FIREBASE_API_KEY"];
  const FB_PROJ = process.env["FIREBASE_PROJECT_ID"];

  const u      = new URL(req.url);
  const action = u.searchParams.get("action") || "history";
  const from   = u.searchParams.get("from") || new Date(Date.now()-30*86400000).toISOString().slice(0,10);
  const to     = u.searchParams.get("to")   || new Date().toISOString().slice(0,10);
  const dir    = u.searchParams.get("direction") || "";
  const t0     = Date.now();

  if (action === "status") {
    return new Response(JSON.stringify({
      configured: !!(ACCT && CID && CSEC),
      missing: [!ACCT&&"ZOOM_ACCOUNT_ID",!CID&&"ZOOM_CLIENT_ID",!CSEC&&"ZOOM_CLIENT_SECRET"].filter(Boolean),
    }), { headers: CORS });
  }

  if (!ACCT || !CID || !CSEC) {
    return new Response(JSON.stringify({ error: "Missing Zoom credentials." }), { status: 500, headers: CORS });
  }

  // ── History — read all day-docs and aggregate ────────────────────────────
  if (action === "history") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({ error: "Firebase not configured", source: "error" }), { status: 500, headers: CORS });
    try {
      const allDayDocs = await fsListAllDocs(FB_PROJ, FB_KEY, "zoom_history_days");
      if (allDayDocs.length === 0) {
        return new Response(JSON.stringify({
          records: [], count: 0, source: "empty",
          message: "Cache is empty. Click Sync from Zoom.",
          synced_at: null, from, to, ms: Date.now()-t0,
        }), { headers: CORS });
      }

      const all: any[] = [];
      for (const doc of allDayDocs) {
        for (const r of (doc.records || [])) all.push(r);
      }
      const aggregated = aggregateForReport(all, from, to, dir);

      // Get sync timestamp from index
      const idx = await fsListAllDocs(FB_PROJ, FB_KEY, "zoom_user_index");
      const synced_at = idx.map((d:any) => d.synced_at).filter(Boolean).sort().reverse()[0] || null;

      return new Response(JSON.stringify({
        records: aggregated, count: aggregated.length,
        source: "cache", synced_at, from, to,
        rawLegs: all.length, dayDocs: allDayDocs.length,
        ms: Date.now()-t0,
      }), { headers: CORS });
    } catch(e: any) {
      return new Response(JSON.stringify({ error: e?.message, source: "error" }), { status: 500, headers: CORS });
    }
  }

  // ── List phone users ─────────────────────────────────────────────────────
  if (action === "users") {
    try {
      const token = await getToken(ACCT, CID, CSEC);
      const users = await getPhoneUsers(token);
      return new Response(JSON.stringify({ users }), { headers: CORS });
    } catch(e: any) {
      return new Response(JSON.stringify({ error: e?.message }), { status: 500, headers: CORS });
    }
  }

  // ── Sync ONE user ────────────────────────────────────────────────────────
  if (action === "sync-user") {
    const userId   = u.searchParams.get("userId") || "";
    const userName = u.searchParams.get("name")   || "";
    const userEmail= u.searchParams.get("email")  || "";
    const userExt  = u.searchParams.get("ext")    || "";
    const syncFrom = u.searchParams.get("from") || new Date(Date.now()-35*86400000).toISOString().slice(0,10);
    const syncTo   = u.searchParams.get("to")   || new Date().toISOString().slice(0,10);
    if (!userId) return new Response(JSON.stringify({ error: "userId required" }), { status: 400, headers: CORS });
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({ error: "Firebase not configured" }), { status: 500, headers: CORS });

    try {
      const token = await getToken(ACCT, CID, CSEC);
      const owner = { name: userName, email: userEmail, ext: userExt };

      // Wipe this user's old day-docs
      await wipeUserDays(FB_PROJ, FB_KEY, userId);

      // Fetch from Zoom
      const result = await getUserCallLogs(token, userId, syncFrom, syncTo);
      const normalized = result.records.map((r: any) => normalizeRecord(r, owner));
      const trulyAnswered = normalized.filter(r => r.isAttributedToUser).length;

      // Save split by day
      const saveResult = await saveUserRecordsByDay(FB_PROJ, FB_KEY, userId, owner, normalized);

      // Update user index
      await fsSet(FB_PROJ, FB_KEY, "zoom_user_index", userId, {
        userId, name: userName, email: userEmail, ext: userExt,
        totalRecords: normalized.length,
        trulyAnswered,
        daysWritten: saveResult.daysWritten,
        recordsWritten: saveResult.totalWritten,
        synced_at: new Date().toISOString(),
        from: syncFrom, to: syncTo,
        truncated: result.truncated || false,
        sync_error: result.error || "",
        save_failures: saveResult.failures.slice(0, 5),
      });

      return new Response(JSON.stringify({
        ok: !result.error && saveResult.failures.length === 0,
        name: userName,
        records: normalized.length, trulyAnswered,
        daysWritten: saveResult.daysWritten,
        recordsWritten: saveResult.totalWritten,
        saveFailures: saveResult.failures.length,
        truncated: result.truncated, error: result.error || null,
        ms: Date.now()-t0,
      }), { headers: CORS });
    } catch(e: any) {
      try {
        await fsSet(FB_PROJ, FB_KEY, "zoom_user_index", userId, {
          userId, name: userName, ext: userExt,
          totalRecords: 0, trulyAnswered: 0,
          synced_at: new Date().toISOString(),
          from: syncFrom, to: syncTo,
          sync_error: String(e?.message || e),
        });
      } catch (_) {}
      return new Response(JSON.stringify({ error: String(e?.message||e), name: userName }), { status: 500, headers: CORS });
    }
  }

  // ── Wipe all caches (clean slate) ────────────────────────────────────────
  if (action === "wipe-all") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });

    // Wipe new collections
    const dayDocs = await fsListAllDocs(FB_PROJ, FB_KEY, "zoom_history_days");
    let wipedCalls = 0;
    for (const doc of dayDocs) {
      if (doc._name) {
        const docId = doc._name.split("/").pop();
        await fsDelete(FB_PROJ, FB_KEY, "zoom_history_days", docId);
        wipedCalls++;
      }
    }
    const idx = await fsListAllDocs(FB_PROJ, FB_KEY, "zoom_user_index");
    let wipedIdx = 0;
    for (const doc of idx) {
      if (doc.userId) { await fsDelete(FB_PROJ, FB_KEY, "zoom_user_index", doc.userId); wipedIdx++; }
    }

    // Wipe old (legacy) collections so they don't pollute reports
    const oldUserLogs = await fsListAllDocs(FB_PROJ, FB_KEY, "zoom_user_logs");
    let wipedOldLogs = 0;
    for (const doc of oldUserLogs) {
      if (doc.userId) { await fsDelete(FB_PROJ, FB_KEY, "zoom_user_logs", doc.userId); wipedOldLogs++; }
    }
    const oldSync = await fsListAllDocs(FB_PROJ, FB_KEY, "zoom_sync_users");
    let wipedOldSync = 0;
    for (const doc of oldSync) {
      if (doc.userId) { await fsDelete(FB_PROJ, FB_KEY, "zoom_sync_users", doc.userId); wipedOldSync++; }
    }

    return new Response(JSON.stringify({
      ok: true,
      wipedDayDocs: wipedCalls, wipedIndex: wipedIdx,
      wipedOldUserLogs: wipedOldLogs, wipedOldSyncUsers: wipedOldSync,
    }), { headers: CORS });
  }

  // ── Debug ────────────────────────────────────────────────────────────────
  if (action === "debug-cache") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const idx = await fsListAllDocs(FB_PROJ, FB_KEY, "zoom_user_index");
    return new Response(JSON.stringify({
      total: idx.length,
      summary: idx.map((d: any) => ({
        name: d.name, ext: d.ext,
        totalRecords: d.totalRecords || 0,
        trulyAnswered: d.trulyAnswered || 0,
        daysWritten: d.daysWritten || 0,
        recordsWritten: d.recordsWritten || 0,
        synced_at: d.synced_at,
        from: d.from, to: d.to,
        truncated: d.truncated || false,
        sync_error: d.sync_error || "",
        save_failures: d.save_failures || [],
      })),
    }), { headers: CORS });
  }

  if (action === "debug-aggregated") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const dayDocs = await fsListAllDocs(FB_PROJ, FB_KEY, "zoom_history_days");
    const all: any[] = [];
    for (const doc of dayDocs) for (const r of (doc.records || [])) all.push(r);
    const agg = aggregateForReport(all, from, to, dir);
    const stats: Record<string, number> = {};
    const byEmployee: Record<string, number> = {};
    for (const r of agg) {
      stats[r.result] = (stats[r.result] || 0) + 1;
      if (r.answeredBy) byEmployee[r.answeredBy] = (byEmployee[r.answeredBy] || 0) + 1;
      else if (r.isAnsweredCall) byEmployee["(answered by uncached user)"] = (byEmployee["(answered by uncached user)"] || 0) + 1;
    }
    return new Response(JSON.stringify({
      totalUniqueCalls: agg.length,
      rawLegsAcrossAllUsers: all.length,
      dayDocsLoaded: dayDocs.length,
      byResult: stats,
      byEmployee,
    }), { headers: CORS });
  }

  return new Response(JSON.stringify({ error: "Unknown action: " + action }), { status: 400, headers: CORS });
};

import type { Context } from "@netlify/functions";

// ═══════════════════════════════════════════════════════════════════════════
// Zoom Phone Reporting — CLEAN REBUILD
// ═══════════════════════════════════════════════════════════════════════════
//
// Approach (after researching Zoom's API docs):
//
// 1. ACCOUNT-LEVEL TOTALS: Use legacy `GET /phone/call_logs` for total call
//    volume (one record per logical call, no queue-leg noise). This gives us
//    accurate total counts and busiest-day analytics.
//
// 2. PER-EMPLOYEE ATTRIBUTION: Use legacy `GET /phone/users/{id}/call_logs`.
//    This endpoint returns clean records with these critical fields:
//      - result: "Call connected" → this user actually answered
//      - result: "Answered by Other Member" → rang user's queue but someone
//        else picked up (DO NOT count as their answered call)
//      - result: "No Answer" / "Call Cancel" / "Rejected" → other miss types
//      - duration: actual talk time in seconds
//      - direction: inbound | outbound
//      - answer_start_time: timestamp when they picked up (set only when
//        they personally answered)
//
// 3. DEDUPLICATION: We use call_id as the unique key. The user-level endpoint
//    already returns one record per leg the user was involved in, but only
//    counts as their "answered" call when result === "Call connected".
//
// 4. NOTE ON DEPRECATION: Zoom plans to deprecate the legacy `call_logs`
//    endpoints (was scheduled for 2025, extended to May 2026). We're using
//    them today because they give cleaner per-user attribution. The newer
//    `call_history` endpoint is fundamentally a different data model that
//    returns one row per call leg/event, not per call.
//
// Actions:
//   status     — check credentials configured
//   users      — list phone-licensed users
//   sync-user  — fetch one user's call logs and write to Firebase
//   history    — read aggregated history from Firebase cache (instant)
//   debug-cache — show what's in cache per user
//   debug-raw  — show raw Zoom response for one user
//   wipe-all   — clear all user caches (clean reset)

const TOKEN_URL = "https://zoom.us/oauth/token";
const API       = "https://api.zoom.us/v2";
const CORS      = { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" };
const sleep     = (ms: number) => new Promise(r => setTimeout(r, ms));

// ── Auth ─────────────────────────────────────────────────────────────────────
async function getToken(acct: string, cid: string, csec: string): Promise<string> {
  const res = await fetch(
    `${TOKEN_URL}?grant_type=account_credentials&account_id=${encodeURIComponent(acct)}`,
    { method: "POST", headers: { "Authorization": "Basic " + btoa(`${cid}:${csec}`), "Content-Type": "application/x-www-form-urlencoded" } }
  );
  const d: any = await res.json();
  if (!res.ok) throw new Error(d.reason || d.message || `Auth failed (${res.status})`);
  return d.access_token;
}

// ── Phone users ──────────────────────────────────────────────────────────────
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

// ── Get one user's call logs (LEGACY ENDPOINT — clean data) ─────────────────
async function getUserCallLogs(
  token: string, userId: string, from: string, to: string
): Promise<{ records: any[]; truncated: boolean; error?: string }> {
  const records: any[] = [];
  let npt = "", pages = 0;
  const MAX_PAGES = 100;

  while (true) {
    const url = `${API}/phone/users/${userId}/call_logs?from=${from}&to=${to}&type=all&page_size=300${npt ? "&next_page_token=" + encodeURIComponent(npt) : ""}`;

    let res: Response | null = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
        if (res.status === 429) { await sleep(2000 * (attempt + 1)); continue; }
        break;
      } catch (e) { await sleep(1000); }
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

// ── Normalize one user-level call_log record ────────────────────────────────
function normalizeRecord(raw: any, owner: { name: string; email: string; ext: string }) {
  // Result interpretation per Zoom's actual values:
  const rawResult = String(raw.result || "");
  let result: string;
  if (rawResult === "Call connected") result = "answered";          // user truly answered
  else if (rawResult === "Answered by Other Member") result = "answered_elsewhere"; // queue rang but someone else picked up
  else if (rawResult === "No Answer") result = "missed";            // user didn't pick up
  else if (rawResult === "Call Cancel") result = "cancelled";       // caller hung up before answer
  else if (rawResult === "Rejected") result = "rejected";           // user declined
  else if (rawResult.toLowerCase().includes("voicemail")) result = "voicemail";
  else result = "other";

  return {
    callId:        raw.call_id || raw.id || "",
    date:          raw.date_time || "",
    answerTime:    raw.answer_start_time || "",
    endTime:       raw.call_end_time || "",
    direction:     String(raw.direction || "").toLowerCase(),

    // The owner of this call_logs feed is the user — that's who answered
    // (when result === "Call connected") or had their queue ring (when "Answered by Other Member")
    answeredBy:    owner.name,
    answeredEmail: owner.email,
    answeredExt:   owner.ext,

    // Caller / callee for display
    callerName:    raw.caller_name || "",
    callerNum:     raw.caller_number || raw.caller_did_number || "",
    calleeName:    raw.callee_name || "",
    calleeNum:     raw.callee_number || raw.callee_did_number || "",

    // Routing
    path:          raw.path || "",          // pstn, autoReceptionist, callQueue, etc.
    department:    raw.department || "",

    // Outcome
    result,
    rawResult,                                // keep original for debugging
    duration:      Number(raw.duration) || 0,
    talkTime:      Number(raw.duration) || 0, // alias for UI back-compat

    // Was this counted as the user's answered call?
    isUserAnswered: result === "answered",    // only true when user personally took it
  };
}

// ── Aggregate cached records by ACCOUNT-LEVEL totals + per-employee ─────────
// Returns:
//   - one record per unique callId (account-level dedup)
//   - employee attribution to whoever has result === "answered" for that callId
function aggregateForReport(allRecords: any[], from: string, to: string, dir: string): any[] {
  const fromMs = new Date(from + "T00:00:00Z").getTime();
  const toMs   = new Date(to   + "T23:59:59.999Z").getTime();

  // Filter by date and direction
  const inRange = allRecords.filter(r => {
    if (!r || !r.date) return false;
    const ms = new Date(r.date).getTime();
    if (isNaN(ms) || ms < fromMs || ms > toMs) return false;
    if (dir && r.direction !== dir) return false;
    return true;
  });

  // Group by call_id
  const byCallId = new Map<string, any>();
  for (const r of inRange) {
    const key = r.callId;
    if (!key) continue;
    const existing = byCallId.get(key);
    if (!existing) {
      byCallId.set(key, r);
      continue;
    }
    // If we have multiple records for the same call_id (one per user that
    // saw it), prefer the one where the user truly answered (result === "answered")
    if (r.isUserAnswered && !existing.isUserAnswered) {
      byCallId.set(key, r);
    }
    // Otherwise keep first (typically the user-level endpoint returns same
    // result for all involved users on inbound queue calls)
  }

  return Array.from(byCallId.values()).sort((a, b) =>
    new Date(b.date).getTime() - new Date(a.date).getTime()
  );
}

// ── Firebase REST helpers ───────────────────────────────────────────────────
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
    throw new Error(`Firestore PATCH ${res.status}: ${err.slice(0,150)}`);
  }
}
async function fsListDocs(proj: string, key: string, col: string): Promise<any[]> {
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}?key=${key}&pageSize=200`;
  const res = await fetch(url);
  if (!res.ok) return [];
  const d: any = await res.json();
  return (d.documents || []).map((doc: any) => fromFV(doc.fields || {}));
}
async function fsDelete(proj: string, key: string, col: string, doc: string) {
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}/${encodeURIComponent(doc)}?key=${key}`;
  await fetch(url, { method: "DELETE" }).catch(() => {});
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

  // ── Status ────────────────────────────────────────────────────────────────
  if (action === "status") {
    return new Response(JSON.stringify({
      configured: !!(ACCT && CID && CSEC),
      missing: [!ACCT&&"ZOOM_ACCOUNT_ID",!CID&&"ZOOM_CLIENT_ID",!CSEC&&"ZOOM_CLIENT_SECRET"].filter(Boolean),
      hasFirebase: !!(FB_KEY && FB_PROJ),
    }), { headers: CORS });
  }

  if (!ACCT || !CID || !CSEC) {
    return new Response(JSON.stringify({ error: "Missing Zoom credentials." }), { status: 500, headers: CORS });
  }

  // ── History — read aggregated data from cache ────────────────────────────
  if (action === "history") {
    if (!FB_KEY || !FB_PROJ) {
      return new Response(JSON.stringify({ error: "Firebase not configured", source: "error" }), { status: 500, headers: CORS });
    }

    try {
      const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_user_logs");

      if (userDocs.length === 0) {
        return new Response(JSON.stringify({
          records: [], count: 0, source: "empty",
          message: "Cache is empty. Click Sync from Zoom to load your data.",
          synced_at: null, from, to, ms: Date.now()-t0,
        }), { headers: CORS });
      }

      // Flatten all records from all users
      const all: any[] = [];
      for (const doc of userDocs) {
        for (const r of (doc.records || [])) all.push(r);
      }

      // Aggregate: one row per unique callId, prefer record where user truly answered
      const aggregated = aggregateForReport(all, from, to, dir);

      const synced_at = userDocs.map((d:any) => d.synced_at).filter(Boolean).sort().reverse()[0] || null;

      return new Response(JSON.stringify({
        records:    aggregated,
        count:      aggregated.length,
        source:     "cache",
        synced_at,
        from, to,
        rawLegs:    all.length,
        users:      userDocs.length,
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

  // ── Sync ONE user (called by UI in a loop, with progress) ────────────────
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

      // Wipe stale cache for this user before writing fresh data
      await fsDelete(FB_PROJ, FB_KEY, "zoom_user_logs", userId);

      const result = await getUserCallLogs(token, userId, syncFrom, syncTo);
      const normalized = result.records.map((r: any) => normalizeRecord(r, owner));

      // Count truly-answered for reporting
      const trulyAnswered = normalized.filter(r => r.isUserAnswered).length;

      await fsSet(FB_PROJ, FB_KEY, "zoom_user_logs", userId, {
        userId, name: userName, email: userEmail, ext: userExt,
        records: normalized,
        count: normalized.length,
        trulyAnswered,
        synced_at: new Date().toISOString(),
        from: syncFrom, to: syncTo,
        truncated: result.truncated || false,
        sync_error: result.error || "",
      });

      return new Response(JSON.stringify({
        ok: !result.error, name: userName,
        records: normalized.length,
        trulyAnswered,
        truncated: result.truncated,
        error: result.error || null,
        ms: Date.now()-t0,
      }), { headers: CORS });
    } catch(e: any) {
      return new Response(JSON.stringify({ error: e?.message, name: userName }), { status: 500, headers: CORS });
    }
  }

  // ── Wipe all user caches ─────────────────────────────────────────────────
  if (action === "wipe-all") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const token = await getToken(ACCT, CID, CSEC);
    const users = await getPhoneUsers(token);
    for (const user of users) {
      await fsDelete(FB_PROJ, FB_KEY, "zoom_user_logs", user.id);
    }
    // Also wipe old collection from previous architecture
    const oldDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_sync_users");
    for (const doc of oldDocs) {
      if (doc.userId) await fsDelete(FB_PROJ, FB_KEY, "zoom_sync_users", doc.userId);
    }
    return new Response(JSON.stringify({ ok: true, wiped: users.length, oldWiped: oldDocs.length }), { headers: CORS });
  }

  // ── Debug: cache state per user ─────────────────────────────────────────
  if (action === "debug-cache") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_user_logs");
    return new Response(JSON.stringify({
      total: userDocs.length,
      summary: userDocs.map((d: any) => {
        const recs = (d.records || []) as any[];
        const truly = recs.filter(r => r.isUserAnswered).length;
        const elsewhere = recs.filter(r => r.result === "answered_elsewhere").length;
        const missed = recs.filter(r => r.result === "missed" || r.result === "cancelled" || r.result === "rejected").length;
        return {
          name: d.name, ext: d.ext,
          totalRecords: recs.length,
          trulyAnswered: truly,
          answeredByOther: elsewhere,
          missed,
          synced_at: d.synced_at,
          from: d.from, to: d.to,
          truncated: d.truncated || false,
          sync_error: d.sync_error || "",
        };
      }),
    }), { headers: CORS });
  }

  // ── Debug: raw Zoom response ─────────────────────────────────────────────
  if (action === "debug-raw") {
    const userId = u.searchParams.get("userId") || "UgG02EaxRauMiyuJ4Hixag";
    const dFrom  = u.searchParams.get("from") || new Date(Date.now()-2*86400000).toISOString().slice(0,10);
    const dTo    = u.searchParams.get("to")   || new Date().toISOString().slice(0,10);
    const token  = await getToken(ACCT, CID, CSEC);
    const url    = `${API}/phone/users/${userId}/call_logs?from=${dFrom}&to=${dTo}&type=all&page_size=10`;
    const res    = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
    const d: any = await res.json();
    return new Response(JSON.stringify({
      userId, from: dFrom, to: dTo, status: res.status,
      total: d.total_records,
      sample: (d.call_logs || []).slice(0, 3),
    }), { headers: CORS });
  }

  return new Response(JSON.stringify({ error: "Unknown action: " + action }), { status: 400, headers: CORS });
};

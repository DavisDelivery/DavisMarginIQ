import type { Context } from "@netlify/functions";

// ═══════════════════════════════════════════════════════════════════════════
// Zoom Phone Reporting — v2 (fixes math + missing employees)
// ═══════════════════════════════════════════════════════════════════════════

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
  const MAX_PAGES = 30;  // 9000 records max — must complete within Netlify timeout

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
  let isAnsweredCall: boolean;  // true = call WAS answered (by anyone)
  let isAttributedToUser: boolean;  // true = THIS user personally answered

  if (rawResult === "Call connected") {
    result = "answered";
    isAnsweredCall = true;
    isAttributedToUser = true;
  } else if (rawResult === "Answered by Other Member") {
    result = "answered";  // count as answered in totals
    isAnsweredCall = true;
    isAttributedToUser = false;  // not by THIS user though
  } else if (rawResult === "No Answer") {
    result = "missed";
    isAnsweredCall = false;
    isAttributedToUser = false;
  } else if (rawResult === "Call Cancel") {
    result = "missed";
    isAnsweredCall = false;
    isAttributedToUser = false;
  } else if (rawResult === "Rejected") {
    result = "missed";
    isAnsweredCall = false;
    isAttributedToUser = false;
  } else if (rawResult.toLowerCase().includes("voicemail")) {
    result = "voicemail";
    isAnsweredCall = false;
    isAttributedToUser = false;
  } else {
    result = "other";
    isAnsweredCall = false;
    isAttributedToUser = false;
  }

  return {
    callId:        raw.call_id || raw.id || "",
    date:          raw.date_time || "",
    answerTime:    raw.answer_start_time || "",
    endTime:       raw.call_end_time || "",
    direction:     String(raw.direction || "").toLowerCase(),
    answeredBy:    isAttributedToUser ? owner.name : "",   // empty if not this user's answer
    answeredEmail: owner.email,
    answeredExt:   owner.ext,
    cacheOwner:    owner.name,                              // user whose cache this came from
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

// ── Aggregate: dedup by callId, attributing to the actual answerer ──────────
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

  // Group records by callId
  const byCallId = new Map<string, any[]>();
  for (const r of inRange) {
    const key = r.callId;
    if (!key) continue;
    if (!byCallId.has(key)) byCallId.set(key, []);
    byCallId.get(key)!.push(r);
  }

  // For each call, build ONE final record with correct attribution
  const result: any[] = [];
  for (const [callId, recs] of byCallId.entries()) {
    // Find the user who personally answered (if anyone in cache did)
    const answerer = recs.find(r => r.isAttributedToUser);

    if (answerer) {
      // We know exactly who answered
      result.push({ ...answerer, answeredBy: answerer.cacheOwner });
    } else {
      // No one in our cache directly answered. Take any record and:
      // - if isAnsweredCall=true (someone did answer, just not in our cache),
      //   leave answeredBy blank but count as answered
      // - otherwise it's a missed/voicemail/etc
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

  if (action === "status") {
    return new Response(JSON.stringify({
      configured: !!(ACCT && CID && CSEC),
      missing: [!ACCT&&"ZOOM_ACCOUNT_ID",!CID&&"ZOOM_CLIENT_ID",!CSEC&&"ZOOM_CLIENT_SECRET"].filter(Boolean),
    }), { headers: CORS });
  }

  if (!ACCT || !CID || !CSEC) {
    return new Response(JSON.stringify({ error: "Missing Zoom credentials." }), { status: 500, headers: CORS });
  }

  if (action === "history") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({ error: "Firebase not configured", source: "error" }), { status: 500, headers: CORS });
    try {
      const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_user_logs");
      if (userDocs.length === 0) {
        return new Response(JSON.stringify({
          records: [], count: 0, source: "empty",
          message: "Cache is empty. Click Sync from Zoom to load your data.",
          synced_at: null, from, to, ms: Date.now()-t0,
        }), { headers: CORS });
      }
      const all: any[] = [];
      for (const doc of userDocs) for (const r of (doc.records || [])) all.push(r);
      const aggregated = aggregateForReport(all, from, to, dir);
      const synced_at = userDocs.map((d:any) => d.synced_at).filter(Boolean).sort().reverse()[0] || null;
      return new Response(JSON.stringify({
        records: aggregated, count: aggregated.length,
        source: "cache", synced_at, from, to,
        rawLegs: all.length, users: userDocs.length,
        ms: Date.now()-t0,
      }), { headers: CORS });
    } catch(e: any) {
      return new Response(JSON.stringify({ error: e?.message, source: "error" }), { status: 500, headers: CORS });
    }
  }

  if (action === "users") {
    try {
      const token = await getToken(ACCT, CID, CSEC);
      const users = await getPhoneUsers(token);
      return new Response(JSON.stringify({ users }), { headers: CORS });
    } catch(e: any) {
      return new Response(JSON.stringify({ error: e?.message }), { status: 500, headers: CORS });
    }
  }

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
      // Don't pre-wipe — keep old data if new sync fails
      const result = await getUserCallLogs(token, userId, syncFrom, syncTo);
      const normalized = result.records.map((r: any) => normalizeRecord(r, owner));
      const trulyAnswered = normalized.filter(r => r.isAttributedToUser).length;

      // Always write — even if zero records (so user appears in cache list)
      await fsSet(FB_PROJ, FB_KEY, "zoom_user_logs", userId, {
        userId, name: userName, email: userEmail, ext: userExt,
        records: normalized, count: normalized.length, trulyAnswered,
        synced_at: new Date().toISOString(),
        from: syncFrom, to: syncTo,
        truncated: result.truncated || false,
        sync_error: result.error || "",
      });

      return new Response(JSON.stringify({
        ok: !result.error, name: userName,
        records: normalized.length, trulyAnswered,
        truncated: result.truncated, error: result.error || null,
        ms: Date.now()-t0,
      }), { headers: CORS });
    } catch(e: any) {
      // Write error doc so we know this user failed
      try {
        await fsSet(FB_PROJ, FB_KEY, "zoom_user_logs", userId, {
          userId, name: userName, email: userEmail, ext: userExt,
          records: [], count: 0, trulyAnswered: 0,
          synced_at: new Date().toISOString(),
          from: syncFrom, to: syncTo,
          truncated: false,
          sync_error: String(e?.message || e),
        });
      } catch (_) {}
      return new Response(JSON.stringify({ error: e?.message, name: userName }), { status: 500, headers: CORS });
    }
  }

  if (action === "wipe-all") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const token = await getToken(ACCT, CID, CSEC);
    const users = await getPhoneUsers(token);
    for (const user of users) await fsDelete(FB_PROJ, FB_KEY, "zoom_user_logs", user.id);
    const oldDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_sync_users");
    for (const doc of oldDocs) if (doc.userId) await fsDelete(FB_PROJ, FB_KEY, "zoom_sync_users", doc.userId);
    return new Response(JSON.stringify({ ok: true, wiped: users.length, oldWiped: oldDocs.length }), { headers: CORS });
  }

  if (action === "debug-cache") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_user_logs");
    return new Response(JSON.stringify({
      total: userDocs.length,
      summary: userDocs.map((d: any) => {
        const recs = (d.records || []) as any[];
        const truly = recs.filter(r => r.isAttributedToUser).length;
        const elsewhere = recs.filter(r => r.rawResult === "Answered by Other Member").length;
        const missed = recs.filter(r => r.result === "missed").length;
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

  if (action === "debug-aggregated") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_user_logs");
    const all: any[] = [];
    for (const doc of userDocs) for (const r of (doc.records || [])) all.push(r);
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
      byResult: stats,
      byEmployee,
    }), { headers: CORS });
  }

  return new Response(JSON.stringify({ error: "Unknown action: " + action }), { status: 400, headers: CORS });
};

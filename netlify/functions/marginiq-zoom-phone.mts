import type { Context } from "@netlify/functions";

// Zoom Phone API — cache-first reads, per-user sync endpoint
//
// Actions:
//   status     — check credentials configured
//   history    — read from Firebase cache (instant)
//   users      — return list of phone users (for UI to iterate)
//   sync-user  — sync ONE user and write to cache
//
// IMPORTANT: dedup uses call_path_id (one per logical inbound call)
// not id (which is per-leg as the call routes through receptionist → queue → user)

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
  return users.map(u => {
    const fn = (u.first_name || "").trim();
    const ln = (u.last_name || "").trim();
    const fullName = [fn, ln].filter(Boolean).join(" ");
    const emailName = u.email ? String(u.email).split("@")[0].replace(/[._-]/g, " ").replace(/\b\w/g, c => c.toUpperCase()) : "";
    return {
      id:    u.id || "",
      name:  u.display_name || u.name || fullName || emailName || u.email || "Unknown",
      email: u.email || "",
      ext:   u.extension_number || "",
    };
  });
}

async function getUserCalls(token: string, userId: string, from: string, to: string): Promise<{records: any[], truncated: boolean, error?: string}> {
  const records: any[] = [];
  let npt = "", pages = 0;
  let truncated = false;
  const MAX_PAGES = 30;  // 3000 records max — keeps us under Netlify timeout
  const MAX_RETRIES_PER_PAGE = 3;

  while (true) {
    const url = `${API}/phone/users/${userId}/call_history?from=${from}&to=${to}&page_size=100${npt ? "&next_page_token=" + encodeURIComponent(npt) : ""}`;
    let res: Response | null = null;
    let lastErr = "";
    let success = false;

    // Retry on 429 with exponential backoff
    for (let attempt = 0; attempt < MAX_RETRIES_PER_PAGE; attempt++) {
      try {
        res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
        if (res.status === 429) {
          await sleep(2000 * (attempt + 1));
          continue;
        }
        success = true;
        break;
      } catch(e: any) {
        lastErr = e?.message || "fetch failed";
        await sleep(1000);
      }
    }

    if (!success || !res) return { records, truncated: true, error: lastErr || "max retries on 429" };

    const d: any = await res.json().catch(() => ({}));
    if (!res.ok) {
      if (res.status === 404 || res.status === 400) return { records, truncated: false };
      return { records, truncated: true, error: d.message || `HTTP ${res.status}` };
    }

    records.push(...(d.call_logs || d.call_history || d.records || []));
    npt = d.next_page_token || "";
    pages++;

    if (!npt) break;
    if (pages >= MAX_PAGES) { truncated = true; break; }
  }

  return { records, truncated };
}

function normalizeRecord(raw: any, user: { name: string; email: string; ext: string }) {
  const rStr = (raw.call_result || raw.result || "").toLowerCase().replace(/_/g, " ");
  const result =
    rStr.includes("answer") || rStr.includes("connect") ? "answered" :
    rStr.includes("voicemail")                           ? "voicemail" :
    rStr.includes("miss") || rStr.includes("no answer") || rStr.includes("abandon") ? "missed" :
    raw.call_result || "unknown";

  // Detect leg type: was this leg the auto-receptionist/queue, or a real user?
  const calleeType = raw.callee_ext_type || "";
  const isHumanLeg = calleeType === "user" || (!calleeType && user.name && user.name !== "Unknown");

  return {
    // Use call_path_id as the unique call identifier (same for every leg of one call)
    callPathId: raw.call_path_id || raw.id || "",
    legId:      raw.id || "",
    date:       raw.start_time || raw.date_time || "",
    direction:  (raw.direction || "").toLowerCase(),
    answeredBy: user.name,
    answeredEmail: user.email,
    answeredExt: user.ext || raw.callee_ext_number || "",
    callerNum:  raw.caller_did_number || raw.caller_number || "",
    callerName: raw.caller_name || "",
    calleeNum:  raw.callee_did_number || raw.callee_number || "",
    calleeType,            // "user" | "auto_receptionist" | "call_queue" | etc.
    isHumanLeg,            // true if this leg represents a real person
    result,
    talkTime:   Number(raw.duration ?? 0),
    waitTime:   0,
    viaQueue:   calleeType === "auto_receptionist" || calleeType === "call_queue",
    queueName:  (calleeType === "auto_receptionist" || calleeType === "call_queue") ? (raw.callee_name || "") : "",
    chain:      [],
    // Keep `id` field too for back-compat with UI (which may still reference it)
    id:         raw.call_path_id || raw.id || "",
  };
}

// ── Dedup logic — one record per logical call ───────────────────────────────
// When a call rings multiple phones simultaneously (queue), each ringer's user history
// gets a record with their name. But only ONE actually picked up. The signal of
// "actually talked" is talk_time > 0. We also collect the names of all ringers
// for the chain field.
//
// Scoring (highest wins):
//   100 = had real talk_time (this person actually conversed)
//    50 = is a human leg + result=answered (fallback if talk_time missing)
//    10 = is a human leg
//     1 = has a non-Unknown name
function dedupeByCall(records: any[]): any[] {
  const byPath = new Map<string, { winner: any; ringers: Set<string> }>();

  for (const r of records) {
    const key = r.callPathId || `${r.date}-${r.callerNum}`;
    let bucket = byPath.get(key);
    if (!bucket) {
      bucket = { winner: r, ringers: new Set() };
      byPath.set(key, bucket);
    }

    // Track everyone whose phone rang for this call
    if (r.isHumanLeg && r.answeredBy && r.answeredBy !== "Unknown") {
      bucket.ringers.add(r.answeredBy);
    }

    const score = (rec: any) => {
      const talkTime = Number(rec.talkTime) || 0;
      const isHuman = rec.isHumanLeg ? 1 : 0;
      const answered = rec.result === "answered" ? 1 : 0;
      const hasName = (rec.answeredBy && rec.answeredBy !== "Unknown") ? 1 : 0;

      // Strongest signal: this leg has actual talk time AND is a human leg
      if (talkTime > 0 && isHuman) return 1000 + talkTime;
      // Next: any human leg with talk_time
      if (talkTime > 0) return 500 + talkTime;
      // Next: human leg + answered
      if (isHuman && answered) return 50 + hasName;
      // Next: any human leg
      if (isHuman) return 10 + hasName;
      // Last resort
      return hasName;
    };

    if (score(r) > score(bucket.winner)) {
      bucket.winner = r;
    }
  }

  // Build final records, attaching the chain of who rang
  return Array.from(byPath.values()).map(({ winner, ringers }) => ({
    ...winner,
    chain: Array.from(ringers),
  }));
}

// ── Firebase helpers ─────────────────────────────────────────────────────────
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
  if (!res.ok) throw new Error(`Firestore PATCH ${res.status}`);
}
async function fsListDocs(proj: string, key: string, col: string): Promise<any[]> {
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}?key=${key}&pageSize=200`;
  const res = await fetch(url);
  if (!res.ok) return [];
  const d: any = await res.json();
  return (d.documents || []).map((doc: any) => fromFV(doc.fields || {}));
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
  const from   = u.searchParams.get("from") || new Date(Date.now()-30*86400000).toISOString().split("T")[0];
  const to     = u.searchParams.get("to")   || new Date().toISOString().split("T")[0];
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

  // ── Read from cache ───────────────────────────────────────────────────────
  if (action === "history") {
    if (FB_KEY && FB_PROJ) {
      try {
        const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_sync_users");
        if (userDocs.length > 0) {
          const fromMs = new Date(from + "T00:00:00Z").getTime();
          const toMs   = new Date(to   + "T23:59:59Z").getTime();
          const allRecs: any[] = [];
          for (const doc of userDocs) {
            for (const r of (doc.records || [])) {
              const ms = new Date(r.date).getTime();
              if (ms >= fromMs && ms <= toMs && (!dir || r.direction === dir)) allRecs.push(r);
            }
          }
          // Dedup by call_path_id, preferring human-answered legs
          const deduped = dedupeByCall(allRecs);
          deduped.sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime());
          const synced_at = userDocs.map(d=>d.synced_at).filter(Boolean).sort().reverse()[0] || null;
          console.log(`[zoom-phone] cache: ${allRecs.length} legs → ${deduped.length} unique calls`);
          return new Response(JSON.stringify({
            records: deduped, count: deduped.length,
            source: "cache", synced_at, from, to,
            rawLegs: allRecs.length,
            ms: Date.now()-t0,
          }), { headers: CORS });
        }
      } catch(e: any) { console.warn("[zoom-phone] cache read:", e?.message); }
    }
    return new Response(JSON.stringify({ records: [], count: 0, source: "warming", synced_at: null, from, to, ms: Date.now()-t0 }), { headers: CORS });
  }

  // ── Debug: pull RAW Zoom data for one user (bypasses normalize) ──────────
  if (action === "debug-zoomraw") {
    const userId = u.searchParams.get("userId") || "UgG02EaxRauMiyuJ4Hixag"; // Jessica
    const dFrom  = u.searchParams.get("from") || new Date(Date.now()-2*86400000).toISOString().split("T")[0];
    const dTo    = u.searchParams.get("to")   || new Date().toISOString().split("T")[0];
    const token  = await getToken(ACCT, CID, CSEC);
    const url    = `${API}/phone/users/${userId}/call_history?from=${dFrom}&to=${dTo}&page_size=5`;
    const res    = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
    const d: any = await res.json();
    if (!res.ok) return new Response(JSON.stringify({error: d.message, status: res.status}), { headers: CORS });
    const records = d.call_logs || d.call_history || d.records || [];
    return new Response(JSON.stringify({
      userId,
      from: dFrom, to: dTo,
      totalReturned: records.length,
      // Top-level fields
      topLevelKeys: records[0] ? Object.keys(records[0]) : [],
      // First 3 raw records UNTOUCHED
      rawSample: records.slice(0, 3),
    }), { headers: CORS });
  }

  // ── Debug: dump raw cached records for one user ─────────────────────────
  if (action === "debug-userraw") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const userName = u.searchParams.get("name") || "Brandi Bradberry";
    const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_sync_users");
    const doc = userDocs.find((d:any) => d.name === userName);
    if (!doc) return new Response(JSON.stringify({error:"User not found in cache",available:userDocs.map((d:any)=>d.name)}), { headers: CORS });
    const recs = (doc.records || []) as any[];
    // Stats
    const totalCount = recs.length;
    const withTalk = recs.filter(r => Number(r.talkTime) > 0);
    const zeroTalk = recs.filter(r => !Number(r.talkTime) || Number(r.talkTime) === 0);
    const byResult: Record<string, number> = {};
    recs.forEach(r => { byResult[r.result||"(none)"] = (byResult[r.result||"(none)"]||0) + 1; });
    const byCalleeType: Record<string, number> = {};
    recs.forEach(r => { byCalleeType[r.calleeType||"(none)"] = (byCalleeType[r.calleeType||"(none)"]||0) + 1; });
    return new Response(JSON.stringify({
      user: userName,
      totalCount,
      withTalkTime: withTalk.length,
      zeroTalkTime: zeroTalk.length,
      byResult,
      byCalleeType,
      // First 3 with talk_time > 0 (real calls)
      sampleAnswered: withTalk.slice(0, 3),
      // First 3 with talk_time = 0 (rang but didn't answer)
      sampleNotAnswered: zeroTalk.slice(0, 3),
      dateRange: {
        oldest: recs.length ? recs[recs.length-1].date : null,
        newest: recs.length ? recs[0].date : null,
      },
    }), { headers: CORS });
  }

  // ── Debug: pull raw record for a specific call_path_id ──────────────────
  // Shows EVERY leg of one call so we can see actual field values
  if (action === "debug-rawcall") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_sync_users");
    // Find a call_path_id that appears in BOTH Brandi and Jessica's caches
    const pathToLegs = new Map<string, any[]>();
    for (const doc of userDocs) {
      for (const r of (doc.records || [])) {
        if (!r.callPathId) continue;
        if (!pathToLegs.has(r.callPathId)) pathToLegs.set(r.callPathId, []);
        pathToLegs.get(r.callPathId)!.push({ ...r, _ownerCache: doc.name });
      }
    }
    // Find calls that appear in 3+ different user caches (queue rang multiple)
    const multi = Array.from(pathToLegs.entries())
      .filter(([_, legs]) => {
        const owners = new Set(legs.map(l => l._ownerCache));
        return owners.size >= 2;
      })
      .slice(0, 5);
    return new Response(JSON.stringify({
      sampleCallsWithMultipleRingers: multi.map(([path, legs]) => ({
        callPathId: path,
        legCount: legs.length,
        legs: legs.map(l => ({
          ownerCache: l._ownerCache,
          answeredBy: l.answeredBy,
          result: l.result,
          talkTime: l.talkTime,
          calleeType: l.calleeType,
          isHumanLeg: l.isHumanLeg,
          date: l.date,
        })),
      })),
    }), { headers: CORS });
  }

  // ── Debug: show employee distribution after dedup ───────────────────────
  if (action === "debug-employees") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_sync_users");
    const fromMs = new Date(from + "T00:00:00Z").getTime();
    const toMs   = new Date(to   + "T23:59:59Z").getTime();
    const allRecs: any[] = [];
    for (const doc of userDocs) {
      for (const r of (doc.records || [])) {
        const ms = new Date(r.date).getTime();
        if (ms >= fromMs && ms <= toMs) allRecs.push(r);
      }
    }
    // Count by answeredBy BEFORE dedup
    const beforeDedup: Record<string, number> = {};
    allRecs.forEach((r:any) => { beforeDedup[r.answeredBy||"(blank)"] = (beforeDedup[r.answeredBy||"(blank)"]||0) + 1; });
    // After dedup
    const deduped = dedupeByCall(allRecs);
    const afterDedup: Record<string, number> = {};
    deduped.forEach((r:any) => { afterDedup[r.answeredBy||"(blank)"] = (afterDedup[r.answeredBy||"(blank)"]||0) + 1; });
    return new Response(JSON.stringify({
      from, to,
      totalLegs: allRecs.length,
      uniqueCalls: deduped.length,
      beforeDedup,
      afterDedup,
      sampleJessica: allRecs.filter((r:any)=>r.answeredBy==="Jessica Sage").slice(0,3),
    }), { headers: CORS });
  }

  // ── Debug: show what's in Firebase cache for each user ──────────────────
  if (action === "debug-cache") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_sync_users");
    return new Response(JSON.stringify({
      total: userDocs.length,
      summary: userDocs.map((d:any) => ({
        name: d.name,
        email: d.email,
        ext: d.ext,
        recordCount: (d.records || []).length,
        synced_at: d.synced_at,
        from: d.from,
        to: d.to,
        sampleDates: (d.records || []).slice(0, 3).map((r:any) => r.date),
      })),
    }), { headers: CORS });
  }

  // ── Debug: return raw user list with all fields ──────────────────────────
  if (action === "debug-users") {
    const token = await getToken(ACCT, CID, CSEC);
    const users: any[] = [];
    let npt = "", pages = 0;
    do {
      const url = `${API}/phone/users?page_size=100${npt ? "&next_page_token=" + encodeURIComponent(npt) : ""}`;
      const res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
      const d: any = await res.json();
      if (!res.ok) return new Response(JSON.stringify({error: d.message, status: res.status}), { status: 500, headers: CORS });
      users.push(...(d.users || []));
      npt = d.next_page_token || "";
      if (++pages >= 5) break;
    } while (npt);
    return new Response(JSON.stringify({
      total: users.length,
      keys: users[0] ? Object.keys(users[0]) : [],
      users: users.map(u => ({
        id: u.id,
        email: u.email,
        first_name: u.first_name,
        last_name: u.last_name,
        display_name: u.display_name,
        name: u.name,
        status: u.status,
        extension_number: u.extension_number,
        phone_user_id: u.phone_user_id,
        type: u.type,
      })),
    }), { headers: CORS });
  }

  // ── Return user list ─────────────────────────────────────────────────────
  if (action === "users") {
    const token = await getToken(ACCT, CID, CSEC);
    const users = await getPhoneUsers(token);
    return new Response(JSON.stringify({ users }), { headers: CORS });
  }

  // ── Sync ONE user ────────────────────────────────────────────────────────
  if (action === "sync-user") {
    const userId   = u.searchParams.get("userId") || "";
    const userName = u.searchParams.get("name")   || "";
    const userEmail= u.searchParams.get("email")  || "";
    const userExt  = u.searchParams.get("ext")    || "";
    if (!userId) return new Response(JSON.stringify({ error: "userId required" }), { status: 400, headers: CORS });

    const syncFrom = new Date(Date.now()-35*86400000).toISOString().split("T")[0];
    const syncTo   = new Date().toISOString().split("T")[0];

    try {
      const token  = await getToken(ACCT, CID, CSEC);
      const result = await getUserCalls(token, userId, syncFrom, syncTo);
      const user   = { name: userName, email: userEmail, ext: userExt };
      const normalized = result.records.map(r => normalizeRecord(r, user));

      // Don't overwrite a populated cache with empty results unless we know it's intentional
      let action_taken = "wrote";
      if (FB_KEY && FB_PROJ) {
        if (normalized.length === 0 && result.error) {
          // Sync failed — keep existing cache, just report error
          action_taken = "failed-kept-cache";
        } else {
          await fsSet(FB_PROJ, FB_KEY, "zoom_sync_users", userId, {
            name: userName, email: userEmail, ext: userExt,
            records: normalized, count: normalized.length,
            synced_at: new Date().toISOString(), from: syncFrom, to: syncTo,
            truncated: result.truncated || false,
            sync_error: result.error || "",
          });
        }
      }
      return new Response(JSON.stringify({
        ok: !result.error, name: userName, count: normalized.length,
        truncated: result.truncated, error: result.error || null,
        action: action_taken, ms: Date.now()-t0,
      }), { headers: CORS });
    } catch(e: any) {
      return new Response(JSON.stringify({ error: String(e?.message||e), name: userName }), { status: 500, headers: CORS });
    }
  }

  return new Response(JSON.stringify({ error: "Unknown action" }), { status: 400, headers: CORS });
};

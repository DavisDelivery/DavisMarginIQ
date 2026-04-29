import type { Context } from "@netlify/functions";

// Zoom Phone API — clean rewrite based on actual Zoom response shape
//
// Key Zoom fields (verified from real API responses):
//   talk_time      — number, seconds. ONLY non-zero on the leg that actually answered.
//   answer_time    — timestamp. Empty string on rings that didn't connect.
//   wait_time      — seconds caller waited.
//   hold_time      — seconds put on hold.
//   result         — "answered", "no_answer", "ring_timeout", "voicemail", "missed"
//   event          — "ring_to_member" for queue-routed calls
//   callee_name    — the human's name on this leg
//   callee_ext_type— "user" | "call_queue" | "auto_receptionist"
//   call_path_id   — unique ID for the logical call (same across all legs)
//   operator_name  — the queue/receptionist name that routed the call
//   operator_ext_type — "call_queue" | "auto_receptionist"
//
// Actions:
//   status, history, users, sync-user, debug-zoomraw, debug-cache

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

async function getUserCalls(token: string, userId: string, from: string, to: string): Promise<{records: any[], truncated: boolean, error?: string, totalAvailable?: number}> {
  const records: any[] = [];
  let npt = "", pages = 0;
  let totalAvailable: number | undefined;
  const MAX_PAGES = 50;  // 5000 records max
  const MAX_RETRIES_PER_PAGE = 3;

  while (true) {
    const url = `${API}/phone/users/${userId}/call_history?from=${from}&to=${to}&page_size=100${npt ? "&next_page_token=" + encodeURIComponent(npt) : ""}`;
    let res: Response | null = null;
    let success = false;

    for (let attempt = 0; attempt < MAX_RETRIES_PER_PAGE; attempt++) {
      try {
        res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
        if (res.status === 429) { await sleep(2000 * (attempt + 1)); continue; }
        success = true;
        break;
      } catch(e: any) { await sleep(1000); }
    }

    if (!success || !res) return { records, truncated: true, error: "fetch failed", totalAvailable };

    const d: any = await res.json().catch(() => ({}));
    if (!res.ok) {
      if (res.status === 404 || res.status === 400) return { records, truncated: false, totalAvailable };
      return { records, truncated: true, error: d.message || `HTTP ${res.status}`, totalAvailable };
    }

    if (totalAvailable === undefined) totalAvailable = d.total_records;
    records.push(...(d.call_logs || []));
    npt = d.next_page_token || "";
    pages++;

    if (!npt) break;
    if (pages >= MAX_PAGES) return { records, truncated: true, totalAvailable };
  }

  return { records, truncated: false, totalAvailable };
}

// ── Normalize: read the ACTUAL fields Zoom returns ──────────────────────────
function normalizeRecord(raw: any, owner: { name: string; email: string; ext: string }) {
  const talkTime    = Number(raw.talk_time) || 0;
  const waitTime    = Number(raw.wait_time) || 0;
  const holdTime    = Number(raw.hold_time) || 0;
  const answered    = !!raw.answer_time && talkTime > 0;
  const calleeType  = raw.callee_ext_type || "";
  const operatorType= raw.operator_ext_type || "";

  // The result field from Zoom: "answered" | "no_answer" | "ring_timeout" | "voicemail" | "missed"
  const rawResult = String(raw.result || "").toLowerCase();
  let result: string;
  if (rawResult === "answered" && answered)     result = "answered";
  else if (rawResult === "voicemail")           result = "voicemail";
  else if (rawResult === "no_answer" || rawResult === "ring_timeout") result = "missed";
  else if (rawResult === "missed")              result = "missed";
  else if (answered)                             result = "answered";
  else                                            result = rawResult || "unknown";

  // Was this call routed via a queue or auto-receptionist?
  const viaQueue  = operatorType === "call_queue" || operatorType === "auto_receptionist"
                  || calleeType === "call_queue"  || calleeType === "auto_receptionist";
  const queueName = raw.operator_name || (calleeType !== "user" ? raw.callee_name : "") || "";

  // The actual person on this leg — could be the user (if callee_ext_type=user) or the queue/auto
  const isHumanLeg = calleeType === "user";
  const personName = isHumanLeg
    ? (raw.callee_name || owner.name)
    : (raw.callee_name || owner.name);  // fall back to owner if leg is opaque

  return {
    callPathId:    raw.call_path_id || raw.id || "",
    legId:         raw.id || "",
    callId:        raw.call_id || "",
    date:          raw.start_time || "",
    answerTime:    raw.answer_time || "",
    endTime:       raw.end_time || "",
    direction:     String(raw.direction || "").toLowerCase(),

    // Attribution
    answeredBy:    personName,
    answeredEmail: raw.callee_email || owner.email || "",
    answeredExt:   String(raw.callee_ext_number || owner.ext || ""),
    isHumanLeg,
    answered,                     // boolean: did this leg actually have a conversation?

    // Caller
    callerNum:     raw.caller_did_number || raw.caller_number || "",
    callerName:    raw.caller_name || "",

    // Routing context
    calleeType,                  // user | call_queue | auto_receptionist
    operatorType,                // queue/receptionist that routed it
    viaQueue,
    queueName,
    department:    raw.department || "",
    event:         raw.event || "",   // ring_to_member, etc.

    // Timing
    result,                       // answered | missed | voicemail
    rawResult,                    // original Zoom string
    talkTime,
    waitTime,
    holdTime,

    // Back-compat
    id:            raw.call_path_id || raw.id || "",
  };
}

// ── Dedup by call_path_id, picking the leg that actually answered ───────────
function dedupeByCall(records: any[]): any[] {
  const byPath = new Map<string, { winner: any; ringers: Set<string> }>();

  for (const r of records) {
    const key = r.callPathId || r.legId || `${r.date}-${r.callerNum}`;
    let bucket = byPath.get(key);
    if (!bucket) {
      bucket = { winner: r, ringers: new Set() };
      byPath.set(key, bucket);
    }

    // Track everyone whose phone rang for this call
    if (r.isHumanLeg && r.answeredBy) bucket.ringers.add(r.answeredBy);

    // Score each leg — answerer wins
    const score = (rec: any) => {
      let s = 0;
      if (rec.answered)              s += 10000;  // had answer_time AND talk_time>0 — definitive
      if (rec.talkTime > 0)          s += 5000 + rec.talkTime;  // at minimum spoke
      if (rec.result === "answered") s += 100;
      if (rec.isHumanLeg)            s += 50;
      if (rec.answeredBy && rec.answeredBy !== "Unknown") s += 1;
      return s;
    };

    if (score(r) > score(bucket.winner)) bucket.winner = r;
  }

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
          const deduped = dedupeByCall(allRecs);
          deduped.sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime());
          const synced_at = userDocs.map(d=>d.synced_at).filter(Boolean).sort().reverse()[0] || null;
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

  // ── Debug: pull RAW Zoom data for one user ───────────────────────────────
  if (action === "debug-zoomraw") {
    const userId = u.searchParams.get("userId") || "UgG02EaxRauMiyuJ4Hixag";
    const dFrom  = u.searchParams.get("from") || new Date(Date.now()-2*86400000).toISOString().split("T")[0];
    const dTo    = u.searchParams.get("to")   || new Date().toISOString().split("T")[0];
    const token  = await getToken(ACCT, CID, CSEC);
    const url    = `${API}/phone/users/${userId}/call_history?from=${dFrom}&to=${dTo}&page_size=5`;
    const res    = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
    const d: any = await res.json();
    if (!res.ok) return new Response(JSON.stringify({error: d.message, status: res.status}), { headers: CORS });
    const records = d.call_logs || [];
    return new Response(JSON.stringify({
      userId, from: dFrom, to: dTo,
      totalAvailable: d.total_records,
      returned: records.length,
      topLevelKeys: records[0] ? Object.keys(records[0]) : [],
      rawSample: records.slice(0, 3),
    }), { headers: CORS });
  }

  // ── Debug: cache state per user ─────────────────────────────────────────
  if (action === "debug-cache") {
    if (!FB_KEY || !FB_PROJ) return new Response(JSON.stringify({error:"Firebase not configured"}), { status: 500, headers: CORS });
    const userDocs = await fsListDocs(FB_PROJ, FB_KEY, "zoom_sync_users");
    return new Response(JSON.stringify({
      total: userDocs.length,
      summary: userDocs.map((d:any) => {
        const recs = (d.records || []) as any[];
        const answered = recs.filter(r => r.answered === true || r.talkTime > 0).length;
        return {
          name: d.name, ext: d.ext,
          totalRecords: recs.length,
          actuallyAnswered: answered,
          ringedButDidntAnswer: recs.length - answered,
          synced_at: d.synced_at,
          truncated: d.truncated || false,
          totalAvailable: d.totalAvailable,
        };
      }),
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

    const syncFrom = u.searchParams.get("from") || new Date(Date.now()-35*86400000).toISOString().split("T")[0];
    const syncTo   = u.searchParams.get("to")   || new Date().toISOString().split("T")[0];

    try {
      const token  = await getToken(ACCT, CID, CSEC);
      const result = await getUserCalls(token, userId, syncFrom, syncTo);
      const owner  = { name: userName, email: userEmail, ext: userExt };
      const normalized = result.records.map(r => normalizeRecord(r, owner));

      let action_taken = "wrote";
      if (FB_KEY && FB_PROJ) {
        if (normalized.length === 0 && result.error) {
          action_taken = "failed-kept-cache";
        } else {
          await fsSet(FB_PROJ, FB_KEY, "zoom_sync_users", userId, {
            name: userName, email: userEmail, ext: userExt,
            records: normalized, count: normalized.length,
            synced_at: new Date().toISOString(),
            from: syncFrom, to: syncTo,
            truncated: result.truncated || false,
            totalAvailable: result.totalAvailable || 0,
            sync_error: result.error || "",
          });
        }
      }
      return new Response(JSON.stringify({
        ok: !result.error, name: userName, count: normalized.length,
        truncated: result.truncated, totalAvailable: result.totalAvailable,
        error: result.error || null, action: action_taken, ms: Date.now()-t0,
      }), { headers: CORS });
    } catch(e: any) {
      return new Response(JSON.stringify({ error: String(e?.message||e), name: userName }), { status: 500, headers: CORS });
    }
  }

  return new Response(JSON.stringify({ error: "Unknown action" }), { status: 400, headers: CORS });
};

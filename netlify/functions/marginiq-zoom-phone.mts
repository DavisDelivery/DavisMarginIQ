import type { Context } from "@netlify/functions";

// Zoom Phone API — reads from Firebase cache written by marginiq-zoom-sync
// On cache miss: triggers background sync and returns empty with status
//
// Env vars: ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET
//           FIREBASE_API_KEY, FIREBASE_PROJECT_ID

const TOKEN_URL = "https://zoom.us/oauth/token";
const API       = "https://api.zoom.us/v2";
const SLEEP_MS  = 500;
const sleep     = (ms: number) => new Promise(r => setTimeout(r, ms));
const CORS      = { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" };

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
    name:  u.display_name || u.name || (u.first_name + " " + (u.last_name || "")).trim() || "Unknown",
    email: u.email || "",
    ext:   u.extension_number || "",
  }));
}

async function getUserCalls(token: string, userId: string, from: string, to: string): Promise<any[]> {
  const records: any[] = [];
  let npt = "", pages = 0;
  do {
    const url = `${API}/phone/users/${userId}/call_history?from=${from}&to=${to}&page_size=100${npt ? "&next_page_token=" + encodeURIComponent(npt) : ""}`;
    const res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
    const d: any = await res.json();
    if (!res.ok) {
      if (res.status === 404 || res.status === 400) return [];
      if (res.status === 429) { await sleep(2000); continue; }
      throw new Error(d.message || `${res.status}`);
    }
    records.push(...(d.call_logs || d.call_history || d.records || []));
    npt = d.next_page_token || "";
    if (++pages >= 20) break;
  } while (npt);
  return records;
}

function normalizeRecord(raw: any, user: { name: string; email: string; ext: string }) {
  const rStr = (raw.call_result || raw.result || "").toLowerCase().replace(/_/g, " ");
  const result =
    rStr.includes("answer") || rStr.includes("connect") ? "answered" :
    rStr.includes("voicemail")                           ? "voicemail" :
    rStr.includes("miss") || rStr.includes("no answer") || rStr.includes("abandon") ? "missed" :
    raw.call_result || "unknown";
  return {
    id: raw.id || raw.call_path_id || "", date: raw.start_time || raw.date_time || "",
    direction: (raw.direction || "").toLowerCase(),
    answeredBy: user.name, answeredEmail: user.email,
    answeredExt: user.ext || raw.callee_ext_number || "",
    callerNum: raw.caller_did_number || raw.caller_number || "",
    callerName: raw.caller_name || "",
    calleeNum: raw.callee_did_number || raw.callee_number || "",
    result, talkTime: Number(raw.duration ?? 0), waitTime: 0,
    viaQueue: raw.callee_ext_type === "auto_receptionist" || raw.callee_ext_type === "call_queue",
    queueName: (raw.callee_ext_type === "auto_receptionist" || raw.callee_ext_type === "call_queue") ? (raw.callee_name || "") : "",
    chain: [],
  };
}

// ── Firebase helpers ─────────────────────────────────────────────────────────
function toFV(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean")        return { booleanValue: v };
  if (typeof v === "number")         return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (typeof v === "string")         return { stringValue: v };
  if (Array.isArray(v))              return { arrayValue: { values: v.map(toFV) } };
  if (typeof v === "object")         return { mapValue: { fields: Object.fromEntries(Object.entries(v).map(([k, val]) => [k, toFV(val)])) } };
  return { stringValue: String(v) };
}
function fromFVVal(v: any): any {
  if (v.nullValue !== undefined)    return null;
  if (v.booleanValue !== undefined) return v.booleanValue;
  if (v.integerValue !== undefined) return Number(v.integerValue);
  if (v.doubleValue !== undefined)  return v.doubleValue;
  if (v.stringValue !== undefined)  return v.stringValue;
  if (v.arrayValue)                 return (v.arrayValue.values || []).map(fromFVVal);
  if (v.mapValue)                   return Object.fromEntries(Object.entries(v.mapValue.fields || {}).map(([k,val]) => [k, fromFVVal(val)]));
  return null;
}
function fromFV(fields: any): any {
  return Object.fromEntries(Object.entries(fields || {}).map(([k, v]) => [k, fromFVVal(v)]));
}

async function fsSet(proj: string, key: string, col: string, doc: string, data: any) {
  const fields = Object.fromEntries(Object.entries(data).map(([k, v]) => [k, toFV(v)]));
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}/${encodeURIComponent(doc)}?key=${key}`;
  const res = await fetch(url, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fields }) });
  if (!res.ok) { const e = await res.text().catch(()=>""); throw new Error(`Firestore ${res.status}: ${e.slice(0,100)}`); }
}

async function fsGetDoc(proj: string, key: string, col: string, doc: string): Promise<any | null> {
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}/${encodeURIComponent(doc)}?key=${key}`;
  const res = await fetch(url);
  if (res.status === 404) return null;
  if (!res.ok) return null;
  const d: any = await res.json();
  return d.fields ? fromFV(d.fields) : null;
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

  // ── Read from Firebase cache ─────────────────────────────────────────────
  if (action === "history" && FB_KEY && FB_PROJ) {
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
        const seen = new Set<string>();
        const deduped = allRecs.filter(r => { const k=r.id||`${r.date}-${r.callerNum}-${r.answeredBy}`; if(seen.has(k)) return false; seen.add(k); return true; });
        deduped.sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime());
        const synced_at = userDocs.map(d => d.synced_at).filter(Boolean).sort().reverse()[0] || null;
        console.log(`[zoom-phone] cache: ${deduped.length} records, ${userDocs.length} users`);
        return new Response(JSON.stringify({ records: deduped, count: deduped.length, source: "cache", synced_at, from, to, ms: Date.now()-t0 }), { headers: CORS });
      }
    } catch(e: any) {
      console.warn("[zoom-phone] cache read error:", e?.message);
    }
    // Cache empty — return empty with warming flag so UI shows correct message
    return new Response(JSON.stringify({ records: [], count: 0, source: "warming", synced_at: null, from, to, ms: Date.now()-t0 }), { headers: CORS });
  }

  // ── Manual sync trigger (?action=sync) ───────────────────────────────────
  // Called by "Sync Now" button — fetches all users sequentially and writes to cache
  if (action === "sync" || action === "refresh") {
    try {
      const token = await getToken(ACCT, CID, CSEC);
      const users = await getPhoneUsers(token);
      // Sync last 35 days
      const syncFrom = new Date(Date.now()-35*86400000).toISOString().split("T")[0];
      const syncTo   = new Date().toISOString().split("T")[0];
      let total = 0;
      for (let i = 0; i < users.length; i++) {
        if (i > 0) await sleep(SLEEP_MS);
        try {
          const recs = await getUserCalls(token, users[i].id, syncFrom, syncTo);
          const normalized = recs.map(r => normalizeRecord(r, users[i]));
          if (FB_KEY && FB_PROJ) {
            await fsSet(FB_PROJ, FB_KEY, "zoom_sync_users", users[i].id, {
              name: users[i].name, email: users[i].email, ext: users[i].ext,
              records: normalized, count: normalized.length,
              synced_at: new Date().toISOString(), from: syncFrom, to: syncTo,
            });
          }
          total += normalized.length;
          console.log(`[zoom-sync] ${users[i].name}: ${normalized.length} records`);
        } catch(e: any) { console.error(`[zoom-sync] ${users[i].name}:`, e?.message); }
      }
      return new Response(JSON.stringify({ ok: true, users: users.length, records: total, ms: Date.now()-t0 }), { headers: CORS });
    } catch(e: any) {
      return new Response(JSON.stringify({ error: String(e?.message||e) }), { status: 500, headers: CORS });
    }
  }

  return new Response(JSON.stringify({ error: "Unknown action" }), { status: 400, headers: CORS });
};

import type { Context } from "@netlify/functions";

// Zoom Phone proxy — fetches per-USER call history so callee_name = actual employee
// Account-level endpoint only shows the IVR/queue leg, not who answered.
// This version: 1) lists all phone users, 2) fetches each user's call history,
// 3) merges by call_id keeping the user-leg record which has the real name.
//
// Env vars: ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET

const TOKEN_URL = "https://zoom.us/oauth/token";
const API       = "https://api.zoom.us/v2";
const MAX_PAGES = 10; // per user

async function getToken(acct: string, cid: string, csec: string): Promise<string> {
  const res = await fetch(
    `${TOKEN_URL}?grant_type=account_credentials&account_id=${encodeURIComponent(acct)}`,
    { method:"POST", headers:{"Authorization":"Basic "+btoa(`${cid}:${csec}`),"Content-Type":"application/x-www-form-urlencoded"} }
  );
  const d: any = await res.json();
  if (!res.ok) throw new Error(d.reason||d.message||`Auth failed (${res.status})`);
  return d.access_token;
}

async function paginate(token: string, url: string, listKey: string): Promise<any[]> {
  const out: any[] = [];
  let npt = "", pages = 0;
  do {
    const u = npt ? `${url}&next_page_token=${encodeURIComponent(npt)}` : url;
    const res = await fetch(u, { headers:{"Authorization":`Bearer ${token}`} });
    const d: any = await res.json();
    if (!res.ok) { const e:any=new Error(d.message||`API ${res.status}`); e.status=res.status; throw e; }
    const items = d[listKey] || d.call_logs || d.call_history || d.records || [];
    out.push(...items);
    npt = d.next_page_token || "";
    if (++pages >= MAX_PAGES) break;
  } while (npt);
  return out;
}

// Get all Zoom Phone users
async function getPhoneUsers(token: string): Promise<{id:string;name:string;email:string;ext:string}[]> {
  const users: any[] = [];
  let npt = "", pages = 0;
  do {
    const url = `${API}/phone/users?page_size=100${npt?`&next_page_token=${encodeURIComponent(npt)}`:""}`;
    const res = await fetch(url, { headers:{"Authorization":`Bearer ${token}`} });
    const d: any = await res.json();
    if (!res.ok) { console.warn("[zoom-phone] /phone/users =>", res.status, d.message); break; }
    users.push(...(d.users||[]));
    npt = d.next_page_token || "";
    if (++pages >= 5) break;
  } while (npt);
  return users.map(u => ({
    id:    u.id,
    name:  u.display_name || u.name || u.first_name+" "+u.last_name || "Unknown",
    email: u.email || "",
    ext:   u.extension_number || u.ext_number || "",
  }));
}

// Get call history for one user
async function getUserCalls(token: string, userId: string, from: string, to: string, dir: string): Promise<any[]> {
  const d = dir ? `&type=${dir}` : "";
  try {
    return await paginate(token, `${API}/phone/users/${userId}/call_history?from=${from}&to=${to}&page_size=100${d}`, "call_logs");
  } catch(e:any) {
    if (e.status === 403 || e.status === 404) {
      // Try old call_logs endpoint for this user
      try {
        return await paginate(token, `${API}/phone/users/${userId}/call_logs?from=${from}&to=${to}&type=all&page_size=100${d}`, "call_logs");
      } catch { return []; }
    }
    return [];
  }
}

function normalizeResult(r: string): string {
  const s = (r||"").toLowerCase().replace(/_/g," ");
  if (s.includes("connect")||s==="answered") return "answered";
  if (s.includes("voicemail"))               return "voicemail";
  if (s.includes("miss")||s.includes("no answer")||s.includes("abandon")) return "missed";
  return r||"unknown";
}

function normalizeUserRecord(raw: any, user: {id:string;name:string;email:string;ext:string}): Record<string,any> {
  const direction = (raw.direction||"").toLowerCase();
  // For per-user records: the user IS the employee
  // caller_name = external caller name for inbound
  return {
    id:           raw.id||raw.call_id||"",
    call_path_id: raw.call_path_id||raw.id||"",
    date:         raw.start_time||raw.date_time||"",
    direction,
    answeredBy:   user.name,
    answeredEmail:user.email,
    answeredExt:  user.ext || raw.callee_ext_number || raw.caller_ext_number || "",
    callerNum:    raw.caller_did_number||raw.caller_number||"",
    callerName:   raw.caller_name||"",
    calleeNum:    raw.callee_did_number||raw.callee_number||"",
    result:       normalizeResult(raw.call_result||raw.result||""),
    talkTime:     Number(raw.duration||0),
    waitTime:     0,
    viaQueue:     raw.callee_ext_type==="auto_receptionist"||raw.callee_ext_type==="call_queue",
    queueName:    raw.callee_ext_type==="auto_receptionist"||raw.callee_ext_type==="call_queue" ? (raw.callee_name||"") : "",
    chain:        [],
  };
}

export default async (req: Request, _ctx: Context) => {
  const CORS = {"Content-Type":"application/json","Access-Control-Allow-Origin":"*"};
  const ACCT = process.env["ZOOM_ACCOUNT_ID"];
  const CID  = process.env["ZOOM_CLIENT_ID"];
  const CSEC = process.env["ZOOM_CLIENT_SECRET"];
  const u    = new URL(req.url);
  const action = u.searchParams.get("action")||"history";

  if (action==="status") {
    return new Response(JSON.stringify({
      configured: !!(ACCT&&CID&&CSEC),
      missing: [!ACCT&&"ZOOM_ACCOUNT_ID",!CID&&"ZOOM_CLIENT_ID",!CSEC&&"ZOOM_CLIENT_SECRET"].filter(Boolean),
    }), {headers:CORS});
  }

  if (!ACCT||!CID||!CSEC) return new Response(JSON.stringify({error:"Missing env vars"}),{status:500,headers:CORS});

  const from = u.searchParams.get("from")||new Date(Date.now()-7*86400000).toISOString().split("T")[0];
  const to   = u.searchParams.get("to")  ||new Date().toISOString().split("T")[0];
  const dir  = u.searchParams.get("direction")||"";
  const t0   = Date.now();

  try {
    const token = await getToken(ACCT, CID, CSEC);

    // 1. Get all phone users
    const users = await getPhoneUsers(token);
    console.log(`[zoom-phone] ${users.length} phone users: ${users.map(u=>u.name).join(", ")}`);

    if (users.length === 0) {
      // Fallback: account-level with callee_name as best guess
      const res = await fetch(`${API}/phone/call_history?from=${from}&to=${to}&page_size=100${dir?`&type=${dir}`:""}`,
        { headers:{"Authorization":`Bearer ${token}`} });
      const d: any = await res.json();
      const recs = (d.call_logs||d.call_history||[]).map((r:any) => ({
        id: r.id||"", date: r.start_time||"", direction:(r.direction||"").toLowerCase(),
        answeredBy: r.callee_name||r.caller_name||"Unknown",
        answeredEmail:"", answeredExt: r.callee_ext_number||"",
        callerNum: r.caller_did_number||"", callerName: r.caller_name||"",
        calleeNum: r.callee_did_number||"",
        result: normalizeResult(r.call_result||""),
        talkTime: Number(r.duration||0), waitTime:0, viaQueue:false, queueName:"", chain:[],
      }));
      return new Response(JSON.stringify({records:recs,count:recs.length,endpoint:"account_fallback",from,to,ms:Date.now()-t0}),{headers:CORS});
    }

    // 2. Fetch each user's calls in parallel (cap at 20 users to avoid timeout)
    const usersToFetch = users.slice(0, 20);
    const perUserRecs = await Promise.all(
      usersToFetch.map(async user => {
        const recs = await getUserCalls(token, user.id, from, to, dir);
        return recs.map(r => normalizeUserRecord(r, user));
      })
    );

    // 3. Merge — deduplicate by call_path_id, keeping user-leg record
    // Multiple users can appear in the same call (transfer). Keep all legs.
    const all = perUserRecs.flat();

    // Deduplicate: if same call_path_id appears for multiple users, keep distinct answeredBy
    const seen = new Map<string, Record<string,any>>();
    for (const r of all) {
      const key = `${r.call_path_id}_${r.answeredBy}`;
      if (!seen.has(key)) seen.set(key, r);
    }
    const records = [...seen.values()].sort((a,b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    const employees = [...new Set(records.map(r=>r.answeredBy))];
    console.log(`[zoom-phone] per-user fetch | ${records.length} records | employees: ${employees.join(", ")}`);

    return new Response(JSON.stringify({
      records, count:records.length, endpoint:"per_user_history",
      users: users.length, employees, from, to, ms:Date.now()-t0
    }), {headers:CORS});

  } catch(e:any) {
    console.error("[zoom-phone]", e?.message);
    return new Response(JSON.stringify({error:String(e?.message||e)}),{status:500,headers:CORS});
  }
};

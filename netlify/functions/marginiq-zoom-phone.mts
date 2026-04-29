import type { Context } from "@netlify/functions";

// Zoom Phone proxy for MarginIQ
// Tries /phone/call_history first, falls back to /phone/call_logs on 403/404.
// Both endpoints normalize to the same output including answeredBy per call.
//
// Env vars: ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET
// GET ?action=status
// GET ?action=history&from=YYYY-MM-DD&to=YYYY-MM-DD&direction=inbound|outbound

const TOKEN_URL = "https://zoom.us/oauth/token";
const API       = "https://api.zoom.us/v2";
const MAX_PAGES = 20;

async function getToken(acct: string, cid: string, csec: string): Promise<string> {
  const res = await fetch(
    `${TOKEN_URL}?grant_type=account_credentials&account_id=${encodeURIComponent(acct)}`,
    { method: "POST", headers: { "Authorization": "Basic " + btoa(`${cid}:${csec}`), "Content-Type": "application/x-www-form-urlencoded" } }
  );
  const d: any = await res.json();
  if (!res.ok) throw new Error(d.reason || d.message || `Auth failed (${res.status})`);
  return d.access_token;
}

async function paginate(token: string, baseUrl: string): Promise<any[]> {
  const out: any[] = [];
  let npt = "", pages = 0;
  do {
    const url = npt ? `${baseUrl}&next_page_token=${encodeURIComponent(npt)}` : baseUrl;
    const res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
    const d: any = await res.json();
    if (!res.ok) { const e: any = new Error(d.message || `API ${res.status}`); e.status = res.status; throw e; }
    out.push(...(d.call_logs || d.call_history || d.records || []));
    npt = d.next_page_token || "";
    if (++pages >= MAX_PAGES) break;
  } while (npt);
  return out;
}

async function fetchCalls(token: string, from: string, to: string, dir: string) {
  const d = dir ? `&type=${dir}` : "";
  // Try new call_history endpoint
  try {
    const recs = await paginate(token, `${API}/phone/call_history?from=${from}&to=${to}&page_size=100${d}`);
    return { recs, endpoint: "call_history" };
  } catch (e: any) {
    if (e.status !== 403 && e.status !== 404) throw e;
    console.log("[zoom-phone] call_history =>", e.status, "falling back to call_logs");
  }
  // Fall back to old call_logs endpoint
  const recs = await paginate(token, `${API}/phone/call_logs?from=${from}&to=${to}&type=all&page_size=100${d}`);
  return { recs, endpoint: "call_logs" };
}

function normalize(raw: any, endpoint: string) {
  const direction = (raw.direction || "").toLowerCase();
  let answeredBy = "", answeredEmail = "", answeredExt = "";
  let callerNum = "", callerName = "", calleeNum = "";
  let talkTime = 0, waitTime = 0;
  let viaQueue = false, queueName = "", chain: string[] = [];

  if (endpoint === "call_history" && (raw.call_elements?.length)) {
    // New API — parse call_elements for per-leg attribution
    const elems: any[] = raw.call_elements;
    const top = elems[0] || {};
    const leg =
      elems.find(e => e.event === "ring_to_member" && e.callee_ext_type === "user" && e.result === "answered") ||
      elems.find(e => e.callee_ext_type === "user");
    answeredBy    = leg?.callee_name  || raw.callee?.name || raw.owner?.name || raw.user_name || "Unknown";
    answeredEmail = leg?.callee_email || raw.callee?.email || raw.owner?.email || "";
    answeredExt   = leg?.callee_ext_number || raw.callee?.extension_number || "";
    viaQueue      = elems.some(e => ["call_queue","auto_receptionist"].includes(e.callee_ext_type));
    queueName     = elems.find(e => ["call_queue","auto_receptionist"].includes(e.callee_ext_type))?.callee_name || "";
    chain         = [...new Set(elems.filter(e => e.callee_ext_type === "user" && e.callee_name).map(e => e.callee_name as string))];
    const active  = leg || top;
    talkTime      = Number(active.talk_time ?? raw.duration ?? 0);
    waitTime      = Number(active.wait_time ?? 0);
    callerNum     = top.caller_did_number || raw.caller?.phone_number || "";
    callerName    = top.caller_name || raw.caller?.name || "";
    calleeNum     = top.callee_did_number || raw.callee?.phone_number || "";
  } else {
    // Old call_logs API — flat record, one per user per call
    // Each record belongs to the user whose log it is.
    // For inbound: callee = employee who answered
    // For outbound: caller = employee who dialed
    if (direction === "inbound") {
      answeredBy    = raw.callee?.name || raw.callee?.display_name || raw.user_name || raw.owner?.name || raw.owner_name || "Unknown";
      answeredEmail = raw.callee?.email || raw.owner?.email || "";
      answeredExt   = raw.callee?.extension_number || raw.owner?.extension_number || "";
    } else {
      answeredBy    = raw.caller?.name || raw.caller?.display_name || raw.user_name || raw.owner?.name || raw.owner_name || "Unknown";
      answeredEmail = raw.caller?.email || raw.owner?.email || "";
      answeredExt   = raw.caller?.extension_number || raw.owner?.extension_number || "";
    }
    talkTime  = Number(raw.duration ?? raw.talk_time ?? 0);
    callerNum  = raw.caller?.phone_number  || raw.caller?.did_number  || raw.caller_number  || "";
    callerName = raw.caller?.name          || raw.caller_name         || "";
    calleeNum  = raw.callee?.phone_number  || raw.callee?.did_number  || raw.callee_number  || "";
  }

  // Result normalization
  const rStr = (raw.result || raw.call_result || "").toLowerCase().replace(/_/g, " ");
  const result =
    rStr.includes("connect") || rStr === "answered" ? "answered" :
    rStr.includes("voicemail")                       ? "voicemail" :
    rStr.includes("miss") || rStr.includes("no answer") || rStr.includes("abandon") ? "missed" :
    raw.result || "unknown";

  return {
    id:           raw.id || raw.call_id || raw.call_history_uuid || "",
    date:         raw.date_time || raw.start_time || "",
    direction,
    answeredBy,
    answeredEmail,
    answeredExt,
    callerNum,
    callerName,
    calleeNum,
    result,
    talkTime,
    waitTime,
    viaQueue,
    queueName,
    chain,
  };
}

export default async (req: Request, _ctx: Context) => {
  const CORS = { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" };
  const ACCT = process.env["ZOOM_ACCOUNT_ID"];
  const CID  = process.env["ZOOM_CLIENT_ID"];
  const CSEC = process.env["ZOOM_CLIENT_SECRET"];

  const u      = new URL(req.url);
  const action = u.searchParams.get("action") || "history";

  if (action === "status") {
    return new Response(JSON.stringify({
      configured: !!(ACCT && CID && CSEC),
      missing: [!ACCT && "ZOOM_ACCOUNT_ID", !CID && "ZOOM_CLIENT_ID", !CSEC && "ZOOM_CLIENT_SECRET"].filter(Boolean),
    }), { headers: CORS });
  }

  if (!ACCT || !CID || !CSEC) {
    return new Response(JSON.stringify({ error: "Missing Zoom env vars." }), { status: 500, headers: CORS });
  }

  const from = u.searchParams.get("from") || new Date(Date.now()-7*86400000).toISOString().split("T")[0];
  const to   = u.searchParams.get("to")   || new Date().toISOString().split("T")[0];
  const dir  = u.searchParams.get("direction") || "";
  const t0   = Date.now();

  try {
    const token              = await getToken(ACCT, CID, CSEC);
    const { recs, endpoint } = await fetchCalls(token, from, to, dir);
    const records            = recs.map(r => normalize(r, endpoint));

    // Log employee spread to Netlify function logs for debugging
    const employees = [...new Set(records.map(r => r.answeredBy))];
    console.log(`[zoom-phone] ${endpoint} | ${records.length} records | employees: ${employees.slice(0,10).join(", ")}`);

    return new Response(JSON.stringify({ records, count: records.length, endpoint, from, to, ms: Date.now()-t0 }), { headers: CORS });
  } catch (e: any) {
    console.error("[zoom-phone]", e?.message);
    return new Response(JSON.stringify({ error: String(e?.message || e) }), { status: 500, headers: CORS });
  }
};

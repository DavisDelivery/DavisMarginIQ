import type { Context } from "@netlify/functions";

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
  try {
    const recs = await paginate(token, `${API}/phone/call_history?from=${from}&to=${to}&page_size=100${d}`);
    return { recs, endpoint: "call_history" };
  } catch (e: any) {
    if (e.status !== 403 && e.status !== 404) throw e;
    console.log("[zoom-phone] call_history =>", e.status, "falling back to call_logs");
  }
  const recs = await paginate(token, `${API}/phone/call_logs?from=${from}&to=${to}&type=all&page_size=100${d}`);
  return { recs, endpoint: "call_logs" };
}

function extractName(raw: any): string {
  // Walk every possible field path Zoom might use for the answering employee
  const direction = (raw.direction || "").toLowerCase();
  const candidates: (string|undefined)[] = [];

  if (direction === "inbound") {
    // Inbound: callee = person who answered
    candidates.push(
      raw.callee?.name, raw.callee?.display_name, raw.callee?.user_name,
      raw.callee?.user?.name, raw.callee?.user?.display_name,
      raw.answerer?.name, raw.answerer?.display_name,
      raw.user?.name, raw.user?.display_name, raw.user_name, raw.user_display_name,
      raw.owner?.name, raw.owner?.display_name, raw.owner_name,
      raw.caller?.name  // last resort
    );
  } else {
    // Outbound: caller = employee
    candidates.push(
      raw.caller?.name, raw.caller?.display_name, raw.caller?.user_name,
      raw.caller?.user?.name,
      raw.user?.name, raw.user?.display_name, raw.user_name, raw.user_display_name,
      raw.owner?.name, raw.owner?.display_name, raw.owner_name,
      raw.callee?.name
    );
  }

  return candidates.find(c => c && c.trim() !== "") || "Unknown";
}

function normalize(raw: any, endpoint: string) {
  const direction = (raw.direction || "").toLowerCase();
  let answeredBy = "", answeredEmail = "", answeredExt = "";
  let callerNum = "", callerName = "", calleeNum = "";
  let talkTime = 0, waitTime = 0;
  let viaQueue = false, queueName = "", chain: string[] = [];

  if (endpoint === "call_history" && raw.call_elements?.length) {
    const elems: any[] = raw.call_elements;
    const top = elems[0] || {};
    const leg =
      elems.find(e => e.event === "ring_to_member" && e.callee_ext_type === "user" && e.result === "answered") ||
      elems.find(e => e.callee_ext_type === "user");
    answeredBy    = leg?.callee_name || leg?.callee_display_name || extractName(raw);
    answeredEmail = leg?.callee_email || raw.callee?.email || "";
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
    answeredBy    = extractName(raw);
    answeredEmail = (direction === "inbound" ? raw.callee?.email : raw.caller?.email) || raw.user?.email || raw.owner?.email || "";
    answeredExt   = (direction === "inbound" ? raw.callee?.extension_number : raw.caller?.extension_number) || raw.user?.extension_number || "";
    talkTime      = Number(raw.duration ?? raw.talk_time ?? 0);
    callerNum     = raw.caller?.phone_number || raw.caller?.did_number || raw.caller_number || "";
    callerName    = raw.caller?.name || raw.caller_name || "";
    calleeNum     = raw.callee?.phone_number || raw.callee?.did_number || raw.callee_number || "";
  }

  const rStr = (raw.result || raw.call_result || "").toLowerCase().replace(/_/g, " ");
  const result =
    rStr.includes("connect") || rStr === "answered" ? "answered" :
    rStr.includes("voicemail")                       ? "voicemail" :
    rStr.includes("miss") || rStr.includes("no answer") || rStr.includes("abandon") ? "missed" :
    raw.result || "unknown";

  return { id: raw.id||raw.call_id||"", date: raw.date_time||raw.start_time||"", direction,
    answeredBy, answeredEmail, answeredExt, callerNum, callerName, calleeNum,
    result, talkTime, waitTime, viaQueue, queueName, chain };
}

export default async (req: Request, _ctx: Context) => {
  const CORS = { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" };
  const ACCT = process.env["ZOOM_ACCOUNT_ID"];
  const CID  = process.env["ZOOM_CLIENT_ID"];
  const CSEC = process.env["ZOOM_CLIENT_SECRET"];
  const u    = new URL(req.url);
  const action = u.searchParams.get("action") || "history";

  if (action === "status") {
    return new Response(JSON.stringify({
      configured: !!(ACCT && CID && CSEC),
      missing: [!ACCT&&"ZOOM_ACCOUNT_ID",!CID&&"ZOOM_CLIENT_ID",!CSEC&&"ZOOM_CLIENT_SECRET"].filter(Boolean),
    }), { headers: CORS });
  }

  // ── debug: return 3 raw records so we can see exact field structure ────────
  if (action === "debug") {
    if (!ACCT || !CID || !CSEC) return new Response(JSON.stringify({error:"Missing env vars"}),{status:500,headers:CORS});
    const from = u.searchParams.get("from") || new Date(Date.now()-2*86400000).toISOString().split("T")[0];
    const to   = u.searchParams.get("to")   || new Date().toISOString().split("T")[0];
    try {
      const token = await getToken(ACCT, CID, CSEC);
      const { recs, endpoint } = await fetchCalls(token, from, to, "");
      // Return first 3 raw records + keys of first record
      const sample = recs.slice(0, 3);
      const keys   = sample[0] ? Object.keys(sample[0]) : [];
      const callerKeys  = sample[0]?.caller  ? Object.keys(sample[0].caller)  : [];
      const calleeKeys  = sample[0]?.callee  ? Object.keys(sample[0].callee)  : [];
      const ownerKeys   = sample[0]?.owner   ? Object.keys(sample[0].owner)   : [];
      const userKeys    = sample[0]?.user    ? Object.keys(sample[0].user)    : [];
      return new Response(JSON.stringify({ endpoint, total: recs.length, topLevelKeys: keys,
        callerKeys, calleeKeys, ownerKeys, userKeys, sample }), { headers: CORS });
    } catch(e:any) {
      return new Response(JSON.stringify({error:String(e.message)}),{status:500,headers:CORS});
    }
  }

  if (!ACCT || !CID || !CSEC) return new Response(JSON.stringify({error:"Missing env vars"}),{status:500,headers:CORS});

  const from = u.searchParams.get("from") || new Date(Date.now()-7*86400000).toISOString().split("T")[0];
  const to   = u.searchParams.get("to")   || new Date().toISOString().split("T")[0];
  const dir  = u.searchParams.get("direction") || "";
  const t0   = Date.now();

  try {
    const token              = await getToken(ACCT, CID, CSEC);
    const { recs, endpoint } = await fetchCalls(token, from, to, dir);
    const records            = recs.map(r => normalize(r, endpoint));
    const employees          = [...new Set(records.map(r => r.answeredBy))];
    console.log(`[zoom-phone] ${endpoint} | ${records.length} records | employees: ${employees.join(", ")}`);
    return new Response(JSON.stringify({ records, count: records.length, endpoint, employees, from, to, ms: Date.now()-t0 }), { headers: CORS });
  } catch (e: any) {
    console.error("[zoom-phone]", e?.message);
    return new Response(JSON.stringify({ error: String(e?.message||e) }), { status: 500, headers: CORS });
  }
};

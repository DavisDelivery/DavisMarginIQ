import type { Context } from "@netlify/functions";

// Zoom Phone Call History proxy for MarginIQ
// Credentials stored as Netlify env vars:
//   ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET
//
// Endpoints:
//   GET ?action=history&from=YYYY-MM-DD&to=YYYY-MM-DD&direction=inbound|outbound
//   GET ?action=status   — check if credentials are configured

const ZOOM_TOKEN_URL = "https://zoom.us/oauth/token";
const ZOOM_API_BASE  = "https://api.zoom.us/v2";
const MAX_PAGES = 20; // 20 × 100 = 2000 calls max

async function getZoomToken(accountId: string, clientId: string, clientSecret: string): Promise<string> {
  const creds = btoa(`${clientId}:${clientSecret}`);
  const res = await fetch(
    `${ZOOM_TOKEN_URL}?grant_type=account_credentials&account_id=${encodeURIComponent(accountId)}`,
    {
      method: "POST",
      headers: {
        "Authorization": `Basic ${creds}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
    }
  );
  const data: any = await res.json();
  if (!res.ok) throw new Error(data.reason || data.message || `Zoom auth failed (${res.status})`);
  return data.access_token;
}

async function fetchCallHistory(token: string, from: string, to: string, direction?: string): Promise<any[]> {
  const records: any[] = [];
  let nextPageToken = "";
  let pages = 0;

  do {
    let url = `${ZOOM_API_BASE}/phone/call_history?from=${from}&to=${to}&page_size=100`;
    if (direction) url += `&type=${direction}`;
    if (nextPageToken) url += `&next_page_token=${encodeURIComponent(nextPageToken)}`;

    const res = await fetch(url, {
      headers: { "Authorization": `Bearer ${token}` },
    });
    const data: any = await res.json();

    if (!res.ok) {
      throw new Error(data.message || `Zoom API ${res.status} — verify scopes: phone:read:list_call_history:admin`);
    }

    const items = data.call_logs || data.call_history || data.records || [];
    records.push(...items);
    nextPageToken = data.next_page_token || "";
    if (++pages >= MAX_PAGES) break;
  } while (nextPageToken);

  return records;
}

// Parse call_elements to find the actual employee who answered
function normalizeRecord(raw: any) {
  const elems: any[] = raw.call_elements || [];
  const top = elems[0] || {};

  const answeredLeg = elems.find(e =>
    e.event === "ring_to_member" && e.callee_ext_type === "user" && e.result === "answered"
  ) || elems.find(e => e.callee_ext_type === "user");

  const answeredBy    = answeredLeg?.callee_name  || raw.callee?.name || raw.user_name || "Unknown";
  const answeredEmail = answeredLeg?.callee_email || raw.callee?.email || "";
  const answeredExt   = answeredLeg?.callee_ext_number || raw.callee?.extension_number || "";

  const viaQueue  = elems.some(e => ["call_queue","auto_receptionist"].includes(e.callee_ext_type));
  const queueName = elems.find(e => ["call_queue","auto_receptionist"].includes(e.callee_ext_type))?.callee_name || "";
  const chain     = [...new Set(elems.filter(e => e.callee_ext_type === "user" && e.callee_name).map(e => e.callee_name))];

  const activeLeg = answeredLeg || top;
  const talkTime  = Number(activeLeg.talk_time  ?? raw.duration ?? 0);
  const waitTime  = Number(activeLeg.wait_time  ?? 0);

  const rStr = (raw.result || raw.call_result || activeLeg.result || "").toLowerCase().replace(/_/g, " ");
  const result = rStr.includes("connect") || rStr === "answered" ? "answered"
    : rStr.includes("voicemail") ? "voicemail"
    : rStr.includes("miss") || rStr.includes("no answer") || rStr.includes("abandon") ? "missed"
    : raw.result || "unknown";

  return {
    id:           raw.id || raw.call_id || raw.call_history_uuid || "",
    date:         raw.date_time || raw.start_time || activeLeg.start_time || "",
    answeredBy, answeredEmail, answeredExt,
    direction:    (raw.direction || top.direction || "").toLowerCase(),
    callerNum:    top.caller_did_number || raw.caller?.phone_number || "",
    callerName:   top.caller_name || raw.caller?.name || "",
    calleeNum:    top.callee_did_number || raw.callee?.phone_number || "",
    result, talkTime, waitTime, viaQueue, queueName, chain,
  };
}

export default async (req: Request, _ctx: Context) => {
  const CORS = { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" };

  const ACCOUNT_ID    = process.env["ZOOM_ACCOUNT_ID"];
  const CLIENT_ID     = process.env["ZOOM_CLIENT_ID"];
  const CLIENT_SECRET = process.env["ZOOM_CLIENT_SECRET"];

  const url    = new URL(req.url);
  const action = url.searchParams.get("action") || "history";

  if (action === "status") {
    return new Response(JSON.stringify({
      configured: !!(ACCOUNT_ID && CLIENT_ID && CLIENT_SECRET),
      missing: [
        !ACCOUNT_ID    && "ZOOM_ACCOUNT_ID",
        !CLIENT_ID     && "ZOOM_CLIENT_ID",
        !CLIENT_SECRET && "ZOOM_CLIENT_SECRET",
      ].filter(Boolean),
    }), { headers: CORS });
  }

  if (!ACCOUNT_ID || !CLIENT_ID || !CLIENT_SECRET) {
    return new Response(JSON.stringify({
      error: "Zoom credentials not configured. Set ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET in Netlify env vars.",
    }), { status: 500, headers: CORS });
  }

  const from      = url.searchParams.get("from") || new Date(Date.now() - 7*86400000).toISOString().split("T")[0];
  const to        = url.searchParams.get("to")   || new Date().toISOString().split("T")[0];
  const direction = url.searchParams.get("direction") || "";
  const startedAt = Date.now();

  try {
    const token   = await getZoomToken(ACCOUNT_ID, CLIENT_ID, CLIENT_SECRET);
    const raw     = await fetchCallHistory(token, from, to, direction);
    const records = raw.map(normalizeRecord);

    return new Response(JSON.stringify({
      records,
      count: records.length,
      from, to,
      ms: Date.now() - startedAt,
    }), { headers: CORS });

  } catch (e: any) {
    console.error("[zoom-phone]", e?.message);
    return new Response(JSON.stringify({ error: String(e?.message || e) }), {
      status: 500, headers: CORS,
    });
  }
};

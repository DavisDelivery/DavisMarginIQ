import type { Context } from "@netlify/functions";

// Zoom Phone proxy — per-user call history for employee attribution
// Strategy: fetch all phone users, then pull /phone/users/{id}/call_history
// for each, tagging every record with that user's name.
//
// Env vars: ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET
// GET ?action=status
// GET ?action=history&from=YYYY-MM-DD&to=YYYY-MM-DD&direction=inbound|outbound
// GET ?action=debug

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

// ── Fetch all phone users ────────────────────────────────────────────────────
async function getPhoneUsers(token: string): Promise<{ id: string; name: string; email: string; ext: string }[]> {
  const users: any[] = [];
  let npt = "", pages = 0;
  do {
    const url = `${API}/phone/users?page_size=100${npt ? "&next_page_token=" + encodeURIComponent(npt) : ""}`;
    const res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
    const d: any = await res.json();
    if (!res.ok) throw new Error(d.message || `Users API ${res.status}`);
    users.push(...(d.users || []));
    npt = d.next_page_token || "";
    if (++pages >= 5) break; // max 500 users
  } while (npt);
  return users.map(u => ({
    id:    u.id || "",
    name:  u.display_name || u.name || u.first_name + " " + (u.last_name||"") || "Unknown",
    email: u.email || "",
    ext:   u.extension_number || u.phone_user?.extension_number || "",
  }));
}

// ── Fetch call history for one user ─────────────────────────────────────────
async function getUserCallHistory(token: string, userId: string, from: string, to: string, dir: string): Promise<any[]> {
  const records: any[] = [];
  let npt = "", pages = 0;
  const dirParam = dir ? `&type=${dir}` : "";
  do {
    const url = `${API}/phone/users/${userId}/call_history?from=${from}&to=${to}&page_size=100${dirParam}${npt ? "&next_page_token=" + encodeURIComponent(npt) : ""}`;
    const res = await fetch(url, { headers: { "Authorization": `Bearer ${token}` } });
    const d: any = await res.json();
    if (!res.ok) {
      // 404 = user has no phone, skip silently
      if (res.status === 404) return [];
      const e: any = new Error(d.message || `User call history ${res.status}`);
      e.status = res.status;
      throw e;
    }
    records.push(...(d.call_logs || d.call_history || d.records || []));
    npt = d.next_page_token || "";
    if (++pages >= MAX_PAGES) break;
  } while (npt);
  return records;
}

// ── Normalize one record, tagging with the known user ───────────────────────
function normalize(raw: any, user: { name: string; email: string; ext: string }) {
  const direction = (raw.direction || "").toLowerCase();

  // The employee is always this user — that's why we fetch per-user
  const answeredBy    = user.name;
  const answeredEmail = user.email;
  const answeredExt   = user.ext || raw.callee_ext_number || raw.caller_ext_number || "";

  const callerNum  = raw.caller_did_number  || raw.caller_number  || "";
  const callerName = raw.caller_name        || "";
  const calleeNum  = raw.callee_did_number  || raw.callee_number  || "";

  const talkTime = Number(raw.duration ?? raw.talk_time ?? 0);

  const rStr = (raw.call_result || raw.result || "").toLowerCase().replace(/_/g, " ");
  const result =
    rStr.includes("connect") || rStr === "answered" || rStr.includes("answer") ? "answered" :
    rStr.includes("voicemail")                                                  ? "voicemail" :
    rStr.includes("miss") || rStr.includes("no answer") || rStr.includes("abandon") ? "missed" :
    raw.call_result || raw.result || "unknown";

  // Was it routed via auto-receptionist / queue?
  const viaQueue  = raw.callee_ext_type === "auto_receptionist" || raw.callee_ext_type === "call_queue"
                  || (direction === "inbound" && raw.callee_name && raw.callee_name !== user.name);
  const queueName = viaQueue ? (raw.callee_name || "") : "";

  return {
    id:           raw.id || raw.call_id || raw.call_path_id || "",
    date:         raw.start_time || raw.date_time || "",
    direction,
    answeredBy,
    answeredEmail,
    answeredExt,
    callerNum,
    callerName,
    calleeNum,
    result,
    talkTime,
    waitTime:     0,
    viaQueue,
    queueName,
    chain:        [],
  };
}

// ── Main handler ─────────────────────────────────────────────────────────────
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

  if (!ACCT || !CID || !CSEC) {
    return new Response(JSON.stringify({ error: "Missing Zoom env vars." }), { status: 500, headers: CORS });
  }

  const from = u.searchParams.get("from") || new Date(Date.now()-7*86400000).toISOString().split("T")[0];
  const to   = u.searchParams.get("to")   || new Date().toISOString().split("T")[0];
  const dir  = u.searchParams.get("direction") || "";
  const t0   = Date.now();

  try {
    const token = await getToken(ACCT, CID, CSEC);

    // ── Debug: show user list ─────────────────────────────────────────────
    if (action === "debug") {
      const users = await getPhoneUsers(token);
      // Fetch 5 records from first user as sample
      const sample = users.length > 0 ? await getUserCallHistory(token, users[0].id, from, to, "") : [];
      return new Response(JSON.stringify({
        users,
        sampleUserCallKeys: sample[0] ? Object.keys(sample[0]) : [],
        sampleRecord: sample[0] || null,
      }), { headers: CORS });
    }

    // ── Fetch per-user call history in parallel ───────────────────────────
    const users = await getPhoneUsers(token);
    console.log(`[zoom-phone] ${users.length} phone users: ${users.map(u=>u.name).join(", ")}`);

    // Fetch all users concurrently (max 10 at a time to avoid rate limits)
    const BATCH = 10;
    const allRecords: any[] = [];
    for (let i = 0; i < users.length; i += BATCH) {
      const batch = users.slice(i, i + BATCH);
      const results = await Promise.all(
        batch.map(async user => {
          const recs = await getUserCallHistory(token, user.id, from, to, dir);
          return recs.map(r => normalize(r, user));
        })
      );
      results.forEach(recs => allRecords.push(...recs));
    }

    // Deduplicate by call_id (same call can appear in multiple users' logs if transferred)
    const seen = new Set<string>();
    const deduped = allRecords.filter(r => {
      const key = r.id || `${r.date}-${r.callerNum}-${r.answeredBy}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    // Sort by date desc
    deduped.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    const employees = [...new Set(deduped.map(r => r.answeredBy))];
    console.log(`[zoom-phone] ${deduped.length} records | employees: ${employees.join(", ")}`);

    return new Response(JSON.stringify({
      records:   deduped,
      count:     deduped.length,
      employees,
      from, to,
      ms: Date.now() - t0,
    }), { headers: CORS });

  } catch (e: any) {
    console.error("[zoom-phone]", e?.message);
    return new Response(JSON.stringify({ error: String(e?.message || e) }), { status: 500, headers: CORS });
  }
};

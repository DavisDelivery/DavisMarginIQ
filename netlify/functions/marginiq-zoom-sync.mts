import type { Config, Context } from "@netlify/functions";

// Zoom Phone background sync — runs every hour via Netlify scheduled function
// Fetches call history per user from Zoom and writes to Firebase REST API
// so the MarginIQ Phone tab reads instantly from Firebase instead of waiting on Zoom.
//
// Firebase collection: zoom_call_history/{YYYY-MM-DD_userId} → { records[], synced_at }
// Firebase collection: zoom_sync_meta/status → { last_sync, users[], record_count }
//
// Env vars: ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET
//           FIREBASE_API_KEY, FIREBASE_PROJECT_ID

export const config: Config = {
  schedule: "0 * * * *",  // every hour
};

const TOKEN_URL = "https://zoom.us/oauth/token";
const API       = "https://api.zoom.us/v2";
const SLEEP_MS  = 400; // 400ms between user fetches = safe under Zoom rate limit

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

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
      if (res.status === 429) { await sleep(2000); continue; } // back off on rate limit
      throw new Error(d.message || `Call history ${res.status} for user ${userId}`);
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
    id:           raw.id || raw.call_path_id || "",
    date:         raw.start_time || raw.date_time || "",
    direction:    (raw.direction || "").toLowerCase(),
    answeredBy:   user.name,
    answeredEmail: user.email,
    answeredExt:  user.ext || raw.callee_ext_number || "",
    callerNum:    raw.caller_did_number || raw.caller_number || "",
    callerName:   raw.caller_name || "",
    calleeNum:    raw.callee_did_number || raw.callee_number || "",
    result,
    talkTime:     Number(raw.duration ?? 0),
    waitTime:     0,
    viaQueue:     raw.callee_ext_type === "auto_receptionist" || raw.callee_ext_type === "call_queue",
    queueName:    (raw.callee_ext_type === "auto_receptionist" || raw.callee_ext_type === "call_queue") ? (raw.callee_name || "") : "",
    chain:        [],
  };
}

// ── Firestore REST write ─────────────────────────────────────────────────────
async function firestoreSet(projectId: string, apiKey: string, collection: string, docId: string, data: any) {
  function toFV(v: any): any {
    if (v === null || v === undefined) return { nullValue: null };
    if (typeof v === "boolean")        return { booleanValue: v };
    if (typeof v === "number")         return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
    if (typeof v === "string")         return { stringValue: v };
    if (Array.isArray(v))              return { arrayValue: { values: v.map(toFV) } };
    if (typeof v === "object")         return { mapValue: { fields: Object.fromEntries(Object.entries(v).map(([k, val]) => [k, toFV(val)])) } };
    return { stringValue: String(v) };
  }
  const fields = Object.fromEntries(Object.entries(data).map(([k, v]) => [k, toFV(v)]));
  const url = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const res = await fetch(url, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fields }) });
  if (!res.ok) {
    const err = await res.text().catch(() => "");
    throw new Error(`Firestore write failed ${res.status}: ${err.slice(0, 200)}`);
  }
}

// ── Scheduled handler ────────────────────────────────────────────────────────
export default async (_req: Request, _ctx: Context) => {
  const ACCT    = process.env["ZOOM_ACCOUNT_ID"];
  const CID     = process.env["ZOOM_CLIENT_ID"];
  const CSEC    = process.env["ZOOM_CLIENT_SECRET"];
  const API_KEY = process.env["FIREBASE_API_KEY"];
  const PROJ    = process.env["FIREBASE_PROJECT_ID"];

  if (!ACCT || !CID || !CSEC || !API_KEY || !PROJ) {
    console.error("[zoom-sync] Missing env vars");
    return new Response("Missing env vars", { status: 500 });
  }

  const t0  = Date.now();
  // Sync last 35 days (5-day buffer so we catch late-arriving records)
  const to  = new Date().toISOString().split("T")[0];
  const fd  = new Date(Date.now() - 35 * 86400000);
  const from = fd.toISOString().split("T")[0];

  console.log(`[zoom-sync] Starting sync ${from} → ${to}`);

  try {
    const token = await getToken(ACCT, CID, CSEC);
    const users = await getPhoneUsers(token);
    console.log(`[zoom-sync] ${users.length} users: ${users.map(u => u.name).join(", ")}`);

    const allRecords: any[] = [];

    for (let i = 0; i < users.length; i++) {
      if (i > 0) await sleep(SLEEP_MS);
      const user = users[i];
      try {
        const recs = await getUserCalls(token, user.id, from, to);
        const normalized = recs.map(r => normalizeRecord(r, user));
        allRecords.push(...normalized);

        // Write per-user slice to Firebase so partial results are available immediately
        await firestoreSet(PROJ, API_KEY, "zoom_sync_users", user.id, {
          name:       user.name,
          email:      user.email,
          ext:        user.ext,
          records:    normalized,
          count:      normalized.length,
          synced_at:  new Date().toISOString(),
          from, to,
        });
        console.log(`[zoom-sync] ${user.name}: ${normalized.length} records`);
      } catch (e: any) {
        console.error(`[zoom-sync] Error for ${user.name}:`, e?.message);
      }
    }

    // Deduplicate
    const seen = new Set<string>();
    const deduped = allRecords.filter(r => {
      const key = r.id || `${r.date}-${r.callerNum}-${r.answeredBy}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    // Write metadata doc so the UI knows when sync last ran
    await firestoreSet(PROJ, API_KEY, "zoom_sync_meta", "status", {
      last_sync:    new Date().toISOString(),
      record_count: deduped.length,
      user_count:   users.length,
      users:        users.map(u => ({ id: u.id, name: u.name })),
      from, to,
      ms: Date.now() - t0,
    });

    console.log(`[zoom-sync] Done. ${deduped.length} records, ${users.length} users, ${Date.now()-t0}ms`);
    return new Response("OK", { status: 200 });

  } catch (e: any) {
    console.error("[zoom-sync] Fatal:", e?.message);
    return new Response(String(e?.message), { status: 500 });
  }
};

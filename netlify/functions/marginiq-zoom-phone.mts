import type { Context } from "@netlify/functions";

// Zoom Phone API proxy — reads from Firebase cache written by marginiq-zoom-sync
// Falls back to direct Zoom fetch only for ?action=refresh (manual trigger)
//
// Env vars: ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET
//           FIREBASE_API_KEY, FIREBASE_PROJECT_ID

const TOKEN_URL = "https://zoom.us/oauth/token";
const API       = "https://api.zoom.us/v2";
const SLEEP_MS  = 400;
const sleep     = (ms: number) => new Promise(r => setTimeout(r, ms));

const CORS = { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" };

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
      throw new Error(d.message || `Call history ${res.status}`);
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

// ── Firebase REST helpers ────────────────────────────────────────────────────
function toFV(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean")        return { booleanValue: v };
  if (typeof v === "number")         return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (typeof v === "string")         return { stringValue: v };
  if (Array.isArray(v))              return { arrayValue: { values: v.map(toFV) } };
  if (typeof v === "object")         return { mapValue: { fields: Object.fromEntries(Object.entries(v).map(([k, val]) => [k, toFV(val)])) } };
  return { stringValue: String(v) };
}

async function fsSet(proj: string, key: string, col: string, doc: string, data: any) {
  const fields = Object.fromEntries(Object.entries(data).map(([k, v]) => [k, toFV(v)]));
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}/${encodeURIComponent(doc)}?key=${key}`;
  const res = await fetch(url, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fields }) });
  if (!res.ok) throw new Error(`Firestore PATCH ${res.status}`);
}

async function fsGetCollection(proj: string, key: string, col: string): Promise<any[]> {
  const url = `https://firestore.googleapis.com/v1/projects/${proj}/databases/(default)/documents/${col}?key=${key}&pageSize=200`;
  const res = await fetch(url, { headers: { "Content-Type": "application/json" } });
  if (!res.ok) return [];
  const d: any = await res.json();
  return (d.documents || []).map((doc: any) => fromFV(doc.fields || {}));
}

function fromFV(fields: any): any {
  const out: any = {};
  for (const [k, v] of Object.entries(fields as any)) {
    out[k] = fromFVValue(v);
  }
  return out;
}
function fromFVValue(v: any): any {
  if (v.nullValue !== undefined)    return null;
  if (v.booleanValue !== undefined) return v.booleanValue;
  if (v.integerValue !== undefined) return Number(v.integerValue);
  if (v.doubleValue !== undefined)  return v.doubleValue;
  if (v.stringValue !== undefined)  return v.stringValue;
  if (v.arrayValue)                 return (v.arrayValue.values || []).map(fromFVValue);
  if (v.mapValue)                   return fromFV(v.mapValue.fields || {});
  return null;
}

// ── Main handler ─────────────────────────────────────────────────────────────
export default async (req: Request, _ctx: Context) => {
  const ACCT    = process.env["ZOOM_ACCOUNT_ID"];
  const CID     = process.env["ZOOM_CLIENT_ID"];
  const CSEC    = process.env["ZOOM_CLIENT_SECRET"];
  const API_KEY = process.env["FIREBASE_API_KEY"];
  const PROJ    = process.env["FIREBASE_PROJECT_ID"];

  const u      = new URL(req.url);
  const action = u.searchParams.get("action") || "history";
  const from   = u.searchParams.get("from") || new Date(Date.now()-30*86400000).toISOString().split("T")[0];
  const to     = u.searchParams.get("to")   || new Date().toISOString().split("T")[0];
  const dir    = u.searchParams.get("direction") || "";

  if (action === "status") {
    return new Response(JSON.stringify({
      configured: !!(ACCT && CID && CSEC),
      missing: [!ACCT&&"ZOOM_ACCOUNT_ID",!CID&&"ZOOM_CLIENT_ID",!CSEC&&"ZOOM_CLIENT_SECRET"].filter(Boolean),
    }), { headers: CORS });
  }

  if (!ACCT || !CID || !CSEC) {
    return new Response(JSON.stringify({ error: "Missing Zoom env vars." }), { status: 500, headers: CORS });
  }

  const t0 = Date.now();

  // ── Read from Firebase cache (instant) ───────────────────────────────────
  if (action === "history" && API_KEY && PROJ) {
    try {
      const userDocs = await fsGetCollection(PROJ, API_KEY, "zoom_sync_users");
      if (userDocs.length > 0) {
        // Flatten all user records, filter by date range
        const fromDate = new Date(from + "T00:00:00Z").getTime();
        const toDate   = new Date(to   + "T23:59:59Z").getTime();
        const allRecords: any[] = [];

        for (const doc of userDocs) {
          const recs: any[] = doc.records || [];
          for (const r of recs) {
            const d = new Date(r.date).getTime();
            if (d >= fromDate && d <= toDate) {
              if (!dir || r.direction === dir) allRecords.push(r);
            }
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
        deduped.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

        const employees = [...new Set(deduped.map((r: any) => r.answeredBy))];
        const syncMeta  = userDocs[0]?.synced_at || null;

        console.log(`[zoom-phone] cache hit: ${deduped.length} records from ${userDocs.length} users`);
        return new Response(JSON.stringify({
          records: deduped, count: deduped.length, employees,
          source: "cache", synced_at: syncMeta, from, to, ms: Date.now()-t0,
        }), { headers: CORS });
      }
    } catch (e: any) {
      console.warn("[zoom-phone] Firebase cache read failed:", e?.message, "— falling through to direct fetch");
    }
  }

  // ── Direct fetch fallback (or ?action=refresh) ────────────────────────────
  // Used when cache is empty (first run) or user manually triggers refresh
  try {
    const token = await getToken(ACCT, CID, CSEC);
    const users = await getPhoneUsers(token);
    const allRecords: any[] = [];

    for (let i = 0; i < users.length; i++) {
      if (i > 0) await sleep(SLEEP_MS);
      const recs = await getUserCalls(token, users[i].id, from, to);
      const normalized = recs.map(r => normalizeRecord(r, users[i]));
      allRecords.push(...normalized);

      // Write to cache as we go
      if (API_KEY && PROJ) {
        await fsSet(PROJ, API_KEY, "zoom_sync_users", users[i].id, {
          name: users[i].name, email: users[i].email, ext: users[i].ext,
          records: normalized, count: normalized.length,
          synced_at: new Date().toISOString(), from, to,
        }).catch(e => console.warn("[zoom-phone] cache write failed:", e?.message));
      }
    }

    const seen = new Set<string>();
    const deduped = allRecords.filter(r => {
      const key = r.id || `${r.date}-${r.callerNum}-${r.answeredBy}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    deduped.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    const employees = [...new Set(deduped.map(r => r.answeredBy))];
    console.log(`[zoom-phone] direct fetch: ${deduped.length} records | ${employees.join(", ")}`);

    return new Response(JSON.stringify({
      records: deduped, count: deduped.length, employees,
      source: "direct", from, to, ms: Date.now()-t0,
    }), { headers: CORS });

  } catch (e: any) {
    console.error("[zoom-phone]", e?.message);
    return new Response(JSON.stringify({ error: String(e?.message || e) }), { status: 500, headers: CORS });
  }
};

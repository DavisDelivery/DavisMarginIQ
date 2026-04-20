import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — one-shot purge endpoint for timeclock_weekly.
 *
 * Deletes every document in the timeclock_weekly Firestore collection, used
 * when a schema change (e.g. week-ending convention) makes old documents
 * inconsistent with freshly ingested data.
 *
 * Protected by a secret token passed as ?token=... in the query string.
 * The token must match the MARGINIQ_ADMIN_TOKEN env var.
 *
 * Usage:
 *   curl "https://davis-marginiq.netlify.app/.netlify/functions/marginiq-purge-timeclock?token=YOUR_TOKEN"
 *
 * Response:
 *   { ok: true, deleted: 68 }
 *
 * Required Netlify env vars:
 *   FIREBASE_API_KEY      — Firestore REST API key
 *   MARGINIQ_ADMIN_TOKEN  — any random string you choose (treat like a password)
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const ADMIN_TOKEN = process.env["MARGINIQ_ADMIN_TOKEN"];

async function listAll(collection: string): Promise<string[]> {
  const ids: string[] = [];
  let pageToken: string | undefined;
  do {
    const params = new URLSearchParams({
      key: FIREBASE_API_KEY || "",
      pageSize: "300",
    });
    if (pageToken) params.set("pageToken", pageToken);
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}?${params.toString()}`;
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`List ${collection} failed: ${resp.status} ${await resp.text()}`);
    const data: any = await resp.json();
    for (const doc of (data.documents || [])) {
      // doc.name = projects/.../documents/timeclock_weekly/2025-01-03
      const parts = String(doc.name).split("/");
      ids.push(parts[parts.length - 1]);
    }
    pageToken = data.nextPageToken;
  } while (pageToken);
  return ids;
}

async function deleteOne(collection: string, docId: string): Promise<boolean> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, { method: "DELETE" });
  return resp.ok;
}

export default async (req: Request, _ctx: Context) => {
  if (!FIREBASE_API_KEY) return json({ error: "FIREBASE_API_KEY not configured" }, 500);
  if (!ADMIN_TOKEN) return json({ error: "MARGINIQ_ADMIN_TOKEN not configured" }, 500);

  const url = new URL(req.url);
  const token = url.searchParams.get("token");
  if (token !== ADMIN_TOKEN) return json({ error: "invalid or missing token" }, 403);

  try {
    const ids = await listAll("timeclock_weekly");
    let deleted = 0;
    const failed: string[] = [];
    for (const id of ids) {
      if (await deleteOne("timeclock_weekly", id)) deleted++;
      else failed.push(id);
    }
    return json({ ok: true, total: ids.length, deleted, failed });
  } catch (err: any) {
    return json({ ok: false, error: String(err.message || err) }, 500);
  }
};

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export const config: Config = {
  path: "/.netlify/functions/marginiq-purge-timeclock",
};

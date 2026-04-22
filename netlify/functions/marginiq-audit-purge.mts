import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Audit Queue Purge (v2.40.18)
 *
 * Deletes every document in the `audit_items` collection. Sync function —
 * 2,000 docs / 500 per batchWrite = ~4 round trips, well inside the 10s
 * budget. If the collection ever grows past ~20K we should split this into
 * a dispatcher + background worker like audit-rebuild; for now, sync is fine.
 *
 * Use case:
 *   The audit rebuild historically upserted new items without deleting
 *   stale ones, so the collection accumulated ghosts from prior runs. This
 *   gives the user a "clean slate" button. v2.40.18 rebuild itself now
 *   preserves dispute history, so purge is mostly a one-time cleanup tool.
 *
 * POST /.netlify/functions/marginiq-audit-purge
 *   → Deletes all audit_items and returns { deleted: N }.
 *
 * Env vars:
 *   FIREBASE_API_KEY — Firestore REST
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

async function listAllDocIds(collection: string): Promise<string[]> {
  const out: string[] = [];
  let pageToken: string | undefined;
  let pages = 0;
  do {
    const params = new URLSearchParams({
      key: FIREBASE_API_KEY || "",
      pageSize: "300",
    });
    if (pageToken) params.set("pageToken", pageToken);
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}?${params.toString()}`;
    const resp = await fetch(url);
    if (!resp.ok) {
      if (resp.status === 404) return out;
      throw new Error(`List ${collection} failed: HTTP ${resp.status} ${await resp.text()}`);
    }
    const data: any = await resp.json();
    for (const doc of (data.documents || [])) {
      const id = doc.name?.split("/").pop();
      if (id) out.push(id);
    }
    pageToken = data.nextPageToken;
    pages++;
    if (pages > 500) {
      console.warn(`purge: hit page ceiling 500, stopping (${out.length} ids so far)`);
      break;
    }
  } while (pageToken);
  return out;
}

async function batchDelete(collection: string, ids: string[]): Promise<{ ok: number; failed: number }> {
  if (ids.length === 0) return { ok: 0, failed: 0 };
  if (ids.length > 500) {
    let ok = 0, failed = 0;
    for (let i = 0; i < ids.length; i += 500) {
      const r = await batchDelete(collection, ids.slice(i, i + 500));
      ok += r.ok; failed += r.failed;
    }
    return { ok, failed };
  }
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents:batchWrite?key=${FIREBASE_API_KEY}`;
  const writes = ids.map(id => ({
    delete: `projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${id}`,
  }));
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ writes }),
  });
  if (!resp.ok) {
    console.error(`batchDelete ${collection} failed: ${resp.status} ${await resp.text()}`);
    return { ok: 0, failed: ids.length };
  }
  const data: any = await resp.json();
  const writeResults = data.writeResults || [];
  const statuses = data.status || [];
  let explicitFailed = 0;
  // v2.40.23: log the first failure so the next bug is diagnosable. The
  // previous silent "2004 failed" message cost Chad a whole round trip.
  let firstErrLogged = false;
  for (const s of statuses) {
    if (s && s.code && s.code !== 0) {
      explicitFailed++;
      if (!firstErrLogged) {
        console.error(`batchDelete ${collection}: rpc.Status code=${s.code} message="${s.message || ""}"`);
        firstErrLogged = true;
      }
    }
  }
  if (writeResults.length > 0) {
    return { ok: writeResults.length - explicitFailed, failed: explicitFailed };
  }
  if (statuses.length === 0) return { ok: ids.length, failed: 0 };
  let okFromStatus = 0;
  for (const s of statuses) {
    if (!s || !s.code || s.code === 0) okFromStatus++;
  }
  return { ok: okFromStatus, failed: statuses.length - okFromStatus };
}

export default async (_req: Request, _context: Context) => {
  if (!FIREBASE_API_KEY) {
    return new Response(JSON.stringify({ error: "FIREBASE_API_KEY not configured" }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }

  try {
    const t0 = Date.now();
    const ids = await listAllDocIds("audit_items");
    console.log(`purge: listed ${ids.length} audit_items ids`);
    if (ids.length === 0) {
      return new Response(JSON.stringify({ ok: true, deleted: 0, message: "audit_items already empty" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    const result = await batchDelete("audit_items", ids);
    const elapsed = Math.round((Date.now() - t0) / 1000);
    console.log(`purge: deleted ${result.ok} / ${ids.length} audit_items in ${elapsed}s (${result.failed} failed)`);
    return new Response(
      JSON.stringify({
        ok: true,
        listed: ids.length,
        deleted: result.ok,
        failed: result.failed,
        elapsed_s: elapsed,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } },
    );
  } catch (e: any) {
    const msg = e?.message || String(e);
    console.error("purge FAILED:", msg);
    return new Response(JSON.stringify({ error: msg }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
};

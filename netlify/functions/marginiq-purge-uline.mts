import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — purge all Uline-source data so it can be re-ingested clean.
 *
 * Deletes:
 *   - uline_weekly (weekly rollups)
 *   - recon_weekly (reconciliation per-week aggregates — Uline billed vs DDIS paid)
 *   - unpaid_stops (only computed from Uline stops vs DDIS)
 *   - audit_items (AuditIQ — computed from Uline vs DDIS)
 *   - source_conflicts (Uline vs Davis correction tracking)
 *   - Uline-source entries in file_log (kind in master/original/accessorials)
 *   - Uline-source entries in source_files (same kinds)
 *
 * Preserves:
 *   - ddis_files (payment data — kept so post-ingest reconciliation works again)
 *   - nuvizz_weekly, nuvizz_stops (driver pay source)
 *   - timeclock_weekly, timeclock_daily
 *   - payroll_weekly
 *   - qbo_history
 *   - fuel_* (all)
 *   - driver_classifications
 *   - customer_ap_contacts
 *   - disputes
 *   - marginiq_config (gmail tokens etc.)
 *
 * Protected by a secret token passed as ?token=... in the query string.
 *
 * Usage:
 *   curl "https://davis-marginiq.netlify.app/.netlify/functions/marginiq-purge-uline?token=YOUR_TOKEN"
 *
 * Response:
 *   { ok: true, deleted: { uline_weekly: 68, recon_weekly: 68, ... } }
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const ADMIN_TOKEN = process.env["MARGINIQ_ADMIN_TOKEN"];

type DocRef = { id: string; kind?: string };

async function listAll(collection: string): Promise<DocRef[]> {
  const refs: DocRef[] = [];
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
      const parts = String(doc.name).split("/");
      const id = parts[parts.length - 1];
      // Extract the kind field so we can filter uline-only entries
      const kind = doc.fields?.kind?.stringValue;
      refs.push({ id, kind });
    }
    pageToken = data.nextPageToken;
  } while (pageToken);
  return refs;
}

async function deleteOne(collection: string, docId: string): Promise<boolean> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, { method: "DELETE" });
  return resp.ok;
}

// Delete in parallel batches of 25 — matches the client-side write batching
// pattern for consistency and avoids overwhelming the Firestore REST endpoint.
async function deleteAll(collection: string, ids: string[]): Promise<number> {
  let deleted = 0;
  const BATCH = 25;
  for (let i = 0; i < ids.length; i += BATCH) {
    const slice = ids.slice(i, i + BATCH);
    const results = await Promise.all(slice.map(id => deleteOne(collection, id)));
    deleted += results.filter(Boolean).length;
  }
  return deleted;
}

// Filter file_log/source_files down to only Uline-source docs so we don't
// accidentally wipe DDIS/NuVizz/timeclock/etc. file audit trails.
const ULINE_KINDS = new Set(["master", "original", "accessorials"]);

export default async (req: Request, _ctx: Context) => {
  if (!FIREBASE_API_KEY) return json({ error: "FIREBASE_API_KEY not configured" }, 500);
  if (!ADMIN_TOKEN) return json({ error: "MARGINIQ_ADMIN_TOKEN not configured" }, 500);

  const url = new URL(req.url);
  const token = url.searchParams.get("token");
  if (token !== ADMIN_TOKEN) return json({ error: "invalid or missing token" }, 403);

  try {
    // Collections to fully wipe
    const fullWipe = ["uline_weekly", "recon_weekly", "unpaid_stops", "audit_items", "source_conflicts"];
    // Collections to partially wipe (only Uline-kind docs)
    const filteredWipe = ["file_log", "source_files"];

    const results: Record<string, { found: number; deleted: number }> = {};

    for (const coll of fullWipe) {
      const refs = await listAll(coll);
      const ids = refs.map(r => r.id);
      const deleted = await deleteAll(coll, ids);
      results[coll] = { found: ids.length, deleted };
    }

    for (const coll of filteredWipe) {
      const refs = await listAll(coll);
      const ids = refs.filter(r => ULINE_KINDS.has(r.kind || "")).map(r => r.id);
      const deleted = await deleteAll(coll, ids);
      results[coll] = { found: ids.length, deleted };
    }

    return json({
      ok: true,
      deleted_at: new Date().toISOString(),
      results,
      note: "Uline data purged. Preserved: DDIS, NuVizz, timeclock, payroll, QBO, fuel, driver classifications, AP contacts, disputes, config. Re-ingest Uline files via Data Ingest or Gmail Sync.",
    });
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
  path: "/.netlify/functions/marginiq-purge-uline",
};

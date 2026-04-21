import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — Daily Firestore backup to Firebase Storage.
 *
 * Runs nightly at 07:00 UTC (3AM EST / 2AM EDT) and on manual trigger.
 * Reads every meaningful collection via Firestore REST API and uploads
 * a single gzipped JSON snapshot to Firebase Storage under:
 *     backups/YYYY-MM-DD/snapshot.json.gz
 * Plus a small manifest file:
 *     backups/YYYY-MM-DD/manifest.json
 * ...with collection counts + byte sizes for fast listing in the UI.
 *
 * Retention (pruned in-line after each successful backup):
 *   - Last 30 daily backups: keep all
 *   - Older than 30 days: keep only the 1st of each month (monthly archive)
 *
 * Endpoints:
 *   GET  /.netlify/functions/marginiq-backup                  → run backup (requires token)
 *   GET  /.netlify/functions/marginiq-backup?action=list      → list all backups
 *   POST /.netlify/functions/marginiq-backup?action=restore   → restore a backup (body: {date})
 *   GET  /.netlify/functions/marginiq-backup?action=download&date=YYYY-MM-DD → signed URL
 *
 * Env vars required (already in Netlify):
 *   FIREBASE_API_KEY          — for Firestore reads + Storage uploads
 *   MARGINIQ_ADMIN_TOKEN      — gates write/restore endpoints
 */

const PROJECT_ID = "davismarginiq";
const STORAGE_BUCKET = "davismarginiq.firebasestorage.app";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const ADMIN_TOKEN = process.env["MARGINIQ_ADMIN_TOKEN"];

// Collections to back up. Full list — every collection that matters.
// Adding a collection here is the only step needed to include it in backups.
const COLLECTIONS = [
  "uline_weekly",
  "recon_weekly",
  "unpaid_stops",
  "audit_items",
  "source_conflicts",
  "source_files",
  "file_log",
  "ddis_files",
  "ddis_payments",
  "nuvizz_weekly",
  "nuvizz_stops",
  "timeclock_weekly",
  "timeclock_daily",
  "payroll_weekly",
  "qbo_history",
  "fuel_by_truck",
  "fuel_weekly",
  "driver_classifications",
  "customer_ap_contacts",
  "disputes",
  "marginiq_config",
];

type DocMap = Record<string, any>;
type Snapshot = {
  taken_at: string;
  project: string;
  collections: Record<string, DocMap>;
  meta: { total_docs: number; per_collection: Record<string, number> };
};

// ─── Firestore REST helpers ────────────────────────────────────────────────

async function listCollection(collection: string): Promise<DocMap> {
  const out: DocMap = {};
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
      // Collection may not exist yet — treat as empty
      if (resp.status === 404) return out;
      throw new Error(`List ${collection} failed: ${resp.status}`);
    }
    const data: any = await resp.json();
    for (const doc of (data.documents || [])) {
      const parts = String(doc.name).split("/");
      const id = parts[parts.length - 1];
      // Store the raw fields object — we'll use it as-is on restore
      out[id] = doc.fields || {};
    }
    pageToken = data.nextPageToken;
    pages++;
    if (pages > 200) break; // safety: cap at 60K docs/collection
  } while (pageToken);
  return out;
}

async function writeDocument(collection: string, docId: string, fields: any): Promise<boolean> {
  // Use patch to upsert. The fields object is already in Firestore REST shape
  // because we stored the raw `doc.fields` from listCollection().
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  return resp.ok;
}

async function deleteDocument(collection: string, docId: string): Promise<boolean> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, { method: "DELETE" });
  return resp.ok;
}

// ─── Firebase Storage helpers (REST API — no SDK needed) ──────────────────

async function uploadToStorage(objectPath: string, body: Uint8Array, contentType: string): Promise<void> {
  // Firebase Storage uses the GCS API underneath. We can POST via the Firebase
  // Storage upload endpoint which accepts the API key for auth.
  const url = `https://firebasestorage.googleapis.com/v0/b/${STORAGE_BUCKET}/o?name=${encodeURIComponent(objectPath)}&key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": contentType },
    body,
  });
  if (!resp.ok) {
    throw new Error(`Upload ${objectPath} failed: ${resp.status} ${await resp.text()}`);
  }
}

async function downloadFromStorage(objectPath: string): Promise<Uint8Array> {
  const url = `https://firebasestorage.googleapis.com/v0/b/${STORAGE_BUCKET}/o/${encodeURIComponent(objectPath)}?alt=media&key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Download ${objectPath} failed: ${resp.status}`);
  const buf = await resp.arrayBuffer();
  return new Uint8Array(buf);
}

async function deleteFromStorage(objectPath: string): Promise<boolean> {
  const url = `https://firebasestorage.googleapis.com/v0/b/${STORAGE_BUCKET}/o/${encodeURIComponent(objectPath)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, { method: "DELETE" });
  return resp.ok || resp.status === 404;
}

async function listStorageFolder(prefix: string): Promise<string[]> {
  const url = `https://firebasestorage.googleapis.com/v0/b/${STORAGE_BUCKET}/o?prefix=${encodeURIComponent(prefix)}&key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url);
  if (!resp.ok) return [];
  const data: any = await resp.json();
  return (data.items || []).map((it: any) => it.name);
}

// ─── Backup operation ──────────────────────────────────────────────────────

async function performBackup(dateOverride?: string) {
  const now = new Date();
  const dateKey = dateOverride || now.toISOString().slice(0, 10); // YYYY-MM-DD
  const takenAt = now.toISOString();

  const collections: Record<string, DocMap> = {};
  const perCollection: Record<string, number> = {};
  let totalDocs = 0;

  for (const coll of COLLECTIONS) {
    const docs = await listCollection(coll);
    collections[coll] = docs;
    const count = Object.keys(docs).length;
    perCollection[coll] = count;
    totalDocs += count;
  }

  const snapshot: Snapshot = {
    taken_at: takenAt,
    project: PROJECT_ID,
    collections,
    meta: { total_docs: totalDocs, per_collection: perCollection },
  };

  const json = JSON.stringify(snapshot);
  // Gzip the payload — at the scale of this app (hundreds of MB of JSON is
  // possible once history accumulates), gzip is the difference between a 30MB
  // upload and a 3MB upload. Node's built-in zlib.gzipSync is synchronous and
  // simple for a single-shot use like this.
  const { gzipSync } = await import("node:zlib");
  const gz = gzipSync(Buffer.from(json, "utf8"));

  const snapshotPath = `backups/${dateKey}/snapshot.json.gz`;
  const manifestPath = `backups/${dateKey}/manifest.json`;

  await uploadToStorage(snapshotPath, new Uint8Array(gz), "application/gzip");

  const manifest = {
    date: dateKey,
    taken_at: takenAt,
    project: PROJECT_ID,
    total_docs: totalDocs,
    per_collection: perCollection,
    compressed_bytes: gz.length,
    uncompressed_bytes: json.length,
    collections_captured: COLLECTIONS.length,
  };
  await uploadToStorage(
    manifestPath,
    new Uint8Array(Buffer.from(JSON.stringify(manifest, null, 2), "utf8")),
    "application/json"
  );

  return manifest;
}

// Retention: keep daily for 30 days, then only 1st-of-month archives.
async function pruneOldBackups() {
  const files = await listStorageFolder("backups/");
  const today = new Date();
  const byDate: Record<string, string[]> = {};
  for (const f of files) {
    // Expected path: backups/YYYY-MM-DD/<filename>
    const m = f.match(/^backups\/(\d{4}-\d{2}-\d{2})\//);
    if (!m) continue;
    const d = m[1];
    if (!byDate[d]) byDate[d] = [];
    byDate[d].push(f);
  }

  const deleted: string[] = [];
  const kept: string[] = [];

  for (const dateKey of Object.keys(byDate).sort()) {
    const backupDate = new Date(dateKey + "T00:00:00Z");
    const ageDays = (today.getTime() - backupDate.getTime()) / (1000 * 60 * 60 * 24);
    const isFirstOfMonth = backupDate.getUTCDate() === 1;

    // Within last 30 days: keep daily. Beyond: keep only monthly archives.
    const shouldKeep = ageDays <= 30 || isFirstOfMonth;

    if (shouldKeep) {
      kept.push(dateKey);
    } else {
      for (const path of byDate[dateKey]) {
        await deleteFromStorage(path);
        deleted.push(path);
      }
    }
  }

  return { kept_dates: kept, deleted_files: deleted };
}

// ─── Restore operation ─────────────────────────────────────────────────────

async function performRestore(dateKey: string): Promise<any> {
  const snapshotPath = `backups/${dateKey}/snapshot.json.gz`;
  const gz = await downloadFromStorage(snapshotPath);

  const { gunzipSync } = await import("node:zlib");
  const json = gunzipSync(Buffer.from(gz)).toString("utf8");
  const snapshot: Snapshot = JSON.parse(json);

  const results: Record<string, { restored: number; failed: number; deleted: number }> = {};

  for (const coll of COLLECTIONS) {
    const target = snapshot.collections[coll] || {};
    const targetIds = new Set(Object.keys(target));

    // 1) Write every doc from snapshot back to Firestore
    let restored = 0, failed = 0;
    for (const [docId, fields] of Object.entries(target)) {
      const ok = await writeDocument(coll, docId, fields);
      if (ok) restored++; else failed++;
    }

    // 2) Delete docs that exist in current Firestore but NOT in snapshot.
    //    This handles the "I ingested something after the backup and now
    //    need to fully roll back" case. Without this, restore would only
    //    ADD missing docs without removing accidental insertions.
    const currentDocs = await listCollection(coll);
    const currentIds = Object.keys(currentDocs);
    let deleted = 0;
    for (const id of currentIds) {
      if (!targetIds.has(id)) {
        const ok = await deleteDocument(coll, id);
        if (ok) deleted++;
      }
    }

    results[coll] = { restored, failed, deleted };
  }

  return {
    ok: true,
    restored_from: dateKey,
    snapshot_taken_at: snapshot.taken_at,
    results,
  };
}

// ─── List backups operation ────────────────────────────────────────────────

async function listBackups() {
  const files = await listStorageFolder("backups/");
  const manifestPaths = files.filter(f => f.endsWith("/manifest.json"));

  const backups: any[] = [];
  for (const path of manifestPaths) {
    try {
      const bytes = await downloadFromStorage(path);
      const manifest = JSON.parse(new TextDecoder().decode(bytes));
      backups.push(manifest);
    } catch (e) {
      // Skip manifests we can't read
    }
  }

  backups.sort((a, b) => String(b.date).localeCompare(String(a.date)));
  return backups;
}

async function getDownloadUrl(dateKey: string): Promise<string> {
  // Firebase Storage media links are public if bucket allows + API key is passed.
  // For secure download, we return the direct alt=media URL which requires the
  // caller to append a short-lived access token. For now, return the path —
  // the UI will construct the authenticated URL client-side using the Firebase SDK.
  const snapshotPath = `backups/${dateKey}/snapshot.json.gz`;
  return `https://firebasestorage.googleapis.com/v0/b/${STORAGE_BUCKET}/o/${encodeURIComponent(snapshotPath)}?alt=media&key=${FIREBASE_API_KEY}`;
}

// ─── Main handler ──────────────────────────────────────────────────────────

export default async (req: Request, _context: Context) => {
  if (!FIREBASE_API_KEY || !ADMIN_TOKEN) {
    return new Response(JSON.stringify({ error: "missing FIREBASE_API_KEY or MARGINIQ_ADMIN_TOKEN env var" }), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }

  const url = new URL(req.url);
  const action = url.searchParams.get("action");
  const token = url.searchParams.get("token");

  try {
    // Listing is read-only — allow without token for Settings UI to display the list
    if (action === "list") {
      const backups = await listBackups();
      return Response.json({ ok: true, backups });
    }

    // All other operations require admin token
    if (token !== ADMIN_TOKEN) {
      // Scheduled runs arrive with no token but also no query params — allow that path
      const isScheduled = !action && !token && req.method === "POST" && req.headers.get("user-agent")?.includes("netlify");
      if (!isScheduled) {
        return new Response(JSON.stringify({ error: "invalid or missing token" }), {
          status: 403, headers: { "Content-Type": "application/json" },
        });
      }
    }

    if (action === "download") {
      const date = url.searchParams.get("date");
      if (!date) return Response.json({ error: "date param required" }, { status: 400 });
      const downloadUrl = await getDownloadUrl(date);
      return Response.json({ ok: true, url: downloadUrl });
    }

    if (action === "restore") {
      const body = await req.json().catch(() => ({}));
      const date = body.date || url.searchParams.get("date");
      if (!date) return Response.json({ error: "date param required" }, { status: 400 });
      const result = await performRestore(date);
      return Response.json(result);
    }

    if (action === "prune") {
      const result = await pruneOldBackups();
      return Response.json({ ok: true, ...result });
    }

    // Default action: run a backup (manual or scheduled)
    const manifest = await performBackup();
    // Auto-prune after each successful backup so old files don't accumulate.
    const pruneResult = await pruneOldBackups();
    return Response.json({ ok: true, manifest, prune: pruneResult });
  } catch (e: any) {
    console.error("backup error:", e);
    return Response.json({ error: e.message || String(e) }, { status: 500 });
  }
};

// Schedule: 07:00 UTC daily = 03:00 EDT / 02:00 EST
export const config: Config = {
  schedule: "0 7 * * *",
};

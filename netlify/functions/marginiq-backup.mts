import type { Context, Config } from "@netlify/functions";

/**
 * Davis MarginIQ — Daily backup to Firestore (chunked).
 *
 * Runs nightly at 07:00 UTC (3AM EST / 2AM EDT) and on manual trigger.
 * Reads every meaningful collection via Firestore REST API and writes
 * gzipped+base64 chunks to two Firestore collections:
 *
 *   backups/{YYYY-MM-DD}
 *     → manifest doc with taken_at, total_docs, per_collection counts,
 *       per_collection chunk counts, and gzipped/uncompressed byte sizes
 *
 *   backups_chunks/{YYYY-MM-DD}__{collection}__{NNN}
 *     → flat collection of chunks. Doc contains:
 *         date, collection, chunk_index, chunk_count, doc_count,
 *         data_b64  (gzipped JSON of { docId: fields, ... })
 *
 * Why Firestore instead of Firebase Storage?
 *   - Firebase Storage REST requires authenticated writes (no unauthenticated
 *     API-key-only writes without opening up Storage rules). Firestore rules
 *     are already permissive for this app's API key.
 *   - Avoids having to configure Storage rules or service account keys.
 *   - Keeps backup data co-located with source data — one auth path.
 *
 * Doc size: Firestore caps at 1 MiB per doc. We target ~500 KB base64 per
 * chunk, which means ~375 KB gzipped, which typically holds 3-5 MB of raw
 * JSON. Collections larger than that get split across multiple chunks.
 *
 * Retention (pruned in-line after each successful backup):
 *   - Last 30 daily backups: keep all
 *   - Older than 30 days: keep only the 1st of each month (monthly archive)
 *
 * Endpoints:
 *   GET  /.netlify/functions/marginiq-backup                  → run backup
 *   GET  /.netlify/functions/marginiq-backup?action=list      → list all backups
 *   POST /.netlify/functions/marginiq-backup?action=restore   → restore (body: {date})
 *                                                               requires token
 *   GET  /.netlify/functions/marginiq-backup?action=prune     → manual prune
 *                                                               requires token
 *
 * Env vars required (already in Netlify):
 *   FIREBASE_API_KEY          — for Firestore reads + writes
 *   MARGINIQ_ADMIN_TOKEN      — gates restore + prune endpoints (optional for
 *                               backup and list — those are non-destructive)
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
const ADMIN_TOKEN = process.env["MARGINIQ_ADMIN_TOKEN"];

// Target chunk size (base64 string bytes). Firestore caps at 1 MiB per doc
// including ALL fields — leave a big safety margin for other fields and
// encoding overhead.
const CHUNK_TARGET_B64_BYTES = 500_000;

// Collections to back up.
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
      if (resp.status === 404) return out;
      throw new Error(`List ${collection} failed: ${resp.status}`);
    }
    const data: any = await resp.json();
    for (const doc of (data.documents || [])) {
      const parts = String(doc.name).split("/");
      const id = parts[parts.length - 1];
      out[id] = doc.fields || {};
    }
    pageToken = data.nextPageToken;
    pages++;
    if (pages > 200) break; // safety cap
  } while (pageToken);
  return out;
}

async function writeDocument(collection: string, docId: string, fields: any): Promise<boolean> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields }),
  });
  return resp.ok;
}

async function readDocument(collection: string, docId: string): Promise<any | null> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url);
  if (resp.status === 404) return null;
  if (!resp.ok) throw new Error(`Read ${collection}/${docId} failed: ${resp.status}`);
  const data: any = await resp.json();
  return data.fields || {};
}

async function deleteDocument(collection: string, docId: string): Promise<boolean> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const resp = await fetch(url, { method: "DELETE" });
  return resp.ok || resp.status === 404;
}

// List doc IDs in a collection. Used for finding chunks by prefix during prune.
// We use a fieldMask to minimize response size — we only care about IDs.
async function listDocumentIds(collection: string): Promise<string[]> {
  const ids: string[] = [];
  let pageToken: string | undefined;
  let pages = 0;
  do {
    const params = new URLSearchParams({
      key: FIREBASE_API_KEY || "",
      pageSize: "300",
      "mask.fieldPaths": "date",
    });
    if (pageToken) params.set("pageToken", pageToken);
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${collection}?${params.toString()}`;
    const resp = await fetch(url);
    if (!resp.ok) {
      if (resp.status === 404) return ids;
      throw new Error(`List IDs ${collection} failed: ${resp.status}`);
    }
    const data: any = await resp.json();
    for (const doc of (data.documents || [])) {
      const parts = String(doc.name).split("/");
      ids.push(parts[parts.length - 1]);
    }
    pageToken = data.nextPageToken;
    pages++;
    if (pages > 100) break;
  } while (pageToken);
  return ids;
}

// Batch write up to 500 docs in one REST call via batchWrite endpoint.
async function batchWriteDocs(
  collection: string,
  docs: Array<{ docId: string; fields: any }>,
): Promise<{ ok: number; failed: number }> {
  if (docs.length === 0) return { ok: 0, failed: 0 };
  if (docs.length > 500) {
    let ok = 0, failed = 0;
    for (let i = 0; i < docs.length; i += 500) {
      const slice = docs.slice(i, i + 500);
      const r = await batchWriteDocs(collection, slice);
      ok += r.ok; failed += r.failed;
    }
    return { ok, failed };
  }
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default):batchWrite?key=${FIREBASE_API_KEY}`;
  const writes = docs.map(d => ({
    update: {
      name: `projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${d.docId}`,
      fields: d.fields,
    },
  }));
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ writes }),
  });
  if (!resp.ok) {
    // Fall back to per-doc writes so one bad doc doesn't fail the whole batch
    let ok = 0, failed = 0;
    for (const d of docs) {
      const r = await writeDocument(collection, d.docId, d.fields);
      if (r) ok++; else failed++;
    }
    return { ok, failed };
  }
  const data: any = await resp.json();
  let ok = 0, failed = 0;
  for (const status of (data.status || [])) {
    if (!status.code || status.code === 0) ok++; else failed++;
  }
  return { ok, failed };
}

// Batch delete up to 500 docs in one REST call via batchWrite endpoint.
// v2.40.4: previously we did sequential DELETEs which, with 50+ chunks to
// purge when re-running the same day's backup, blew past Netlify's 10s
// sync-function timeout.
async function batchDeleteDocs(
  collection: string,
  docIds: string[],
): Promise<{ ok: number; failed: number }> {
  if (docIds.length === 0) return { ok: 0, failed: 0 };
  if (docIds.length > 500) {
    let ok = 0, failed = 0;
    for (let i = 0; i < docIds.length; i += 500) {
      const slice = docIds.slice(i, i + 500);
      const r = await batchDeleteDocs(collection, slice);
      ok += r.ok; failed += r.failed;
    }
    return { ok, failed };
  }
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default):batchWrite?key=${FIREBASE_API_KEY}`;
  const writes = docIds.map(id => ({
    delete: `projects/${PROJECT_ID}/databases/(default)/documents/${collection}/${id}`,
  }));
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ writes }),
  });
  if (!resp.ok) {
    // Fall back to per-doc deletes so one bad doc doesn't fail the whole batch
    let ok = 0, failed = 0;
    for (const id of docIds) {
      const r = await deleteDocument(collection, id);
      if (r) ok++; else failed++;
    }
    return { ok, failed };
  }
  const data: any = await resp.json();
  let ok = 0, failed = 0;
  for (const status of (data.status || [])) {
    if (!status.code || status.code === 0) ok++; else failed++;
  }
  return { ok, failed };
}

// Firestore REST value wrappers
function toFirestoreStringField(s: string) { return { stringValue: s }; }
function toFirestoreIntField(n: number) { return { integerValue: String(n) }; }

// ─── Chunking logic ────────────────────────────────────────────────────────

type Chunk = {
  chunkIndex: number;
  docCount: number;
  dataB64: string;
  uncompressedBytes: number;
};

async function chunkCollection(docs: DocMap): Promise<Chunk[]> {
  const { gzipSync } = await import("node:zlib");
  const entries = Object.entries(docs);
  if (entries.length === 0) {
    const gz = gzipSync(Buffer.from("{}", "utf8"));
    return [{
      chunkIndex: 0,
      docCount: 0,
      dataB64: gz.toString("base64"),
      uncompressedBytes: 2,
    }];
  }
  const chunks: Chunk[] = [];
  let current: Record<string, any> = {};
  let currentCount = 0;
  let chunkIndex = 0;

  const flush = () => {
    if (currentCount === 0) return;
    const json = JSON.stringify(current);
    const gz = gzipSync(Buffer.from(json, "utf8"));
    const b64 = gz.toString("base64");
    chunks.push({
      chunkIndex,
      docCount: currentCount,
      dataB64: b64,
      uncompressedBytes: json.length,
    });
    chunkIndex++;
    current = {};
    currentCount = 0;
  };

  // Check size every 100 docs to avoid re-gzipping on every add.
  let sinceLastCheck = 0;
  for (const [id, fields] of entries) {
    current[id] = fields;
    currentCount++;
    sinceLastCheck++;
    if (sinceLastCheck >= 100) {
      sinceLastCheck = 0;
      const json = JSON.stringify(current);
      const gz = gzipSync(Buffer.from(json, "utf8"));
      if (gz.toString("base64").length >= CHUNK_TARGET_B64_BYTES) {
        flush();
      }
    }
  }
  flush();
  return chunks;
}

// Decode a Firestore map-value field back to a plain { key: number } dict.
function decodeFirestoreMap(field: any): Record<string, number> {
  const out: Record<string, number> = {};
  const fields = field?.mapValue?.fields || {};
  for (const [k, v] of Object.entries<any>(fields)) {
    out[k] = parseInt(v?.integerValue || "0", 10);
  }
  return out;
}

// ─── Backup operation ──────────────────────────────────────────────────────

async function performBackup(dateOverride?: string) {
  const now = new Date();
  const dateKey = dateOverride || now.toISOString().slice(0, 10);
  const takenAt = now.toISOString();

  const perCollection: Record<string, number> = {};
  const perCollectionChunks: Record<string, number> = {};
  let totalDocs = 0;
  let totalCompressed = 0;
  let totalUncompressed = 0;

  // If a backup for this date already exists, delete its stale chunks first
  // so we don't leave orphan chunks from a failed previous run.
  await deleteBackupChunks(dateKey);

  for (const coll of COLLECTIONS) {
    const docs = await listCollection(coll);
    const count = Object.keys(docs).length;
    perCollection[coll] = count;
    totalDocs += count;

    const chunks = await chunkCollection(docs);
    perCollectionChunks[coll] = chunks.length;

    const chunkDocs = chunks.map(ch => ({
      docId: `${dateKey}__${coll}__${String(ch.chunkIndex).padStart(3, "0")}`,
      fields: {
        date: toFirestoreStringField(dateKey),
        collection: toFirestoreStringField(coll),
        chunk_index: toFirestoreIntField(ch.chunkIndex),
        chunk_count: toFirestoreIntField(chunks.length),
        doc_count: toFirestoreIntField(ch.docCount),
        uncompressed_bytes: toFirestoreIntField(ch.uncompressedBytes),
        data_b64: toFirestoreStringField(ch.dataB64),
      },
    }));

    const result = await batchWriteDocs("backups_chunks", chunkDocs);
    if (result.failed > 0) {
      throw new Error(`Failed to write ${result.failed}/${chunkDocs.length} chunks for ${coll}`);
    }

    for (const ch of chunks) {
      totalCompressed += Math.ceil(ch.dataB64.length * 3 / 4);
      totalUncompressed += ch.uncompressedBytes;
    }
  }

  const manifest = {
    date: toFirestoreStringField(dateKey),
    taken_at: toFirestoreStringField(takenAt),
    project: toFirestoreStringField(PROJECT_ID),
    total_docs: toFirestoreIntField(totalDocs),
    per_collection: {
      mapValue: {
        fields: Object.fromEntries(
          Object.entries(perCollection).map(([k, v]) => [k, toFirestoreIntField(v)]),
        ),
      },
    },
    per_collection_chunks: {
      mapValue: {
        fields: Object.fromEntries(
          Object.entries(perCollectionChunks).map(([k, v]) => [k, toFirestoreIntField(v)]),
        ),
      },
    },
    compressed_bytes: toFirestoreIntField(totalCompressed),
    uncompressed_bytes: toFirestoreIntField(totalUncompressed),
    collections_captured: toFirestoreIntField(COLLECTIONS.length),
  };

  const ok = await writeDocument("backups", dateKey, manifest);
  if (!ok) throw new Error(`Failed to write manifest for ${dateKey}`);

  // Return decoded manifest for the API response
  return {
    date: dateKey,
    taken_at: takenAt,
    project: PROJECT_ID,
    total_docs: totalDocs,
    per_collection: perCollection,
    per_collection_chunks: perCollectionChunks,
    compressed_bytes: totalCompressed,
    uncompressed_bytes: totalUncompressed,
    collections_captured: COLLECTIONS.length,
  };
}

// ─── Restore operation ─────────────────────────────────────────────────────

async function performRestore(dateKey: string): Promise<any> {
  const manifestFields = await readDocument("backups", dateKey);
  if (!manifestFields) throw new Error(`No backup found for date ${dateKey}`);

  const perCollection = decodeFirestoreMap(manifestFields.per_collection);
  const perCollectionChunks = decodeFirestoreMap(manifestFields.per_collection_chunks);

  const { gunzipSync } = await import("node:zlib");
  const results: Record<string, { restored: number; failed: number; deleted: number }> = {};

  for (const coll of Object.keys(perCollection)) {
    const chunkCount = perCollectionChunks[coll] ?? 1;

    // Read each chunk, decompress, merge into one DocMap for this collection
    const collectionDocs: DocMap = {};
    for (let i = 0; i < chunkCount; i++) {
      const chunkId = `${dateKey}__${coll}__${String(i).padStart(3, "0")}`;
      const chunkFields = await readDocument("backups_chunks", chunkId);
      if (!chunkFields) continue;
      const dataB64 = chunkFields.data_b64?.stringValue || "";
      if (!dataB64) continue;
      const gz = Buffer.from(dataB64, "base64");
      const json = gunzipSync(gz).toString("utf8");
      const parsed = JSON.parse(json);
      Object.assign(collectionDocs, parsed);
    }

    // Write all backed-up docs to the target collection via batchWrite
    const docsToWrite = Object.entries(collectionDocs).map(([docId, fields]) => ({
      docId, fields,
    }));
    const writeResult = await batchWriteDocs(coll, docsToWrite);

    // Delete docs that exist now but weren't in the backup (full rollback)
    const targetIds = new Set(Object.keys(collectionDocs));
    const currentDocs = await listCollection(coll);
    let deleted = 0;
    for (const id of Object.keys(currentDocs)) {
      if (!targetIds.has(id)) {
        const ok = await deleteDocument(coll, id);
        if (ok) deleted++;
      }
    }

    results[coll] = {
      restored: writeResult.ok,
      failed: writeResult.failed,
      deleted,
    };
  }

  return {
    ok: true,
    restored_from: dateKey,
    snapshot_taken_at: manifestFields.taken_at?.stringValue || null,
    results,
  };
}

// ─── Prune old backups ─────────────────────────────────────────────────────

async function deleteBackupChunks(dateKey: string): Promise<number> {
  const allIds = await listDocumentIds("backups_chunks");
  const matching = allIds.filter(id => id.startsWith(dateKey + "__"));
  if (matching.length === 0) return 0;
  // v2.40.4: batched delete (was sequential, caused timeouts when re-running
  // same day's backup with 50+ existing chunks to purge).
  const result = await batchDeleteDocs("backups_chunks", matching);
  return result.ok;
}

async function pruneOldBackups() {
  const manifestDocs = await listCollection("backups");
  const dates = Object.keys(manifestDocs).sort();
  const today = new Date();

  const deleted: string[] = [];
  const kept: string[] = [];

  for (const dateKey of dates) {
    const backupDate = new Date(dateKey + "T00:00:00Z");
    if (isNaN(backupDate.getTime())) continue;
    const ageDays = (today.getTime() - backupDate.getTime()) / (1000 * 60 * 60 * 24);
    const isFirstOfMonth = backupDate.getUTCDate() === 1;
    const shouldKeep = ageDays <= 30 || isFirstOfMonth;

    if (shouldKeep) {
      kept.push(dateKey);
    } else {
      await deleteBackupChunks(dateKey);
      await deleteDocument("backups", dateKey);
      deleted.push(dateKey);
    }
  }
  return { kept_dates: kept, deleted_dates: deleted };
}

// ─── List backups operation ────────────────────────────────────────────────

async function listBackups() {
  const manifestDocs = await listCollection("backups");
  const backups: any[] = [];
  for (const [dateKey, fields] of Object.entries<any>(manifestDocs)) {
    try {
      backups.push({
        date: fields.date?.stringValue || dateKey,
        taken_at: fields.taken_at?.stringValue || null,
        project: fields.project?.stringValue || PROJECT_ID,
        total_docs: parseInt(fields.total_docs?.integerValue || "0", 10),
        per_collection: decodeFirestoreMap(fields.per_collection),
        per_collection_chunks: decodeFirestoreMap(fields.per_collection_chunks),
        compressed_bytes: parseInt(fields.compressed_bytes?.integerValue || "0", 10),
        uncompressed_bytes: parseInt(fields.uncompressed_bytes?.integerValue || "0", 10),
        collections_captured: parseInt(fields.collections_captured?.integerValue || "0", 10),
      });
    } catch {
      // skip
    }
  }
  backups.sort((a, b) => String(b.date).localeCompare(String(a.date)));
  return backups;
}

// ─── Main handler ──────────────────────────────────────────────────────────

export default async (req: Request, _context: Context) => {
  if (!FIREBASE_API_KEY) {
    return new Response(JSON.stringify({ error: "missing FIREBASE_API_KEY env var" }), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }

  const url = new URL(req.url);
  const action = url.searchParams.get("action");
  const token = url.searchParams.get("token");

  const requiresToken = action === "restore" || action === "prune";
  if (requiresToken) {
    if (!ADMIN_TOKEN) {
      return new Response(JSON.stringify({ error: "MARGINIQ_ADMIN_TOKEN not configured — set it in Netlify env vars to use restore/prune" }), {
        status: 500, headers: { "Content-Type": "application/json" },
      });
    }
    if (token !== ADMIN_TOKEN) {
      return new Response(JSON.stringify({ error: "invalid or missing admin token (required for restore/prune)" }), {
        status: 403, headers: { "Content-Type": "application/json" },
      });
    }
  }

  try {
    if (action === "list") {
      const backups = await listBackups();
      return Response.json({ ok: true, backups });
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

    // Default: run a backup
    const manifest = await performBackup();
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

import type { Context } from "@netlify/functions";
import { gunzipSync } from "node:zlib";

/**
 * Davis MarginIQ — Source File Re-Parse (v2.52.3)
 *
 * Reads a previously-staged raw file from source_files_raw +
 * source_file_chunks, decompresses it, and feeds it back through the
 * appropriate ingest dispatcher. This means parser bugs can be fixed
 * once and applied to all historical data without re-fetching from Gmail.
 *
 * Endpoints:
 *   GET  /.netlify/functions/marginiq-reparse
 *        Lists all files in source_files_raw with metadata.
 *   GET  /.netlify/functions/marginiq-reparse?file_id=XXX
 *        Returns metadata for a specific file.
 *   POST /.netlify/functions/marginiq-reparse
 *        body: { file_id, action: 'reparse' }
 *        Reads + decompresses + dispatches to the right ingest function
 *        based on file_id prefix ('nuvizz__...' → marginiq-nuvizz-ingest,
 *        'ddis__...' → marginiq-ddis-ingest-background).
 *
 * Why this matters: We told user "we keep all data forever now" — they
 * pushed back that all earlier ingests dropped fields. Even with v2.52.0
 * fixing parsers going forward, historical data is incomplete. This
 * function lets us fix that without asking them to re-upload anything.
 *
 * Env vars: FIREBASE_API_KEY
 */

const PROJECT_ID = "davismarginiq";
const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

async function fsGetDoc(coll: string, docId: string): Promise<any | null> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${coll}/${encodeURIComponent(docId)}?key=${FIREBASE_API_KEY}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
}

async function fsListDocs(coll: string, limit = 100): Promise<any[]> {
  const out: any[] = [];
  let pageToken: string | undefined;
  do {
    const params = new URLSearchParams({
      key: FIREBASE_API_KEY || "",
      pageSize: String(Math.min(300, limit - out.length)),
    });
    if (pageToken) params.set("pageToken", pageToken);
    const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${coll}?${params.toString()}`;
    const r = await fetch(url);
    if (!r.ok) break;
    const data: any = await r.json();
    out.push(...(data.documents || []));
    pageToken = data.nextPageToken;
    if (out.length >= limit) break;
  } while (pageToken);
  return out;
}

function fromFs(v: any): any {
  if (!v) return null;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return v.doubleValue;
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  return null;
}

function unwrapDoc(doc: any): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(doc.fields || {})) out[k] = fromFs(v);
  return out;
}

async function loadFileBytes(fileId: string): Promise<Buffer | null> {
  // Discover chunk count from parent doc
  const parent = await fsGetDoc("source_files_raw", fileId);
  if (!parent) return null;
  const meta = unwrapDoc(parent);
  const chunkCount: number = Number(meta.chunk_count || 0);
  if (chunkCount <= 0) return null;

  // Pull every chunk in order
  const chunks: string[] = [];
  for (let i = 0; i < chunkCount; i++) {
    const chunkId = `${fileId}__${String(i).padStart(3, "0")}`;
    const c = await fsGetDoc("source_file_chunks", chunkId);
    if (!c) return null;
    const cf = unwrapDoc(c);
    chunks[i] = cf.data_b64 || "";
  }
  const b64 = chunks.join("");
  const gz = Buffer.from(b64, "base64");
  return gunzipSync(gz);
}

async function dispatchToParser(siteOrigin: string, fileId: string, filename: string, source: string, bytes: Buffer): Promise<{ ok: boolean; result?: any; error?: string }> {
  // Decompress already happened — bytes is the original CSV/XLSX bytes.
  // For text-format sources (NuVizz, DDIS), decode to text and POST to the
  // appropriate auto-ingest function in reparse_csv mode (added in v2.52.3).
  // The auto-ingest function then runs its current parser (with v2.52.0+
  // raw-row preservation) and dispatches to the ingest function — same
  // pipeline that runs nightly, just with input substituted.
  if (source === "nuvizz") {
    const csvText = bytes.toString("utf8");
    const r = await fetch(`${siteOrigin}/.netlify/functions/marginiq-nuvizz-auto-ingest?mode=reparse_csv`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ csv_text: csvText, file_id: fileId, filename }),
    });
    const data: any = await r.json().catch(() => ({}));
    return { ok: r.ok && data.ok !== false, result: data };
  }
  if (source === "ddis") {
    const csvText = bytes.toString("utf8");
    const r = await fetch(`${siteOrigin}/.netlify/functions/marginiq-ddis-auto-ingest?mode=reparse_csv`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ csv_text: csvText, file_id: fileId, filename }),
    });
    const data: any = await r.json().catch(() => ({}));
    return { ok: r.ok && data.ok !== false, result: data };
  }
  // Uline DAS files are XLSX (binary). Reparse path TBD — would require an
  // XLSX-aware parser endpoint. For now, return the file ready for manual
  // re-import via the UI.
  if (source === "uline" || source === "das") {
    return {
      ok: true,
      result: {
        action: "decompressed_only",
        source,
        file_id: fileId,
        filename,
        bytes_length: bytes.length,
        note: "Uline DAS files are XLSX (binary). Re-import via the UI Data Ingest tab to re-run the parser. The decompressed bytes are available via the GET endpoint on this file_id.",
      },
    };
  }
  return { ok: false, error: `Unknown source '${source}' — no reparse pipeline defined yet` };
}

export default async (req: Request, _context: Context) => {
  const url = new URL(req.url);

  if (req.method === "GET") {
    const fileIdParam = url.searchParams.get("file_id");
    if (fileIdParam) {
      const doc = await fsGetDoc("source_files_raw", fileIdParam);
      if (!doc) return json({ ok: false, error: "file_id not found" }, 404);
      return json({ ok: true, file: { id: fileIdParam, ...unwrapDoc(doc) } });
    }
    // List all
    const docs = await fsListDocs("source_files_raw", 200);
    const files = docs.map(d => ({
      id: d.name?.split("/").pop(),
      ...unwrapDoc(d),
    }));
    // Group by source
    const bySource: Record<string, number> = {};
    for (const f of files) {
      const s = f.source || "unknown";
      bySource[s] = (bySource[s] || 0) + 1;
    }
    return json({
      ok: true,
      total: files.length,
      by_source: bySource,
      files: files.slice(0, 50), // avoid huge response
      truncated: files.length > 50,
    });
  }

  if (req.method !== "POST") return new Response("method not allowed", { status: 405 });

  try {
    const body = await req.json();
    const action = body.action || "reparse";

    // BATCH REPARSE: re-runs reparse for every staged file from a given
    // source. The purpose: parsers were fixed in v2.52.0/v2.52.1 to
    // preserve raw rows, but historical data ingested before that fix
    // is still missing the `raw` field. Running this against every old
    // file applies the new parser logic and writes the missing data.
    //
    // body: { action: 'batch_reparse', source: 'nuvizz'|'ddis', limit?: 100 }
    if (action === "batch_reparse") {
      const source = body.source;
      const limit = Math.min(Number(body.limit || 50), 200);
      if (!source) return json({ ok: false, error: "source required (e.g. 'nuvizz')" }, 400);

      const baseUrl = `${url.protocol}//${url.host}`;
      const allDocs = await fsListDocs("source_files_raw", 500);
      const matching = allDocs
        .map(d => ({ id: d.name?.split("/").pop()!, ...unwrapDoc(d) }))
        .filter(f => f.source === source)
        .slice(0, limit);

      const results: any[] = [];
      let okCount = 0;
      let failCount = 0;

      for (const f of matching) {
        try {
          const bytes = await loadFileBytes(f.id);
          if (!bytes) {
            results.push({ file_id: f.id, ok: false, error: "Failed to decompress" });
            failCount++;
            continue;
          }
          const filename = String(f.filename || f.id);
          const r = await dispatchToParser(baseUrl, f.id, filename, source, bytes);
          results.push({
            file_id: f.id,
            filename,
            ok: r.ok,
            stops_or_payments: r.result?.stops_parsed ?? r.result?.payments_parsed ?? null,
            error: r.error,
          });
          if (r.ok) okCount++; else failCount++;
        } catch (e: any) {
          results.push({ file_id: f.id, ok: false, error: e?.message || String(e) });
          failCount++;
        }
      }

      return json({
        ok: failCount === 0,
        action: "batch_reparse",
        source,
        files_processed: matching.length,
        succeeded: okCount,
        failed: failCount,
        total_in_collection: allDocs.filter(d => unwrapDoc(d).source === source).length,
        results,
      });
    }

    const fileId = body.file_id;
    if (!fileId) {
      return json({ ok: false, error: "file_id required (or use action: 'batch_reparse' with source)" }, 400);
    }

    // Read file metadata
    const metaDoc = await fsGetDoc("source_files_raw", fileId);
    if (!metaDoc) return json({ ok: false, error: "file_id not found in source_files_raw" }, 404);
    const meta = unwrapDoc(metaDoc);
    const source = meta.source || "unknown";

    // Decompress
    const bytes = await loadFileBytes(fileId);
    if (!bytes) return json({ ok: false, error: "Failed to load file bytes — chunks may be missing" }, 500);

    if (action === "decompress" || action === "preview") {
      // Just return file info without re-parsing
      return json({
        ok: true,
        file_id: fileId,
        source,
        size_bytes: bytes.length,
        text_preview: bytes.toString("utf8").slice(0, 1000),
      });
    }

    if (action === "reparse") {
      const baseUrl = `${url.protocol}//${url.host}`;
      const filename = String(meta.filename || fileId);
      const result = await dispatchToParser(baseUrl, fileId, filename, source, bytes);
      return json({ ok: result.ok, result: result.result, error: result.error });
    }

    return json({ ok: false, error: `Unknown action '${action}' — supported: decompress, preview, reparse, batch_reparse` }, 400);
  } catch (e: any) {
    console.error("reparse failed:", e);
    return json({ ok: false, error: e?.message || String(e) }, 500);
  }
};

import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Historical Backfill Dispatcher (v2.52.6)
 *
 * Sync entrypoint that handles:
 *   - GET (preview/listing)  — runs inline, no Gmail downloads (fast)
 *   - POST { dry_run: true } — runs inline preview (fast)
 *   - POST                   — fire-and-forget to background, returns 202
 *
 * The background function (-background.mts) does the heavy work: Gmail
 * downloads, gzip, chunked Firestore writes (90+ MB across 50+ chunks).
 */

const PROJECT_ID = "davismarginiq";

const SEARCH_QUERIES: Record<string, string> = {
  nuvizz: "from:nuvizzapps@nuvizzapps.com has:attachment",
  ddis:   "from:noreply@ddisinc.com has:attachment",
};

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (typeof v === "string") return { stringValue: v };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(toFsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

async function fsGetDoc(coll: string, docId: string, apiKey: string): Promise<any | null> {
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/${coll}/${encodeURIComponent(docId)}?key=${apiKey}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return await r.json();
}

type TokenDoc = { docId: string; email: string; refresh_token: string };

async function listConnectedAccounts(apiKey: string): Promise<TokenDoc[]> {
  const accounts: Record<string, TokenDoc> = {};
  const url = `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config?key=${apiKey}&pageSize=100`;
  const r = await fetch(url);
  if (!r.ok) return [];
  const data: any = await r.json();
  for (const d of (data.documents || [])) {
    const docId = (d.name || "").split("/").pop() || "";
    if (docId !== "gmail_tokens" && !docId.startsWith("gmail_tokens_")) continue;
    const f = d.fields || {};
    const refreshToken = f.refresh_token?.stringValue;
    const email = f.email?.stringValue || "unknown";
    if (!refreshToken) continue;
    const existing = accounts[email];
    if (!existing || (existing.docId === "gmail_tokens" && docId !== "gmail_tokens")) {
      accounts[email] = { docId, email, refresh_token: refreshToken };
    }
  }
  return Object.values(accounts);
}

async function getFreshAccessToken(clientId: string, clientSecret: string, refreshToken: string): Promise<string | null> {
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
      grant_type: "refresh_token",
    }),
  });
  if (!r.ok) return null;
  const data: any = await r.json();
  return data.access_token || null;
}

async function gmailSearch(accessToken: string, q: string, maxResults = 100): Promise<Array<{ id: string }>> {
  const url = `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${encodeURIComponent(q)}&maxResults=${maxResults}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!r.ok) return [];
  const data: any = await r.json();
  return data.messages || [];
}

async function gmailGetMessage(accessToken: string, messageId: string): Promise<any | null> {
  const url = `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}?format=metadata&metadataHeaders=Subject`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
  if (!r.ok) return null;
  return await r.json();
}

function getHeader(payload: any, name: string): string {
  const headers = payload?.headers || [];
  const h = headers.find((x: any) => (x.name || "").toLowerCase() === name.toLowerCase());
  return h?.value || "";
}

function sanitizeFileId(filename: string): string {
  return filename.replace(/[^a-z0-9._-]/gi, "_").slice(0, 140);
}

// Lightweight preview: search Gmail, list message IDs and subjects, check
// which are already staged. Doesn't download attachments — fast enough
// for sync responses.
async function preview(source: string, limit: number, olderThan: string | undefined, newerThan: string | undefined, apiKey: string, clientId: string, clientSecret: string) {
  let query = SEARCH_QUERIES[source];
  if (newerThan) query += ` after:${newerThan.replace(/-/g, "/")}`;
  if (olderThan) query += ` before:${olderThan.replace(/-/g, "/")}`;

  const accounts = await listConnectedAccounts(apiKey);
  if (accounts.length === 0) return { ok: false, error: "No connected Gmail accounts" };

  const candidates: Array<{ messageId: string; subject: string; account: string; internalDate: number }> = [];
  for (const acct of accounts) {
    if (candidates.length >= limit * 3) break;
    const accessToken = await getFreshAccessToken(clientId, clientSecret, acct.refresh_token);
    if (!accessToken) continue;
    const msgs = await gmailSearch(accessToken, query, Math.min(limit * 3, 100));
    for (const m of msgs) {
      if (candidates.length >= limit * 3) break;
      const det = await gmailGetMessage(accessToken, m.id);
      if (!det) continue;
      candidates.push({
        messageId: m.id,
        subject: getHeader(det.payload, "Subject"),
        account: acct.email,
        internalDate: Number(det.internalDate || 0),
      });
    }
  }

  // Check which are already staged. We need a filename to compute file_id but
  // metadata-only fetch doesn't include attachments. So we estimate by checking
  // any source_files_raw doc whose email_message_id matches.
  let alreadyStaged = 0;
  const stagedMessageIds = new Set<string>();
  // Sample-only check: if 0 are staged, we get a clean signal; otherwise we
  // err toward processing (idempotent — re-staging is a no-op).
  for (const c of candidates) {
    // Look up by message_id field — would need a query. Skipping for speed;
    // background function does the precise check before staging.
  }

  return {
    ok: true,
    source,
    query,
    messages_found: candidates.length,
    candidates: candidates.slice(0, limit).map(c => ({
      message_id: c.messageId,
      subject: c.subject.slice(0, 80),
      account: c.account,
      email_date: new Date(c.internalDate).toISOString(),
    })),
    note: "Use POST without dry_run to actually stage them. Background will skip already-staged.",
  };
}

export default async (req: Request, _context: Context) => {
  const CLIENT_ID = process.env["GOOGLE_CLIENT_ID"];
  const CLIENT_SECRET = process.env["GOOGLE_CLIENT_SECRET"];
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  if (!CLIENT_ID || !CLIENT_SECRET || !FIREBASE_API_KEY) {
    return json({ error: "OAuth/Firebase not configured" }, 500);
  }

  const url = new URL(req.url);

  // GET — preview/listing
  if (req.method === "GET") {
    const source = url.searchParams.get("source") || "";
    if (!source || !SEARCH_QUERIES[source]) {
      return json({ ok: false, error: `source must be one of: ${Object.keys(SEARCH_QUERIES).join(", ")}` }, 400);
    }
    const limit = Math.min(Number(url.searchParams.get("limit") || "10"), 50);
    const olderThan = url.searchParams.get("older_than") || undefined;
    const newerThan = url.searchParams.get("newer_than") || undefined;
    const result = await preview(source, limit, olderThan, newerThan, FIREBASE_API_KEY, CLIENT_ID, CLIENT_SECRET);
    return json(result);
  }

  if (req.method !== "POST") return new Response("method not allowed", { status: 405 });

  try {
    const body = await req.json();
    const source = body.source || "";
    const limit = Math.min(Number(body.limit || 10), 50);
    const olderThan = body.older_than;
    const newerThan = body.newer_than;
    const dryRun = !!body.dry_run;

    if (!source || !SEARCH_QUERIES[source]) {
      return json({ ok: false, error: `source must be one of: ${Object.keys(SEARCH_QUERIES).join(", ")}` }, 400);
    }

    if (dryRun) {
      const result = await preview(source, limit, olderThan, newerThan, FIREBASE_API_KEY, CLIENT_ID, CLIENT_SECRET);
      return json(result);
    }

    // Real run — fire-and-forget to background
    const targetUrl = `${url.protocol}//${url.host}/.netlify/functions/marginiq-historical-backfill-background`;
    fetch(targetUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).catch(e => console.error("dispatch failed:", e));

    return json({
      ok: true,
      dispatched: true,
      source,
      limit,
      message: `Background backfill started. Files will be staged to source_files_raw over the next ~60-300s. Poll historical_backfill_logs collection (filter source='${source}', most recent state='running'/'complete') for progress.`,
      poll_command: `curl -s 'https://davis-marginiq.netlify.app/.netlify/functions/marginiq-reparse' | jq '.by_source'`,
    }, 202);
  } catch (e: any) {
    return json({ ok: false, error: e?.message || String(e) }, 500);
  }
};

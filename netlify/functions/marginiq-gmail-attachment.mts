import type { Context, Config } from "@netlify/functions";

// Slugify an email address into a valid Firestore doc ID suffix.
// Mirrors the helper in marginiq-gmail-callback.mts.
function emailSlug(email: string): string {
  return String(email || "unknown")
    .toLowerCase()
    .replace(/@/g, "_at_")
    .replace(/[^a-z0-9_]/g, "_")
    .slice(0, 100);
}

export default async (req: Request, context: Context) => {
  if (req.method !== "POST") {
    return json({ error: "POST required" }, 405);
  }

  const CLIENT_ID = process.env["GOOGLE_CLIENT_ID"];
  const CLIENT_SECRET = process.env["GOOGLE_CLIENT_SECRET"];
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  const PROJECT_ID = "davismarginiq";

  if (!CLIENT_ID || !CLIENT_SECRET || !FIREBASE_API_KEY) {
    return json({ error: "OAuth not configured" }, 500);
  }

  try {
    const body = await req.json();
    const { messageId, attachmentId } = body;
    if (!messageId || !attachmentId) {
      return json({ error: "Missing messageId or attachmentId" }, 400);
    }

    // v2.40: caller may pin which account's token to use (for multi-account
    // routing). Preference order:
    //   1. body.account_doc_id — explicit doc id (e.g. gmail_tokens_billing_at_…)
    //   2. body.account_email  — compute docId from the email
    //   3. fallback: legacy singleton `gmail_tokens`
    //
    // Fallback is important for any old UI code path that doesn't send
    // account metadata, and for the initial legacy token that pre-dates
    // per-account storage.
    const accountDocId: string = (body.account_doc_id || "").trim();
    const accountEmail: string = (body.account_email || "").trim();

    const candidates: string[] = [];
    if (accountDocId) candidates.push(accountDocId);
    if (accountEmail) candidates.push(`gmail_tokens_${emailSlug(accountEmail)}`);
    candidates.push("gmail_tokens"); // legacy fallback

    let refreshToken: string | null = null;
    let resolvedDocId = "";
    for (const docId of candidates) {
      const resp = await fetch(
        `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/${docId}?key=${FIREBASE_API_KEY}`
      );
      if (!resp.ok) continue;
      const data = await resp.json();
      const tok = data?.fields?.refresh_token?.stringValue;
      if (tok) { refreshToken = tok; resolvedDocId = docId; break; }
    }

    if (!refreshToken) {
      return json({ error: "Gmail not connected for this account. Reconnect Gmail." }, 400);
    }

    // Refresh access token
    const refreshResp = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: CLIENT_ID,
        client_secret: CLIENT_SECRET,
        refresh_token: refreshToken,
        grant_type: "refresh_token",
      }),
    });
    const refreshData = await refreshResp.json();
    if (!refreshResp.ok || !refreshData.access_token) {
      return json({ error: "Token refresh failed (" + resolvedDocId + ")" }, 500);
    }

    // Fetch attachment
    const attResp = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}/attachments/${attachmentId}`,
      { headers: { Authorization: `Bearer ${refreshData.access_token}` } }
    );
    const attData = await attResp.json();
    if (!attResp.ok) {
      return json({ error: "Attachment fetch failed: " + JSON.stringify(attData).substring(0, 300) }, 500);
    }

    // Gmail returns base64url — convert to standard base64
    const b64 = (attData.data || "").replace(/-/g, "+").replace(/_/g, "/");
    return json({ data: b64, size: attData.size || 0, account_doc_id: resolvedDocId });
  } catch (err: any) {
    return json({ error: err.message || "Proxy error" }, 500);
  }
};

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

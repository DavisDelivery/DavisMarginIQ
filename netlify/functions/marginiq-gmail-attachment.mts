import type { Context, Config } from "@netlify/functions";

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
    const { messageId, attachmentId } = await req.json();
    if (!messageId || !attachmentId) {
      return json({ error: "Missing messageId or attachmentId" }, 400);
    }

    // Load refresh token
    const tokDocResp = await fetch(
      `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/gmail_tokens?key=${FIREBASE_API_KEY}`
    );
    if (!tokDocResp.ok) {
      return json({ error: "Gmail not connected." }, 400);
    }
    const tokDoc = await tokDocResp.json();
    const refreshToken = tokDoc?.fields?.refresh_token?.stringValue;
    if (!refreshToken) {
      return json({ error: "No refresh_token. Reconnect Gmail." }, 400);
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
      return json({ error: "Token refresh failed" }, 500);
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
    return json({ data: b64, size: attData.size || 0 });
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

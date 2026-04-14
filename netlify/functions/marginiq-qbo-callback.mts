import type { Context, Config } from "@netlify/functions";

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  const code = url.searchParams.get("code");
  const realmId = url.searchParams.get("realmId");
  const error = url.searchParams.get("error");
  const SITE_URL = "https://davis-marginiq.netlify.app";
  const REDIRECT_URI = "https://davis-marginiq.netlify.app/.netlify/functions/marginiq-qbo-callback";

  if (error || !code || !realmId) {
    return Response.redirect(`${SITE_URL}?qbo=error&reason=${error || "missing_params"}`, 302);
  }

  const QBO_CLIENT_ID = process.env["QBO_CLIENT_ID"];
  const QBO_CLIENT_SECRET = process.env["QBO_CLIENT_SECRET"];
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  const PROJECT_ID = "davismarginiq";

  if (!QBO_CLIENT_ID || !QBO_CLIENT_SECRET || !FIREBASE_API_KEY) {
    const missing = [!QBO_CLIENT_ID&&"CLIENT_ID",!QBO_CLIENT_SECRET&&"SECRET",!FIREBASE_API_KEY&&"FB_KEY"].filter(Boolean).join(",");
    return Response.redirect(`${SITE_URL}?qbo=error&reason=missing_env_${missing}`, 302);
  }

  try {
    const tokenResp = await fetch("https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: "Basic " + btoa(`${QBO_CLIENT_ID}:${QBO_CLIENT_SECRET}`),
        Accept: "application/json",
      },
      body: new URLSearchParams({ grant_type: "authorization_code", code, redirect_uri: REDIRECT_URI }),
    });

    if (!tokenResp.ok) {
      const errBody = await tokenResp.text();
      // Put the actual Intuit error in the URL so we can see it
      return Response.redirect(`${SITE_URL}?qbo=error&reason=token_exchange&detail=${encodeURIComponent(errBody.substring(0,400))}`, 302);
    }

    const tokens = await tokenResp.json();

    if (!tokens.access_token) {
      return Response.redirect(`${SITE_URL}?qbo=error&reason=no_token`, 302);
    }

    const firestoreResp = await fetch(
      `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/qbo_tokens?key=${FIREBASE_API_KEY}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          fields: {
            access_token: { stringValue: tokens.access_token },
            refresh_token: { stringValue: tokens.refresh_token },
            realm_id: { stringValue: realmId },
            expires_at: { integerValue: String(Date.now() + tokens.expires_in * 1000) },
            refresh_expires_at: { integerValue: String(Date.now() + (tokens.x_refresh_token_expires_in || 8726400) * 1000) },
            updated_at: { stringValue: new Date().toISOString() },
          },
        }),
      }
    );

    if (!firestoreResp.ok) {
      const errText = await firestoreResp.text();
      return Response.redirect(`${SITE_URL}?qbo=error&reason=firestore_write&detail=${encodeURIComponent(errText.substring(0,400))}`, 302);
    }

    return Response.redirect(`${SITE_URL}?qbo=connected`, 302);
  } catch (e: any) {
    return Response.redirect(`${SITE_URL}?qbo=error&reason=crash_${encodeURIComponent(e.message||"unknown")}`, 302);
  }
};

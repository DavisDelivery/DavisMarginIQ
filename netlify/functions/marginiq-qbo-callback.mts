import type { Context, Config } from "@netlify/functions";

const FIREBASE_API_KEY = "AIzaSyDY2OceDzBWMHPR3C3O1oxktrCIy3mKMqU";
const PROJECT_ID = "glorybounddispatch";

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  const code = url.searchParams.get("code");
  const realmId = url.searchParams.get("realmId");
  const error = url.searchParams.get("error");
  const SITE_URL = Netlify.env.get("URL") || "https://davis-marginiq.netlify.app";

  if (error || !code || !realmId) {
    return Response.redirect(`${SITE_URL}?qbo=error&reason=${error || "missing_params"}`, 302);
  }

  const QBO_CLIENT_ID = Netlify.env.get("QBO_CLIENT_ID");
  const QBO_CLIENT_SECRET = Netlify.env.get("QBO_CLIENT_SECRET");
  const REDIRECT_URI = `${SITE_URL}/.netlify/functions/marginiq-qbo-callback`;

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
      console.error("QBO token exchange failed:", await tokenResp.text());
      return Response.redirect(`${SITE_URL}?qbo=error&reason=token_exchange`, 302);
    }

    const tokens = await tokenResp.json();

    // Store in Firebase
    await fetch(
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
            refresh_expires_at: { integerValue: String(Date.now() + tokens.x_refresh_token_expires_in * 1000) },
            updated_at: { stringValue: new Date().toISOString() },
          },
        }),
      }
    );

    return Response.redirect(`${SITE_URL}?qbo=connected`, 302);
  } catch (e) {
    console.error("QBO callback error:", e);
    return Response.redirect(`${SITE_URL}?qbo=error&reason=server_error`, 302);
  }
};

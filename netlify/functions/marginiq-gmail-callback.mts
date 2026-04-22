import type { Context, Config } from "@netlify/functions";

// Slugify an email address into a valid Firestore doc ID suffix.
// chad@davisdelivery.com → chad_at_davisdelivery_com
function emailSlug(email: string): string {
  return String(email || "unknown")
    .toLowerCase()
    .replace(/@/g, "_at_")
    .replace(/[^a-z0-9_]/g, "_")
    .slice(0, 100);
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  const code = url.searchParams.get("code");
  const error = url.searchParams.get("error");
  const SITE_URL = "https://davis-marginiq.netlify.app";
  const REDIRECT_URI = "https://davis-marginiq.netlify.app/.netlify/functions/marginiq-gmail-callback";

  if (error || !code) {
    return Response.redirect(`${SITE_URL}?tab=gmail&gmail=error&reason=${error || "missing_code"}`, 302);
  }

  const CLIENT_ID = process.env["GOOGLE_CLIENT_ID"];
  const CLIENT_SECRET = process.env["GOOGLE_CLIENT_SECRET"];
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  const PROJECT_ID = "davismarginiq";

  if (!CLIENT_ID || !CLIENT_SECRET || !FIREBASE_API_KEY) {
    const missing = [!CLIENT_ID && "CLIENT_ID", !CLIENT_SECRET && "SECRET", !FIREBASE_API_KEY && "FB_KEY"].filter(Boolean).join(",");
    return Response.redirect(`${SITE_URL}?tab=gmail&gmail=error&reason=missing_env_${missing}`, 302);
  }

  try {
    // Exchange code for tokens
    const tokenResp = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        code,
        client_id: CLIENT_ID,
        client_secret: CLIENT_SECRET,
        redirect_uri: REDIRECT_URI,
        grant_type: "authorization_code",
      }),
    });

    if (!tokenResp.ok) {
      const errBody = await tokenResp.text();
      return Response.redirect(`${SITE_URL}?tab=gmail&gmail=error&reason=token_exchange&detail=${encodeURIComponent(errBody.substring(0, 400))}`, 302);
    }

    const tokens = await tokenResp.json();
    if (!tokens.refresh_token) {
      return Response.redirect(`${SITE_URL}?tab=gmail&gmail=error&reason=no_refresh_token`, 302);
    }

    // Get user's email
    let userEmail = "unknown";
    try {
      const profileResp = await fetch(
        "https://gmail.googleapis.com/gmail/v1/users/me/profile",
        { headers: { Authorization: `Bearer ${tokens.access_token}` } }
      );
      const profile = await profileResp.json();
      userEmail = profile.emailAddress || "unknown";
    } catch {}

    // v2.40+ per-account token storage at marginiq_config/gmail_tokens_{slug}.
    // Lets us fan out Gmail searches across chad@ + billing@ + any additional
    // inbox the user connects, instead of overwriting a singleton. Legacy
    // marginiq_config/gmail_tokens doc is left alone — the search function
    // picks it up automatically as one more account.
    const docId = `gmail_tokens_${emailSlug(userEmail)}`;
    const firestoreResp = await fetch(
      `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/${docId}?key=${FIREBASE_API_KEY}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          fields: {
            refresh_token: { stringValue: tokens.refresh_token },
            email: { stringValue: userEmail },
            scope: { stringValue: tokens.scope || "" },
            connected_at: { stringValue: new Date().toISOString() },
            updated_at: { stringValue: new Date().toISOString() },
          },
        }),
      }
    );

    if (!firestoreResp.ok) {
      const errText = await firestoreResp.text();
      return Response.redirect(`${SITE_URL}?tab=gmail&gmail=error&reason=firestore_write&detail=${encodeURIComponent(errText.substring(0, 400))}`, 302);
    }

    return Response.redirect(`${SITE_URL}?tab=gmail&gmail=connected&email=${encodeURIComponent(userEmail)}`, 302);
  } catch (e: any) {
    return Response.redirect(`${SITE_URL}?tab=gmail&gmail=error&reason=crash_${encodeURIComponent(e.message || "unknown")}`, 302);
  }
};
// env: GOOGLE_CLIENT_SECRET set 2026-04-20T18:01:38Z
// env vars updated 2026-04-20T18:03:27Z — force fresh function build
// v2.40.0: per-account token storage (gmail_tokens_{slug})

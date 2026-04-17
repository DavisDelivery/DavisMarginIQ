import type { Context, Config } from "@netlify/functions";

export default async (req: Request, context: Context) => {
  const CLIENT_ID = process.env["GOOGLE_CLIENT_ID"];
  const REDIRECT_URI = "https://davis-marginiq.netlify.app/.netlify/functions/marginiq-gmail-callback";

  if (!CLIENT_ID) {
    return new Response("GOOGLE_CLIENT_ID not configured", { status: 500 });
  }

  const params = new URLSearchParams({
    client_id: CLIENT_ID,
    redirect_uri: REDIRECT_URI,
    response_type: "code",
    scope: "https://www.googleapis.com/auth/gmail.readonly",
    access_type: "offline",     // required for refresh_token
    prompt: "consent",           // forces refresh_token return every auth
    include_granted_scopes: "true",
  });

  return Response.redirect(
    `https://accounts.google.com/o/oauth2/v2/auth?${params.toString()}`,
    302
  );
};

import type { Context, Config } from "@netlify/functions";

export default async (req: Request, context: Context) => {
  const QBO_CLIENT_ID = process.env["QBO_CLIENT_ID"];
  const REDIRECT_URI = "https://davis-marginiq.netlify.app/.netlify/functions/marginiq-qbo-callback";

  if (!QBO_CLIENT_ID) {
    return new Response("QBO_CLIENT_ID not configured", { status: 500 });
  }

  const scopes = "com.intuit.quickbooks.accounting";
  const state = crypto.randomUUID();

  const authUrl = new URL("https://appcenter.intuit.com/connect/oauth2");
  authUrl.searchParams.set("client_id", QBO_CLIENT_ID);
  authUrl.searchParams.set("redirect_uri", REDIRECT_URI);
  authUrl.searchParams.set("scope", scopes);
  authUrl.searchParams.set("response_type", "code");
  authUrl.searchParams.set("state", state);

  return Response.redirect(authUrl.toString(), 302);
};

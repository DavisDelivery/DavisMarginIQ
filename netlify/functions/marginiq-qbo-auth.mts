import type { Context, Config } from "@netlify/functions";

export default async (req: Request, context: Context) => {
  const QBO_CLIENT_ID = Netlify.env.get("QBO_CLIENT_ID");
  const SITE_URL = Netlify.env.get("URL") || "https://davis-marginiq.netlify.app";
  const REDIRECT_URI = `${SITE_URL}/.netlify/functions/marginiq-qbo-callback`;

  const scopes = "com.intuit.quickbooks.accounting";
  const state = crypto.randomUUID();

  const authUrl = new URL("https://appcenter.intuit.com/connect/oauth2");
  authUrl.searchParams.set("client_id", QBO_CLIENT_ID!);
  authUrl.searchParams.set("redirect_uri", REDIRECT_URI);
  authUrl.searchParams.set("scope", scopes);
  authUrl.searchParams.set("response_type", "code");
  authUrl.searchParams.set("state", state);

  return Response.redirect(authUrl.toString(), 302);
};

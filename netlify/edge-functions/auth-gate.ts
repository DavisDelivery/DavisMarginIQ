// Davis MarginIQ — Site auth gate
// HTTP Basic Auth on every request. Browser prompts once per session/origin.
// Public exclusions: OAuth callbacks (QBO, Gmail) and Zoom webhook need to
// be reachable by external services without credentials.

import type { Config, Context } from "@netlify/edge-functions";

const PUBLIC_PATH_PREFIXES = [
  "/.netlify/functions/marginiq-qbo-callback",
  "/.netlify/functions/marginiq-gmail-callback",
  "/.netlify/functions/marginiq-zoom-webhook",
];

// Constant-time string compare to prevent timing attacks
function safeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let result = 0;
  for (let i = 0; i < a.length; i++) result |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return result === 0;
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);

  // Allow OAuth callbacks + webhooks through unauthenticated
  for (const prefix of PUBLIC_PATH_PREFIXES) {
    if (url.pathname.startsWith(prefix)) return context.next();
  }

  // Allow CORS preflight to pass through
  if (req.method === "OPTIONS") return context.next();

  const password = Netlify.env.get("SITE_PASSWORD");
  if (!password) {
    // If no password configured, fail open with a warning header rather than
    // locking everyone out. We'd rather know it's misconfigured than be unable
    // to deploy a fix.
    const resp = await context.next();
    resp.headers.set("X-Auth-Warning", "SITE_PASSWORD not configured — site is not protected");
    return resp;
  }

  const auth = req.headers.get("authorization") || "";
  const expected = "Basic " + btoa("davis:" + password);

  if (!safeEqual(auth, expected)) {
    return new Response(
      `<!DOCTYPE html><html><head><title>Authentication Required</title>
       <style>body{font-family:system-ui;background:#f0f4f8;color:#0f172a;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
       div{text-align:center;padding:40px;background:white;border-radius:12px;box-shadow:0 4px 12px rgba(0,0,0,0.1);max-width:400px}
       h1{color:#1e5b92;margin:0 0 12px;font-size:20px}
       p{color:#64748b;font-size:14px;line-height:1.5}</style></head>
       <body><div><h1>🔒 Davis MarginIQ</h1>
       <p>This site requires authentication. Reload and enter the password.</p></div></body></html>`,
      {
        status: 401,
        headers: {
          "WWW-Authenticate": 'Basic realm="Davis MarginIQ"',
          "Content-Type": "text/html; charset=utf-8",
        },
      }
    );
  }

  return context.next();
};

export const config: Config = {
  path: "/*",
};

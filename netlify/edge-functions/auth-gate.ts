// Davis MarginIQ — Site auth gate
// HTTP Basic Auth on every request. Browser prompts once per session.
// Defensive: ALWAYS fails open (passes through) on any unexpected error
// so a bug here can never lock the user out of their site again.

const PUBLIC_PATH_PREFIXES = [
  "/.netlify/functions/marginiq-qbo-callback",
  "/.netlify/functions/marginiq-gmail-callback",
  "/.netlify/functions/marginiq-zoom-webhook",
];

export default async (request, context) => {
  try {
    const url = new URL(request.url);

    // Allow OAuth callbacks + webhooks through unauthenticated
    for (const p of PUBLIC_PATH_PREFIXES) {
      if (url.pathname.startsWith(p)) return;
    }

    // Allow CORS preflight
    if (request.method === "OPTIONS") return;

    // Resolve password from env. If anything fails, pass through (fail open).
    let password = "";
    try {
      // @ts-ignore - Netlify global at runtime
      if (typeof Netlify !== "undefined" && Netlify.env && typeof Netlify.env.get === "function") {
        password = Netlify.env.get("SITE_PASSWORD") || "";
      }
    } catch (_e) {
      password = "";
    }

    if (!password) return; // not configured → don't gate

    const auth = request.headers.get("authorization") || "";
    const expected = "Basic " + btoa("davis:" + password);

    if (auth !== expected) {
      return new Response(
        "Authentication required. Reload to enter password.",
        {
          status: 401,
          headers: {
            "WWW-Authenticate": 'Basic realm="Davis MarginIQ"',
            "Content-Type": "text/plain; charset=utf-8",
          },
        }
      );
    }

    // Authenticated — let request through to origin
    return;
  } catch (err) {
    // Never crash the site over auth code. Pass through.
    console.error("auth-gate fatal:", err);
    return;
  }
};

export const config = { path: "/*" };

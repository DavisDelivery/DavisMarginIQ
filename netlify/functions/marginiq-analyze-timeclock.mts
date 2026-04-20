import type { Context, Config } from "@netlify/functions";

// Davis MarginIQ — Time Clock AI analyst.
// POST body: { system?: string, messages: [...], max_tokens?: number }
// Proxies to Anthropic's messages API with the server-held API key.
// The client constructs a compact summary of the weekly rollups and sends it
// inline in the first user message, along with the user's question.

export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") return json({ error: "POST required" }, 405);

  const apiKey = process.env["ANTHROPIC_API_KEY"];
  if (!apiKey) return json({ error: "ANTHROPIC_API_KEY not configured" }, 500);

  try {
    const body = await req.json();
    const messages = body.messages;
    const system = body.system;
    if (!Array.isArray(messages) || messages.length === 0) {
      return json({ error: "messages array required" }, 400);
    }

    const max_tokens = Math.min(body.max_tokens || 2000, 4096);

    const payload: any = {
      model: "claude-sonnet-4-6",
      max_tokens,
      messages,
    };
    if (system) payload.system = system;

    const resp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify(payload),
    });

    const respText = await resp.text();
    return new Response(respText, {
      status: resp.status,
      headers: { "Content-Type": "application/json" },
    });
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

export const config: Config = {
  path: "/.netlify/functions/marginiq-analyze-timeclock",
};

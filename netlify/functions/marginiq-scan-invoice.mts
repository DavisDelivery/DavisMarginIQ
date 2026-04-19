import type { Context, Config } from "@netlify/functions";

// Davis MarginIQ — Claude vision invoice scanner.
// POST body: { messages: [...], max_tokens?: number }
// Proxies to Anthropic's messages API with the server-held API key.
// Client converts PDF pages to PNG via pdf.js, sends as image blocks.
// Prompt rules are baked in by the client per vendor (Quick Fuel, FuelFox).

export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") return json({ error: "POST required" }, 405);

  const apiKey = process.env["ANTHROPIC_API_KEY"];
  if (!apiKey) return json({ error: "ANTHROPIC_API_KEY not configured" }, 500);

  try {
    const body = await req.json();
    const messages = body.messages;
    if (!Array.isArray(messages) || messages.length === 0) {
      return json({ error: "messages array required" }, 400);
    }

    const max_tokens = Math.min(body.max_tokens || 4096, 8192);

    const resp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-6",
        max_tokens,
        messages,
      }),
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

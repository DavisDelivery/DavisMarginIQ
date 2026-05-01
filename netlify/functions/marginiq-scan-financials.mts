import type { Context, Config } from "@netlify/functions";

// Davis MarginIQ — Audited Financials PDF scanner.
// POST body: { messages: [...], max_tokens?: number }
// Proxies to Anthropic's messages API with server-held API key.
// Client converts PDF pages to PNG via pdf.js, sends as image blocks.
// Extracts P&L, Balance Sheet, and Cash Flow from CPA-issued PDFs.
//
// v2.48.4: extend the function execution wall from the default 10s to the
// platform max of 26s. Vision calls on multi-page CPA PDFs run 12-25s for
// the median case; the previous 10s ceiling guaranteed 504 "Inactivity
// Timeout" responses. PDFs that legitimately take >26s will still 504 —
// reducing render scale or page count is the next lever for those.

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

    const max_tokens = Math.min(body.max_tokens || 8192, 8192);

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

export const config: Config = {
  // Maximum allowed on standard plans is 26s (default is 10s).
  // Required for Anthropic vision calls on multi-page CPA financial PDFs.
  timeout: 26,
};

function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

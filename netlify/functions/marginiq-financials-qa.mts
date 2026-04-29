import type { Context } from "@netlify/functions";

// Davis MarginIQ — Audited Financials Q&A
// Conversational endpoint over the user's audited financial statements.
// POST body: { question: string, financials: [...records...], history?: [{role, content}, ...] }
// Returns: { answer: string }

export default async (req: Request, _ctx: Context) => {
  if (req.method !== "POST") return json({ error: "POST required" }, 405);

  const API_KEY = process.env["ANTHROPIC_API_KEY"];
  if (!API_KEY) return json({ error: "ANTHROPIC_API_KEY not configured" }, 500);

  try {
    const body = await req.json();
    const question: string = (body.question || "").trim();
    const financials: any[] = body.financials || [];
    const history: any[] = body.history || [];

    if (!question) return json({ error: "question required" }, 400);
    if (!Array.isArray(financials) || financials.length === 0) {
      return json({ answer: "I don't see any audited financial statements in the system yet. Once you import some monthly statements from AMP CPAs (📋 Audited Financials → 📧 Gmail Sync), I can answer questions about them." });
    }

    // Build a compact JSON of the financial data — Claude does NOT need every line item.
    // Trim each record to the structured numbers and any CPA notes.
    const compact = financials
      .filter(f => f.period)
      .sort((a,b) => (a.period||"").localeCompare(b.period||""))
      .map(f => ({
        period: f.period,
        report_date: f.report_date || null,
        pl: f.pl ? {
          revenue: f.pl.revenue,
          cogs: f.pl.cost_of_goods_sold,
          gross_profit: f.pl.gross_profit,
          operating_expenses: f.pl.operating_expenses,
          operating_income: f.pl.operating_income,
          other_income: f.pl.other_income,
          other_expenses: f.pl.other_expenses,
          net_income: f.pl.net_income,
          // include line items but truncated
          line_items: (f.pl.line_items || []).slice(0, 40),
        } : null,
        balance_sheet: f.balance_sheet ? {
          total_assets: f.balance_sheet.total_assets,
          current_assets: f.balance_sheet.current_assets,
          fixed_assets: f.balance_sheet.fixed_assets,
          total_liabilities: f.balance_sheet.total_liabilities,
          current_liabilities: f.balance_sheet.current_liabilities,
          long_term_liabilities: f.balance_sheet.long_term_liabilities,
          equity: f.balance_sheet.equity,
          line_items: (f.balance_sheet.line_items || []).slice(0, 40),
        } : null,
        cash_flow: f.cash_flow || null,
        notes: f.notes || null,
      }));

    const systemPrompt = `You are a financial analyst helping Chad Davis run Davis Delivery Service Inc., a family-owned LTL delivery company in Atlanta, GA serving primarily Uline (~90% of revenue).

You have access to monthly audited financial statements prepared by AMP CPAs. Your job is to answer Chad's questions about his financial performance with precision and clarity.

Rules:
- Be DIRECT and SPECIFIC. Chad runs the company day-to-day; he wants useful answers, not lectures.
- ALWAYS cite specific numbers from the data when answering. Reference the period (e.g. "March 2025") when quoting figures.
- If the question can't be answered from the data, say so plainly — don't invent figures.
- Format numbers with $ and commas. Use percentages where appropriate.
- For trend questions, compute month-over-month or year-over-year changes when relevant.
- For comparison questions, show the comparison clearly with both numbers side-by-side.
- Keep responses concise — 100-250 words is ideal. Use bullet points sparingly, only when listing.
- Never make up information. If the data is missing for a period, say so.
- If asked for advice, give it briefly and ground it in the numbers.`;

    // Build messages — include conversation history if provided
    const messages: any[] = [];

    // System context message with the data
    const dataContext = `Here is Chad's audited financial data (sorted by period, oldest to newest):

${JSON.stringify(compact, null, 2)}

Total months on file: ${compact.length}
Date range: ${compact[0]?.period || "unknown"} to ${compact[compact.length-1]?.period || "unknown"}`;

    // Inject prior history (if any) for follow-up questions
    if (history.length > 0) {
      // Only include the last 6 turns to keep context manageable
      const recent = history.slice(-6);
      for (const h of recent) {
        if (h.role === "user" || h.role === "assistant") {
          messages.push({ role: h.role, content: String(h.content || "") });
        }
      }
    }

    // First message includes the data context, all subsequent reuse it via history
    if (history.length === 0) {
      messages.push({
        role: "user",
        content: `${dataContext}\n\n---\n\nMy question: ${question}`,
      });
    } else {
      // For follow-ups, just send the new question — the data was already in the first message
      messages.push({
        role: "user",
        content: question,
      });
    }

    const resp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-6",
        max_tokens: 1500,
        system: systemPrompt,
        messages,
      }),
    });

    const respJson = await resp.json();
    if (!resp.ok) {
      return json({ error: "Anthropic API error", detail: respJson }, resp.status);
    }

    const answer = (respJson.content || [])
      .map((b: any) => b.type === "text" ? b.text : "")
      .join("")
      .trim();

    return json({ answer, periods: compact.length });
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

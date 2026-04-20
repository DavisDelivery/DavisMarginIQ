import type { Context } from "@netlify/functions";

// Proxy to Anthropic's Claude API for financial analysis.
// Called by the <AIInsight> component with:
//   { context: "uline-revenue"|"command-center"|...,
//     data: { ...contextual data ... },
//     sanityFindings: [{severity, title, detail, metric}...] }
//
// Returns { analysis: "markdown text from Claude" } or { error }.

export default async (req: Request, _ctx: Context) => {
  if (req.method !== "POST") {
    return json({ error: "POST required" }, 405);
  }

  const API_KEY = process.env["ANTHROPIC_API_KEY"];
  if (!API_KEY) return json({ error: "ANTHROPIC_API_KEY not configured" }, 500);

  try {
    const body = await req.json();
    const context: string = body.context || "generic";
    const data = body.data || {};
    const sanityFindings: any[] = body.sanityFindings || [];

    // Build a compact prompt. Don't dump full data — Claude doesn't need raw
    // 100k-row arrays; send aggregates + a small sample.
    const summary = summarizeDataForContext(context, data);

    const systemPrompt = `You are a financial analyst helping Chad Davis run Davis Delivery Service Inc., an LTL delivery company serving primarily Uline.

Your job is to review the summary data provided and point out anything suspicious, interesting, or actionable. Be DIRECT and SPECIFIC — Chad prefers concise analysis over fluff.

Rules:
- No marketing language. No "Great job!" praise.
- If something looks off, say so clearly and suggest a concrete next step.
- If nothing looks off, say that honestly in 1-2 sentences.
- Reference specific numbers from the data.
- Organize your response with headings and bullets where it helps, but keep it tight.
- Max 300 words.`;

    const userPrompt = `Context: ${context}

Data summary:
${JSON.stringify(summary, null, 2)}

Deterministic sanity checks already run (${sanityFindings.length} findings):
${sanityFindings.length === 0 ? "None — no anomalies detected automatically." : sanityFindings.map((f,i) => `${i+1}. [${f.severity}] ${f.title} — ${f.detail}`).join("\n")}

Please:
1. Confirm or push back on the sanity findings above (agree / disagree / add nuance)
2. Surface any patterns the deterministic checks missed
3. Suggest the highest-ROI next action

Keep it to 300 words or less.`;

    const resp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 1000,
        system: systemPrompt,
        messages: [{ role: "user", content: userPrompt }],
      }),
    });

    if (!resp.ok) {
      const errText = await resp.text();
      return json({ error: `Claude API ${resp.status}: ${errText.slice(0, 400)}` }, 500);
    }

    const respData = await resp.json();
    // Extract text blocks
    const textBlocks = (respData.content || [])
      .filter((b: any) => b.type === "text")
      .map((b: any) => b.text)
      .join("\n\n");

    return json({ analysis: textBlocks, model: respData.model, usage: respData.usage });
  } catch (e: any) {
    return json({ error: e.message || "Analyzer failed" }, 500);
  }
};

// Reduce raw data down to a compact summary Claude can reason over quickly.
function summarizeDataForContext(context: string, data: any) {
  if (context === "uline-revenue" && Array.isArray(data.weeklyRollups)) {
    const weeks = data.weeklyRollups;
    const totalRev = weeks.reduce((s: number, w: any) => s + (w.revenue || 0), 0);
    const totalDelRev = weeks.reduce((s: number, w: any) => s + (w.delivery_revenue || 0), 0);
    const totalAccRev = weeks.reduce((s: number, w: any) => s + (w.accessorial_revenue || 0), 0);
    const totalTkRev = weeks.reduce((s: number, w: any) => s + (w.truckload_revenue || 0), 0);
    const totalStops = weeks.reduce((s: number, w: any) => s + (w.stops || 0), 0);

    // Sample a handful of weeks for Claude to see individual patterns
    const sorted = [...weeks].sort((a, b) => (a.week_ending || "").localeCompare(b.week_ending || ""));
    const sample = sorted.slice(-12).map(w => ({
      we: w.week_ending,
      stops: w.stops,
      revenue: Math.round(w.revenue || 0),
      delivery_rev: Math.round(w.delivery_revenue || 0),
      truckload_rev: Math.round(w.truckload_revenue || 0),
      accessorial_rev: Math.round(w.accessorial_revenue || 0),
      accessorial_count: w.accessorial_count || 0,
    }));

    return {
      total_weeks: weeks.length,
      first_week: sorted[0]?.week_ending,
      last_week: sorted[sorted.length-1]?.week_ending,
      totals: {
        revenue: Math.round(totalRev),
        delivery_revenue: Math.round(totalDelRev),
        accessorial_revenue: Math.round(totalAccRev),
        truckload_revenue: Math.round(totalTkRev),
        stops: totalStops,
      },
      pct_of_revenue: {
        delivery: totalRev > 0 ? +((totalDelRev/totalRev)*100).toFixed(1) : 0,
        accessorial: totalRev > 0 ? +((totalAccRev/totalRev)*100).toFixed(1) : 0,
        truckload: totalRev > 0 ? +((totalTkRev/totalRev)*100).toFixed(1) : 0,
      },
      weekly_avg: {
        revenue: weeks.length > 0 ? Math.round(totalRev/weeks.length) : 0,
        stops: weeks.length > 0 ? Math.round(totalStops/weeks.length) : 0,
      },
      recent_12_weeks: sample,
    };
  }

  if (context === "command-center") {
    const c = data.completeness || {};
    const m = data.margins || {};
    return {
      margins: {
        daily_revenue: Math.round(m.dailyRevenue || 0),
        daily_cost: Math.round(m.dailyCost || 0),
        daily_margin: Math.round(m.dailyMargin || 0),
        margin_pct: +(m.dailyMarginPct || 0).toFixed(1),
        daily_stops: m.dailyStops,
        revenue_per_stop: Math.round(m.revenuePerStop || 0),
        break_even_stops: m.breakEvenStopsDaily,
      },
      completeness: {
        missing_weeks: (c.gaps || []).length,
        sparse_weeks: (c.sparseWeeks || []).length,
        missing_accessorials: (c.missingAccessorials || []).length,
        avg_stops_per_week: c.avgStops,
        date_range: c.firstWE && c.lastWE ? `${c.firstWE} to ${c.lastWE}` : null,
      },
    };
  }

  // Fallback: pass raw (risks token bloat, but never blocks)
  return data;
}

function json(payload: any, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

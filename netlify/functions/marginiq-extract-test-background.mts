import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Background worker for synchronous extraction tests.
 *
 * Companion to marginiq-extract-test.mts. Receives a single Gmail
 * message_id + attachment_id, runs the v2.50.0 extraction, writes the
 * result to Firestore at:
 *
 *     extract_test_results/{job_id}
 *
 * The router function returns the job_id immediately (200 from sync side,
 * 202 from this background side) so the caller can poll
 * ?action=fetch-result&job_id=... until state is "complete" or "failed".
 *
 * This is the verification path. Production batch fanout
 * (marginiq-extract-financial-background.mts) needs the same prefill
 * removal applied separately.
 */

const PROJECT_ID = "davismarginiq";

// ── Firestore helpers (mirror marginiq-extract-financial-background.mts) ───
function fsBase(): string {
  return `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
}

function toFsValue(v: any): any {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number") return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (typeof v === "string") return { stringValue: v };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(toFsValue) } };
  if (typeof v === "object") {
    const fields: Record<string, any> = {};
    for (const [k, val] of Object.entries(v)) fields[k] = toFsValue(val);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}
function toFsFields(obj: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(obj)) out[k] = toFsValue(v);
  return out;
}

async function writeDoc(path: string, fields: Record<string, any>, apiKey: string): Promise<boolean> {
  const resp = await fetch(`${fsBase()}/${path}?key=${apiKey}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: toFsFields(fields) }),
  });
  return resp.ok;
}

function fsValueToJs(v: any): any {
  if (!v) return null;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return Number(v.doubleValue);
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  return null;
}

// ── Gmail helpers ──────────────────────────────────────────────────────────
function emailSlug(email: string): string {
  return String(email || "unknown")
    .toLowerCase()
    .replace(/@/g, "_at_")
    .replace(/[^a-z0-9_]/g, "_")
    .slice(0, 100);
}

async function readRefreshToken(
  apiKey: string,
  candidates: string[]
): Promise<{ refreshToken: string; resolvedDocId: string } | null> {
  for (const docId of candidates) {
    const resp = await fetch(
      `${fsBase()}/marginiq_config/${docId}?key=${apiKey}`
    );
    if (!resp.ok) continue;
    const data: any = await resp.json();
    const tok = fsValueToJs(data?.fields?.refresh_token);
    if (typeof tok === "string" && tok) {
      return { refreshToken: tok, resolvedDocId: docId };
    }
  }
  return null;
}

async function getAccessToken(
  clientId: string, clientSecret: string, refreshToken: string
): Promise<{ accessToken?: string; error?: string }> {
  const r = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
      grant_type: "refresh_token",
    }),
  });
  const data: any = await r.json();
  if (!r.ok || !data.access_token) {
    return { error: "Token refresh failed: " + JSON.stringify(data).substring(0, 300) };
  }
  return { accessToken: data.access_token };
}

// ── v2.50.0 extraction prompt (kept in sync with the sync router) ─────────
const EXTRACTION_PROMPT = `You are extracting financial data from audited financial statements prepared by a CPA firm for Davis Delivery Services Inc, a trucking/delivery company.

Read every page carefully. Return ONLY valid JSON — no markdown code fences, no preamble, no explanation. Start your response with the opening brace { and end with the closing brace }.

CRITICAL COLUMN RULES — these statements typically have multiple numeric columns. Distinguish them:
  - "1 Month Ended" or "Current Month" or "Month" column → "month" value
  - "12 Months Ended" or "Year-To-Date" or "YTD" column → "ytd" value
  - "%" columns → "month_pct" or "ytd_pct" (percent of revenue)
  - Prior-year columns (e.g., "December 31, 2024" when statement date is December 31, 2025) → "prior_month" or "prior_ytd"
  - "Variance" columns → "month_variance" or "ytd_variance"

If a column does not exist on the page, set its field to null. If only ONE numeric column exists per line, store it as "month" (NOT "ytd").

Schema:

{
  "period": "YYYY-MM",
  "period_end_date": "YYYY-MM-DD",
  "company": "...",
  "pl_line_items": [
    {
      "label": "exact label from the PDF",
      "section": "revenue | cost_of_sales | operating_expense | other_income | other_expense",
      "month": number | null,
      "month_pct": number | null,
      "ytd": number | null,
      "ytd_pct": number | null,
      "prior_month": number | null,
      "prior_ytd": number | null,
      "month_variance": number | null,
      "ytd_variance": number | null
    }
  ],
  "pl_totals": {
    "total_revenue":            { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "total_cost_of_sales":      { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "gross_profit":             { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "total_operating_expenses": { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "operating_income":         { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "total_other_income":       { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "total_other_expense":      { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... },
    "net_income":               { "month": ..., "ytd": ..., "prior_month": ..., "prior_ytd": ... }
  },
  "ebitda_inputs": {
    "depreciation_month": number | null,
    "depreciation_ytd": number | null,
    "amortization_month": number | null,
    "amortization_ytd": number | null,
    "interest_expense_month": number | null,
    "interest_expense_ytd": number | null,
    "income_tax_month": number | null,
    "income_tax_ytd": number | null
  },
  "balance_sheet": {
    "as_of_date": "YYYY-MM-DD",
    "line_items": [
      {
        "label": "exact label from the PDF",
        "section": "current_asset | fixed_asset | other_asset | current_liability | long_term_liability | equity",
        "amount": number
      }
    ],
    "subtotals": {
      "total_current_assets": number | null,
      "total_fixed_assets": number | null,
      "total_other_assets": number | null,
      "total_assets": number | null,
      "total_current_liabilities": number | null,
      "total_long_term_liabilities": number | null,
      "total_liabilities": number | null,
      "total_equity": number | null,
      "total_liabilities_and_equity": number | null
    }
  },
  "cash_flow": {
    "operating_activities": number | null,
    "investing_activities": number | null,
    "financing_activities": number | null,
    "net_change_in_cash": number | null,
    "beginning_cash": number | null,
    "ending_cash": number | null
  },
  "notes": "anything noteworthy: comparative pages present? cash flow statement present? unusual items?"
}

EXTRACTION RULES:
- All amounts in dollars as plain numbers (no $ signs, no commas, no parentheses)
- Negative numbers stay negative: a loss of (341,118.33) becomes -341118.33
- Accumulated Depreciation in balance sheet typically shows as negative
- Net Income (Loss): if the line shows (loss) or parentheses, it is negative
- pl_line_items must include EVERY line on the income statement, including zero-value rows. Do not aggregate, do not skip lines, do not normalize labels.
- For ebitda_inputs: pull these specific values from the line items themselves. Set null if the line is not present.
- For income_tax: include only federal/state INCOME taxes. Do NOT include payroll taxes, property taxes, sales tax, or franchise tax. If S-corp with no income tax line, set null.
- period: YYYY-MM derived from period end date (e.g., December 31, 2025 → "2025-12")

Cross-check: total_revenue.ytd should equal the sum of pl_line_items where section="revenue" of their .ytd values. If not, recheck columns.`;

// ── Worker ─────────────────────────────────────────────────────────────────
export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") return new Response("POST required", { status: 405 });

  const clientId = process.env["GOOGLE_CLIENT_ID"];
  const clientSecret = process.env["GOOGLE_CLIENT_SECRET"];
  const firebaseKey = process.env["FIREBASE_API_KEY"];
  const anthropicKey = process.env["ANTHROPIC_API_KEY"];

  if (!clientId || !clientSecret || !firebaseKey || !anthropicKey) {
    return new Response("env vars missing", { status: 500 });
  }

  let body: any;
  try { body = await req.json(); }
  catch { return new Response("invalid JSON body", { status: 400 }); }

  const jobId: string = body.job_id;
  const messageId: string = body.message_id;
  const attachmentId: string = body.attachment_id;
  const accountDocId: string = body.account_doc_id || "";
  const accountEmail: string = body.account_email || "";

  if (!jobId || !messageId || !attachmentId) {
    return new Response("job_id, message_id, attachment_id required", { status: 400 });
  }

  const jobPath = `extract_test_results/${jobId}`;
  const t0 = Date.now();

  try {
    await writeDoc(jobPath, {
      state: "running",
      started_at: new Date().toISOString(),
      message_id: messageId,
    }, firebaseKey);

    // 1. Resolve refresh token
    const candidates: string[] = [];
    if (accountDocId) candidates.push(accountDocId);
    if (accountEmail) candidates.push(`gmail_tokens_${emailSlug(accountEmail)}`);
    candidates.push("gmail_tokens");

    const tok = await readRefreshToken(firebaseKey, candidates);
    if (!tok) throw new Error("Gmail not connected. Reconnect.");

    // 2. Refresh access token
    const at = await getAccessToken(clientId, clientSecret, tok.refreshToken);
    if (!at.accessToken) throw new Error(at.error || "Token refresh failed");

    // 3. Fetch attachment
    const attResp = await fetch(
      `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}/attachments/${attachmentId}`,
      { headers: { Authorization: `Bearer ${at.accessToken}` } }
    );
    const attData: any = await attResp.json();
    if (!attResp.ok) {
      throw new Error("Attachment fetch failed: " + JSON.stringify(attData).substring(0, 300));
    }
    const pdfB64 = String(attData.data || "").replace(/-/g, "+").replace(/_/g, "/");
    const pdfSize = attData.size || 0;
    if (!pdfB64) throw new Error("Empty attachment data");

    const tFetch = Date.now() - t0;
    const t1 = Date.now();

    // 4. Call Anthropic — NO assistant prefill (Sonnet 4.6 rejects it)
    const anthropicResp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": anthropicKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-6",
        max_tokens: 16384,
        messages: [
          {
            role: "user",
            content: [
              {
                type: "document",
                source: { type: "base64", media_type: "application/pdf", data: pdfB64 },
              },
              { type: "text", text: EXTRACTION_PROMPT },
            ],
          },
        ],
      }),
    });
    const respText = await anthropicResp.text();
    const tClaude = Date.now() - t1;

    if (!anthropicResp.ok) {
      let errMsg = `Anthropic API ${anthropicResp.status}`;
      try {
        const j = JSON.parse(respText);
        if (j.error?.message) errMsg = `Anthropic API ${anthropicResp.status}: ${j.error.message}`;
      } catch { /* ignore */ }
      throw new Error(errMsg);
    }

    const anthropicJson: any = JSON.parse(respText);
    const modelText: string = (anthropicJson.content || []).map((b: any) => b.text || "").join("").trim();
    if (!modelText) throw new Error(`Empty model output. stop_reason=${anthropicJson.stop_reason || "?"}`);

    // 5. Strip any markdown fences (defensive — model is told to start with "{")
    const cleaned = modelText
      .replace(/^```json\s*\n?/i, "")
      .replace(/^```\s*\n?/, "")
      .replace(/```\s*$/, "")
      .trim();

    let extracted: any;
    try {
      extracted = JSON.parse(cleaned);
    } catch (e: any) {
      throw new Error(
        `JSON parse failed: ${e.message}. ` +
        `stop_reason=${anthropicJson.stop_reason || "?"}, ` +
        `output_tokens=${anthropicJson.usage?.output_tokens || "?"}, ` +
        `head=${cleaned.slice(0, 80)}, ` +
        `tail=${cleaned.slice(-80)}`
      );
    }

    // 6. Spot-check Dec 2025 against known targets
    const targets =
      extracted?.period === "2025-12"
        ? {
            sales_month_target: 1281356.12,
            sales_ytd_target: 13922075.72,
            net_income_month_target: -341118.33,
            net_income_ytd_target: 1165230.70,
            depreciation_month_target: 108546.57,
            depreciation_ytd_target: 450767.43,
            interest_expense_ytd_target: 18638.64,
            actual: {
              sales_month: extracted?.pl_totals?.total_revenue?.month ?? null,
              sales_ytd: extracted?.pl_totals?.total_revenue?.ytd ?? null,
              net_income_month: extracted?.pl_totals?.net_income?.month ?? null,
              net_income_ytd: extracted?.pl_totals?.net_income?.ytd ?? null,
              depreciation_month: extracted?.ebitda_inputs?.depreciation_month ?? null,
              depreciation_ytd: extracted?.ebitda_inputs?.depreciation_ytd ?? null,
              interest_expense_ytd: extracted?.ebitda_inputs?.interest_expense_ytd ?? null,
              income_tax_ytd: extracted?.ebitda_inputs?.income_tax_ytd ?? null,
            },
          }
        : null;

    // 7. Persist result
    await writeDoc(jobPath, {
      state: "complete",
      completed_at: new Date().toISOString(),
      message_id: messageId,
      account_doc_id: tok.resolvedDocId,
      pdf_size_bytes: pdfSize,
      timing_ms: { gmail_fetch: tFetch, claude: tClaude, total: Date.now() - t0 },
      anthropic_input_tokens: anthropicJson.usage?.input_tokens || null,
      anthropic_output_tokens: anthropicJson.usage?.output_tokens || null,
      anthropic_stop_reason: anthropicJson.stop_reason || null,
      pl_line_count: (extracted.pl_line_items || []).length,
      bs_line_count: (extracted.balance_sheet?.line_items || []).length,
      // Stash the full extraction as JSON-string (Firestore map could clobber number precision)
      extracted_json: JSON.stringify(extracted),
      period: extracted?.period || null,
      pl_totals: extracted?.pl_totals || null,
      ebitda_inputs: extracted?.ebitda_inputs || null,
      balance_sheet_subtotals: extracted?.balance_sheet?.subtotals || null,
      cash_flow: extracted?.cash_flow || null,
      notes: extracted?.notes || null,
      spot_check_dec_2025: targets,
    }, firebaseKey);

    return new Response(JSON.stringify({ ok: true, job_id: jobId }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });

  } catch (e: any) {
    const errMsg = (e && e.message) || String(e);
    console.error(`[extract-test ${jobId}] FAILED: ${errMsg}`);
    await writeDoc(jobPath, {
      state: "failed",
      error: errMsg.slice(0, 500),
      completed_at: new Date().toISOString(),
    }, firebaseKey);
    return new Response(JSON.stringify({ error: errMsg }), { status: 500 });
  }
};

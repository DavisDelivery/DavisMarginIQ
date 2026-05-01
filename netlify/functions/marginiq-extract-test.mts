import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Synchronous extraction test endpoint (v2.50.x).
 *
 * Purpose: verify the v2 native-PDF extraction pipeline end-to-end against
 * a known PDF (Dec 2025 audited financials) BEFORE building the production
 * batch fanout. No Firebase Storage, no background queue — just Gmail →
 * Claude → JSON returned inline so we can eyeball the numbers.
 *
 * Three actions, all GET so they run from a browser tab:
 *
 *   GET /.netlify/functions/marginiq-extract-test?action=health
 *     - Confirms the function deployed and required env vars are present.
 *     - Does NOT call Gmail or Anthropic.
 *
 *   GET /.netlify/functions/marginiq-extract-test?action=list-cpa-emails
 *     - Lists every AMP CPAs email across every connected Gmail account
 *       (so chad@ AND billing@ if both have AMP threads).
 *     - Returns: [{ emailId, emailDate, emailSubject, from, attachments:
 *       [{ filename, attachmentId, size }], account_email, account_doc_id }]
 *     - Optional ?limit=N (default 50, max 200).
 *
 *   GET /.netlify/functions/marginiq-extract-test
 *       ?action=extract
 *       &message_id=...
 *       &attachment_id=...
 *       [&account_doc_id=...]
 *       [&account_email=...]
 *     - Pulls the PDF from Gmail, sends to Claude with native PDF document
 *       block + the v2.50.0 extraction prompt, returns the parsed JSON
 *       inline along with timing + token usage.
 *     - Does NOT write to Firestore. Pure verification.
 *
 * Env vars required:
 *   GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET — Gmail OAuth refresh
 *   FIREBASE_API_KEY                       — read marginiq_config token docs
 *   ANTHROPIC_API_KEY                      — Claude API
 *
 * Auth: none (read-only test endpoint, behind Netlify URL only). The
 * production batch path will live behind the existing UI auth.
 */

const PROJECT_ID = "davismarginiq";
const AMP_CPAS_QUERY = 'from:@ampcpas.com filename:"Financial Statements" filename:pdf';

// ── tiny JSON response helper ───────────────────────────────────────────────
function json(data: any, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// ── Firestore minimal value reader (only need stringValue for tokens) ──────
function fsValueToJs(v: any): any {
  if (!v) return null;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return Number(v.doubleValue);
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  return null;
}

// ── Gmail OAuth helpers (mirrored from marginiq-gmail-attachment.mts) ──────
function emailSlug(email: string): string {
  return String(email || "unknown")
    .toLowerCase()
    .replace(/@/g, "_at_")
    .replace(/[^a-z0-9_]/g, "_")
    .slice(0, 100);
}

type TokenDoc = { docId: string; email: string; refresh_token: string };

async function listConnectedAccounts(apiKey: string): Promise<TokenDoc[]> {
  const accounts: Record<string, TokenDoc> = {};
  const listResp = await fetch(
    `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config?key=${apiKey}&pageSize=100`
  );
  if (!listResp.ok) return [];
  const listData: any = await listResp.json();
  for (const d of (listData.documents || [])) {
    const docId = (d.name || "").split("/").pop() || "";
    if (docId !== "gmail_tokens" && !docId.startsWith("gmail_tokens_")) continue;
    const fields = d.fields || {};
    const refreshToken = fields.refresh_token?.stringValue;
    const email = fields.email?.stringValue || "unknown";
    if (!refreshToken) continue;
    const existing = accounts[email];
    // Prefer per-account doc over legacy singleton when both exist for same email.
    if (!existing || (existing.docId === "gmail_tokens" && docId !== "gmail_tokens")) {
      accounts[email] = { docId, email, refresh_token: refreshToken };
    }
  }
  return Object.values(accounts);
}

async function readTokenByCandidates(
  apiKey: string,
  candidates: string[]
): Promise<{ refreshToken: string; resolvedDocId: string } | null> {
  for (const docId of candidates) {
    const resp = await fetch(
      `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/${docId}?key=${apiKey}`
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
  clientId: string,
  clientSecret: string,
  refreshToken: string
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

// ── List AMP CPAs emails across all connected accounts ─────────────────────
async function listAmpCpaEmails(
  clientId: string,
  clientSecret: string,
  firebaseKey: string,
  limit: number
): Promise<any[]> {
  const accounts = await listConnectedAccounts(firebaseKey);
  const all: any[] = [];

  for (const acct of accounts) {
    const tok = await getAccessToken(clientId, clientSecret, acct.refresh_token);
    if (!tok.accessToken) {
      all.push({ account_email: acct.email, account_doc_id: acct.docId, error: tok.error });
      continue;
    }

    // Page through Gmail messages list
    const messages: Array<{ id: string }> = [];
    let pageToken: string | undefined;
    let pages = 0;
    while (messages.length < limit && pages < 10) {
      const url = new URL("https://gmail.googleapis.com/gmail/v1/users/me/messages");
      url.searchParams.set("q", AMP_CPAS_QUERY);
      url.searchParams.set("maxResults", String(Math.min(500, limit - messages.length)));
      if (pageToken) url.searchParams.set("pageToken", pageToken);
      const r = await fetch(url.toString(), {
        headers: { Authorization: `Bearer ${tok.accessToken}` },
      });
      const d: any = await r.json();
      if (!r.ok) {
        all.push({
          account_email: acct.email,
          account_doc_id: acct.docId,
          error: "Gmail list failed: " + JSON.stringify(d).substring(0, 300),
        });
        break;
      }
      const batch = d.messages || [];
      if (batch.length === 0) break;
      messages.push(...batch);
      pageToken = d.nextPageToken;
      pages += 1;
      if (!pageToken) break;
    }

    // Fetch headers + attachment metadata for each
    for (const msg of messages) {
      try {
        const fr = await fetch(
          `https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}`,
          { headers: { Authorization: `Bearer ${tok.accessToken}` } }
        );
        const full: any = await fr.json();
        const headers = full.payload?.headers || [];
        const getHeader = (n: string) =>
          headers.find((h: any) => h.name.toLowerCase() === n.toLowerCase())?.value || "";

        const attachments: any[] = [];
        const walkParts = (part: any) => {
          if (part.filename && part.filename.length > 0 && part.body?.attachmentId) {
            const fn = String(part.filename).toLowerCase();
            if (fn.endsWith(".pdf")) {
              attachments.push({
                filename: part.filename,
                size: part.body?.size || 0,
                attachmentId: part.body.attachmentId,
                mimeType: part.mimeType || "",
              });
            }
          }
          if (part.parts) part.parts.forEach(walkParts);
        };
        if (full.payload) walkParts(full.payload);

        const dateStr = getHeader("Date");
        const dateObj = dateStr ? new Date(dateStr) : null;
        const dateISO = dateObj && !isNaN(dateObj.getTime()) ? dateObj.toISOString() : null;

        all.push({
          emailId: msg.id,
          emailDate: dateISO,
          emailSubject: getHeader("Subject"),
          from: getHeader("From"),
          attachments,
          account_email: acct.email,
          account_doc_id: acct.docId,
        });
      } catch (e: any) {
        all.push({
          emailId: msg.id,
          error: e.message || "details fetch failed",
          account_email: acct.email,
          account_doc_id: acct.docId,
        });
      }
    }
  }

  // Newest first
  all.sort((a, b) => {
    const ta = a.emailDate ? Date.parse(a.emailDate) : 0;
    const tb = b.emailDate ? Date.parse(b.emailDate) : 0;
    return tb - ta;
  });
  return all;
}

// ── v2.50.0 extraction prompt (must stay in sync with
// marginiq-extract-financial-background.mts) ─────────────────────────────
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

// ── Pull a Gmail attachment, run Claude, return JSON ───────────────────────
async function extractOne(
  clientId: string,
  clientSecret: string,
  firebaseKey: string,
  anthropicKey: string,
  messageId: string,
  attachmentId: string,
  accountDocId: string,
  accountEmail: string
): Promise<any> {
  const t0 = Date.now();

  // 1. Resolve refresh token (mirror marginiq-gmail-attachment.mts logic)
  const candidates: string[] = [];
  if (accountDocId) candidates.push(accountDocId);
  if (accountEmail) candidates.push(`gmail_tokens_${emailSlug(accountEmail)}`);
  candidates.push("gmail_tokens"); // legacy fallback

  const tokenInfo = await readTokenByCandidates(firebaseKey, candidates);
  if (!tokenInfo) {
    return { error: "Gmail not connected for this account. Reconnect Gmail." };
  }

  // 2. Refresh access token
  const at = await getAccessToken(clientId, clientSecret, tokenInfo.refreshToken);
  if (!at.accessToken) {
    return { error: at.error || "Token refresh failed", account_doc_id: tokenInfo.resolvedDocId };
  }

  // 3. Fetch attachment bytes
  const attResp = await fetch(
    `https://gmail.googleapis.com/gmail/v1/users/me/messages/${messageId}/attachments/${attachmentId}`,
    { headers: { Authorization: `Bearer ${at.accessToken}` } }
  );
  const attData: any = await attResp.json();
  if (!attResp.ok) {
    return { error: "Attachment fetch failed: " + JSON.stringify(attData).substring(0, 300) };
  }

  // Gmail returns base64url — convert to standard base64 for Anthropic
  const pdfB64 = String(attData.data || "").replace(/-/g, "+").replace(/_/g, "/");
  const pdfSize = attData.size || 0;
  const tFetch = Date.now() - t0;

  if (!pdfB64) {
    return { error: "Empty attachment data", attachment_size: pdfSize };
  }

  // 4. Call Anthropic with native PDF document block + JSON prefill
  const t1 = Date.now();
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
        {
          role: "assistant",
          content: [{ type: "text", text: "{" }],
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
    } catch { /* fall through */ }
    return {
      error: errMsg,
      attachment_size: pdfSize,
      timing_ms: { gmail_fetch: tFetch, claude: tClaude },
    };
  }

  let anthropicJson: any;
  try {
    anthropicJson = JSON.parse(respText);
  } catch (e: any) {
    return { error: "Anthropic response not JSON: " + e.message, raw: respText.slice(0, 500) };
  }

  if (!anthropicJson.content || !Array.isArray(anthropicJson.content)) {
    return {
      error: "Anthropic response missing content",
      raw: respText.slice(0, 500),
    };
  }
  const modelText = anthropicJson.content.map((b: any) => b.text || "").join("").trim();
  if (!modelText) {
    return {
      error: `Anthropic returned empty text. stop_reason=${anthropicJson.stop_reason || "?"}`,
      usage: anthropicJson.usage || null,
    };
  }

  // 5. Reconstruct + parse JSON (we prefilled "{")
  const fullJson = "{" + modelText;
  const cleaned = fullJson.replace(/```json\s*$/i, "").replace(/```\s*$/, "").trim();

  let extracted: any;
  let parseError: string | null = null;
  try {
    extracted = JSON.parse(cleaned);
  } catch (e: any) {
    parseError = e.message;
  }

  // 6. Spot-check against Dec 2025 targets if applicable (informational only)
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

  return {
    ok: !parseError,
    parse_error: parseError,
    parse_error_tail: parseError ? cleaned.slice(-200) : null,
    period: extracted?.period ?? null,
    period_end_date: extracted?.period_end_date ?? null,
    pl_line_count: (extracted?.pl_line_items || []).length,
    bs_line_count: (extracted?.balance_sheet?.line_items || []).length,
    pl_totals: extracted?.pl_totals ?? null,
    ebitda_inputs: extracted?.ebitda_inputs ?? null,
    balance_sheet_subtotals: extracted?.balance_sheet?.subtotals ?? null,
    cash_flow: extracted?.cash_flow ?? null,
    notes: extracted?.notes ?? null,
    spot_check_dec_2025: targets,
    extracted, // full payload — useful for diffing the prompt
    timing_ms: { gmail_fetch: tFetch, claude: tClaude, total: Date.now() - t0 },
    pdf_size_bytes: pdfSize,
    anthropic_usage: anthropicJson.usage || null,
    anthropic_stop_reason: anthropicJson.stop_reason || null,
    account_doc_id: tokenInfo.resolvedDocId,
  };
}

// ── Router ─────────────────────────────────────────────────────────────────
export default async (req: Request, _context: Context) => {
  const url = new URL(req.url);
  const action = (url.searchParams.get("action") || "").toLowerCase();

  const clientId = process.env["GOOGLE_CLIENT_ID"];
  const clientSecret = process.env["GOOGLE_CLIENT_SECRET"];
  const firebaseKey = process.env["FIREBASE_API_KEY"];
  const anthropicKey = process.env["ANTHROPIC_API_KEY"];

  if (action === "health" || action === "") {
    return json({
      ok: true,
      function: "marginiq-extract-test",
      version: "v2.50.x-verify",
      env: {
        GOOGLE_CLIENT_ID: !!clientId,
        GOOGLE_CLIENT_SECRET: !!clientSecret,
        FIREBASE_API_KEY: !!firebaseKey,
        ANTHROPIC_API_KEY: !!anthropicKey,
      },
      usage: {
        list_emails: "?action=list-cpa-emails[&limit=50]",
        extract: "?action=extract&message_id=...&attachment_id=...&account_doc_id=...",
      },
    });
  }

  if (!clientId || !clientSecret || !firebaseKey || !anthropicKey) {
    return json({ error: "Required env vars missing", env_present: { clientId: !!clientId, clientSecret: !!clientSecret, firebaseKey: !!firebaseKey, anthropicKey: !!anthropicKey } }, 500);
  }

  if (action === "list-cpa-emails") {
    const rawLimit = parseInt(url.searchParams.get("limit") || "50", 10);
    const limit = Math.min(Math.max(rawLimit || 50, 1), 200);
    try {
      const emails = await listAmpCpaEmails(clientId, clientSecret, firebaseKey, limit);
      return json({ ok: true, count: emails.length, emails });
    } catch (e: any) {
      return json({ error: e.message || "list failed" }, 500);
    }
  }

  if (action === "extract") {
    const messageId = url.searchParams.get("message_id") || "";
    const attachmentId = url.searchParams.get("attachment_id") || "";
    const accountDocId = url.searchParams.get("account_doc_id") || "";
    const accountEmail = url.searchParams.get("account_email") || "";
    if (!messageId || !attachmentId) {
      return json({ error: "message_id and attachment_id are required" }, 400);
    }
    try {
      const result = await extractOne(
        clientId, clientSecret, firebaseKey, anthropicKey,
        messageId, attachmentId, accountDocId, accountEmail
      );
      return json(result);
    } catch (e: any) {
      return json({ error: e.message || "extract failed" }, 500);
    }
  }

  return json({ error: `unknown action: ${action}`, valid: ["health", "list-cpa-emails", "extract"] }, 400);
};

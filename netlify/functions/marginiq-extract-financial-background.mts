import type { Context } from "@netlify/functions";

/**
 * Davis MarginIQ — Single-PDF financial extraction worker (v2.50.1).
 *
 * Background function (15-min wall clock). Runs ONE financial PDF through
 * the full pipeline:
 *
 *   1. Read extract_jobs/{period} doc.
 *   2. Acquire PDF bytes — preferred path is Gmail-direct (job carries
 *      gmail_message_id + gmail_attachment_id + gmail_account_doc_id).
 *      Falls back to Firebase Storage if the job has pdf_storage_path
 *      instead (legacy path).
 *   3. POST to Anthropic with native PDF document block.
 *   4. Parse extracted JSON, write audited_financials_v2/{period}.
 *   5. Update extract_jobs/{period} state -> complete | failed.
 *
 * Triggered by: marginiq-extract-financials-batch dispatcher, one
 * invocation per PDF, up to N concurrent (controlled by the dispatcher).
 *
 * Why this design:
 *   - Server-side: browser doesn't need to stay open
 *   - Native PDF: Claude reads PDF text layer directly, more accurate than
 *     image-based OCR, no client rendering, no scale tuning
 *   - Per-job state: failures don't kill the batch, retryable individually
 *   - 16K max_tokens: full line-item schema fits without truncation
 *   - Sonnet 4.6 / Opus 4.6 / Opus 4.7 reject assistant-message prefill
 *     with a 400. Rely on the prompt's "Start with {" instruction plus
 *     two-sided markdown-fence stripping in the parser.
 */

const PROJECT_ID = "davismarginiq";
const STORAGE_BUCKET = "davismarginiq.firebasestorage.app";

function fsBase(): string {
  return `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents`;
}
function storageUrl(path: string): string {
  return `https://firebasestorage.googleapis.com/v0/b/${STORAGE_BUCKET}/o/${encodeURIComponent(path)}?alt=media`;
}

// ── Firestore helpers ───────────────────────────────────────────────────────
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
function fsValueToJs(v: any): any {
  if (!v) return null;
  if ("stringValue" in v) return v.stringValue;
  if ("integerValue" in v) return Number(v.integerValue);
  if ("doubleValue" in v) return Number(v.doubleValue);
  if ("booleanValue" in v) return v.booleanValue;
  if ("nullValue" in v) return null;
  if ("arrayValue" in v) return (v.arrayValue.values || []).map(fsValueToJs);
  if ("mapValue" in v) {
    const out: Record<string, any> = {};
    for (const [k, val] of Object.entries(v.mapValue.fields || {})) out[k] = fsValueToJs(val);
    return out;
  }
  return null;
}

async function readDoc(path: string): Promise<any | null> {
  const apiKey = Netlify.env.get("FIREBASE_API_KEY");
  const resp = await fetch(`${fsBase()}/${path}?key=${apiKey}`);
  if (!resp.ok) return null;
  const data: any = await resp.json();
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(data.fields || {})) out[k] = fsValueToJs(v);
  return out;
}

// Writes (PATCHes) a Firestore doc. Defaults to MERGE semantics — only the
// fields you pass in are touched; everything else is preserved. Set
// merge=false to replace the entire document.
//
// Why this matters: Firestore REST's PATCH replaces the whole document
// unless `updateMask.fieldPaths` is specified. The original code relied on
// merge but never set the mask, which silently wiped fields on every write
// after the first. Surfaced when the batch counter started incrementing
// but other batch fields disappeared.
async function writeDoc(
  path: string,
  fields: Record<string, any>,
  merge: boolean = true,
): Promise<boolean> {
  const apiKey = Netlify.env.get("FIREBASE_API_KEY");
  const params = new URLSearchParams();
  params.set("key", apiKey || "");
  if (merge) {
    for (const k of Object.keys(fields)) params.append("updateMask.fieldPaths", k);
  }
  const resp = await fetch(`${fsBase()}/${path}?${params.toString()}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ fields: toFsFields(fields) }),
  });
  return resp.ok;
}

// ── The extraction prompt (matches v2.50.0 schema) ─────────────────────────
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

export default async (req: Request, _context: Context) => {
  if (req.method !== "POST") return new Response("POST required", { status: 405 });

  const apiKey = Netlify.env.get("ANTHROPIC_API_KEY");
  const fbKey = Netlify.env.get("FIREBASE_API_KEY");
  if (!apiKey || !fbKey) return new Response("API keys not configured", { status: 500 });

  let body: any;
  try { body = await req.json(); }
  catch { return new Response("Invalid JSON", { status: 400 }); }

  const period: string = body.period;
  const batchId: string | null = body.batch_id || null;
  if (!period || !/^\d{4}-\d{2}$/.test(period)) {
    return new Response("period required (YYYY-MM)", { status: 400 });
  }

  const jobPath = `extract_jobs/${period}`;

  try {
    // 1. Read job doc
    const job = await readDoc(jobPath);
    if (!job) {
      return new Response(`Job ${period} not found`, { status: 404 });
    }

    // Two source modes:
    //   (A) Gmail-direct (preferred): job has gmail_message_id +
    //       gmail_attachment_id + gmail_account_doc_id (and optionally
    //       gmail_account_email). The worker refreshes the OAuth token
    //       and pulls the PDF bytes from Gmail.
    //   (B) Storage (legacy): job has pdf_storage_path pointing at
    //       Firebase Storage. Kept for backward compatibility.
    const gmailMessageId: string = job.gmail_message_id || "";
    const gmailAttachmentId: string = job.gmail_attachment_id || "";
    const gmailAccountDocId: string = job.gmail_account_doc_id || "";
    const gmailAccountEmail: string = job.gmail_account_email || "";
    const pdfPath: string = job.pdf_storage_path || "";

    const useGmail = Boolean(gmailMessageId && gmailAttachmentId);

    if (!useGmail && !pdfPath) {
      await writeDoc(jobPath, {
        state: "failed",
        error: "Job missing both gmail_message_id and pdf_storage_path",
        completed_at: new Date().toISOString(),
      });
      return new Response("Bad job", { status: 400 });
    }

    await writeDoc(jobPath, {
      state: "running",
      started_at: new Date().toISOString(),
    });

    // 2. Acquire PDF bytes (Gmail or Storage)
    let pdfBytes: Uint8Array;
    if (useGmail) {
      const fbKey2 = Netlify.env.get("FIREBASE_API_KEY")!;
      const clientId = Netlify.env.get("GOOGLE_CLIENT_ID");
      const clientSecret = Netlify.env.get("GOOGLE_CLIENT_SECRET");
      if (!clientId || !clientSecret) {
        throw new Error("Gmail OAuth env vars missing (GOOGLE_CLIENT_ID/SECRET)");
      }

      // Resolve refresh token: prefer the docId stored on the job, then a
      // computed slug from the email, then the legacy singleton.
      const candidates: string[] = [];
      if (gmailAccountDocId) candidates.push(gmailAccountDocId);
      if (gmailAccountEmail) {
        const slug = gmailAccountEmail.toLowerCase()
          .replace(/@/g, "_at_").replace(/[^a-z0-9_]/g, "_").slice(0, 100);
        candidates.push(`gmail_tokens_${slug}`);
      }
      candidates.push("gmail_tokens"); // legacy fallback

      let refreshToken: string | null = null;
      for (const docId of candidates) {
        const r = await fetch(
          `${fsBase()}/marginiq_config/${docId}?key=${fbKey2}`
        );
        if (!r.ok) continue;
        const d: any = await r.json();
        const tok = d?.fields?.refresh_token?.stringValue;
        if (tok) { refreshToken = tok; break; }
      }
      if (!refreshToken) throw new Error("Gmail not connected. Reconnect.");

      // Refresh access token
      const refreshResp = await fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          client_id: clientId,
          client_secret: clientSecret,
          refresh_token: refreshToken,
          grant_type: "refresh_token",
        }),
      });
      const refreshData: any = await refreshResp.json();
      if (!refreshResp.ok || !refreshData.access_token) {
        throw new Error("Token refresh failed: " + JSON.stringify(refreshData).substring(0, 300));
      }

      // Fetch attachment bytes (Gmail returns base64url)
      const attResp = await fetch(
        `https://gmail.googleapis.com/gmail/v1/users/me/messages/${gmailMessageId}/attachments/${gmailAttachmentId}`,
        { headers: { Authorization: `Bearer ${refreshData.access_token}` } }
      );
      const attData: any = await attResp.json();
      if (!attResp.ok) {
        throw new Error("Attachment fetch failed: " + JSON.stringify(attData).substring(0, 300));
      }
      const b64url: string = attData.data || "";
      if (!b64url) throw new Error("Empty attachment data");
      const stdB64 = b64url.replace(/-/g, "+").replace(/_/g, "/");
      pdfBytes = Uint8Array.from(Buffer.from(stdB64, "base64"));
      console.log(`[extract ${period}] PDF (Gmail): ${pdfBytes.length} bytes`);
    } else {
      // Legacy Storage path
      const pdfResp = await fetch(storageUrl(pdfPath));
      if (!pdfResp.ok) {
        const errText = await pdfResp.text();
        throw new Error(`PDF download failed (${pdfResp.status}): ${errText.slice(0, 200)}`);
      }
      pdfBytes = new Uint8Array(await pdfResp.arrayBuffer());
      console.log(`[extract ${period}] PDF (Storage): ${pdfBytes.length} bytes`);
    }

    const pdfB64 = Buffer.from(pdfBytes).toString("base64");


    // 3. Call Anthropic with native PDF document block.
    // NOTE: Sonnet 4.6 / Opus 4.6 / Opus 4.7 reject assistant-message prefill
    // with a 400. We rely on the prompt's "Start your response with the
    // opening brace {" instruction plus two-sided fence stripping below.
    const anthropicResp = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
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

    if (!anthropicResp.ok) {
      let errMsg = `Vision API ${anthropicResp.status}`;
      try {
        const j = JSON.parse(respText);
        if (j.error?.message) errMsg = `Vision API ${anthropicResp.status}: ${j.error.message}`;
      } catch { /* fall through */ }
      throw new Error(errMsg);
    }

    const anthropicJson: any = JSON.parse(respText);
    if (!anthropicJson.content || !Array.isArray(anthropicJson.content)) {
      throw new Error(`Vision response missing content: ${respText.slice(0, 200)}`);
    }
    let modelText = anthropicJson.content.map((b: any) => b.text || "").join("").trim();
    if (!modelText) throw new Error(`Vision returned empty text. stop_reason=${anthropicJson.stop_reason || "?"}`);

    // Parse JSON. Model is instructed to start with "{" — strip any
    // markdown fences the model may have added on either side.
    let cleaned = modelText
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

    // 4. Write to audited_financials_v2/{period}
    const record = {
      ...extracted,
      period,
      // Source provenance — useful for re-extraction debugging.
      pdf_storage_path: pdfPath || null,
      gmail_message_id: gmailMessageId || null,
      gmail_attachment_id: gmailAttachmentId || null,
      gmail_account_doc_id: gmailAccountDocId || null,
      source: useGmail ? "gmail" : "storage",
      extracted_at: new Date().toISOString(),
      extraction_version: "2.50.1-gmail-direct",
      anthropic_input_tokens: anthropicJson.usage?.input_tokens || null,
      anthropic_output_tokens: anthropicJson.usage?.output_tokens || null,
    };
    const writeOk = await writeDoc(`audited_financials_v2/${period}`, record);
    if (!writeOk) throw new Error(`Failed to write audited_financials_v2/${period}`);

    // 5. Mark job complete
    await writeDoc(jobPath, {
      state: "complete",
      completed_at: new Date().toISOString(),
      input_tokens: anthropicJson.usage?.input_tokens || null,
      output_tokens: anthropicJson.usage?.output_tokens || null,
      pl_line_count: (extracted.pl_line_items || []).length,
      bs_line_count: (extracted.balance_sheet?.line_items || []).length,
    });

    // 6. Increment batch counter (best effort — non-atomic but fine for status display)
    if (batchId) {
      const batch = await readDoc(`extract_batches/${batchId}`);
      if (batch) {
        const newCompleted = (batch.completed_count || 0) + 1;
        const failed = batch.failed_count || 0;
        const total = batch.total_count || 0;
        const finished = total > 0 && (newCompleted + failed) >= total;
        await writeDoc(`extract_batches/${batchId}`, {
          completed_count: newCompleted,
          updated_at: new Date().toISOString(),
          ...(finished ? { state: failed > 0 ? "complete_with_errors" : "complete", finished_at: new Date().toISOString() } : {}),
        });
      }
    }

    console.log(`[extract ${period}] OK ${(extracted.pl_line_items || []).length} P&L lines, ${(extracted.balance_sheet?.line_items || []).length} BS lines`);
    return new Response(JSON.stringify({ ok: true, period }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });

  } catch (e: any) {
    const errMsg = e.message || String(e);
    console.error(`[extract ${period}] FAILED: ${errMsg}`);
    await writeDoc(jobPath, {
      state: "failed",
      error: errMsg.slice(0, 500),
      completed_at: new Date().toISOString(),
    });
    if (batchId) {
      const batch = await readDoc(`extract_batches/${batchId}`);
      if (batch) {
        const completed = batch.completed_count || 0;
        const newFailed = (batch.failed_count || 0) + 1;
        const total = batch.total_count || 0;
        const finished = total > 0 && (completed + newFailed) >= total;
        await writeDoc(`extract_batches/${batchId}`, {
          failed_count: newFailed,
          updated_at: new Date().toISOString(),
          ...(finished ? { state: completed > 0 ? "complete_with_errors" : "failed", finished_at: new Date().toISOString() } : {}),
        });
      }
    }
    return new Response(JSON.stringify({ error: errMsg }), { status: 500 });
  }
};

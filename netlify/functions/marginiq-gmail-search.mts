import type { Context, Config } from "@netlify/functions";

// Vendor-specific Gmail search queries for MarginIQ data sources.
// Each query returns emails likely to contain importable weekly reports.
const VENDOR_QUERIES: Record<string, string> = {
  // NuVizz: daily/weekly driver stops CSV export
  nuvizz: 'from:nuvizzapps@nuvizzapps.com has:attachment',

  // Uline weekly billing (DAS files — the delivery/truckload/accessorial
  // xlsx files). Filename-based filter because billing emails arrive from
  // various @uline.com senders AND from Chad's own billing@davisdelivery.com
  // (used for corrected/updated accessorial + TK files). The attachments
  // always start with 'das ' (e.g. 'das 20250301-20250307.xlsx'). Excludes
  // the AP-Freight DDIS remittance emails which have their own vendor entry.
  uline: '(from:@uline.com OR from:billing@davisdelivery.com) filename:das filename:xlsx -from:APFreight@uline.com',

  // Uline DDIS payment remittance — CSV files listing paid PROs. From the
  // APFreight@uline.com sender with subject = filename (DDIS820_*.csv).
  ddis: 'from:APFreight@uline.com filename:csv',

  // FuelFox: invoices sent VIA QuickBooks on FuelFox's behalf.
  // Subject always contains "FuelFox Atlanta". Each email has 2 PDFs
  // (Service Log + Invoice_DDxxx) that must be processed as a pair.
  fuelfox: '(from:quickbooks@notification.intuit.com subject:"FuelFox Atlanta") has:attachment',

  // Quick Fuel: weekly fuel card invoice from Flyers Energy
  // Locked to sender only — loose text matching pulls unrelated docs
  quickfuel: 'from:ebilling@4flyers.com has:attachment',

  // AMP CPAs: monthly audited financials (P&L, Balance Sheet, Cash Flow).
  // Filename always starts with "Financial Statements" and ends with "DDS.pdf".
  // Invoices from the same sender (Invoice #XXXXX) are intentionally excluded
  // by filtering on the "Financial" filename keyword.
  ampcpas: 'from:@ampcpas.com filename:"Financial Statements" filename:pdf',

  // v2.40.2: billing@ → Uline. Only emails billing@davisdelivery.com sent
  // out to an @uline.com recipient, with an attachment. Focuses on outbound
  // correspondence to Uline (disputes, corrections, POD replies, reshipments,
  // etc.) and filters out everything else billing@ has sent. Gmail's to:
  // operator matches To/Cc/Bcc so CC'd Uline reps are included.
  billing_sent: 'from:billing@davisdelivery.com to:@uline.com has:attachment',
};

// v2.40: a connected Gmail account with enough info to run a search on its behalf.
type TokenDoc = { docId: string; email: string; refresh_token: string };

// List every connected Gmail account stored in marginiq_config. Accepts both
// the legacy singleton (`gmail_tokens`) and per-account docs (`gmail_tokens_*`).
// If the same email has both, the per-account doc wins.
async function listConnectedAccounts(projectId: string, apiKey: string): Promise<TokenDoc[]> {
  const accounts: Record<string, TokenDoc> = {};
  const listResp = await fetch(
    `https://firestore.googleapis.com/v1/projects/${projectId}/databases/(default)/documents/marginiq_config?key=${apiKey}&pageSize=100`
  );
  if (!listResp.ok) return [];
  const listData = await listResp.json();
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

async function getFreshAccessToken(
  clientId: string, clientSecret: string, refreshToken: string
): Promise<{ accessToken?: string; error?: string }> {
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
  const data = await refreshResp.json();
  if (!refreshResp.ok || !data.access_token) {
    return { error: "Token refresh failed: " + JSON.stringify(data).substring(0, 300) };
  }
  return { accessToken: data.access_token };
}

// Run a vendor search against a single inbox. Each returned result is tagged
// with account_email + account_doc_id so the dedup pass and the attachment
// fetcher can route correctly.
async function searchOneInbox(
  account: TokenDoc, accessToken: string, query: string, maxResults: number
): Promise<{ account: string; results: any[]; error?: string }> {
  try {
    // v2.40.11: page Gmail list API. Single call returns at most 500 message
    // IDs and a nextPageToken; older DDIS history (100+ weekly files over
    // ~2 years) was being clipped. We loop pageToken until we have maxResults
    // OR Gmail says there are no more pages.
    const messages: Array<{ id: string }> = [];
    let pageToken: string | undefined;
    let pages = 0;
    const GMAIL_PAGE_SIZE = 500; // Gmail API hard max per call
    while (messages.length < maxResults && pages < 20) {
      const url = new URL("https://gmail.googleapis.com/gmail/v1/users/me/messages");
      url.searchParams.set("q", query);
      url.searchParams.set("maxResults", String(Math.min(GMAIL_PAGE_SIZE, maxResults - messages.length)));
      if (pageToken) url.searchParams.set("pageToken", pageToken);
      const searchResp = await fetch(url.toString(), { headers: { Authorization: `Bearer ${accessToken}` } });
      const searchData = await searchResp.json();
      if (!searchResp.ok) {
        return { account: account.email, results: [], error: "Gmail search failed: " + JSON.stringify(searchData).substring(0, 300) };
      }
      const batch = searchData.messages || [];
      if (batch.length === 0) break;
      messages.push(...batch);
      pageToken = searchData.nextPageToken;
      pages += 1;
      if (!pageToken) break;
    }
    if (messages.length === 0) return { account: account.email, results: [] };

    const results = await Promise.all(messages.map(async (msg: any) => {
      try {
        const fullResp = await fetch(
          `https://gmail.googleapis.com/gmail/v1/users/me/messages/${msg.id}`,
          { headers: { Authorization: `Bearer ${accessToken}` } }
        );
        const full = await fullResp.json();
        const headers = full.payload?.headers || [];
        const getHeader = (name: string) =>
          headers.find((h: any) => h.name.toLowerCase() === name.toLowerCase())?.value || "";

        // Walk MIME tree for attachments
        const attachments: any[] = [];
        const walkParts = (part: any) => {
          if (part.filename && part.filename.length > 0 && part.body?.attachmentId) {
            attachments.push({
              filename: part.filename,
              size: part.body?.size || 0,
              attachmentId: part.body.attachmentId,
              mimeType: part.mimeType || "",
            });
          }
          if (part.parts) part.parts.forEach(walkParts);
        };
        if (full.payload) walkParts(full.payload);

        // Filter to data attachments (xlsx, xls, csv, pdf)
        const dataAttachments = attachments.filter(a => {
          const fn = a.filename.toLowerCase();
          return fn.endsWith(".xlsx") || fn.endsWith(".xls") || fn.endsWith(".csv") || fn.endsWith(".pdf");
        });

        const dateStr = getHeader("Date");
        const dateObj = dateStr ? new Date(dateStr) : null;
        const dateISO = dateObj && !isNaN(dateObj.getTime()) ? dateObj.toISOString() : null;

        return {
          emailId: msg.id,
          emailDate: dateISO,
          emailSubject: getHeader("Subject"),
          from: getHeader("From"),
          snippet: (full.snippet || "").substring(0, 200),
          attachments: dataAttachments,
          // v2.40: account routing metadata
          account_email: account.email,
          account_doc_id: account.docId,
        };
      } catch (e: any) {
        return {
          emailId: msg.id,
          error: e.message || "Failed to fetch details",
          account_email: account.email,
          account_doc_id: account.docId,
        };
      }
    }));

    return { account: account.email, results };
  } catch (e: any) {
    return { account: account.email, results: [], error: e.message || "Proxy error" };
  }
}

// Cross-account dedup. The same email thread can appear in both the sender's
// Sent folder (e.g. billing@) and the recipient's Inbox (e.g. chad@) when
// both are connected. Key by minute-bucket + sorted filename set. Prefer the
// result whose From: header matches the connected account (the sender's own
// copy is the canonical one — same bytes, simpler attribution).
function dedupeResults(results: any[]): any[] {
  const byKey: Record<string, any> = {};
  for (const r of results) {
    if (!r.attachments?.length) {
      // No attachments → can't collide on filename set; key uniquely per account.
      byKey[`noatt|${r.account_email}|${r.emailId}`] = r;
      continue;
    }
    const minuteBucket = (r.emailDate || "").slice(0, 16); // YYYY-MM-DDTHH:MM
    const filenames = r.attachments
      .map((a: any) => (a.filename || "").toLowerCase())
      .sort().join("|");
    const key = `${minuteBucket}|${filenames}`;
    const existing = byKey[key];
    if (!existing) { byKey[key] = r; continue; }
    const fromMatches = (c: any) => {
      if (!c.from || !c.account_email) return false;
      return c.from.toLowerCase().includes(c.account_email.toLowerCase());
    };
    if (fromMatches(r) && !fromMatches(existing)) byKey[key] = r;
  }
  return Object.values(byKey);
}

export default async (req: Request, context: Context) => {
  if (req.method !== "POST") {
    return json({ error: "POST required" }, 405);
  }

  const CLIENT_ID = process.env["GOOGLE_CLIENT_ID"];
  const CLIENT_SECRET = process.env["GOOGLE_CLIENT_SECRET"];
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  const PROJECT_ID = "davismarginiq";

  if (!CLIENT_ID || !CLIENT_SECRET || !FIREBASE_API_KEY) {
    return json({ error: "OAuth not configured" }, 500);
  }

  try {
    const body = await req.json();
    const vendor: string = (body.vendor || "").toLowerCase();
    const afterDate: string = body.afterDate || ""; // YYYY/MM/DD
    const beforeDate: string = body.beforeDate || ""; // YYYY/MM/DD
    const maxResults: number = Math.min(body.maxResults || 20, 1000);
    const accountFilter: string = (body.account_email || "").toLowerCase();

    if (!vendor || !VENDOR_QUERIES[vendor]) {
      return json({ error: `Unknown vendor. Supported: ${Object.keys(VENDOR_QUERIES).join(", ")}` }, 400);
    }

    // v2.40: enumerate all connected accounts (legacy singleton + per-account)
    const accounts = await listConnectedAccounts(PROJECT_ID, FIREBASE_API_KEY);
    if (!accounts.length) {
      return json({ error: "No Gmail accounts connected. Go to Settings → Gmail → Connect." }, 400);
    }

    // Optional single-account filter — caller can pin a search to one inbox.
    const targets = accountFilter
      ? accounts.filter(a => a.email.toLowerCase() === accountFilter)
      : accounts;
    if (!targets.length) {
      return json({ error: `No connected account matches ${accountFilter}` }, 400);
    }

    // Build query (shared across all target inboxes)
    let query = VENDOR_QUERIES[vendor];
    if (afterDate) query += ` after:${afterDate}`;
    if (beforeDate) query += ` before:${beforeDate}`;

    // Fan out across inboxes in parallel
    const accountResults = await Promise.all(targets.map(async (acct) => {
      const { accessToken, error: authErr } = await getFreshAccessToken(CLIENT_ID, CLIENT_SECRET, acct.refresh_token);
      if (authErr || !accessToken) {
        return { account: acct.email, results: [], error: authErr || "no_access_token" };
      }
      return await searchOneInbox(acct, accessToken, query, maxResults);
    }));

    const flatResults = accountResults.flatMap(r => r.results);
    const deduped = dedupeResults(flatResults);
    deduped.sort((a: any, b: any) => (b.emailDate || "").localeCompare(a.emailDate || ""));

    return json({
      results: deduped,
      query,
      count: deduped.length,
      raw_count: flatResults.length,
      accounts_searched: targets.map(t => t.email),
      account_errors: accountResults.filter(r => r.error).map(r => ({ account: r.account, error: r.error })),
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

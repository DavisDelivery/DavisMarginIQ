import type { Context, Config } from "@netlify/functions";

// Vendor-specific Gmail search queries for MarginIQ data sources.
// Each query returns emails likely to contain importable weekly reports.
const VENDOR_QUERIES: Record<string, string> = {
  // NuVizz: daily/weekly driver stops CSV export
  nuvizz: 'from:nuvizzapps@nuvizzapps.com has:attachment',

  // Uline weekly billing (DAS files — the delivery/truckload/accessorial
  // xlsx files). Excludes the AP-Freight DDIS remittance emails which have
  // their own vendor entry below.
  uline: 'from:@uline.com -from:APFreight@uline.com has:attachment',

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
};

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
    const maxResults: number = Math.min(body.maxResults || 20, 50);

    if (!vendor || !VENDOR_QUERIES[vendor]) {
      return json({ error: `Unknown vendor. Supported: ${Object.keys(VENDOR_QUERIES).join(", ")}` }, 400);
    }

    // Load refresh token from Firestore
    const tokDocResp = await fetch(
      `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/gmail_tokens?key=${FIREBASE_API_KEY}`
    );
    if (!tokDocResp.ok) {
      return json({ error: "Gmail not connected. Go to Settings → Gmail → Connect." }, 400);
    }
    const tokDoc = await tokDocResp.json();
    const refreshToken = tokDoc?.fields?.refresh_token?.stringValue;
    if (!refreshToken) {
      return json({ error: "No refresh_token stored. Reconnect Gmail." }, 400);
    }

    // Get fresh access token
    const refreshResp = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: CLIENT_ID,
        client_secret: CLIENT_SECRET,
        refresh_token: refreshToken,
        grant_type: "refresh_token",
      }),
    });

    const refreshData = await refreshResp.json();
    if (!refreshResp.ok || !refreshData.access_token) {
      return json({ error: "Token refresh failed: " + JSON.stringify(refreshData).substring(0, 300) }, 500);
    }

    const accessToken = refreshData.access_token;

    // Build query
    let query = VENDOR_QUERIES[vendor];
    if (afterDate) query += ` after:${afterDate}`;

    // Search messages
    const searchUrl = `https://gmail.googleapis.com/gmail/v1/users/me/messages?q=${encodeURIComponent(query)}&maxResults=${maxResults}`;
    const searchResp = await fetch(searchUrl, { headers: { Authorization: `Bearer ${accessToken}` } });
    const searchData = await searchResp.json();

    if (!searchResp.ok) {
      return json({ error: "Gmail search failed: " + JSON.stringify(searchData).substring(0, 300) }, 500);
    }

    const messages = searchData.messages || [];
    if (messages.length === 0) {
      return json({ results: [], query });
    }

    // Fetch details for each message in parallel
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
        };
      } catch (e: any) {
        return { emailId: msg.id, error: e.message || "Failed to fetch details" };
      }
    }));

    // Sort newest first
    results.sort((a: any, b: any) => (b.emailDate || "").localeCompare(a.emailDate || ""));

    return json({ results, query, count: results.length });
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

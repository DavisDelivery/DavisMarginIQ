import type { Context, Config } from "@netlify/functions";

const FIREBASE_API_KEY = "AIzaSyDY2OceDzBWMHPR3C3O1oxktrCIy3mKMqU";
const PROJECT_ID = "glorybounddispatch";
const QBO_BASE = "https://quickbooks.api.intuit.com/v3/company";

async function getTokens() {
  const resp = await fetch(
    `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/qbo_tokens?key=${FIREBASE_API_KEY}`
  );
  if (!resp.ok) return null;
  const doc = await resp.json();
  if (!doc.fields) return null;
  return {
    access_token: doc.fields.access_token?.stringValue,
    refresh_token: doc.fields.refresh_token?.stringValue,
    realm_id: doc.fields.realm_id?.stringValue,
    expires_at: parseInt(doc.fields.expires_at?.integerValue || "0"),
  };
}

async function refreshTokens(refreshToken: string) {
  const QBO_CLIENT_ID = Netlify.env.get("QBO_CLIENT_ID");
  const QBO_CLIENT_SECRET = Netlify.env.get("QBO_CLIENT_SECRET");

  const resp = await fetch("https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: "Basic " + btoa(`${QBO_CLIENT_ID}:${QBO_CLIENT_SECRET}`),
      Accept: "application/json",
    },
    body: new URLSearchParams({ grant_type: "refresh_token", refresh_token: refreshToken }),
  });

  if (!resp.ok) throw new Error("Token refresh failed");
  const tokens = await resp.json();

  await fetch(
    `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/qbo_tokens?key=${FIREBASE_API_KEY}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        fields: {
          access_token: { stringValue: tokens.access_token },
          refresh_token: { stringValue: tokens.refresh_token },
          realm_id: { stringValue: tokens.realm_id || "" },
          expires_at: { integerValue: String(Date.now() + tokens.expires_in * 1000) },
          updated_at: { stringValue: new Date().toISOString() },
        },
      }),
    }
  );
  return tokens.access_token;
}

async function qboFetch(endpoint: string, token: string, realmId: string) {
  const resp = await fetch(`${QBO_BASE}/${realmId}/${endpoint}`, {
    headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
  });
  if (!resp.ok) throw new Error(`QBO ${resp.status}: ${await resp.text()}`);
  return resp.json();
}

export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  const action = url.searchParams.get("action");

  try {
    const tokens = await getTokens();
    if (!tokens?.access_token) {
      return new Response(JSON.stringify({ error: "Not connected to QuickBooks" }), { status: 401 });
    }

    let accessToken = tokens.access_token;
    if (Date.now() > tokens.expires_at - 60000) {
      accessToken = await refreshTokens(tokens.refresh_token!);
    }

    const rid = tokens.realm_id!;
    let data;

    switch (action) {
      case "status":
        return new Response(JSON.stringify({ connected: true, realm_id: rid }));

      case "company":
        data = await qboFetch(`companyinfo/${rid}`, accessToken, rid);
        break;

      case "invoices": {
        const start = url.searchParams.get("start") || "2026-01-01";
        const end = url.searchParams.get("end") || new Date().toISOString().split("T")[0];
        data = await qboFetch(`query?query=${encodeURIComponent(`SELECT * FROM Invoice WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' ORDERBY TxnDate DESC MAXRESULTS 500`)}`, accessToken, rid);
        break;
      }

      case "bills": {
        const start = url.searchParams.get("start") || "2026-01-01";
        const end = url.searchParams.get("end") || new Date().toISOString().split("T")[0];
        data = await qboFetch(`query?query=${encodeURIComponent(`SELECT * FROM Bill WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' ORDERBY TxnDate DESC MAXRESULTS 500`)}`, accessToken, rid);
        break;
      }

      case "expenses": {
        const start = url.searchParams.get("start") || "2026-01-01";
        const end = url.searchParams.get("end") || new Date().toISOString().split("T")[0];
        data = await qboFetch(`query?query=${encodeURIComponent(`SELECT * FROM Purchase WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' ORDERBY TxnDate DESC MAXRESULTS 500`)}`, accessToken, rid);
        break;
      }

      case "vendors":
        data = await qboFetch(`query?query=${encodeURIComponent("SELECT * FROM Vendor MAXRESULTS 200")}`, accessToken, rid);
        break;

      case "customers":
        data = await qboFetch(`query?query=${encodeURIComponent("SELECT * FROM Customer WHERE Active = true MAXRESULTS 200")}`, accessToken, rid);
        break;

      case "accounts":
        data = await qboFetch(`query?query=${encodeURIComponent("SELECT * FROM Account WHERE Active = true MAXRESULTS 200")}`, accessToken, rid);
        break;

      case "pnl": {
        const start = url.searchParams.get("start") || "2026-01-01";
        const end = url.searchParams.get("end") || new Date().toISOString().split("T")[0];
        data = await qboFetch(`reports/ProfitAndLoss?start_date=${start}&end_date=${end}&minorversion=65`, accessToken, rid);
        break;
      }

      case "attachment": {
        const id = url.searchParams.get("id");
        if (!id) return new Response(JSON.stringify({ error: "Missing attachment ID" }), { status: 400 });
        data = await qboFetch(`attachable/${id}`, accessToken, rid);
        break;
      }

      default:
        return new Response(JSON.stringify({ error: "Unknown action" }), { status: 400 });
    }

    return new Response(JSON.stringify(data));
  } catch (e: any) {
    console.error("QBO proxy error:", e);
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
};

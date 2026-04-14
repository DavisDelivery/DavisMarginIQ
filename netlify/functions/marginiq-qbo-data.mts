import type { Context, Config } from "@netlify/functions";

const QBO_BASE = "https://quickbooks.api.intuit.com/v3/company";
const PROJECT_ID = "davismarginiq";

// ── Firestore helpers ─────────────────────────────────────────────────────────
async function getTokens() {
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
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

async function saveTokens(tokens: any, realmId: string) {
  const FIREBASE_API_KEY = process.env["FIREBASE_API_KEY"];
  await fetch(
    `https://firestore.googleapis.com/v1/projects/${PROJECT_ID}/databases/(default)/documents/marginiq_config/qbo_tokens?key=${FIREBASE_API_KEY}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        fields: {
          access_token: { stringValue: tokens.access_token },
          refresh_token: { stringValue: tokens.refresh_token },
          realm_id: { stringValue: realmId },
          expires_at: { integerValue: String(Date.now() + tokens.expires_in * 1000) },
          updated_at: { stringValue: new Date().toISOString() },
        },
      }),
    }
  );
}

async function refreshAccessToken(refreshToken: string, realmId: string): Promise<string> {
  const QBO_CLIENT_ID = process.env["QBO_CLIENT_ID"];
  const QBO_CLIENT_SECRET = process.env["QBO_CLIENT_SECRET"];

  const resp = await fetch("https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Authorization: "Basic " + btoa(`${QBO_CLIENT_ID}:${QBO_CLIENT_SECRET}`),
      Accept: "application/json",
    },
    body: new URLSearchParams({ grant_type: "refresh_token", refresh_token: refreshToken }),
  });

  if (!resp.ok) throw new Error(`Token refresh failed: ${await resp.text()}`);
  const tokens = await resp.json();
  await saveTokens(tokens, realmId);
  return tokens.access_token;
}

// ── QBO API fetch ─────────────────────────────────────────────────────────────
async function qbo(path: string, token: string, realmId: string) {
  const resp = await fetch(`${QBO_BASE}/${realmId}/${path}`, {
    headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
  });
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`QBO ${resp.status}: ${body}`);
  }
  return resp.json();
}

function qql(query: string) {
  return `query?query=${encodeURIComponent(query)}&minorversion=65`;
}

// ── Main handler ──────────────────────────────────────────────────────────────
export default async (req: Request, context: Context) => {
  const url = new URL(req.url);
  const action = url.searchParams.get("action");
  const start = url.searchParams.get("start") || `${new Date().getFullYear()}-01-01`;
  const end = url.searchParams.get("end") || new Date().toISOString().slice(0, 10);

  try {
    // Load and auto-refresh tokens
    const stored = await getTokens();
    if (!stored?.access_token) {
      return new Response(JSON.stringify({ error: "Not connected to QuickBooks", connected: false }), {
        status: 401, headers: { "Content-Type": "application/json" },
      });
    }

    let token = stored.access_token;
    const realmId = stored.realm_id!;

    if (Date.now() > stored.expires_at - 120000) {
      token = await refreshAccessToken(stored.refresh_token!, realmId);
    }

    let data: any;

    switch (action) {

      // ── Connection status ──────────────────────────────────────────────────
      case "status":
        return new Response(JSON.stringify({ connected: true, realm_id: realmId }), {
          headers: { "Content-Type": "application/json" },
        });

      // ── Company info ───────────────────────────────────────────────────────
      case "company":
        data = await qbo(`companyinfo/${realmId}`, token, realmId);
        break;

      // ── P&L Report ─────────────────────────────────────────────────────────
      case "pnl":
        data = await qbo(
          `reports/ProfitAndLoss?start_date=${start}&end_date=${end}&accounting_method=Accrual&minorversion=65`,
          token, realmId
        );
        break;

      // ── Balance Sheet ──────────────────────────────────────────────────────
      case "balance_sheet":
        data = await qbo(
          `reports/BalanceSheet?date=${end}&accounting_method=Accrual&minorversion=65`,
          token, realmId
        );
        break;

      // ── Invoices (revenue) ─────────────────────────────────────────────────
      case "invoices":
        data = await qbo(
          qql(`SELECT * FROM Invoice WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' ORDERBY TxnDate DESC MAXRESULTS 1000`),
          token, realmId
        );
        break;

      // ── Bills (AP/costs) ───────────────────────────────────────────────────
      case "bills":
        data = await qbo(
          qql(`SELECT * FROM Bill WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' ORDERBY TxnDate DESC MAXRESULTS 1000`),
          token, realmId
        );
        break;

      // ── Expenses / Purchases ───────────────────────────────────────────────
      case "expenses":
        data = await qbo(
          qql(`SELECT * FROM Purchase WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' ORDERBY TxnDate DESC MAXRESULTS 1000`),
          token, realmId
        );
        break;

      // ── Payroll expenses (checks written to employees) ─────────────────────
      case "payroll":
        data = await qbo(
          qql(`SELECT * FROM VendorCredit WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' MAXRESULTS 500`),
          token, realmId
        );
        // Also grab payroll checks
        const checks = await qbo(
          qql(`SELECT * FROM Check WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' ORDERBY TxnDate DESC MAXRESULTS 500`),
          token, realmId
        );
        data = { payroll_checks: checks, vendor_credits: data };
        break;

      // ── Fuel & vehicle expenses ────────────────────────────────────────────
      case "fuel":
        data = await qbo(
          qql(`SELECT * FROM Purchase WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' AND AccountRef IN (SELECT Id FROM Account WHERE Name LIKE '%Fuel%' OR Name LIKE '%Gas%' OR Name LIKE '%Vehicle%') MAXRESULTS 500`),
          token, realmId
        );
        break;

      // ── Customers ─────────────────────────────────────────────────────────
      case "customers":
        data = await qbo(
          qql(`SELECT * FROM Customer WHERE Active = true MAXRESULTS 500`),
          token, realmId
        );
        break;

      // ── Vendors ───────────────────────────────────────────────────────────
      case "vendors":
        data = await qbo(
          qql(`SELECT * FROM Vendor WHERE Active = true MAXRESULTS 500`),
          token, realmId
        );
        break;

      // ── Chart of accounts ──────────────────────────────────────────────────
      case "accounts":
        data = await qbo(
          qql(`SELECT * FROM Account WHERE Active = true MAXRESULTS 500`),
          token, realmId
        );
        break;

      // ── Employees ─────────────────────────────────────────────────────────
      case "employees":
        data = await qbo(
          qql(`SELECT * FROM Employee WHERE Active = true MAXRESULTS 200`),
          token, realmId
        );
        break;

      // ── Dashboard summary (all key numbers in one call) ────────────────────
      case "dashboard": {
        const [invoices, bills, expenses, accounts] = await Promise.all([
          qbo(qql(`SELECT * FROM Invoice WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' MAXRESULTS 1000`), token, realmId),
          qbo(qql(`SELECT * FROM Bill WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' MAXRESULTS 1000`), token, realmId),
          qbo(qql(`SELECT * FROM Purchase WHERE TxnDate >= '${start}' AND TxnDate <= '${end}' MAXRESULTS 1000`), token, realmId),
          qbo(qql(`SELECT * FROM Account WHERE Active = true MAXRESULTS 500`), token, realmId),
        ]);

        const invList = invoices?.QueryResponse?.Invoice || [];
        const billList = bills?.QueryResponse?.Bill || [];
        const expList = expenses?.QueryResponse?.Purchase || [];

        const totalRevenue = invList.reduce((s: number, i: any) => s + parseFloat(i.TotalAmt || 0), 0);
        const totalBills = billList.reduce((s: number, b: any) => s + parseFloat(b.TotalAmt || 0), 0);
        const totalExpenses = expList.reduce((s: number, e: any) => s + parseFloat(e.TotalAmt || 0), 0);
        const totalCosts = totalBills + totalExpenses;
        const grossProfit = totalRevenue - totalCosts;
        const margin = totalRevenue > 0 ? (grossProfit / totalRevenue) * 100 : 0;

        data = {
          period: { start, end },
          revenue: totalRevenue,
          costs: totalCosts,
          bills: totalBills,
          expenses: totalExpenses,
          gross_profit: grossProfit,
          margin_pct: margin,
          invoice_count: invList.length,
          bill_count: billList.length,
          expense_count: expList.length,
        };
        break;
      }

      default:
        return new Response(JSON.stringify({ error: `Unknown action: ${action}` }), {
          status: 400, headers: { "Content-Type": "application/json" },
        });
    }

    return new Response(JSON.stringify(data), {
      headers: { "Content-Type": "application/json" },
    });

  } catch (e: any) {
    console.error("QBO proxy error:", e);
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500, headers: { "Content-Type": "application/json" },
    });
  }
};

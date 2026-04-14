# Davis MarginIQ v2.0 — Rebuilt by Opus

## What Changed (v1.3.0 → v2.0.0)

### The Problem
v1.3 was a shell — partial API plumbing with no actual margin intelligence.
QBO OAuth untested, no Uline parser, no cost engine, no profitability analysis.

### What v2.0 Adds
1. **Margin Engine** — Cross-references all data sources to calculate:
   - Cost per stop (fully loaded: labor + facility + insurance + overhead)
   - Margin per stop, per driver, per route, per truck
   - Break-even daily stop count
   - Annual/monthly/daily P&L projections

2. **Uline XLSX Parser** — Upload weekly audit files directly in the app
   - Parses all columns (pro, order, customer, city, zip, cost, weight, etc.)
   - Revenue by city, by weight band, by week
   - Saves to Firebase for historical analysis

3. **Cost Structure Config** — Pre-loaded with your actual numbers:
   - Warehouse $450K, 14 forklifts, 10 forklift ops
   - 16 box truck drivers @ $23/hr, 19 tractor @ $27.50/hr
   - 2 dispatchers, 3 admin, 2 mechanics
   - Editable and saves to Firebase

4. **Command Center** — One-screen view of:
   - Daily revenue/cost/margin with real-time calculations
   - Cost breakdown donut chart
   - Per-unit economics (per stop, per driver, per truck)
   - All data source connection status

5. **Davis Brand Blue Theme** — Matches the dispatch app aesthetic

### What's Preserved from v1.3
- All Netlify functions (QBO OAuth, QBO data, NuVizz proxy, Motive proxy)
- Firebase project (davismarginiq)
- CyberPay scraper + GitHub Actions workflow
- Deploy.html for self-deployment

## Deployment
Same as before — deploy via deploy.html (GitHub API) or push to GitHub.
Files to deploy: `public/index.html`, `public/MarginIQ.jsx`, `public/deploy.html`
Netlify functions deploy automatically from `netlify/functions/`

## Tabs
1. **🎯 Command Center** — KPIs, margin health, cost breakdown, data source status
2. **📦 Uline** — Upload & analyze Uline weekly audit XLSX files
3. **🚚 Operations** — NuVizz stop analytics with profitability overlay
4. **🚛 Fleet** — Motive vehicles, driver roster, fleet economics
5. **💰 QuickBooks** — QBO connection, P&L, invoices, expenses (raw data explorer)
6. **⚙️ Costs** — Full cost structure editor with live calculated totals
7. **🔧 Settings** — Connection status, system info

## Next Steps (Priority)
1. Test QBO OAuth flow end-to-end
2. Upload first Uline XLSX and verify parsing
3. Tweak cost structure numbers if anything has changed
4. Build formatted QBO data views (right now it shows raw JSON)
5. Add Motive mileage to margin calculations
6. Historical trend charts (week-over-week margin tracking)
7. Uline PRO → NuVizz stop cross-reference for per-stop actual revenue

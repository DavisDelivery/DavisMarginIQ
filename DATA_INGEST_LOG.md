# Davis MarginIQ — Data Ingest Log

Living record of what data has been fed into Davis MarginIQ. Updated after every ingestion.

Last updated: **2026-04-20** (v2.9.0 — Time Clock tab + AI analyst)

---

## Data streams

| Stream | Source | Cadence | Collection(s) | Status |
|---|---|---|---|---|
| Time Clock | B600 CyberPay web UI (`b600.atlantafreightquotes.com`) | Weekly, Mondays 9AM ET | `timeclock_weekly` | 🟢 Backfilled Jan 2025–Apr 2026 (67 weeks) · auto-pull pending endpoint config |
| QuickBooks | QBO OAuth connector | Continuous (OAuth-refreshed) | `qbo_*` / `marginiq_config/qbo_tokens` | 🟡 Infrastructure built, no data feed yet |
| NuVizz | NuVizz stop exports | Weekly | `nuvizz_stops`, `nuvizz_weekly` | 🟡 Function exists, no data feed yet |
| Uline | Weekly `.xlsx` from `@uline.com` senders via Gmail | Weekly (Friday week-ending) | `uline_weekly` | 🟡 Gmail search wired, no data feed yet |
| Payroll (checkstubs) | CyberPay PDF stubs | Weekly | `payroll_weekly` (via scan fn) | 🟡 Parser deployed (v2.3), no data feed yet |
| Reconciliation | DDIS820 CSVs (Uline payments) | Irregular | `recon_weekly`, `unpaid_stops`, `ddis_files` | 🟡 Limited window of historical data |
| Fuel | FuelFox + Quick Fuel PDFs via Gmail | Weekly | `fuel_weekly` | ⚪ Parser exists in Fleet Mgmt, not yet in MarginIQ |

---

## Time Clock (B600 CyberPay)

### Ingested batches

| Batch date | Window | Files / method | Rows | Weeks | Hours | Saved by |
|---|---|---|---|---|---|---|
| 2026-04-20 (Friday-keyed, superseded) | 2025-01-01 → 2026-04-10 | 4 CSV backfill (Data Ingest tab upload) | 14,773 | 67 | 126,893.57 | Chad (manual upload, v2.9.1) |
| 2026-04-20 (pending re-ingest) | 2025-01-01 → 2026-04-18 | 4 backfill CSVs + 1 icon CSV (Saturday-keyed) | — | — | — | Chad (after purge + re-upload, v2.10.0) |

### Coverage gaps
_None detected in the 4-CSV backfill — continuous weekly coverage Jan 2025 through Apr 10 2026._

### Auto-pull schedule
- Function: `marginiq-b600-timeclock.mts`
- Cron: `0 13 * * 1` (every Monday 9AM ET / 13:00 UTC)
- Window pulled: previous Mon–Sun
- Env vars required: `B600_BASE_URL`, `B600_USERNAME`, `B600_PASSWORD`, `B600_EXPORT_PATH`, `FIREBASE_API_KEY`
- Last run status logged in `marginiq_config/b600_last_pull`

### Auto-pull: B600 protocol (fully characterized 2026-04-20)

The B600 is an **Icon Time TotalPass B600** hardware clock (serial `B005-109-637`, sw v4.0.10103) at `b600.atlantafreightquotes.com`. The **Reports → Timecards → Export → CSV Extended** workflow produces the same 25-column CSV format as the manual backfill files. Flow:

1. `GET /login.html` → seed session cookie
2. `POST /login.html` body `username=X&password=Y&buttonClicked=Submit` (form-encoded) → authenticated session, 302 → `/index.html`
3. `GET /report.html?rt=2&from=MM/DD/YY&to=MM/DD/YY&eid=ss&export=1` → returns the CSV directly

Parameters:
- `rt=2` — Timecards report
- `eid=ss` — all employees
- `export=1` — CSV Extended (25 cols with header; `export=0`/no param would be CSV basic)

The `/payroll.html` POST preamble and the `/export.html` endpoint that the earlier "Paycom" investigation turned up are **not used** — they return a different, unheadered format that the MarginIQ parser doesn't speak. The Timecard Report export is the clean path.

### Known issues
- The direct `GET /export.html` returns **503** if hit without the preceding POST to `/payroll.html`. The browser's Submit button works because it POSTs first. The Netlify function replicates this, but the exact POST body the browser sends is still not fully characterized. If the first real run 503s, capture the browser's POST body via Dev Tools → Network → payroll.html → Payload.
- The schedule (`0 13 * * 1`) is **intentionally commented out** in the function until a successful manual test confirms the flow works end-to-end.

### Manual test command
Once env vars are set in Netlify (`B600_USERNAME`, `B600_PASSWORD`, `B600_BASE_URL`, `FIREBASE_API_KEY`):
```
curl https://davis-marginiq.netlify.app/.netlify/functions/marginiq-b600-timeclock
```
Successful response: `{"ok":true,"rows_fetched":N,"weeks_saved":M}`. Error response includes the specific failure reason.

### Fixes / history
- **2026-04-20 (commit `77eca82`)** — Fixed: SheetJS auto-converts CSV date columns (`01/17/25`) to Excel serial numbers (`45674`), causing every timeclock row to have `date=null` and the weekly rollup to produce 0 weeks. `parseDateMDYFlexible()` now handles numeric Excel serial input transparently.
- **2026-04-20 (v2.9.0)** — New Time Clock tab with overview stats, weekly drill-in (top 20 employees per week), anomaly detection (spike/drop vs trailing-8-week avg, OT >20%, staff drops), and AI chat (ask-anything via `marginiq-analyze-timeclock` Netlify function).
- **2026-04-20 (v2.9.1, commit `9e1cf83`)** — Fixed silent Firestore write failures on timeclock_weekly. Root cause: `buildTimeClockWeekly` returned `employees: undefined`, which Firestore rejects by default. Enabled `ignoreUndefinedProperties: true` globally and rewrote the return shape explicitly. First successful backfill: 14,773 entries → 67 weeks → 126,893.57 hrs.
- **2026-04-20 (v2.10.0)** — **Switched Time Clock to Saturday week-endings** (was Friday). B600's native "Last Week" export is Sun→Sat, so Friday-ending was splitting every B600 week across two MarginIQ buckets. A partial re-upload on 2026-04-20 that included 2 Sunday punches (4/12/26) overwrote the week-4/10 rollup and lost ~1,830 hrs. Added `weekEndingSaturday()` to the client, updated `parseTimeClock` + the B600 auto-pull function, and added `marginiq-purge-timeclock` endpoint (token-protected) to wipe the collection cleanly before the Saturday-keyed backfill.

---

## QuickBooks (QBO)
_No transactions ingested yet. Token storage works (`marginiq_config/qbo_tokens`). Next step: define which reports/entities to pull and on what cadence._

---

## NuVizz
_No stop data ingested yet. `nuvizz.mts` scraper exists; need to confirm auth + export format._

---

## Uline
_No weekly Excel files ingested yet. `marginiq-gmail-search.mts` finds them; ingest flow needs to pull, parse, and roll up to `uline_weekly`._

---

## Payroll (checkstubs)
_`marginiq-scan-payroll.mts` deployed (v2.3, commit `e85cec3`). No scans performed yet._

---

## Conventions

- **Week ending** = Friday, ISO date `YYYY-MM-DD`
- **Employee name** normalization: title-case, trimmed (see `normalizeName()` in `MarginIQ.jsx`)
- **Collections with `_weekly` suffix** are aggregate rollups keyed by Friday week-ending
- **Collections with `_daily` or no suffix** are raw row-level records
- All writes use `set({...}, {merge:true})` so re-ingestion is idempotent

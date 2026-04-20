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
| **2026-04-20** | 2025-01-01 → 2026-04-10 | 4 CSV backfill (Data Ingest tab upload) | 14,773 | 67 | 126,893.57 | Chad (manual upload, v2.9.1) |

### Coverage gaps
_None detected in the 4-CSV backfill — continuous weekly coverage Jan 2025 through Apr 10 2026._

### Auto-pull schedule
- Function: `marginiq-b600-timeclock.mts`
- Cron: `0 13 * * 1` (every Monday 9AM ET / 13:00 UTC)
- Window pulled: previous Mon–Sun
- Env vars required: `B600_BASE_URL`, `B600_USERNAME`, `B600_PASSWORD`, `B600_EXPORT_PATH`, `FIREBASE_API_KEY`
- Last run status logged in `marginiq_config/b600_last_pull`

### Known issues
- Auto-pull endpoint (`B600_EXPORT_PATH`) assumed to be `/reports/timeclock/export` with `?from=MM/DD/YY&to=MM/DD/YY&format=csv` — **needs verification against actual B600 Reports → Export CSV URL**.
- Auth method assumed Basic; may need session-cookie flow depending on how CyberPay login works.

### Fixes / history
- **2026-04-20 (commit `77eca82`)** — Fixed: SheetJS auto-converts CSV date columns (`01/17/25`) to Excel serial numbers (`45674`), causing every timeclock row to have `date=null` and the weekly rollup to produce 0 weeks. `parseDateMDYFlexible()` now handles numeric Excel serial input transparently.
- **2026-04-20 (v2.9.0)** — New Time Clock tab with overview stats, weekly drill-in (top 20 employees per week), anomaly detection (spike/drop vs trailing-8-week avg, OT >20%, staff drops), and AI chat (ask-anything via `marginiq-analyze-timeclock` Netlify function).

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

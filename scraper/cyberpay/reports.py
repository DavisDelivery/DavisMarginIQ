"""CyberPay report fetchers.

Two public entry points:
    fetch_latest_weekly_payroll(client, company_code) -> (meta, pdf_bytes)
    fetch_1099_report(client, company_code, from_date, to_date) -> (meta, file_bytes)

The list endpoints use Kendo grid JSON; the report-builder endpoints are
HTML forms that respond with a PDF/HTML report. First-live-run TODOs are
called out inline — see README for the verification checklist.
"""
from __future__ import annotations
import logging
from datetime import date, datetime, timedelta
from typing import Optional, Tuple
from urllib.parse import urlencode

from bs4 import BeautifulSoup

from cyberpay.session import CyberPayClient
from cyberpay.models import WeeklyPayrollReport, Report1099, PayrollGridRow

log = logging.getLogger("cyberpay.reports")

# --- endpoints -----------------------------------------------------------------

PAYROLL_GRID_READ = "/CPO/PayrollReport/PayrollReport_Read"
BUILT_IN_REPORT = "/CPO/PayrollReport/BuiltInreportBuild/{id}"
BUILT_IN_REPORT_1099_ID = 0
# TODO: verify on first live run — the PDF download endpoint.
# Based on the grid row's "StoredFile" value, the download is likely
# /CPO/PayrollReport/DownloadProviderUpload?fileName=... or similar.
PDF_DOWNLOAD_CANDIDATES = [
    "/CPO/PayrollReport/DownloadProviderUpload",
    "/CPO/PayrollReport/OpenReport",
    "/CPO/PayrollReport/DownloadReport",
]


# --- weekly payroll PDF --------------------------------------------------------

def fetch_latest_weekly_payroll(
    client: CyberPayClient,
    company_code: Optional[str] = None,
) -> Tuple[WeeklyPayrollReport, bytes]:
    """Grab the most recent weekly payroll PDF for the given company."""
    company = company_code or client.config.company_code
    rows = _list_payroll_grid(client)
    company_rows = [r for r in rows if (r.CompanyCode or "").startswith(company)]
    if not company_rows:
        raise RuntimeError(
            f"No payroll rows found for company {company} (got {len(rows)} total rows)"
        )
    # Pick the row with the most recent CheckDate.
    def _parse(d: str) -> date:
        return _parse_us_date(d or "1900-01-01")

    latest = max(company_rows, key=lambda r: _parse(r.CheckDate or ""))
    log.info(
        "Latest payroll for %s: Run %s, CheckDate %s, File %s",
        company, latest.RunID, latest.CheckDate, latest.StoredFile,
    )
    pdf_bytes, download_url = _download_stored_file(client, latest.StoredFile)
    meta = WeeklyPayrollReport(
        company_code=company,
        run_id=latest.RunID or "",
        check_date=_parse(latest.CheckDate or ""),
        from_date=_parse(latest.FromDate or ""),
        to_date=_parse(latest.ToDate or ""),
        pdf_filename=latest.StoredFile or "",
        pdf_download_url=download_url,
        pdf_size_bytes=len(pdf_bytes),
        created_at=_try_parse_datetime(latest.CreatedAt),
    )
    return meta, pdf_bytes


def list_all_weekly_payrolls(
    client: CyberPayClient,
    company_code: Optional[str] = None,
    since: Optional[date] = None,
) -> list[PayrollGridRow]:
    """Return every payroll grid row for the company (optionally filtered by date)."""
    company = company_code or client.config.company_code
    rows = _list_payroll_grid(client)
    out = [r for r in rows if (r.CompanyCode or "").startswith(company)]
    if since:
        out = [
            r for r in out
            if r.CheckDate and _parse_us_date(r.CheckDate) >= since
        ]
    return out


def _list_payroll_grid(client: CyberPayClient) -> list[PayrollGridRow]:
    """Kendo grids expect form-encoded paging params: page, pageSize, skip, take."""
    # Set take=500 to get everything in one pass.
    form = {
        "page": 1,
        "pageSize": 500,
        "skip": 0,
        "take": 500,
    }
    resp = client.post(PAYROLL_GRID_READ, data=form)
    resp.raise_for_status()
    # Kendo's default response shape: {"Data": [...], "Total": N} OR a plain array.
    payload = resp.json()
    data = payload.get("Data") if isinstance(payload, dict) else payload
    if data is None:
        log.warning("Unexpected payroll grid payload shape: %s", list(payload.keys()))
        data = []
    return [PayrollGridRow.model_validate(row) for row in data]


def _download_stored_file(
    client: CyberPayClient, filename: str
) -> Tuple[bytes, str]:
    """Try each candidate download endpoint until one returns a PDF."""
    last_err = None
    for path in PDF_DOWNLOAD_CANDIDATES:
        url = f"{path}?{urlencode({'fileName': filename})}"
        try:
            resp = client.get(url)
            ct = resp.headers.get("content-type", "")
            if resp.status_code == 200 and ("pdf" in ct.lower() or len(resp.content) > 1024):
                return resp.content, url
            last_err = f"{path} -> {resp.status_code} ({ct})"
        except Exception as e:
            last_err = f"{path} -> {e}"
    raise RuntimeError(
        f"Could not download PDF for {filename!r}. Tried: {last_err}. "
        "See README 'First live run' checklist — the PDF endpoint needs verification."
    )


# --- 1099 report ---------------------------------------------------------------

def fetch_1099_report(
    client: CyberPayClient,
    from_date: date,
    to_date: date,
    company_code: Optional[str] = None,
) -> Tuple[Report1099, bytes]:
    """Run the built-in 1099 Employees report for the given date range."""
    company = company_code or client.config.company_code
    build_url = BUILT_IN_REPORT.format(id=BUILT_IN_REPORT_1099_ID)
    # GET the builder page so we can extract hidden fields + anti-forgery token.
    form_page = client.get(build_url)
    form_page.raise_for_status()
    hidden_fields = _extract_hidden_fields(form_page.text)

    # TODO: verify these form field names on first live run. Based on the page
    # interactive dump: SearchBy radio (code/name), CompanyCode selector, and
    # either PredefinedRange or CustomRange with StartDate/EndDate.
    form = {
        **hidden_fields,
        "SearchBy": "CodeName",
        "CompanyCode": company,
        "RangeType": "Custom",
        "StartDate": from_date.strftime("%m/%d/%Y"),
        "EndDate": to_date.strftime("%m/%d/%Y"),
    }
    resp = client.post(
        build_url,
        data=form,
        headers={"Referer": f"{client.config.base_url}{build_url}"},
    )
    resp.raise_for_status()
    ct = resp.headers.get("content-type", "application/octet-stream")
    filename = f"1099-{company}-{from_date.isoformat()}_{to_date.isoformat()}.pdf"
    meta = Report1099(
        company_code=company,
        from_date=from_date,
        to_date=to_date,
        filename=filename,
        content_type=ct,
        size_bytes=len(resp.content),
    )
    return meta, resp.content


def _extract_hidden_fields(html: str) -> dict[str, str]:
    """Pull every <input type=hidden> on the page into a dict."""
    soup = BeautifulSoup(html, "lxml")
    out: dict[str, str] = {}
    for inp in soup.find_all("input", {"type": "hidden"}):
        name = inp.get("name")
        if name:
            out[name] = inp.get("value", "")
    return out


# --- date helpers --------------------------------------------------------------

def last_completed_week(today: Optional[date] = None) -> Tuple[date, date]:
    """Return (Monday, Sunday) of the most recently completed week."""
    today = today or date.today()
    # ISO: Monday=0 .. Sunday=6
    days_since_monday = today.weekday()
    this_monday = today - timedelta(days=days_since_monday)
    last_monday = this_monday - timedelta(days=7)
    last_sunday = last_monday + timedelta(days=6)
    return last_monday, last_sunday


def _parse_us_date(s: str) -> date:
    s = (s or "").strip()
    if not s:
        return date(1900, 1, 1)
    # Strip Kendo's "/Date(1234567890)/" wrapper if present.
    if s.startswith("/Date(") and s.endswith(")/"):
        ms = int(s[6:-2].split("+")[0].split("-")[0])
        return datetime.utcfromtimestamp(ms / 1000).date()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {s!r}")


def _try_parse_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None

"""Unit tests for date helpers and grid parsing."""
from datetime import date

from cyberpay.reports import _parse_us_date, last_completed_week
from cyberpay.models import PayrollGridRow


def test_parse_us_date_variants():
    assert _parse_us_date("04/10/2026") == date(2026, 4, 10)
    assert _parse_us_date("2026-04-10") == date(2026, 4, 10)
    assert _parse_us_date("") == date(1900, 1, 1)


def test_parse_us_date_kendo_format():
    # Kendo's JSON date format: /Date(milliseconds)/
    ms = int(date(2026, 4, 10).strftime("%s")) * 1000
    assert _parse_us_date(f"/Date({ms})/") == date(2026, 4, 10)


def test_last_completed_week_from_wednesday():
    # Wednesday 2026-04-15 -> last completed week is Mon 4/6 - Sun 4/12
    mon, sun = last_completed_week(today=date(2026, 4, 15))
    assert mon == date(2026, 4, 6)
    assert sun == date(2026, 4, 12)


def test_last_completed_week_from_monday():
    # Monday 2026-04-13 -> last completed week is Mon 4/6 - Sun 4/12
    mon, sun = last_completed_week(today=date(2026, 4, 13))
    assert mon == date(2026, 4, 6)
    assert sun == date(2026, 4, 12)


def test_payroll_grid_row_accepts_extra_fields():
    row = PayrollGridRow.model_validate({
        "CompanyCode": "0190",
        "RunId": "139",
        "CheckDate": "04/10/2026",
        "FromDate": "03/30/2026",
        "ToDate": "04/04/2026",
        "StoredFile": "Paper Delivery_0190_Combined_007.pdf",
        "ExtraFieldFromServer": "ignored-gracefully",
    })
    assert row.CompanyCode == "0190"
    assert row.RunID == "139"
    assert row.StoredFile.endswith(".pdf")

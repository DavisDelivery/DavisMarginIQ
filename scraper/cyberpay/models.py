"""Typed models for scraped CyberPay data."""
from __future__ import annotations
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class WeeklyPayrollReport(BaseModel):
    """A single weekly payroll run + its generated PDF."""
    company_code: str
    run_id: str
    check_date: date
    from_date: date
    to_date: date
    pdf_filename: str
    pdf_download_url: str
    pdf_size_bytes: Optional[int] = None
    created_at: Optional[datetime] = None


class Report1099(BaseModel):
    """1099 Employees report for a date range."""
    company_code: str
    from_date: date
    to_date: date
    filename: str
    content_type: str
    size_bytes: int


class PayrollGridRow(BaseModel):
    """Raw row from /CPO/PayrollReport/PayrollReport_Read (Kendo grid)."""
    # Field names here are a best-guess against the Kendo JSON shape;
    # confirm on first live run and adjust.
    CompanyCode: Optional[str] = None
    CompanyName: Optional[str] = None
    RunID: Optional[str] = Field(None, alias="RunId")
    CheckDate: Optional[str] = None
    FromDate: Optional[str] = None
    ToDate: Optional[str] = None
    StoredFile: Optional[str] = None
    CreatedAt: Optional[str] = None

    class Config:
        populate_by_name = True
        extra = "allow"

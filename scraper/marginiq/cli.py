"""marginiq CLI — scrape CyberPay and drop artifacts into data/."""
from __future__ import annotations
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import typer

from cyberpay.session import CyberPayClient
from cyberpay.reports import (
    fetch_latest_weekly_payroll,
    fetch_1099_report,
    last_completed_week,
    list_all_weekly_payrolls,
    _download_stored_file,
    _parse_us_date,
)
from cyberpay.models import WeeklyPayrollReport

# Firebase push is optional — only runs if FIREBASE_CREDENTIALS_JSON is set
def _maybe_push_to_firebase(meta, pdf_bytes, report_type="payroll"):
    if not os.environ.get("FIREBASE_CREDENTIALS_JSON") and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return
    try:
        from cyberpay.firebase_writer import write_payroll_run, write_1099_report
        if report_type == "payroll":
            doc_id = write_payroll_run(meta, pdf_bytes)
            logging.info("Firebase: payroll_runs/%s written", doc_id)
        else:
            doc_id = write_1099_report(meta, pdf_bytes)
            logging.info("Firebase: reports_1099/%s written", doc_id)
    except Exception as e:
        logging.warning("Firebase push failed (non-fatal): %s", e)

app = typer.Typer(help="CyberPayOnline scraper for marginiq.")
scrape_app = typer.Typer(help="Scrape subcommands.")
app.add_typer(scrape_app, name="scrape")


def _setup_logging(debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


def _ensure_out(out: Path) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    return out


@scrape_app.command("weekly-payroll")
def weekly_payroll(
    out: Path = typer.Option(Path("data"), help="Output directory."),
    debug: bool = typer.Option(False, help="Verbose HTTP logging."),
):
    """Download the most recent weekly payroll PDF."""
    _setup_logging(debug)
    _ensure_out(out)
    with CyberPayClient() as client:
        meta, pdf = fetch_latest_weekly_payroll(client)
        stem = f"weekly-payroll-{meta.company_code}-{meta.check_date.isoformat()}"
        pdf_path = out / f"{stem}.pdf"
        meta_path = out / f"{stem}.meta.json"
        pdf_path.write_bytes(pdf)
        meta_path.write_text(meta.model_dump_json(indent=2))
        typer.echo(f"Wrote {pdf_path} ({len(pdf):,} bytes)")
        typer.echo(f"Wrote {meta_path}")


@scrape_app.command("1099")
def scrape_1099(
    from_date_str: str = typer.Option(..., "--from", help="YYYY-MM-DD"),
    to_date_str: str = typer.Option(..., "--to", help="YYYY-MM-DD"),
    out: Path = typer.Option(Path("data"), help="Output directory."),
    debug: bool = typer.Option(False, help="Verbose HTTP logging."),
):
    """Run the 1099 Employees report for a date range and save the PDF."""
    _setup_logging(debug)
    _ensure_out(out)
    frm = date.fromisoformat(from_date_str)
    to = date.fromisoformat(to_date_str)
    with CyberPayClient() as client:
        meta, pdf = fetch_1099_report(client, frm, to)
        stem = f"1099-{meta.company_code}-{frm.isoformat()}_{to.isoformat()}"
        pdf_path = out / f"{stem}.pdf"
        meta_path = out / f"{stem}.meta.json"
        pdf_path.write_bytes(pdf)
        meta_path.write_text(meta.model_dump_json(indent=2))
        typer.echo(f"Wrote {pdf_path} ({len(pdf):,} bytes)")
        typer.echo(f"Wrote {meta_path}")


@scrape_app.command("weekly-all")
def weekly_all(
    out: Path = typer.Option(Path("data"), help="Output directory."),
    debug: bool = typer.Option(False, help="Verbose HTTP logging."),
):
    """Default weekly pull: latest payroll PDF + 1099 for last Mon-Sun."""
    _setup_logging(debug)
    _ensure_out(out)
    mon, sun = last_completed_week()
    logging.info("Last completed week: %s .. %s", mon, sun)
    with CyberPayClient() as client:
        p_meta, p_pdf = fetch_latest_weekly_payroll(client)
        stem = f"weekly-payroll-{p_meta.company_code}-{p_meta.check_date.isoformat()}"
        (out / f"{stem}.pdf").write_bytes(p_pdf)
        (out / f"{stem}.meta.json").write_text(p_meta.model_dump_json(indent=2))
        _maybe_push_to_firebase(p_meta, p_pdf, "payroll")

        t_meta, t_pdf = fetch_1099_report(client, mon, sun)
        stem = f"1099-{t_meta.company_code}-{mon.isoformat()}_{sun.isoformat()}"
        (out / f"{stem}.pdf").write_bytes(t_pdf)
        (out / f"{stem}.meta.json").write_text(t_meta.model_dump_json(indent=2))
        _maybe_push_to_firebase(t_meta, t_pdf, "1099")
        typer.echo("Weekly pull complete.")


@scrape_app.command("backfill")
def backfill(
    year: int = typer.Option(..., help="YYYY — pull all payrolls this year."),
    out: Path = typer.Option(Path("data"), help="Output directory."),
    debug: bool = typer.Option(False, help="Verbose HTTP logging."),
):
    """Backfill every weekly payroll PDF for a given year."""
    _setup_logging(debug)
    _ensure_out(out)
    with CyberPayClient() as client:
        rows = list_all_weekly_payrolls(client, since=date(year, 1, 1))
        typer.echo(f"Found {len(rows)} payroll rows for {year}.")
        for row in rows:
            if not row.StoredFile:
                continue
            try:
                pdf, url = _download_stored_file(client, row.StoredFile)
            except Exception as e:
                typer.echo(f"  FAILED {row.StoredFile}: {e}", err=True)
                continue
            check = row.CheckDate or "unknown"
            safe_check = check.replace("/", "-")
            pdf_path = out / f"weekly-payroll-{row.CompanyCode}-{safe_check}.pdf"
            pdf_path.write_bytes(pdf)
            typer.echo(f"  Wrote {pdf_path} ({len(pdf):,} bytes)")


if __name__ == "__main__":
    app()

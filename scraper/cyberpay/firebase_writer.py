"""Firebase writer — pushes scraped CyberPay data into Firestore and Storage.

Uses the Firebase Admin SDK (service account) so it can bypass Firestore
security rules (which deny client writes to payroll_runs, reports_1099).

Required env vars:
    FIREBASE_CREDENTIALS_JSON  — full service account JSON (as a string)
    OR
    GOOGLE_APPLICATION_CREDENTIALS — path to service account JSON file

Optional:
    FIREBASE_PROJECT_ID — defaults to "davismarginiq"
"""
from __future__ import annotations
import json
import logging
import os
from datetime import date, datetime
from typing import Optional

log = logging.getLogger("cyberpay.firebase_writer")

_db = None
_bucket = None


def _init():
    global _db, _bucket
    if _db is not None:
        return

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore, storage
    except ImportError:
        raise RuntimeError(
            "firebase-admin not installed. Run: pip install firebase-admin"
        )

    project_id = os.environ.get("FIREBASE_PROJECT_ID", "davismarginiq")
    storage_bucket = os.environ.get("FIREBASE_STORAGE_BUCKET", "davismarginiq.firebasestorage.app")

    creds_json = os.environ.get("FIREBASE_CREDENTIALS_JSON")
    if creds_json:
        cred = credentials.Certificate(json.loads(creds_json))
    else:
        # Falls back to GOOGLE_APPLICATION_CREDENTIALS env var automatically
        cred = credentials.ApplicationDefault()

    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred, {
            "projectId": project_id,
            "storageBucket": storage_bucket,
        })

    _db = firestore.client()
    _bucket = storage.bucket()
    log.info("Firebase Admin SDK initialized for project %s", project_id)


# ── Payroll runs ─────────────────────────────────────────────────────────────

def write_payroll_run(meta, pdf_bytes: bytes) -> str:
    """Write a WeeklyPayrollReport to Firestore and upload PDF to Storage.

    Returns the Firestore document ID.
    """
    _init()
    run_id = f"{meta.company_code}-{meta.check_date.isoformat()}-{meta.run_id}"

    # Upload PDF to Storage: payroll/pdfs/YYYY/filename.pdf
    year = meta.check_date.year
    storage_path = f"payroll/pdfs/{year}/{meta.pdf_filename}"
    blob = _bucket.blob(storage_path)
    blob.upload_from_string(pdf_bytes, content_type="application/pdf")
    blob.make_public()
    pdf_url = blob.public_url
    log.info("Uploaded PDF to gs://%s/%s", _bucket.name, storage_path)

    # Write metadata to Firestore
    doc_data = {
        "company_code": meta.company_code,
        "run_id": meta.run_id,
        "check_date": meta.check_date.isoformat(),
        "from_date": meta.from_date.isoformat(),
        "to_date": meta.to_date.isoformat(),
        "pdf_filename": meta.pdf_filename,
        "pdf_storage_path": storage_path,
        "pdf_url": pdf_url,
        "pdf_size_bytes": meta.pdf_size_bytes,
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "status": "success",
    }
    _db.collection("payroll_runs").document(run_id).set(doc_data, merge=True)
    log.info("Wrote payroll_runs/%s to Firestore", run_id)
    return run_id


# ── 1099 reports ─────────────────────────────────────────────────────────────

def write_1099_report(meta, pdf_bytes: bytes) -> str:
    """Write a Report1099 to Firestore and upload PDF to Storage."""
    _init()
    doc_id = f"{meta.company_code}-{meta.from_date.isoformat()}_{meta.to_date.isoformat()}"

    year = meta.from_date.year
    storage_path = f"payroll/1099/{year}/{meta.filename}"
    blob = _bucket.blob(storage_path)
    blob.upload_from_string(pdf_bytes, content_type="application/pdf")
    blob.make_public()
    pdf_url = blob.public_url

    doc_data = {
        "company_code": meta.company_code,
        "from_date": meta.from_date.isoformat(),
        "to_date": meta.to_date.isoformat(),
        "filename": meta.filename,
        "pdf_storage_path": storage_path,
        "pdf_url": pdf_url,
        "size_bytes": meta.size_bytes,
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "status": "success",
    }
    _db.collection("reports_1099").document(doc_id).set(doc_data, merge=True)
    log.info("Wrote reports_1099/%s to Firestore", doc_id)
    return doc_id


# ── Margin summary ────────────────────────────────────────────────────────────

def write_margin_summary(week_of: date, data: dict) -> str:
    """Write a weekly margin summary snapshot to Firestore."""
    _init()
    doc_id = week_of.isoformat()
    doc_data = {
        "week_of": week_of.isoformat(),
        "updated_at": datetime.utcnow().isoformat() + "Z",
        **data,
    }
    _db.collection("margin_summary").document(doc_id).set(doc_data, merge=True)
    log.info("Wrote margin_summary/%s to Firestore", doc_id)
    return doc_id

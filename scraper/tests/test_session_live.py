"""Live smoke test — requires CYBERPAY_USER/PASS in env. Skipped otherwise."""
from __future__ import annotations
import os

import pytest

from cyberpay.session import CyberPayClient
from cyberpay.reports import _list_payroll_grid


@pytest.mark.live
def test_live_login_and_list_payrolls():
    if not (os.environ.get("CYBERPAY_USER") and os.environ.get("CYBERPAY_PASS")):
        pytest.skip("CYBERPAY_USER/PASS not set")
    with CyberPayClient() as client:
        rows = _list_payroll_grid(client)
        assert len(rows) > 0, "Expected at least one payroll row"

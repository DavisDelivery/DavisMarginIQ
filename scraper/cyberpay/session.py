"""CyberPayOnline Phoenix session management.

Handles login (with anti-forgery token), cookie persistence, and
auto-reauth when the server 302s us back to /Account/Login.
"""
from __future__ import annotations
import logging
import time
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from cyberpay.config import Config

log = logging.getLogger("cyberpay.session")

LOGIN_PATH = "/Account/Login"


class CyberPayClient:
    """Wraps httpx.Client with login, rate limiting, and retry."""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config.from_env()
        self._client = httpx.Client(
            base_url=self.config.base_url,
            timeout=self.config.timeout_seconds,
            follow_redirects=True,
            headers={
                "User-Agent": "marginiq/0.1 (+https://github.com/DavisDelivery/marginiq)",
            },
        )
        self._logged_in = False
        self._last_request_at = 0.0

    # ---- context manager ---------------------------------------------------

    def __enter__(self) -> "CyberPayClient":
        self.login()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self._client.get(LOGIN_PATH.replace("Login", "LogOff"))
        except Exception:
            pass
        self._client.close()

    # ---- auth --------------------------------------------------------------

    def login(self) -> None:
        """POST /Account/Login with anti-forgery token."""
        log.info("Logging in as %s", self.config.user)
        resp = self._raw_request("GET", LOGIN_PATH)
        soup = BeautifulSoup(resp.text, "lxml")
        token_input = soup.find("input", {"name": "__RequestVerificationToken"})
        if not token_input or not token_input.get("value"):
            raise RuntimeError("Could not find __RequestVerificationToken on login page")
        token = token_input["value"]

        form = {
            "UserName": self.config.user,
            "Password": self.config.password,
            "__RequestVerificationToken": token,
            "RememberMe": "false",
        }
        resp = self._raw_request(
            "POST",
            LOGIN_PATH,
            data=form,
            headers={"Referer": f"{self.config.base_url}{LOGIN_PATH}"},
        )
        # On success the server 302s us to the dashboard; httpx follows.
        if LOGIN_PATH.lower() in str(resp.url).lower():
            # If we're still on login, credentials failed or there's a MFA wall.
            raise RuntimeError(
                "Login failed — still on /Account/Login after POST. Check credentials."
            )
        self._logged_in = True
        log.info("Login successful (landed on %s)", resp.url.path)

    def _ensure_logged_in(self) -> None:
        if not self._logged_in:
            self.login()

    # ---- request primitives ------------------------------------------------

    def request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Authenticated request with retry + auto-reauth."""
        self._ensure_logged_in()
        resp = self._request_with_retry(method, path, **kwargs)
        if self._looks_like_login_page(resp):
            log.warning("Session expired — re-authenticating")
            self._logged_in = False
            self.login()
            resp = self._request_with_retry(method, path, **kwargs)
        return resp

    def get(self, path: str, **kwargs) -> httpx.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs) -> httpx.Response:
        return self.request("POST", path, **kwargs)

    # ---- internals ---------------------------------------------------------

    def _request_with_retry(self, method: str, path: str, **kwargs) -> httpx.Response:
        last_err: Optional[Exception] = None
        for attempt in range(self.config.max_retries):
            try:
                resp = self._raw_request(method, path, **kwargs)
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    log.warning(
                        "HTTP %s on %s (attempt %d)", resp.status_code, path, attempt + 1
                    )
                    time.sleep(2 ** attempt)
                    continue
                return resp
            except (httpx.TransportError, httpx.TimeoutException) as e:
                last_err = e
                log.warning("Transport error on %s (attempt %d): %s", path, attempt + 1, e)
                time.sleep(2 ** attempt)
        if last_err:
            raise last_err
        # fall through: last resp was 5xx/429 on every try
        return resp  # noqa — the loop variable

    def _raw_request(self, method: str, path: str, **kwargs) -> httpx.Response:
        self._rate_limit()
        log.debug(">> %s %s", method, path)
        resp = self._client.request(method, path, **kwargs)
        log.debug("<< %s %s -> %s", method, path, resp.status_code)
        return resp

    def _rate_limit(self) -> None:
        elapsed = (time.monotonic() - self._last_request_at) * 1000
        remaining = self.config.min_interval_ms - elapsed
        if remaining > 0:
            time.sleep(remaining / 1000)
        self._last_request_at = time.monotonic()

    @staticmethod
    def _looks_like_login_page(resp: httpx.Response) -> bool:
        if LOGIN_PATH.lower() in str(resp.url).lower():
            return True
        ct = resp.headers.get("content-type", "")
        if "text/html" in ct and b'name="__RequestVerificationToken"' in resp.content:
            return True
        return False

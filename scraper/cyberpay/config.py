"""Config loaded from environment."""
from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    base_url: str
    user: str
    password: str
    company_code: str
    min_interval_ms: int = 500
    max_retries: int = 3
    timeout_seconds: int = 30

    @classmethod
    def from_env(cls) -> "Config":
        user = os.environ.get("CYBERPAY_USER")
        password = os.environ.get("CYBERPAY_PASS")
        if not user or not password:
            raise RuntimeError(
                "CYBERPAY_USER and CYBERPAY_PASS must be set (see .env.example)"
            )
        return cls(
            base_url=os.environ.get("CYBERPAY_BASE_URL", "https://phoenix.cyberpayonline.com"),
            user=user,
            password=password,
            company_code=os.environ.get("CYBERPAY_COMPANY_CODE", "0190"),
            min_interval_ms=int(os.environ.get("CYBERPAY_MIN_INTERVAL_MS", "500")),
            max_retries=int(os.environ.get("CYBERPAY_MAX_RETRIES", "3")),
            timeout_seconds=int(os.environ.get("CYBERPAY_TIMEOUT", "30")),
        )

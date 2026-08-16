"""
app/config.py
=============
Pydantic-driven configuration for the distributed crawler.

All settings are read from environment variables or a ``.env`` file.
Misconfiguration surfaces immediately at import time with a clear error.

Proxy fields accept comma-separated URL strings:

    DATACENTER_PROXIES=http://u:p@host1:8080,http://u:p@host2:8080
    RESIDENTIAL_PROXIES=http://u:p@host1:9090,http://u:p@host2:9090

``DATACENTER_PROXIES`` → ``List[str]`` for ``FetcherSession``'s ``ProxyRotator``.
``RESIDENTIAL_PROXIES`` → ``List[Dict[str, str]]`` for ``AsyncStealthySession``'s ``ProxyRotator``.
Both default to ``[]`` (graceful degradation to local IP) when absent or empty.
"""

from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import urlparse

from pydantic import field_validator, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    REDIS_URL: str = "redis://localhost:6379/0"

    WORKER_COUNT: int = 2
    GLOBAL_MAX_PAGES: int = 50
    MAX_PAGES_PER_RUN: int = 100
    MAX_DEPTH: int = 2

    #: Set to 0 to disable per-domain concurrency throttling.
    MAX_CONCURRENT_PER_DOMAIN: int = 2

    PEL_TIMEOUT_MS: int = 300_000
    MAX_RETRIES: int = 3

    HOSTNAME: str = ""

    DATACENTER_PROXIES: Any = ""
    RESIDENTIAL_PROXIES: Any = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
    )

    @field_validator("DATACENTER_PROXIES", mode="before")
    @classmethod
    def parse_datacenter_proxies(cls, v: Any) -> List[str]:
        """
        Parse a comma-separated proxy string into ``List[str]``.

        Returns an empty list when the variable is absent or blank.
        """
        if not v:
            return []
        return [p.strip() for p in str(v).split(",") if p.strip()]

    @field_validator("RESIDENTIAL_PROXIES", mode="before")
    @classmethod
    def parse_residential_proxies(cls, v: Any) -> List[Dict[str, str]]:
        """
        Parse a comma-separated proxy string into Playwright proxy dicts.

        Each entry is parsed into ``{"server": ..., "username": ..., "password": ...}``.
        Returns an empty list when the variable is absent or blank.
        """
        if not v:
            return []

        result: List[Dict[str, str]] = []
        for raw in str(v).split(","):
            raw = raw.strip()
            if not raw:
                continue
            parsed = urlparse(raw)
            server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
            result.append(
                {
                    "server": server,
                    "username": parsed.username or "",
                    "password": parsed.password or "",
                }
            )
        return result


settings = Settings()

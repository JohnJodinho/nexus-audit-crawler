"""
app/spider.py
=============
``AuditSpider`` -- single-page fetcher for the Enterprise AI Audit Crawler.

Fetches one URL per instantiation.  Discovered links are published directly
to the crawl-scoped task stream; extraction results to the results stream;
drop events to the telemetry stream.  No crawl state is held on the instance;
all shared state lives in Redis.

Sessions
--------
Primary (default):  ``FetcherSession`` (HTTP) + optional datacenter proxies.
Fallback (lazy):    ``AsyncStealthySession`` (Playwright) + optional residential proxies.

The Playwright session launches only when ``retry_blocked_request()`` pivots
a blocked request to it.

Link routing
------------
``DENY_PATTERN`` suppresses functional, chronological, and e-commerce noise.
All same-domain links not matched by the pattern are queued (Gate 4).
Depth and global budget are enforced by the worker loop before the spider runs.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import re
from typing import Any, AsyncGenerator, Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse, unquote

import redis.asyncio as aioredis

from scrapling.fetchers import AsyncStealthySession, FetcherSession, ProxyRotator
from scrapling.spiders import Request, Response, SessionManager, Spider

from app.extraction import run_triple_threat
from app.logger import LOG_FILE_PATH
from app.telemetry import DropReason
from app.redis_client import (
    tasks_key,
    results_key,
    telemetry_key,
    queued_key,
)
from app.utils.contacts import (
    extract_contacts_from_text,
    validate_email,
    validate_phone,
)
from app.utils.utilities import canonicalize_url, get_fingerprint
from app.utils.audits import compile_full_audit
from app.utils.screenshots import capture_stitched_screenshot
from app.storage.appwrite_client import storage_client
from app.config import settings


_DATACENTER_PROXIES: list[str] = settings.DATACENTER_PROXIES
_RESIDENTIAL_PROXIES: list[dict] = settings.RESIDENTIAL_PROXIES


DENY_PATTERN: re.Pattern[str] = re.compile(
    r"/("
    r"blog|news|press|events|updates|announcements|insights|resources"
    r"|privacy|terms|legal|cookie|cookies|gdpr|disclaimer|accessibility"
    r"|login|logout|signin|signup|register|auth|account|password|reset"
    r"|cart|checkout|basket|order|payment|subscribe|billing"
    r"|careers|jobs|vacancies|hiring|work-with-us"
    r"|faq|help|support|sitemap|rss|feed|search"
    r"|cdn-cgi|wp-admin|wp-login|wp-json|_next|__nuxt"
    r"|assets|static|media|images|img|css|js|fonts"
    r")",
    re.IGNORECASE,
)

_BLOCKED_STATUS_CODES: frozenset[int] = frozenset({403, 429, 503})

_BLOCK_BODY_SIGNALS: tuple[str, ...] = (
    "cf-browser-verification",
    "cloudflare",
    "just a moment",
    "datadome",
    "enable javascript",
    "access denied",
    "403 forbidden",
    "rate limit exceeded",
    "too many requests",
)


class AuditSpider(Spider):
    """
    Single-page fetcher for the distributed audit crawler.

    Receives one URL as its seed via ``start_urls``, fetches it through the
    waterfall session architecture, and routes all outputs to Redis streams.

    Class attributes set per-task by the worker
    -------------------------------------------
    start_urls:
        Set to ``[url]`` by the worker before calling ``spider.stream()``.
    allowed_domains:
        Set to ``{domain}`` by the worker to enforce the domain fence.
    """

    name: str = "audit_spider"

    start_urls: list[str] = ["https://www.example.com"]
    allowed_domains: Set[str] = {"example.com"}

    concurrent_requests: int = 3
    concurrent_requests_per_domain: int = 2
    download_delay: float = 1.5

    max_blocked_retries: int = 3

    log_file: Optional[str] = LOG_FILE_PATH
    logging_level: int = logging.INFO

    _SID_HTTP: str = "primary_http"
    _SID_STEALTH: str = "fallback_stealth"

    def __init__(
        self,
        *,
        redis_client: aioredis.Redis,
        crawl_id: str,
        task_depth: int = 0,
        task_domain: str = "",
        **kwargs: Any,
    ) -> None:
        """
        Parameters
        ----------
        redis_client:
            Shared async Redis connection pool from the worker loop.
        crawl_id:
            The crawl namespace identifier — used to build crawl-scoped
            Redis key names.
        task_depth:
            Crawl depth of this page from the seed URL.
        task_domain:
            Domain being crawled; propagated to child task payloads.
        """
        super().__init__(**kwargs)
        self._redis: aioredis.Redis = redis_client
        self._crawl_id: str = crawl_id
        self._task_depth: int = task_depth
        self._task_domain: str = task_domain

    def configure_sessions(self, manager: SessionManager) -> None:
        """
        Configure the primary HTTP session and the lazy Playwright stealth fallback.

        Proxy lists are read from ``settings`` at import time.  If a list is
        empty, the session falls back to the host machine's local IP.
        """
        fetcher_kwargs: dict = {
            "impersonate": "chrome",
            "stealthy_headers": True,
            "timeout": 30,
            "retries": 2,
            "retry_delay": 2,
        }

        if _DATACENTER_PROXIES:
            fetcher_kwargs["proxy_rotator"] = ProxyRotator(_DATACENTER_PROXIES)
            self.logger.info(
                "[SESSION] Primary HTTP session: %d datacenter proxy(ies) loaded.",
                len(_DATACENTER_PROXIES),
            )
        else:
            self.logger.warning(
                "[SESSION] Primary HTTP session: no datacenter proxies configured -- "
                "falling back to local IP."
            )

        primary_session = FetcherSession(**fetcher_kwargs)
        manager.add(self._SID_HTTP, primary_session, default=True)

        stealth_kwargs: dict = {
            "headless":          True,
            "block_webrtc":      True,
            "hide_canvas":       True,
            "disable_resources": True,
            "network_idle":      False,
            "google_search":     True,
            "timeout":           30_000,
            "retries":           2,
        }

        if _RESIDENTIAL_PROXIES:
            stealth_kwargs["proxy_rotator"] = ProxyRotator(_RESIDENTIAL_PROXIES)
            self.logger.info(
                "[SESSION] Stealth session: %d residential proxy(ies) loaded.",
                len(_RESIDENTIAL_PROXIES),
            )
        else:
            self.logger.warning(
                "[SESSION] Stealth session: no residential proxies configured -- "
                "falling back to local IP."
            )

        stealth_session = AsyncStealthySession(**stealth_kwargs)
        manager.add(self._SID_STEALTH, stealth_session, lazy=True)

    async def is_blocked(self, response: Response) -> bool:
        """Return ``True`` if the response is a bot-detection challenge or block."""
        if response.status in _BLOCKED_STATUS_CODES:
            self.logger.warning(
                "[BLOCKED] HTTP %d detected for %s – will pivot to stealth.",
                response.status,
                response.url,
            )
            return True

        try:
            body_sample: str = (
                response.body[:8192].decode("utf-8", errors="replace").lower()
            )
        except AttributeError:
            body_sample = str(response.body)[:8192].lower()

        for signal in _BLOCK_BODY_SIGNALS:
            if signal in body_sample:
                self.logger.warning(
                    "[BLOCKED] Challenge fingerprint '%s' detected in body for %s.",
                    signal,
                    response.url,
                )
                return True

        # Check for unrendered Single Page Application (SPA) skeletons (React/Vue/Vite/Next.js)
        # when fetched via primary HTTP session.
        req_sid = getattr(getattr(response, "request", None), "sid", None) or self._SID_HTTP
        if req_sid == self._SID_HTTP:
            is_spa_skeleton = (
                '<div id="root"></div>' in body_sample
                or '<div id="root"> </div>' in body_sample
                or '<div id="root">' in body_sample and "<a " not in body_sample
                or '<div id="app"></div>' in body_sample
                or '<div id="__next"></div>' in body_sample
            )
            if not is_spa_skeleton and len(body_sample) < 5000:
                if ("<script" in body_sample or "/assets/" in body_sample) and "<a " not in body_sample:
                    is_spa_skeleton = True

            if is_spa_skeleton:
                self.logger.info(
                    "[SPA_DETECTED] Client-rendered SPA skeleton detected for %s – pivoting to stealth browser.",
                    response.url,
                )
                return True

        return False


    async def retry_blocked_request(
        self, request: Request, response: Response
    ) -> Request:
        """Pivot a blocked request to the stealth Playwright session."""
        self.logger.info(
            "[RETRY] Pivoting blocked URL %s -> session '%s'.",
            request.url,
            self._SID_STEALTH,
        )

        request.sid = self._SID_STEALTH

        request_kwargs: dict = getattr(request, "kwargs", {})
        request_kwargs.pop("proxy", None)
        request_kwargs.pop("proxies", None)
        request_kwargs["network_idle"] = True

        return request

    async def parse(self, response: Response) -> AsyncGenerator[Any, None]:
        """
        Main parsing callback.

        Evaluates each discovered href through five sequential gates and
        publishes qualifying links to the crawl-scoped task stream.  Runs the
        cumulative Triple-Threat extraction pipeline and yields the canonical
        payload.

        Gates
        -----
        1. Per-page duplicate (page-local set, no Redis I/O).
        2. Contact interception / scheme check (mailto:, tel:, non-HTTP).
        2.5. Domain fence (must match ``task_domain`` exactly or as subdomain).
        3. Deny list (``DENY_PATTERN``).
        4. Atomic discovery ledger (``SADD crawl:{id}:set:queued_fingerprints``).
        5. Accept -- ``XADD crawl:{id}:stream:audit_tasks``.
        """
        self.logger.info(
            "[PARSE] Processing (depth=%d): %s (HTTP %d)",
            self._task_depth,
            response.url,
            response.status,
        )

        page_emails: Set[str] = set()
        page_phones: Set[str] = set()
        page_seen_urls: Set[str] = set()

        links_found: int = 0
        links_queued: int = 0

        _now = lambda: datetime.datetime.now(datetime.UTC).isoformat()
        _telemetry_stream = telemetry_key(self._crawl_id)

        for href in response.css("a::attr(href)").getall():
            if not href:
                continue

            links_found += 1

            try:
                abs_url: str = urljoin(response.url, href)
            except ValueError:
                continue

            # Gate 1: per-page duplicate
            if abs_url in page_seen_urls:
                self.logger.debug("[GATE-1] PAGE-DUPLICATE: %s", abs_url)
                continue

            page_seen_urls.add(abs_url)

            # Gate 2: contact interception + scheme check
            raw_lower: str = href.lower().lstrip()

            if raw_lower.startswith("mailto:"):
                try:
                    raw_email: str = href[len("mailto:"):].split("?")[0]
                    clean_email: Optional[str] = validate_email(raw_email)
                except Exception:
                    clean_email = None

                if clean_email:
                    page_emails.add(clean_email)
                    self.logger.debug("[GATE-2/CONTACT] Email extracted: %s", clean_email)

                await self._redis.xadd(
                    _telemetry_stream,
                    {
                        "schema_version": "1",
                        "crawl_id": self._crawl_id,
                        "timestamp_utc": _now(),
                        "source_url": response.url,
                        "target_url": href,
                        "drop_reason": DropReason.CONTACT_EXTRACTED_EMAIL,
                    },
                )
                continue

            if raw_lower.startswith("tel:"):
                try:
                    raw_phone: str = href[len("tel:"):]
                    clean_phone: Optional[str] = validate_phone(raw_phone)
                except Exception:
                    clean_phone = None

                if clean_phone:
                    page_phones.add(clean_phone)
                    self.logger.debug("[GATE-2/CONTACT] Phone extracted: %s", clean_phone)

                await self._redis.xadd(
                    _telemetry_stream,
                    {
                        "schema_version": "1",
                        "crawl_id": self._crawl_id,
                        "timestamp_utc": _now(),
                        "source_url": response.url,
                        "target_url": href,
                        "drop_reason": DropReason.CONTACT_EXTRACTED_PHONE,
                    },
                )
                continue

            try:
                scheme: str = urlparse(abs_url).scheme.lower()
            except Exception:
                continue

            if scheme not in ("http", "https"):
                await self._redis.xadd(
                    _telemetry_stream,
                    {
                        "schema_version": "1",
                        "crawl_id": self._crawl_id,
                        "timestamp_utc": _now(),
                        "source_url": response.url,
                        "target_url": abs_url,
                        "drop_reason": DropReason.INVALID_SCHEME,
                    },
                )
                self.logger.debug("[GATE-2] INVALID_SCHEME (%s): %s", scheme, abs_url)
                continue

            try:
                parsed_url = urlparse(abs_url)
            except Exception:
                continue

            # Gate 2.5: domain fence
            netloc: str = parsed_url.netloc.split(":")[0].lower()
            target_domain: str = self._task_domain.lower()

            is_exact_match = netloc == target_domain
            is_valid_subdomain = netloc.endswith(f".{target_domain}")

            if not (is_exact_match or is_valid_subdomain):
                await self._redis.xadd(
                    _telemetry_stream,
                    {
                        "schema_version": "1",
                        "crawl_id": self._crawl_id,
                        "timestamp_utc": _now(),
                        "source_url": response.url,
                        "target_url": abs_url,
                        "drop_reason": "OFFSITE_DOMAIN",
                    },
                )
                self.logger.debug("[GATE-2.5/FENCE] Offsite link dropped: %s", abs_url)
                continue

            # Gate 3: deny list
            if DENY_PATTERN.search(parsed_url.path):
                await self._redis.xadd(
                    _telemetry_stream,
                    {
                        "schema_version": "1",
                        "crawl_id": self._crawl_id,
                        "timestamp_utc": _now(),
                        "source_url": response.url,
                        "target_url": abs_url,
                        "drop_reason": DropReason.DENY_LIST,
                    },
                )
                self.logger.debug("[GATE-3/DENY] %s", abs_url)
                continue

            # Gate 4: atomic discovery ledger
            abs_url_hash: str = get_fingerprint(abs_url)
            is_new_discovery: int = await self._redis.sadd(
                queued_key(self._crawl_id), abs_url_hash
            )
            if not is_new_discovery:
                self.logger.debug(
                    "[GATE-4/DEDUP] Already queued, skipping XADD: %s", abs_url
                )
                continue

            # Gate 5: accept
            links_queued += 1
            self.logger.debug(
                "[GATE-5] XADD task (depth->%d): %s",
                self._task_depth + 1,
                abs_url,
            )
            await self._redis.xadd(
                tasks_key(self._crawl_id),
                {
                    "schema_version": "1",
                    "crawl_id": self._crawl_id,
                    "url": abs_url,
                    "depth": str(self._task_depth + 1),
                    "retry_count": "0",
                    "throttle_count": "0",
                    "domain": self._task_domain,
                    "published_at": _now(),
                },
            )

        self.logger.info(
            "[PARSE] Link scan complete: %d found, %d queued (depth=%d) for %s.",
            links_found,
            links_queued,
            self._task_depth,
            response.url,
        )

        loop = asyncio.get_event_loop()
        extraction: Dict[str, Any] = await loop.run_in_executor(
            None, run_triple_threat, response, self.logger
        )

        # Extract plain text contacts from rendered Markdown
        raw_md: str = extraction.get("raw_markdown", "") or ""
        text_contacts = extract_contacts_from_text(raw_md)
        page_emails.update(text_contacts["emails"])
        page_phones.update(text_contacts["phones"])

        extraction["contacts"] = {
            "emails": sorted(page_emails),
            "phones": sorted(page_phones),
        }

        # -------------------------------------------------------------------
        # Phase 3: Comprehensive Enterprise Audit & Quality Signals
        # -------------------------------------------------------------------
        canonical_url = canonicalize_url(response.url)
        headers_dict: Dict[str, str] = {}
        if hasattr(response, "headers") and response.headers:
            try:
                headers_dict = dict(response.headers)
            except Exception:
                headers_dict = {}

        html_text = ""
        try:
            if hasattr(response, "text") and response.text:
                html_text = str(response.text)
            elif hasattr(response, "body") and response.body:
                html_text = response.body.decode("utf-8", errors="replace")
        except Exception:
            html_text = ""

        audit_metadata = compile_full_audit(
            html_text=html_text,
            headers=headers_dict,
            canonical_url=canonical_url,
            markdown_text=raw_md,
            request_url=response.url,
            response_time_ms=0.0,
        )
        extraction["metadata"] = audit_metadata

        # -------------------------------------------------------------------
        # Optional Screenshot via Playwright & Appwrite
        # -------------------------------------------------------------------
        screenshot_file_id: Optional[str] = None
        if settings.SCREENSHOT_ENABLED and hasattr(response, "page") and response.page is not None:
            try:
                shot_bytes = await capture_stitched_screenshot(response.page)
                if shot_bytes:
                    fingerprint = get_fingerprint(canonical_url)
                    screenshot_file_id = await storage_client.upload_screenshot(
                        crawl_id=self._crawl_id,
                        fingerprint=fingerprint,
                        image_bytes=shot_bytes,
                    )
            except Exception as shot_exc:
                self.logger.warning("[SCREENSHOT] Screenshot capture failed for %s: %s", response.url, shot_exc)

        extraction["screenshot_file_id"] = screenshot_file_id

        if page_emails or page_phones:
            self.logger.info(
                "[CONTACTS] %s -- emails: %s | phones: %s",
                response.url,
                sorted(page_emails) if page_emails else "(none)",
                sorted(page_phones) if page_phones else "(none)",
            )

        yield extraction

    async def on_scraped_item(self, item: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Validate and publish each scraped item to the crawl-scoped results stream.

        An item is accepted if ``raw_markdown`` is non-empty OR at least one
        JSON extraction strategy produced output.  Items where all three
        strategies returned empty results are dropped.
        """
        url: str = item.get("url", "<unknown>")
        has_xhr: bool = bool(item.get("xhr_payloads"))
        has_hydration: bool = bool(item.get("hydration_state"))
        has_markdown: bool = bool(item.get("raw_markdown", "").strip())
        has_metadata: bool = bool(item.get("metadata"))

        if not has_xhr and not has_hydration and not has_markdown and not has_metadata:
            self.logger.warning(
                "[PIPELINE] DROP -- no data extracted for %s.",
                url,
            )
            return None

        # If raw_markdown is empty on JavaScript/SPA shells, supply fallback text for Appwrite/storage
        if not has_markdown:
            item["raw_markdown"] = f"# {url}\n\n*Client-rendered page or empty DOM body.*"

        extraction_methods = item.get("extraction_methods", ["none"])
        self.logger.info(
            "[PIPELINE] ACCEPT -- item from %s via %s.",
            url,
            ", ".join(extraction_methods),
        )


        redis_payload: Dict[str, str] = {
            "schema_version": "1",
            "crawl_id": self._crawl_id,
            "url": url,
            "xhr_payloads": json.dumps(item.get("xhr_payloads", []), ensure_ascii=False),
            "hydration_state": json.dumps(item.get("hydration_state"), ensure_ascii=False),
            "raw_markdown": item.get("raw_markdown", ""),
            "extraction_methods": json.dumps(extraction_methods),
            "contacts": json.dumps(
                item.get("contacts", {"emails": [], "phones": []}),
                ensure_ascii=False,
            ),
            "metadata": json.dumps(item.get("metadata", {}), ensure_ascii=False),
            "screenshot_file_id": item.get("screenshot_file_id") or "",
            "scraped_at_utc": datetime.datetime.now(datetime.UTC).isoformat(),
            "depth": str(self._task_depth),
            "domain": self._task_domain,
        }

        try:
            msg_id: str = await self._redis.xadd(results_key(self._crawl_id), redis_payload)
            self.logger.info(
                "[REDIS] Result pushed to %s: url=%s msg_id=%s",
                results_key(self._crawl_id),
                url,
                msg_id,
            )
        except Exception as exc:
            self.logger.error(
                "[REDIS] Failed to push result for %s: %s",
                url,
                exc,
                exc_info=True,
            )

        return item

    async def on_error(self, request: Request, error: Exception) -> None:
        """Log unhandled request exceptions."""
        self.logger.error(
            "[ERROR] Request FAILED for %s (session=%s): %s: %s",
            request.url,
            getattr(request, "sid", "default"),
            type(error).__name__,
            error,
            exc_info=True,
        )

    async def on_start(self, resuming: bool = False) -> None:
        """Log spider startup."""
        self.logger.info(
            "[LIFECYCLE] Spider '%s'. Seed: %s | depth=%d | crawl_id=%s",
            self.name,
            ", ".join(self.start_urls),
            self._task_depth,
            self._crawl_id,
        )

    async def on_close(self) -> None:
        """Log spider shutdown with final stats."""
        self.logger.info(
            "[LIFECYCLE] Spider '%s' shutting down. "
            "Items scraped: %d | Requests: %d | Blocked retries: %d",
            self.name,
            self.stats.items_scraped if self.stats else -1,
            self.stats.requests_count if self.stats else -1,
            self.stats.blocked_requests_count if self.stats else -1,
        )

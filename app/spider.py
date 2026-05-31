"""
app/spider.py
=============
Enterprise AI Audit Crawler -- Spider Definition.

Architectural overview
-----------------------
This module defines ``AuditSpider``, a subclass of Scrapling's ``Spider``
that implements the "Waterfall Session Architecture":

  Primary (default) session:   ``FetcherSession`` (pure HTTP, fast)
                                -+- Datacenter proxies via ``ProxyRotator``
  Fallback session (lazy):     ``AsyncStealthySession`` (Playwright stealth)
                                -+- Residential proxies via ``ProxyRotator``
                                -+- WebRTC blocked, media resources disabled
                                -+- XHR capture enabled (captures all API calls)

Negative-Space Routing
-----------------------
Instead of an allow whitelist (which silently dropped valuable pages like
``/our-firm`` or ``/areas-of-practice``), the spider now follows ALL
same-domain links and suppresses only an aggressively curated deny list.
This captures the full semantic surface of any target domain without
requiring prior knowledge of its URL taxonomy.

  ``DENY_PATTERN``:  A broad blocklist of functional, chronological, and
                     e-commerce noise -- blog, news, events, login, cart,
                     careers, etc.  Everything not explicitly denied is
                     eligible to crawl.

Depth Bounding
---------------
Because the allow whitelist has been removed, unbounded crawling is
prevented by two complementary controls:

  ``max_depth``:   Maximum hops from the seed URL (0 = seed page only,
                   1 = first-click pages, 2 = second-click pages, etc.).
                   Depth is propagated through ``response.meta["depth"]``
                   and incremented in Gate 4 when yielding follow-up
                   requests.  When ``current_depth >= max_depth``, link
                   extraction is skipped entirely for that page.

  ``max_pages``:   Hard cap on total pages scraped per run.  When the
                   counter reaches this limit, ``self.pause()`` is called
                   from ``on_scraped_item()`` to trigger a graceful shutdown.

Blocking detection and pivot
-----------------------------
``is_blocked()`` detects HTTP 403/429 and Cloudflare/Datadome challenge pages.
``retry_blocked_request()`` pivots the request to the stealth session, clears
any stale proxy kwargs, and returns the mutated request for re-queuing.

Extraction
-----------
The actual data extraction is fully delegated to ``extraction.run_triple_threat()``
which implements the XHR -> Hydration -> MarkItDown waterfall.

Logging
--------
The spider's built-in ``self.logger`` is routed to a file by setting the
``log_file`` class attribute to the canonical path from ``logger.LOG_FILE_PATH``.
No manual handler wiring is required; the Spider base class handles it.
"""

from __future__ import annotations

import logging
import re
from typing import Any, AsyncGenerator, Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse, unquote

from scrapling.fetchers import AsyncStealthySession, FetcherSession, ProxyRotator
from scrapling.spiders import Request, Response, SessionManager, Spider

# Import the extraction pipeline (Triple-Threat waterfall)
from app.extraction import run_triple_threat

# Import the canonical log-file path constant
from app.logger import LOG_FILE_PATH

# Import the structured telemetry sink and its drop-reason vocabulary
from app.telemetry import DropReason, TelemetrySink


# ===========================================================================
# Proxy pool definitions
# ===========================================================================
# These are hardcoded placeholder proxies as required by the specification.
# In production, replace with live proxy provider credentials.
# ---------------------------------------------------------------------------

#: Datacenter proxies – used by the primary HTTP session.
#: Fast, cheap, and suitable for pages without anti-bot protection.
_DATACENTER_PROXIES: list[str] = [
    "http://dc-user1:pass1@datacenter-proxy1.example.com:8080",
    "http://dc-user2:pass2@datacenter-proxy2.example.com:8080",
    "http://dc-user3:pass3@datacenter-proxy3.example.com:8080",
    "http://dc-user4:pass4@datacenter-proxy4.example.com:8080",
]

#: Residential proxies – used by the fallback Playwright stealth session.
#: Browser sessions require Playwright's dict format (server/username/password).
_RESIDENTIAL_PROXIES: list[dict] = [
    {"server": "http://residential-proxy1.example.com:9090",
        "username": "res-user1", "password": "res-pass1"},
    {"server": "http://residential-proxy2.example.com:9090",
        "username": "res-user2", "password": "res-pass2"},
    {"server": "http://residential-proxy3.example.com:9090",
        "username": "res-user3", "password": "res-pass3"},
    {"server": "http://residential-proxy4.example.com:9090",
        "username": "res-user4", "password": "res-pass4"},
]


# ===========================================================================
# Link boundary: Negative-Space Routing -- DENY-only pattern
# ===========================================================================
# "Negative-Space Routing" inverts the classic allow-whitelist approach.
# Instead of listing every path segment we want, we only list the ones we
# DON'T want.  Everything not explicitly denied is eligible to crawl.
#
# Rationale: an allow whitelist (e.g. /about|/services|/pricing) silently
# drops high-value pages whose vocabulary doesn't match our preset list --
# e.g. /our-firm, /areas-of-practice, /what-we-do, /why-choose-us.
# Telemetry confirmed this was causing significant data loss.
#
# Unbounded crawling is prevented by two complementary controls defined on
# the spider class: ``max_depth`` (hop limit from seed) and ``max_pages``
# (hard page-count cap).  See on_start() for their initialisation.
# ---------------------------------------------------------------------------

#: Deny list -- functional, chronological, and e-commerce noise.
#: Any URL whose path matches this pattern is suppressed before it enters
#: the scheduler.  Ordered roughly by expected match frequency.
DENY_PATTERN: re.Pattern[str] = re.compile(
    r"/("
    # --- Chronological / content noise ------------------------------------
    r"blog|news|press|events|updates|announcements|insights|resources"
    # --- Legal / compliance boilerplate ----------------------------------
    r"|privacy|terms|legal|cookie|cookies|gdpr|disclaimer|accessibility"
    # --- Authentication / account flows ----------------------------------
    r"|login|logout|signin|signup|register|auth|account|password|reset"
    # --- E-commerce / transactional --------------------------------------
    r"|cart|checkout|basket|order|payment|subscribe|billing"
    # --- Recruitment ------------------------------------------------------
    r"|careers|jobs|vacancies|hiring|work-with-us"
    # --- Support / utility -----------------------------------------------
    r"|faq|help|support|sitemap|rss|feed|search"
    # --- CMS / framework internals ---------------------------------------
    r"|cdn-cgi|wp-admin|wp-login|wp-json|_next|__nuxt"
    # --- Static asset paths ----------------------------------------------
    r"|assets|static|media|images|img|css|js|fonts"
    r")",
    re.IGNORECASE,
)


# ===========================================================================
# Block-detection signals
# ===========================================================================

#: HTTP status codes that definitively indicate a block or rate-limit.
_BLOCKED_STATUS_CODES: frozenset[int] = frozenset({403, 429, 503})

#: Substrings that identify Cloudflare or Datadome challenge pages.
#: Checked against the decoded response body (case-insensitive).
_BLOCK_BODY_SIGNALS: tuple[str, ...] = (
    "cf-browser-verification",      # Cloudflare JS challenge page marker
    "cloudflare",                   # Generic Cloudflare branding
    "just a moment",                # Cloudflare "Just a moment…" interstitial
    "datadome",                     # Datadome challenge marker
    "enable javascript",            # Generic bot-wall fallback
    "access denied",                # Generic 403-like text wall
    "403 forbidden",                # Text-based 403
    "rate limit exceeded",          # Explicit rate-limit message
    "too many requests",            # 429-style text response
)

#: XHR capture pattern – intercept all API calls made by the page.
#: The broad pattern ``.*`` ensures we capture every XHR/fetch response
#: so the extraction layer can decide which ones are relevant.
_XHR_CAPTURE_PATTERN: str = r".*"


# ===========================================================================
# Spider class
# ===========================================================================

class AuditSpider(Spider):
    """
    Enterprise AI Audit Crawler -- main spider class.

    This spider crawls a target domain using Negative-Space Routing:
    all same-domain links are followed unless their path matches the
    ``DENY_PATTERN`` blocklist.  Unbounded traversal is prevented by
    ``max_depth`` (hop limit) and ``max_pages`` (hard page-count cap).

    Blocked requests are automatically pivoted to the lazy Playwright
    stealth session.  Each response is fed through the Triple-Threat
    extraction pipeline (XHR -> Hydration -> MarkItDown).

    Class attributes
    ----------------
    name:
        Unique spider identifier -- used by Scrapling for logging namespacing.
    start_urls:
        Seed URL(s).  Override at instantiation via ``main.py`` configuration.
    allowed_domains:
        Hard domain fence.  Links pointing outside this set are silently
        dropped by the Spider base class before they even reach ``parse()``.
    log_file:
        Path to the log file.  The Spider base class creates the directory
        and attaches a ``FileHandler`` automatically.
    """

    name: str = "audit_spider"

    # Seed URL – override this with your actual target before running.
    # Example: start_urls = ["https://www.acmecorp.com"]
    start_urls: list[str] = ["https://www.example.com"]

    # Hard domain fence – links outside these domains are dropped.
    # Subdomains (e.g. www.example.com) are matched automatically.
    allowed_domains: Set[str] = {"example.com"}

    # Concurrency settings – conservative to avoid triggering rate limits.
    concurrent_requests: int = 3
    concurrent_requests_per_domain: int = 2
    download_delay: float = 1.5          # 1.5 s between requests per domain

    # Retry budget for blocked requests before giving up on a URL.
    max_blocked_retries: int = 3

    # Logging – the Spider base class routes self.logger to this file.
    # The directory is created automatically by the base class.
    log_file: Optional[str] = LOG_FILE_PATH
    logging_level: int = logging.INFO

    # Session ID constants – used to route requests and pivot on block.
    _SID_HTTP: str = "primary_http"
    _SID_STEALTH: str = "fallback_stealth"

    # -----------------------------------------------------------------------
    # Session configuration: Waterfall Architecture
    # -----------------------------------------------------------------------

    def configure_sessions(self, manager: SessionManager) -> None:
        """
        Configure the two-layer session architecture.

        Primary session (``primary_http``)
        ------------------------------------
        A ``FetcherSession`` (pure HTTP, no browser) using:
          - Chrome impersonation for realistic headers.
          - Stealthy header generation enabled.
          - A ``ProxyRotator`` over the datacenter proxy pool.
          - Set as the *default* session so all initial requests use it.

        Fallback session (``fallback_stealth``)
        -----------------------------------------
        An ``AsyncStealthySession`` (Playwright Chromium) using:
          - Headless mode.
          - WebRTC blocking to prevent local IP leakage through the proxy.
          - ``disable_resources=True`` to block media/image/font downloads,
            reducing bandwidth and noise during stealth crawls.
          - XHR capture with the broad pattern ``.*`` so every API call is
            captured for Strategy 1 of the extraction pipeline.
          - A ``ProxyRotator`` over the residential proxy pool.
          - Registered with ``lazy=True`` so the Playwright browser is
            **not** launched at spider startup; it starts on-demand only
            when ``retry_blocked_request()`` pivots a request to this session.
            This saves substantial memory and startup time for pages that
            the primary session can handle without challenge.

        Parameters
        ----------
        manager:
            The ``SessionManager`` provided by the Spider base class.
        """
        # --- Primary: Datacenter HTTP session --------------------------------
        datacenter_rotator = ProxyRotator(_DATACENTER_PROXIES)

        primary_session = FetcherSession(
            impersonate="chrome",       # Realistic Chrome TLS fingerprint + headers
            stealthy_headers=True,      # Auto-generate real browser request headers
            # proxy_rotator=datacenter_rotator,
            timeout=30,
            # Retry transient network errors (not blocks)
            retries=2,
            retry_delay=2,
        )

        # The first session added (or the one with default=True) becomes the
        # default.  We explicitly mark it to make the intent crystal-clear.
        manager.add(self._SID_HTTP, primary_session, default=True)
        self.logger.info(
            "Registered primary HTTP session '%s' with %d datacenter proxies.",
            self._SID_HTTP,
            len(_DATACENTER_PROXIES),
        )

        # --- Fallback: Residential Playwright stealth session ----------------
        residential_rotator = ProxyRotator(_RESIDENTIAL_PROXIES)

        stealth_session = AsyncStealthySession(
            headless=True,
            block_webrtc=True,          # Prevent local IP leak through proxy
            disable_resources=True,     # Block media/images/fonts for speed
            capture_xhr=_XHR_CAPTURE_PATTERN,   # Intercept ALL XHR/fetch calls
            # proxy_rotator=residential_rotator,
            network_idle=True,          # Wait for all network activity to quiesce
            google_search=True,         # Set Google as referer to mimic organic traffic
            timeout=60000,              # 60 s – needed for Cloudflare challenge solving
            retries=2,
        )

        # ``lazy=True`` means the Playwright browser is NOT launched at spider
        # startup.  It starts only when the first request is routed to this
        # session, which happens in retry_blocked_request().
        manager.add(self._SID_STEALTH, stealth_session, lazy=True)
        self.logger.info(
            "Registered stealth session '%s' (lazy) with %d residential proxies.",
            self._SID_STEALTH,
            len(_RESIDENTIAL_PROXIES),
        )

    # -----------------------------------------------------------------------
    # Block detection
    # -----------------------------------------------------------------------

    async def is_blocked(self, response: Response) -> bool:
        """
        Detect whether a response is a bot-detection challenge or block.

        Detection criteria
        ------------------
        1. HTTP status code is in the ``_BLOCKED_STATUS_CODES`` set (403, 429, 503).
        2. The response body contains any known Cloudflare or Datadome
           challenge fingerprint string.

        Both checks are intentionally conservative (low false-positive rate)
        because triggering an unnecessary stealth retry costs time and proxy
        bandwidth.

        Parameters
        ----------
        response:
            The ``Response`` object received from the session.

        Returns
        -------
        bool
            ``True`` if the response is a bot-detection block, ``False`` otherwise.
        """
        # ---- Check 1: HTTP status code ----
        if response.status in _BLOCKED_STATUS_CODES:
            self.logger.warning(
                "[BLOCKED] HTTP %d detected for %s – will pivot to stealth.",
                response.status,
                response.url,
            )
            return True

        # ---- Check 2: Challenge body fingerprints ----
        # Decode only as much as we need for the fingerprint check.
        try:
            # response.body is bytes in v0.4; decode with error replacement.
            body_sample: str = response.body[:8192].decode(
                "utf-8", errors="replace").lower()
        except AttributeError:
            # Fallback if .body is already a string (shouldn't happen in v0.4).
            body_sample = str(response.body)[:8192].lower()

        for signal in _BLOCK_BODY_SIGNALS:
            if signal in body_sample:
                self.logger.warning(
                    "[BLOCKED] Challenge fingerprint '%s' detected in body for %s.",
                    signal,
                    response.url,
                )
                return True

        return False

    # -----------------------------------------------------------------------
    # Blocked request retry: Stealth pivot
    # -----------------------------------------------------------------------

    async def retry_blocked_request(self, request: Request, response: Response) -> Request:
        """
        Pivot a blocked request to the stealth Playwright session.

        When the crawler engine determines a request was blocked (via
        ``is_blocked()``), it calls this method *before* re-queuing the
        request.  We mutate the request object in-place:

        1. ``request.sid`` is changed to ``_SID_STEALTH`` so the Session
           Manager routes the retry through the stealth Playwright session.
        2. ``proxy`` and ``proxies`` kwargs are cleared from the request.
           This is *required* because the residential ``ProxyRotator`` on the
           stealth session needs to assign a fresh proxy; stale datacenter
           proxy kwargs from the original request would override the rotator.
        3. Per-request stealth arguments are injected: ``block_webrtc`` and
           ``solve_cloudflare`` are set so the Playwright page actively works
           to bypass Cloudflare interstitials.

        Note: The base class already sets ``dont_filter=True`` and reduces
        priority before re-queuing, so we do not need to handle those here.

        Parameters
        ----------
        request:
            A *copy* of the original blocked request (the engine copies it
            before calling this method, so mutation is safe).
        response:
            The blocked response, available for inspection if needed.

        Returns
        -------
        Request
            The mutated request, ready for re-queuing to the stealth session.
        """
        self.logger.info(
            "[RETRY] Pivoting blocked URL %s -> session '%s'.",
            request.url,
            self._SID_STEALTH,
        )

        # Route to the stealth session.
        request.sid = self._SID_STEALTH

        # Clear any stale proxy overrides from the original request's kwargs.
        # The stealth session's ProxyRotator will assign a fresh residential proxy.
        request_kwargs: dict = getattr(request, "kwargs", {})
        request_kwargs.pop("proxy", None)
        request_kwargs.pop("proxies", None)

        return request

    # -----------------------------------------------------------------------
    # Main parsing callback
    # -----------------------------------------------------------------------

    async def parse(self, response: Response) -> AsyncGenerator[Any, None]:
        """
        Main parsing callback -- processes every fetched page.

        Responsibilities
        ----------------
        1. **Depth gate**: If the current crawl depth has reached ``max_depth``,
           link extraction is skipped entirely for this page.  A single
           ``MAX_DEPTH_REACHED`` telemetry event is fired to record that the
           depth limit was the reason no links were queued from this page.

        2. **Link extraction via 4-Gate Evaluation Matrix** (when depth allows):
           Every raw href is evaluated through four gates in strict order.
           Any link that fails a gate fires an async telemetry event and is
           discarded.  Only links clearing all gates reach the crawl queue.

           Gate order (fail-fast, cheapest checks first):
             [1] INVALID_SCHEME   -- non-crawlable href
             [2] DENY_LIST        -- path matched DENY_PATTERN (Negative-Space)
             [3] DUPLICATE        -- URL already queued this session
             [4] ACCEPT           -- add to _seen_urls, yield with depth+1

        3. **Triple-Threat extraction**: Delegates to
           ``extraction.run_triple_threat()``, which orchestrates the
           XHR -> Hydration -> MarkItDown waterfall.

        4. **Item yield**: Yields the rigid canonical payload dict.  The
           ``on_scraped_item()`` hook validates it and enforces the page cap.

        Parameters
        ----------
        response:
            The ``Response`` object for the current page.

        Yields
        ------
        Request:
            Follow-up crawl requests (only when depth < max_depth).
        dict:
            The canonical extraction payload for this page.
        """
        # -------------------------------------------------------------------
        # Depth Bounding: read the current depth from request meta.
        # -------------------------------------------------------------------
        # The seed URL has no meta, so we default to depth 0.  Each follow-up
        # request injected in Gate 4 carries ``{"depth": current_depth + 1}``
        # so the depth increments automatically as the crawl descends.
        current_depth: int = response.meta.get("depth", 0)

        self.logger.info(
            "[PARSE] Processing (depth=%d): %s (HTTP %d)",
            current_depth,
            response.url,
            response.status,
        )

        # -------------------------------------------------------------------
        # Step 1: 4-Gate Evaluation Matrix (skipped entirely at max_depth)
        # -------------------------------------------------------------------
        # Contact intelligence accumulators -- per-page, local sets so that
        # the same email/phone found on multiple <a> tags is deduplicated.
        # Declared here (outside the depth gate) so they are always in scope
        # when we assemble the final payload, regardless of depth.
        page_emails: Set[str] = set()
        page_phones: Set[str] = set()

        links_found: int = 0
        links_queued: int = 0

        if current_depth >= self.max_depth:
            # ---------------------------------------------------------------
            # DEPTH GATE: we are at or beyond the configured depth limit.
            # ---------------------------------------------------------------
            # Fire a single telemetry event using the page URL as both source
            # and target -- this records *which page* triggered the depth
            # ceiling, not individual links (those are never evaluated).
            # Using response.url as target_url is intentional: we are saying
            # "the decision to stop extracting links was made AT this URL".
            await self._telemetry.record(
                source_url=response.url,
                target_url=response.url,
                drop_reason=DropReason.MAX_DEPTH_REACHED,
            )
            self.logger.info(
                "[DEPTH] max_depth=%d reached at depth=%d for %s -- "
                "link extraction skipped.",
                self.max_depth,
                current_depth,
                response.url,
            )
        else:
            # ---------------------------------------------------------------
            # Negative-Space Routing: follow every same-domain link that is
            # not explicitly denied.  No allow whitelist -- the deny list is
            # the only filter between a found href and the crawl queue.
            # ---------------------------------------------------------------
            for href in response.css("a::attr(href)").getall():
                if not href:
                    continue

                links_found += 1

                # Normalise to an absolute URL.  urljoin handles relative,
                # protocol-relative, and already-absolute URLs uniformly.
                try:
                    abs_url: str = urljoin(response.url, href)
                except ValueError:
                    continue

                # -----------------------------------------------------------
                # Gate 1: Contact Interception + Scheme Check
                # -----------------------------------------------------------
                # Before resolving schemes via urlparse we inspect the raw href
                # directly.  We MUST use the raw href here (not abs_url) because
                # urljoin() above will silently mangle non-HTTP schemes:
                #   urljoin("https://example.com", "mailto:a@b.com")
                #   -> "mailto:a@b.com"  (safe here, but scheme detection is
                #      easier on the original string before any processing).
                #
                # Strategy: raw_href.lower().startswith() is the cheapest
                # possible check and avoids touching urlparse at all for the
                # common mailto/tel case.
                raw_lower: str = href.lower().lstrip()

                # -- mailto: interception ------------------------------------
                if raw_lower.startswith("mailto:"):
                    # Extract the email address from the href.
                    # Safe cleaning pipeline:
                    #   1. Strip the "mailto:" prefix (case-insensitive).
                    #   2. Split on "?" to remove query params (?subject=, etc.).
                    #   3. URL-decode percent-encoded characters (%40 -> @).
                    #   4. Strip whitespace.
                    # We use href (original case) not href.lower() so the
                    # email address itself preserves its original casing.
                    try:
                        raw_email: str = href[len("mailto:"):].split("?")[0]
                        clean_email: str = unquote(raw_email).strip()
                    except Exception:
                        clean_email = ""

                    if clean_email:
                        page_emails.add(clean_email)
                        self.logger.debug(
                            "[GATE-1/CONTACT] Email extracted: %s", clean_email
                        )

                    await self._telemetry.record(
                        source_url=response.url,
                        target_url=href,   # raw href as audit trail
                        drop_reason=DropReason.CONTACT_EXTRACTED_EMAIL,
                    )
                    continue   # do NOT yield a crawl request

                # -- tel: interception ----------------------------------------
                if raw_lower.startswith("tel:"):
                    # Extract the phone number.
                    # Safe cleaning pipeline:
                    #   1. Strip the "tel:" prefix.
                    #   2. URL-decode (%20 -> space, etc.).
                    #   3. Strip whitespace.
                    # We do NOT normalise the number format (e.g. strip dashes)
                    # because international formats vary; the downstream AI
                    # pipeline handles normalisation.
                    try:
                        raw_phone: str = href[len("tel:"):]
                        clean_phone: str = unquote(raw_phone).strip()
                    except Exception:
                        clean_phone = ""

                    if clean_phone:
                        page_phones.add(clean_phone)
                        self.logger.debug(
                            "[GATE-1/CONTACT] Phone extracted: %s", clean_phone
                        )

                    await self._telemetry.record(
                        source_url=response.url,
                        target_url=href,   # raw href as audit trail
                        drop_reason=DropReason.CONTACT_EXTRACTED_PHONE,
                    )
                    continue   # do NOT yield a crawl request

                # -- all other non-HTTP/S schemes -----------------------------
                # javascript:, data:, blob:, ftp:, etc.
                # Resolve scheme via urlparse on abs_url (already computed).
                try:
                    scheme: str = urlparse(abs_url).scheme.lower()
                except Exception:
                    continue

                if scheme not in ("http", "https"):
                    await self._telemetry.record(
                        source_url=response.url,
                        target_url=abs_url,
                        drop_reason=DropReason.INVALID_SCHEME,
                    )
                    self.logger.debug(
                        "[GATE-1] INVALID_SCHEME (%s): %s", scheme, abs_url
                    )
                    continue

                # -----------------------------------------------------------
                # Gate 2: DENY_LIST  (Negative-Space filter)
                # -----------------------------------------------------------
                # There is no allow pattern.  The deny list is the sole
                # vocabulary-based filter.  Any URL not matching DENY_PATTERN
                # is eligible -- this captures custom path segments like
                # /our-firm, /areas-of-practice, /why-choose-us, etc.
                try:
                    parsed_path: str = urlparse(abs_url).path
                except Exception:
                    continue

                if DENY_PATTERN.search(parsed_path):
                    await self._telemetry.record(
                        source_url=response.url,
                        target_url=abs_url,
                        drop_reason=DropReason.DENY_LIST,
                    )
                    self.logger.debug("[GATE-2/DENY] %s", abs_url)
                    continue

                # -----------------------------------------------------------
                # Gate 3: DUPLICATE
                # -----------------------------------------------------------
                # URL is not denied.  Check whether it was already queued
                # from a prior parse() iteration.  No async race: asyncio is
                # single-threaded; set lookup and add() are in the same
                # synchronous frame between await suspension points.
                if abs_url in self._seen_urls:
                    await self._telemetry.record(
                        source_url=response.url,
                        target_url=abs_url,
                        drop_reason=DropReason.DUPLICATE,
                    )
                    self.logger.debug("[GATE-3] DUPLICATE: %s", abs_url)
                    continue

                # -----------------------------------------------------------
                # Gate 4: ACCEPT
                # -----------------------------------------------------------
                # The URL is new, crawlable, and not denied.  Register it and
                # yield a follow-up request carrying the incremented depth in
                # meta so the next parse() call knows how deep it is.
                self._seen_urls.add(abs_url)
                links_queued += 1
                self.logger.debug("[GATE-4] QUEUED (depth->%d): %s",
                                  current_depth + 1, abs_url)
                yield response.follow(
                    abs_url,
                    callback=self.parse,
                    meta={"depth": current_depth + 1},
                )

            self.logger.info(
                "[PARSE] Link scan complete: %d found, %d queued "
                "(depth=%d/%d) for %s.",
                links_found,
                links_queued,
                current_depth,
                self.max_depth,
                response.url,
            )

        # -------------------------------------------------------------------
        # Step 2: Triple-Threat extraction pipeline
        # -------------------------------------------------------------------
        payload: Dict[str, Any] = run_triple_threat(
            response, logger=self.logger)

        # -------------------------------------------------------------------
        # Step 3: Inject contact intelligence into the payload
        # -------------------------------------------------------------------
        # Contacts are collected during the Gate 1 loop above (page_emails /
        # page_phones sets) and merged here into the rigid output schema.
        # Using sorted() gives deterministic output for downstream diffing
        # and deduplication, since sets have no guaranteed iteration order.
        #
        # The contacts block is ALWAYS present in the payload, even when both
        # lists are empty.  This keeps the schema rigid and simplifies
        # downstream consumers that check for key presence.
        payload["contacts"] = {
            "emails": sorted(page_emails),
            "phones": sorted(page_phones),
        }

        if page_emails or page_phones:
            self.logger.info(
                "[CONTACTS] %s -- emails: %s | phones: %s",
                response.url,
                sorted(page_emails) if page_emails else "(none)",
                sorted(page_phones) if page_phones else "(none)",
            )

        # -------------------------------------------------------------------
        # Step 4: Yield the canonical payload item
        # -------------------------------------------------------------------
        yield payload

    # -----------------------------------------------------------------------
    # Lifecycle hook: Item validation pipeline
    # -----------------------------------------------------------------------

    async def on_scraped_item(self, item: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Validate and gate each scraped item before it is recorded.

        An item is valid if and only if at least one of the two data fields
        (``extracted_json_state`` or ``extracted_markdown``) is non-empty.
        Items where *both* fields are empty indicate that all three extraction
        strategies failed for that page and the item should be discarded.

        Dropping empty items here, rather than in ``parse()``, keeps the
        extraction logic clean and separates concerns: ``parse()`` always
        yields a payload, and this hook is the quality gate.

        Parameters
        ----------
        item:
            The canonical payload dictionary from ``parse()``.

        Returns
        -------
        dict | None
            The item unchanged if valid, or ``None`` to silently drop it.
        """
        url: str = item.get("url", "<unknown>")
        has_json: bool = bool(item.get("extracted_json_state"))
        has_markdown: bool = bool(item.get("extracted_markdown", "").strip())

        if not has_json and not has_markdown:
            self.logger.warning(
                "[PIPELINE] DROP -- no data extracted for %s. "
                "All three strategies returned empty results.",
                url,
            )
            return None  # Returning None drops the item silently.

        extraction_method = (
            "XHR/Hydration JSON" if has_json else "MarkItDown Markdown"
        )
        self.logger.info(
            "[PIPELINE] ACCEPT -- item from %s via %s.",
            url,
            extraction_method,
        )

        # -------------------------------------------------------------------
        # Page Hardcap (compute budget enforcement)
        # -------------------------------------------------------------------
        # Increment the page counter AFTER accepting the item (empty pages
        # that were dropped above don't count against the budget).
        # If the count has reached max_pages, call self.pause() to trigger
        # Scrapling's graceful shutdown.  pause() is the documented API for
        # stopping a running stream() crawl from within spider code
        # (spiders_07_advanced.md, line 259).  There is no close_spider() in
        # Scrapling v0.4 -- that method does not exist in the public API.
        self.pages_scraped += 1
        self.logger.info(
            "[BUDGET] Pages scraped: %d / %d",
            self.pages_scraped,
            self.max_pages,
        )
        if self.pages_scraped >= self.max_pages:
            self.logger.warning(
                "[BUDGET] max_pages=%d reached -- calling self.pause() "
                "to shut down gracefully.",
                self.max_pages,
            )
            self.pause()

        return item

    # -----------------------------------------------------------------------
    # Lifecycle hook: Error handling
    # -----------------------------------------------------------------------

    async def on_error(self, request: Request, error: Exception) -> None:
        """
        Log unhandled request exceptions to the crawler log file.

        This hook is called by the Crawler Engine when a request raises an
        unhandled exception (e.g. network timeout, SSL error, DNS failure).
        We log the full traceback at ERROR level so that crash analysis is
        possible without re-running the spider.

        Parameters
        ----------
        request:
            The ``Request`` object that caused the error.
        error:
            The exception that was raised.
        """
        self.logger.error(
            "[ERROR] Request FAILED for %s (session=%s): %s: %s",
            request.url,
            getattr(request, "sid", "default"),
            type(error).__name__,
            error,
            exc_info=True,   # Include full traceback in the log file.
        )

    # -----------------------------------------------------------------------
    # Lifecycle hooks: Start / Close
    # -----------------------------------------------------------------------

    async def on_start(self, resuming: bool = False) -> None:
        """
        Called once before the crawl loop begins.

        Responsibilities
        ----------------
        1. Open the ``TelemetrySink`` so the async file handle is ready before
           the first ``parse()`` call fires.

        2. Initialise instance state for Depth Bounding and the Page Hardcap:

           ``_seen_urls``  -- O(1) set for Gate 3 duplicate detection.
           ``max_depth``   -- Maximum hops from the seed URL.
           ``max_pages``   -- Hard cap on total accepted pages per run.
           ``pages_scraped`` -- Running counter incremented in on_scraped_item.

        Instance attributes created here
        ---------------------------------
        ``_telemetry``   : TelemetrySink  -- async JSONL drop-event writer.
        ``_seen_urls``   : Set[str]        -- URLs already queued this session.
        ``max_depth``    : int             -- Depth limit (0 = seed only).
        ``max_pages``    : int             -- Page budget for this run.
        ``pages_scraped``: int             -- Live count of accepted pages.
        """
        # Open the telemetry sink first so it is available to parse().
        self._telemetry: TelemetrySink = TelemetrySink()
        await self._telemetry.open()
        self.logger.info(
            "[TELEMETRY] Sink opened: %s", self._telemetry._path
        )

        # Seen-URL set for Gate 3 duplicate detection.
        self._seen_urls: Set[str] = set()

        # Depth Bounding controls.
        # max_depth=2: seed (0) -> first-click (1) -> second-click (2).
        # At depth 2, parse() skips link extraction entirely.
        self.max_depth: int = 2

        # Page Hardcap (compute budget).
        # max_pages=10 prevents any single domain from consuming unbounded
        # compute.  pages_scraped is incremented in on_scraped_item().
        self.max_pages: int = 10
        self.pages_scraped: int = 0

        mode = "RESUMING from checkpoint" if resuming else "FRESH start"
        self.logger.info(
            "[LIFECYCLE] Spider '%s' -- %s. Seeds: %s",
            self.name,
            mode,
            ", ".join(self.start_urls),
        )
        self.logger.info(
            "[CONFIG] max_depth=%d | max_pages=%d",
            self.max_depth,
            self.max_pages,
        )

    async def on_close(self) -> None:
        """Called once after the crawl loop ends (completed or paused)."""
        self.logger.info(
            "[LIFECYCLE] Spider '%s' shutting down. "
            "Items scraped: %d | Requests: %d | Blocked retries: %d",
            self.name,
            self.stats.items_scraped if self.stats else -1,
            self.stats.requests_count if self.stats else -1,
            self.stats.blocked_requests_count if self.stats else -1,
        )

        # Close the telemetry sink – flushes any pending writes and releases
        # the async file handle.  Safe even if on_start never ran (getattr
        # guard) or if the file open failed (TelemetrySink.close() is a no-op
        # when _file is None).
        telemetry: Optional[TelemetrySink] = getattr(self, "_telemetry", None)
        if telemetry is not None:
            await telemetry.close()
            self.logger.info(
                "[TELEMETRY] Sink closed. Drop events written to: %s",
                telemetry._path,
            )

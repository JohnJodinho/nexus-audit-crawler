"""
app/extraction.py
=================
Cumulative Triple-Threat extraction pipeline for the Enterprise AI Audit Crawler.

All three strategies run for every page regardless of success or failure.
Results are accumulated rather than short-circuited so that downstream
consumers (embeddings, knowledge graph, structured fact extraction) each
receive the data format they require:

- ``xhr_payloads``      list[dict]      — structured JSON from XHR/fetch calls
- ``hydration_state``   dict | None     — embedded SPA hydration state
- ``raw_markdown``      str             — full-text Markdown from the DOM

Strategies
----------
1. XHR Capture       — collects all non-tracking JSON responses intercepted by
                        Playwright.  Filtered by a URL deny-list of known analytics
                        and tracking endpoints rather than an allow-list of assumed
                        business keys.
2. Hydration State   — extracts embedded SPA state (Next.js, Nuxt, Remix, etc.)
                        via regex patterns on the raw HTML body.
3. MarkItDown        — converts raw HTML to Markdown.  MarkItDown is instantiated
                        per-call to avoid shared mutable state under concurrent
                        ThreadPoolExecutor invocations.
"""

from __future__ import annotations

import io
import json
import logging
import re
from typing import Any, Dict, List, Optional

from markitdown import MarkItDown


# ---------------------------------------------------------------------------
# XHR filtering: deny-list approach
# ---------------------------------------------------------------------------

#: URL substrings that identify analytics, tracking, and telemetry endpoints.
#: XHR responses whose request URL contains any of these are discarded.
#: This is intentionally broad — false positives (discarding a legitimate
#: response) are far less harmful than false negatives (storing tracking noise
#: in the knowledge graph).
_XHR_TRACKING_DENY_PATTERNS: tuple[str, ...] = (
    "google-analytics",
    "googletagmanager",
    "doubleclick",
    "hotjar",
    "segment.io",
    "mixpanel",
    "amplitude",
    "sentry",
    "datadog",
    "newrelic",
    "facebook.com/tr",
    "bat.bing",
    "clarity.ms",
    "analytics.",
    ".tracking.",
    "beacon",
    "telemetry",
    "pixel",
)

_XHR_MIN_KEYS: int = 2  # Reject trivial heartbeat responses: {"ok": true}


def _is_tracking_xhr(request_url: str) -> bool:
    """Return ``True`` if ``request_url`` belongs to a known tracking endpoint."""
    url_lower = request_url.lower()
    return any(pattern in url_lower for pattern in _XHR_TRACKING_DENY_PATTERNS)


# ---------------------------------------------------------------------------
# Hydration state patterns
# ---------------------------------------------------------------------------

_HYDRATION_PATTERNS: List[tuple[str, re.Pattern[str]]] = [
    (
        "__NEXT_DATA__",
        re.compile(
            r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
            re.DOTALL | re.IGNORECASE,
        ),
    ),
    (
        "__NUXT__",
        re.compile(
            r"window\.__NUXT__\s*=\s*(\{.*?\})(?:\s*;|$)",
            re.DOTALL,
        ),
    ),
    (
        "__remixContext",
        re.compile(
            r"window\.__remixContext\s*=\s*(\{.*?\})(?:\s*;|$)",
            re.DOTALL,
        ),
    ),
    (
        "__APP_STATE__/__INITIAL_STATE__",
        re.compile(
            r"window\.__(?:APP_STATE|INITIAL_STATE)__\s*=\s*(\{.*?\})(?:\s*;|$)",
            re.DOTALL,
        ),
    ),
]


# ---------------------------------------------------------------------------
# Extraction functions
# ---------------------------------------------------------------------------


def extract_from_xhr(
    captured_xhr: list,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """
    Collect all non-tracking XHR/fetch JSON responses.

    Accepts every response whose request URL does not match
    ``_XHR_TRACKING_DENY_PATTERNS`` and whose body is a JSON object with at
    least ``_XHR_MIN_KEYS`` keys.  Returns all accepted payloads rather than
    only the first so that downstream consumers can work with the full set.

    Parameters
    ----------
    captured_xhr:
        The ``response.captured_xhr`` list from a Scrapling ``Response``.
    logger:
        Optional logger.

    Returns
    -------
    list[dict]
        All accepted structured JSON payloads.  Empty list if none found.
    """
    if not captured_xhr:
        if logger:
            logger.debug("[XHR] No captured XHR responses available.")
        return []

    if logger:
        logger.info("[XHR] Scanning %d captured XHR response(s).", len(captured_xhr))

    accepted: List[Dict[str, Any]] = []

    for xhr in captured_xhr:
        request_url: str = getattr(xhr, "url", "") or ""

        if _is_tracking_xhr(request_url):
            if logger:
                logger.debug("[XHR] Denied (tracking URL): %s", request_url)
            continue

        payload: Optional[Dict[str, Any]] = None
        try:
            if hasattr(xhr, "json") and callable(xhr.json):
                payload = xhr.json()
            elif hasattr(xhr, "body"):
                raw = xhr.body
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", errors="replace")
                payload = json.loads(raw)
            elif hasattr(xhr, "text"):
                payload = json.loads(xhr.text)
        except (json.JSONDecodeError, AttributeError, UnicodeDecodeError):
            continue

        if not isinstance(payload, dict):
            continue

        if len(payload) < _XHR_MIN_KEYS:
            if logger:
                logger.debug("[XHR] Skipped trivial response (%d keys): %s", len(payload), request_url)
            continue

        if logger:
            logger.info("[XHR] Accepted payload from %s (%d root keys).", request_url, len(payload))
        accepted.append(payload)

    if logger:
        logger.info("[XHR] %d / %d XHR response(s) accepted.", len(accepted), len(captured_xhr))

    return accepted


def extract_from_hydration(
    html_body: str,
    logger: Optional[logging.Logger] = None,
) -> Optional[Dict[str, Any]]:
    """
    Sweep raw HTML for embedded SPA hydration/state JSON objects.

    Tries each pattern in ``_HYDRATION_PATTERNS`` in order and returns the
    first successfully parsed dict.

    Parameters
    ----------
    html_body:
        Raw HTML response body as a decoded string.
    logger:
        Optional logger.

    Returns
    -------
    dict | None
        Parsed hydration JSON, or ``None`` if no pattern matched.
    """
    if not html_body:
        return None

    for label, pattern in _HYDRATION_PATTERNS:
        match = pattern.search(html_body)
        if match:
            raw_json = match.group(1)
            try:
                payload = json.loads(raw_json)
                if isinstance(payload, dict):
                    if logger:
                        logger.info(
                            "[HYDRATION] Extracted '%s' state block (%d root keys).",
                            label,
                            len(payload),
                        )
                    return payload
            except json.JSONDecodeError as exc:
                if logger:
                    logger.warning(
                        "[HYDRATION] Matched '%s' pattern but JSON parse failed: %s",
                        label,
                        exc,
                    )
                continue

    if logger:
        logger.debug("[HYDRATION] No hydration state blocks detected in HTML.")
    return None


def extract_via_markitdown(
    raw_html_bytes: bytes,
    logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    """
    Convert raw HTML to Markdown via ``MarkItDown``.

    A fresh ``MarkItDown`` instance is created per call to avoid shared mutable
    state when this function is dispatched to a ``ThreadPoolExecutor`` by
    multiple concurrent workers.

    Parameters
    ----------
    raw_html_bytes:
        Raw HTML body as bytes (``response.body`` in Scrapling v0.4+).
    logger:
        Optional logger.

    Returns
    -------
    str | None
        Markdown document, or ``None`` if conversion failed or output is empty.
    """
    if not raw_html_bytes:
        if logger:
            logger.warning("[MARKITDOWN] Received empty HTML body; skipping conversion.")
        return None

    try:
        converter = MarkItDown(enable_plugins=False)
        result = converter.convert_stream(
            io.BytesIO(raw_html_bytes),
            file_extension=".html",
        )
        markdown_text: str = result.text_content or ""

        if not markdown_text.strip():
            if logger:
                logger.warning("[MARKITDOWN] Conversion produced empty Markdown output.")
            return None

        if logger:
            logger.info(
                "[MARKITDOWN] Converted HTML -> Markdown (%d chars).",
                len(markdown_text),
            )
        return markdown_text

    except Exception as exc:
        if logger:
            logger.error(
                "[MARKITDOWN] Conversion raised an unexpected error: %s",
                exc,
                exc_info=True,
            )
        return None


def _determine_methods(
    xhr_payloads: List[Dict[str, Any]],
    hydration_state: Optional[Dict[str, Any]],
    raw_markdown: str,
) -> List[str]:
    """Return the list of extraction strategies that produced output."""
    methods: List[str] = []
    if xhr_payloads:
        methods.append("xhr")
    if hydration_state:
        methods.append("hydration")
    if raw_markdown.strip():
        methods.append("markitdown")
    return methods or ["none"]


def run_triple_threat(
    response: Any,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Orchestrate the cumulative Triple-Threat extraction pipeline.

    All three strategies are attempted for every page.  Results are
    accumulated rather than short-circuited so that downstream consumers
    (embeddings, knowledge graph, structured fact extraction) each receive
    the representation they require.

    Returns a dict with exactly these keys:

    .. code-block:: python

        {
            "url":              str,
            "xhr_payloads":     list[dict],   # all non-tracking XHR responses
            "hydration_state":  dict | None,  # first matched SPA hydration block
            "raw_markdown":     str,          # full-text Markdown (empty str on failure)
            "extraction_methods": list[str],  # which strategies produced output
        }

    Parameters
    ----------
    response:
        A Scrapling ``Response`` with ``.url``, ``.body``, and ``.captured_xhr``.
    logger:
        Optional logger.
    """
    url: str = getattr(response, "url", "")

    # --- Strategy 1: XHR (fast — pure Python dict iteration) ----------------
    captured_xhr = getattr(response, "captured_xhr", []) or []
    xhr_payloads: List[Dict[str, Any]] = extract_from_xhr(captured_xhr, logger)

    # --- Strategy 2: Hydration (fast — regex on HTML string) ----------------
    raw_bytes: bytes = getattr(response, "body", b"") or b""
    encoding: str = getattr(response, "encoding", "utf-8") or "utf-8"
    try:
        html_str = raw_bytes.decode(encoding, errors="replace")
    except (LookupError, UnicodeDecodeError):
        html_str = raw_bytes.decode("utf-8", errors="replace")

    hydration_state: Optional[Dict[str, Any]] = extract_from_hydration(html_str, logger)

    # --- Strategy 3: MarkItDown (CPU-bound — runs in executor) --------------
    raw_markdown: str = extract_via_markitdown(raw_bytes, logger) or ""

    if not raw_markdown.strip() and logger:
        logger.warning("[PIPELINE] MarkItDown produced no output for: %s", url)

    extraction_methods = _determine_methods(xhr_payloads, hydration_state, raw_markdown)

    return {
        "url": url,
        "xhr_payloads": xhr_payloads,
        "hydration_state": hydration_state,
        "raw_markdown": raw_markdown,
        "extraction_methods": extraction_methods,
    }

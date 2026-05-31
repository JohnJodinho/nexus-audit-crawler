"""
app/extraction.py
=================
Triple-Threat Extraction Pipeline for the Enterprise AI Audit Crawler.

Architectural intent
--------------------
This module is the *data-intelligence layer*.  It is deliberately separated
from the spider so that the extraction logic can be unit-tested, extended, or
swapped out independently of Scrapling's crawl mechanics.

The three extraction strategies are applied in strict waterfall order:

  1. **XHR Interception** (fastest, richest, zero parsing overhead)
     The Stealthy Playwright session captures XHR/fetch calls made by the page
     while it loads.  If the page's API returns machine-readable JSON that
     contains substantive business data, we take it directly rather than
     re-parsing the DOM.

  2. **Hydration State Scraping** (medium cost)
     Server-Side-Rendered frameworks (Next.js → ``__NEXT_DATA__``,
     Nuxt.js → ``__NUXT__``, SvelteKit → ``__SVELTEKIT_DATA__``,
     etc.) embed their full application state as a ``<script>`` tag in the
     HTML.  Extracting this is orders of magnitude more reliable than
     scraping rendered DOM nodes and gives us structured JSON without
     JavaScript execution.

  3. **MarkItDown HTML-to-Markdown DOM Fallback** (last resort)
     When neither of the above yields data, we delegate the full,
     *unmodified* raw HTML to Microsoft's ``markitdown`` library.  We do
     NOT prune, flatten, or selectively extract HTML before passing it in;
     the library is trusted to produce a clean Markdown document that
     preserves all semantically meaningful content.

Each strategy returns ``None`` on failure, allowing the caller to cascade
to the next one transparently.
"""

from __future__ import annotations

import io
import json
import logging
import re
from typing import Any, Dict, List, Optional

# markitdown is available in the venv (confirmed during setup).
from markitdown import MarkItDown


# ---------------------------------------------------------------------------
# Module-level MarkItDown singleton
# ---------------------------------------------------------------------------

# We instantiate MarkItDown once at module load.  Plugin discovery is
# disabled so the conversion is deterministic and offline – no cloud calls.
_MARKITDOWN = MarkItDown(enable_plugins=False)

# ---------------------------------------------------------------------------
# Known hydration-state script patterns
# ---------------------------------------------------------------------------

# A registry of (label, compiled regex) pairs.  Each regex must capture
# group 1 as the raw JSON string embedded in the page's script tags.
# Ordered from most common to least common to short-circuit quickly.
_HYDRATION_PATTERNS: List[tuple[str, re.Pattern[str]]] = [
    # Next.js:  <script id="__NEXT_DATA__" type="application/json">…</script>
    (
        "__NEXT_DATA__",
        re.compile(
            r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
            re.DOTALL | re.IGNORECASE,
        ),
    ),
    # Nuxt 2 / 3 (legacy):  window.__NUXT__ = { … }
    (
        "__NUXT__",
        re.compile(
            r"window\.__NUXT__\s*=\s*(\{.*?\})(?:\s*;|$)",
            re.DOTALL,
        ),
    ),
    # Remix:  window.__remixContext = { … }
    (
        "__remixContext",
        re.compile(
            r"window\.__remixContext\s*=\s*(\{.*?\})(?:\s*;|$)",
            re.DOTALL,
        ),
    ),
    # Generic __APP_STATE__ or __INITIAL_STATE__ pattern used by many SPAs
    (
        "__APP_STATE__/__INITIAL_STATE__",
        re.compile(
            r"window\.__(?:APP_STATE|INITIAL_STATE)__\s*=\s*(\{.*?\})(?:\s*;|$)",
            re.DOTALL,
        ),
    ),
]

# Keywords that suggest a captured XHR payload is substantive business data
# rather than analytics pings, CDN health checks, or config calls.
_XHR_RELEVANCE_KEYS: frozenset[str] = frozenset(
    {
        # Generic structured-content signals
        "title", "name", "description", "content", "text", "body",
        # Commercial / SaaS page signals
        "pricing", "price", "plan", "feature", "features", "services",
        "product", "products", "solution", "solutions",
        # Company / about-page signals
        "about", "team", "mission", "values",
        # CRM / lead-gen signals
        "contact", "email", "phone",
        # API envelope signals
        "data", "result", "results", "items", "records",
    }
)


# ---------------------------------------------------------------------------
# Strategy 1: XHR Interception
# ---------------------------------------------------------------------------

def extract_from_xhr(
    captured_xhr: list,
    logger: Optional[logging.Logger] = None,
) -> Optional[Dict[str, Any]]:
    """
    Search captured XHR/fetch responses for a substantive JSON payload.

    Scrapling's ``AsyncStealthySession`` with ``capture_xhr`` enabled
    populates ``response.captured_xhr`` with a list of ``CapturedXHR``-like
    objects.  Each object exposes ``.url``, ``.status``, and ``.json()`` (or
    similar accessor).  We iterate that list, attempt to decode each body as
    JSON, and check whether the parsed object contains at least one of our
    relevance keys before accepting it.

    The *first* matching payload wins.  If multiple XHR responses are
    relevant, the caller receives only the first one; extend this function
    with scoring logic if richer selection is needed.

    Parameters
    ----------
    captured_xhr:
        The ``response.captured_xhr`` list from a Scrapling ``Response``.
        Items are ``CapturedXHR`` objects (url, status, body bytes, json()).
    logger:
        Optional logger for debug/info messages.

    Returns
    -------
    dict | None
        The first relevant JSON payload, or ``None`` if no match was found.
    """
    if not captured_xhr:
        if logger:
            logger.debug("[XHR] No captured XHR responses available.")
        return None

    if logger:
        logger.info("[XHR] Scanning %d captured XHR response(s).", len(captured_xhr))

    for xhr in captured_xhr:
        # Attempt JSON decoding – XHR objects may expose .json(), .body, or
        # .text depending on the Scrapling version.  We handle all variants.
        payload: Optional[Dict[str, Any]] = None
        try:
            # Prefer the dedicated .json() helper if it exists.
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
            # Not JSON or undecodable – try the next item.
            continue

        if not isinstance(payload, dict):
            # Top-level arrays or primitives are not our target structure.
            continue

        # Relevance check: at least one key anywhere in the payload must match.
        all_keys: set[str] = _collect_keys_recursive(payload)
        matched_keys = all_keys & _XHR_RELEVANCE_KEYS
        if matched_keys:
            url_hint = getattr(xhr, "url", "<unknown>")
            if logger:
                logger.info(
                    "[XHR] [OK] Found relevant payload at %s (matched keys: %s).",
                    url_hint,
                    ", ".join(sorted(matched_keys)),
                )
            return payload

    if logger:
        logger.debug("[XHR] No relevant JSON payload found in XHR responses.")
    return None


# ---------------------------------------------------------------------------
# Strategy 2: Hydration State Scraping
# ---------------------------------------------------------------------------

def extract_from_hydration(
    html_body: str,
    logger: Optional[logging.Logger] = None,
) -> Optional[Dict[str, Any]]:
    """
    Sweep the raw HTML for embedded SPA hydration/state JSON objects.

    Modern SSR frameworks (Next.js, Nuxt, Remix, etc.) serialise the entire
    page's initial data state into a ``<script>`` block in the HTML response.
    This function attempts to match each of the patterns in
    ``_HYDRATION_PATTERNS`` and returns the first successfully parsed object.

    We intentionally do *not* limit which keys are accepted here – if the
    hydration block exists, it is by definition the source of truth for the
    page's data.

    Parameters
    ----------
    html_body:
        The raw HTML response body as a decoded string.
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
                            "[HYDRATION] [OK] Extracted '%s' state block (%d keys at root).",
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
                # Try the next pattern rather than bailing entirely.
                continue

    if logger:
        logger.debug("[HYDRATION] No hydration state blocks detected in HTML.")
    return None


# ---------------------------------------------------------------------------
# Strategy 3: MarkItDown DOM Fallback
# ---------------------------------------------------------------------------

def extract_via_markitdown(
    raw_html_bytes: bytes,
    logger: Optional[logging.Logger] = None,
) -> Optional[str]:
    """
    Convert the *complete, unmodified* HTML payload to Markdown via MarkItDown.

    Per the architectural directive, we do NOT pre-process, prune, or scope
    the HTML before handing it to MarkItDown.  The full document is passed
    verbatim so that MarkItDown's own heading detection, table parsing, and
    list recognition can operate on the complete semantic structure.

    MarkItDown is invoked via ``convert_stream()`` with the ``file_extension``
    hint set to ``.html``.  This avoids any file-system I/O and runs entirely
    in memory.

    Parameters
    ----------
    raw_html_bytes:
        The raw HTML body as bytes (``response.body`` in Scrapling v0.4+).
    logger:
        Optional logger.

    Returns
    -------
    str | None
        The Markdown document, or ``None`` if conversion failed or the output
        is empty/whitespace-only.
    """
    if not raw_html_bytes:
        if logger:
            logger.warning("[MARKITDOWN] Received empty HTML body; skipping conversion.")
        return None

    try:
        result = _MARKITDOWN.convert_stream(
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
                "[MARKITDOWN] [OK] Converted HTML -> Markdown (%d chars).",
                len(markdown_text),
            )
        return markdown_text

    except Exception as exc:  # pragma: no cover
        if logger:
            logger.error(
                "[MARKITDOWN] Conversion raised an unexpected error: %s",
                exc,
                exc_info=True,
            )
        return None


# ---------------------------------------------------------------------------
# Public orchestrator
# ---------------------------------------------------------------------------

def run_triple_threat(
    response: Any,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Orchestrate the full Triple-Threat extraction waterfall.

    This function is the single entry point called from ``spider.py``.  It
    accepts a Scrapling ``Response`` object, runs all three strategies in
    order, and returns the canonical payload dictionary.

    The returned dictionary always has exactly these three keys:

    .. code-block:: python

        {
            "url":                   str,   # The page URL
            "extracted_json_state":  dict,  # From XHR or hydration (may be {})
            "extracted_markdown":    str,   # From MarkItDown (may be "")
        }

    Callers should check ``on_scraped_item`` to validate that at least one
    of ``extracted_json_state`` or ``extracted_markdown`` is non-empty before
    accepting the item.

    Parameters
    ----------
    response:
        A Scrapling ``Response`` object with ``.url``, ``.body`` (bytes),
        and ``.captured_xhr`` attributes.
    logger:
        Optional logger, typically ``self.logger`` from the Spider.

    Returns
    -------
    dict
        The canonical payload dictionary described above.
    """
    url: str = getattr(response, "url", "")

    # -----------------------------------------------------------------------
    # Strategy 1 – XHR Interception
    # -----------------------------------------------------------------------
    captured_xhr = getattr(response, "captured_xhr", []) or []
    json_state: Optional[Dict[str, Any]] = extract_from_xhr(captured_xhr, logger)

    # -----------------------------------------------------------------------
    # Strategy 2 – Hydration State (only if Strategy 1 failed)
    # -----------------------------------------------------------------------
    if json_state is None:
        raw_bytes: bytes = getattr(response, "body", b"") or b""
        # Decode to string for regex matching.  Use response encoding if
        # available; fall back to UTF-8 with replacement for safety.
        encoding: str = getattr(response, "encoding", "utf-8") or "utf-8"
        try:
            html_str = raw_bytes.decode(encoding, errors="replace")
        except (LookupError, UnicodeDecodeError):
            html_str = raw_bytes.decode("utf-8", errors="replace")

        json_state = extract_from_hydration(html_str, logger)

    # -----------------------------------------------------------------------
    # Strategy 3 – MarkItDown DOM Fallback (only if Strategies 1 & 2 failed)
    # -----------------------------------------------------------------------
    markdown_content: str = ""
    if json_state is None:
        raw_bytes = getattr(response, "body", b"") or b""
        markdown_content = extract_via_markitdown(raw_bytes, logger) or ""
        if not markdown_content and logger:
            logger.warning(
                "[PIPELINE] All three extraction strategies failed for: %s", url
            )

    # -----------------------------------------------------------------------
    # Assemble rigid output payload
    # -----------------------------------------------------------------------
    return {
        "url": url,
        "extracted_json_state": json_state if json_state is not None else {},
        "extracted_markdown": markdown_content,
    }


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _collect_keys_recursive(obj: Any, depth: int = 0, max_depth: int = 5) -> set[str]:
    """
    Recursively collect all dictionary keys from a nested structure.

    Stops at ``max_depth`` to avoid stack overflows on deeply nested payloads.

    Parameters
    ----------
    obj:
        The value to traverse.
    depth:
        Current recursion depth (internal use).
    max_depth:
        Maximum allowed depth before short-circuiting.

    Returns
    -------
    set[str]
        All string keys found anywhere in the structure.
    """
    keys: set[str] = set()
    if depth > max_depth:
        return keys
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str):
                keys.add(k.lower())
            keys |= _collect_keys_recursive(v, depth + 1, max_depth)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            keys |= _collect_keys_recursive(item, depth + 1, max_depth)
    return keys

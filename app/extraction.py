"""
app/extraction.py
=================
Triple-Threat extraction pipeline for the Enterprise AI Audit Crawler.

Three strategies are applied in waterfall order, each returning ``None`` on
failure so the next strategy is tried:

1. XHR Interception       -- parse captured API JSON from Playwright XHR.
2. Hydration State        -- extract embedded SPA state (Next.js, Nuxt, Remix, etc.).
3. MarkItDown DOM Fallback -- convert raw HTML to Markdown via ``markitdown``.
"""

from __future__ import annotations

import io
import json
import logging
import re
from typing import Any, Dict, List, Optional

from markitdown import MarkItDown


_MARKITDOWN = MarkItDown(enable_plugins=False)

_HYDRATION_PATTERNS: List[tuple[str, re.Pattern[str]]] = [
    (
        "__NEXT_DATA__",
        re.compile(
            r'<script[^>]*id=["|\']__NEXT_DATA__["|\'][^>]*>\s*(\{.*?\})\s*</script>',
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

_XHR_RELEVANCE_KEYS: frozenset[str] = frozenset(
    {
        "title", "name", "description", "content", "text", "body",
        "pricing", "price", "plan", "feature", "features", "services",
        "product", "products", "solution", "solutions",
        "about", "team", "mission", "values",
        "contact", "email", "phone",
        "data", "result", "results", "items", "records",
    }
)


def extract_from_xhr(
    captured_xhr: list,
    logger: Optional[logging.Logger] = None,
) -> Optional[Dict[str, Any]]:
    """
    Search captured XHR/fetch responses for a substantive JSON payload.

    Returns the first response whose keys overlap with ``_XHR_RELEVANCE_KEYS``.

    Parameters
    ----------
    captured_xhr:
        The ``response.captured_xhr`` list from a Scrapling ``Response``.
    logger:
        Optional logger.

    Returns
    -------
    dict | None
        First relevant JSON payload, or ``None`` if no match found.
    """
    if not captured_xhr:
        if logger:
            logger.debug("[XHR] No captured XHR responses available.")
        return None

    if logger:
        logger.info("[XHR] Scanning %d captured XHR response(s).", len(captured_xhr))

    for xhr in captured_xhr:
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

    The full, unmodified HTML is passed verbatim; no pre-processing or pruning
    is applied before conversion.

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

    except Exception as exc:
        if logger:
            logger.error(
                "[MARKITDOWN] Conversion raised an unexpected error: %s",
                exc,
                exc_info=True,
            )
        return None


def run_triple_threat(
    response: Any,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Orchestrate the Triple-Threat extraction waterfall.

    Runs XHR → Hydration → MarkItDown in order, short-circuiting on success.
    Always returns a dict with exactly three keys:

    .. code-block:: python

        {
            "url":                   str,
            "extracted_json_state":  dict,
            "extracted_markdown":    str,
        }

    Parameters
    ----------
    response:
        A Scrapling ``Response`` with ``.url``, ``.body``, and ``.captured_xhr``.
    logger:
        Optional logger, typically ``self.logger`` from the Spider.

    Returns
    -------
    dict
        Canonical extraction payload.
    """
    url: str = getattr(response, "url", "")

    captured_xhr = getattr(response, "captured_xhr", []) or []
    json_state: Optional[Dict[str, Any]] = extract_from_xhr(captured_xhr, logger)

    if json_state is None:
        raw_bytes: bytes = getattr(response, "body", b"") or b""
        encoding: str = getattr(response, "encoding", "utf-8") or "utf-8"
        try:
            html_str = raw_bytes.decode(encoding, errors="replace")
        except (LookupError, UnicodeDecodeError):
            html_str = raw_bytes.decode("utf-8", errors="replace")

        json_state = extract_from_hydration(html_str, logger)

    markdown_content: str = ""
    if json_state is None:
        raw_bytes = getattr(response, "body", b"") or b""
        markdown_content = extract_via_markitdown(raw_bytes, logger) or ""
        if not markdown_content and logger:
            logger.warning(
                "[PIPELINE] All three extraction strategies failed for: %s", url
            )

    return {
        "url": url,
        "extracted_json_state": json_state if json_state is not None else {},
        "extracted_markdown": markdown_content,
    }


def _collect_keys_recursive(obj: Any, depth: int = 0, max_depth: int = 5) -> set[str]:
    """
    Recursively collect all dictionary keys from a nested structure.

    Parameters
    ----------
    obj:
        Value to traverse.
    depth:
        Current recursion depth (internal use).
    max_depth:
        Maximum depth before short-circuiting (guards against stack overflow).

    Returns
    -------
    set[str]
        All string keys found anywhere in the structure, lowercased.
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

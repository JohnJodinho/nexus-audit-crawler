"""
app/utils/utilities.py
======================
URL canonicalization, fingerprinting, and Dead Letter Queue routing.
"""

from __future__ import annotations

import datetime
import hashlib
import logging
from typing import Dict
from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl

import redis.asyncio as aioredis


def canonicalize_url(url: str) -> str:
    """
    Normalize a URL to a canonical form for consistent fingerprinting.

    Normalization rules applied:
    - Lowercase scheme and hostname.
    - Remove URL fragment (``#section``) entirely.
    - Strip default ports (80 for http, 443 for https).
    - Strip trailing slash from the path (except bare root ``/``).
    - Preserve the query string as-is (query params may distinguish resources).

    Parameters
    ----------
    url:
        Absolute URL string to normalize.

    Returns
    -------
    str
        Normalized URL. Returns the original string unchanged if parsing fails.
    """
    try:
        p = urlparse(url)
    except Exception:
        return url

    scheme = p.scheme.lower()
    hostname = (p.hostname or "").lower()

    # Strip default ports.
    port = p.port
    if (scheme == "https" and port == 443) or (scheme == "http" and port == 80):
        port = None

    if port:
        netloc = f"{hostname}:{port}"
    else:
        netloc = hostname

    # Strip trailing slash unless path is root.
    path = p.path.rstrip("/") or "/"

    # Fragment deliberately omitted — fragments are client-side only.
    return urlunparse((scheme, netloc, path, "", p.query, ""))


def get_fingerprint(url: str) -> str:
    """
    Generate a stable SHA-256 fingerprint for a URL.

    The URL is canonicalized via ``canonicalize_url()`` before hashing so
    that semantically equivalent URLs (differing only in scheme casing,
    trailing slash, default port, or fragment) produce identical fingerprints.

    Parameters
    ----------
    url:
        Absolute URL string.

    Returns
    -------
    str
        64-character lowercase hex digest.
    """
    return hashlib.sha256(canonicalize_url(url).encode("utf-8")).hexdigest()


async def _route_to_dlq(
    redis: aioredis.Redis,
    task_fields: Dict[str, str],
    reason: str,
    dlq_key: str,
    log: logging.Logger = logging.getLogger(__name__),
) -> None:
    """
    Write a failed task to the Dead Letter Queue stream.

    Parameters
    ----------
    redis:
        Active async Redis client.
    task_fields:
        The original task payload fields.
    reason:
        Human-readable description of why the task is being dead-lettered.
    dlq_key:
        The crawl-scoped DLQ stream key, e.g. ``crawl:abc123:stream:dlq``.
    log:
        Logger for recording the DLQ event.
    """
    dlq_payload: Dict[str, str] = {
        **task_fields,
        "schema_version": "1",
        "dlq_reason": reason,
        "dlq_at_utc": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    await redis.xadd(dlq_key, dlq_payload)
    log.warning(
        "[DLQ] Task routed to %s: url=%s reason=%s",
        dlq_key,
        task_fields.get("url", "<unknown>"),
        reason,
    )

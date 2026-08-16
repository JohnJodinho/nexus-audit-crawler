from typing import Dict
import hashlib
import redis.asyncio as aioredis
import datetime
import logging
from app.redis_client import STREAM_DLQ


def get_fingerprint(url: str) -> str:
    """
    Generate a SHA-256 fingerprint for a given URL.

    Trailing slashes are stripped before hashing so that
    ``https://example.com`` and ``https://example.com/`` produce the
    same fingerprint.  This prevents the seed URL (typically supplied
    without a trailing slash from the CLI) and the same URL later
    discovered by the spider (which may include one) from being treated
    as two distinct pages.
    """
    return hashlib.sha256(url.rstrip("/").encode("utf-8")).hexdigest()


async def _route_to_dlq(
    redis: aioredis.Redis,
    task_fields: Dict[str, str],
    reason: str,
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
    log:
        Logger for recording the DLQ event.
    """
    dlq_payload: Dict[str, str] = {
        **task_fields,
        "dlq_reason":   reason,
        "dlq_at_utc":  datetime.datetime.now(datetime.UTC).isoformat(),
    }
    await redis.xadd(STREAM_DLQ, dlq_payload)
    log.warning(
        "[DLQ] Task routed to %s: url=%s reason=%s",
        STREAM_DLQ,
        task_fields.get("url", "<unknown>"),
        reason,
    )

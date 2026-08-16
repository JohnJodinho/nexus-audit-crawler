"""
app/redis_client.py
===================
Redis connection factory, stream/key name constants, and domain semaphore helpers.

All stream and key names are defined here as the single source of truth.
Import these constants rather than using string literals across modules.
"""

from __future__ import annotations

import os
from typing import Optional

import redis.asyncio as aioredis

from app.config import settings

# ---------------------------------------------------------------------------
# Stream / key name constants
# ---------------------------------------------------------------------------

STREAM_TASKS: str = "stream:audit_tasks"
STREAM_RESULTS: str = "stream:audit_results"
STREAM_TELEMETRY: str = "stream:dropped_telemetry"
STREAM_DLQ: str = "stream:dlq"

SET_VISITED: str = "set:visited_fingerprints"

CONSUMER_GROUP: str = "audit_workers_group"

PEL_TIMEOUT_MS = settings.PEL_TIMEOUT_MS
MAX_RETRIES = settings.MAX_RETRIES
MAX_DEPTH = settings.MAX_DEPTH
MAX_PAGES_PER_RUN = settings.MAX_PAGES_PER_RUN

#: Max workers scraping the same domain concurrently. 0 = disabled.
MAX_CONCURRENT_PER_DOMAIN: int = settings.MAX_CONCURRENT_PER_DOMAIN

_DOMAIN_SLOT_PREFIX: str = "throttle:domain:"
_DOMAIN_SLOT_TTL_S: int = 90


# ---------------------------------------------------------------------------
# Connection factory
# ---------------------------------------------------------------------------


def get_redis_url() -> str:
    """
    Return the Redis connection URL from settings.

    Returns
    -------
    str
        Redis URL in the form ``redis[s]://[:password@]host[:port][/db]``.
    """
    return settings.REDIS_URL


def create_redis_pool(
    url: Optional[str] = None,
    max_connections: int = 20,
    decode_responses: bool = True,
) -> aioredis.Redis:
    """
    Create and return an async Redis connection pool.

    Parameters
    ----------
    url:
        Redis connection URL.  Defaults to ``get_redis_url()``.
    max_connections:
        Pool size.  Size to (num_workers × pipeline_depth).
    decode_responses:
        When ``True``, Redis returns ``str`` instead of ``bytes``.

    Returns
    -------
    redis.asyncio.Redis
        Client backed by a connection pool.  Call ``await client.aclose()`` on exit.
    """
    target_url = url or get_redis_url()
    pool = aioredis.ConnectionPool.from_url(
        target_url,
        max_connections=max_connections,
        decode_responses=decode_responses,
    )

    return aioredis.Redis(connection_pool=pool, socket_timeout=10.0)


async def ensure_consumer_group(redis: aioredis.Redis) -> None:
    """
    Idempotently create the consumer group on ``STREAM_TASKS``.

    Uses ``MKSTREAM`` so the stream is created if it does not yet exist.
    The ``BUSYGROUP`` error is swallowed — calling this at every worker
    startup is safe.

    Parameters
    ----------
    redis:
        An active async Redis client.
    """
    try:
        await redis.xgroup_create(
            name=STREAM_TASKS,
            groupname=CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
    except aioredis.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            pass
        else:
            raise


async def acquire_domain_slot(redis: aioredis.Redis, domain: str) -> bool:
    """
    Attempt to acquire one concurrency slot for ``domain``.

    Atomically increments ``throttle:domain:{domain}``.  If the resulting
    value exceeds ``MAX_CONCURRENT_PER_DOMAIN``, decrements back and returns
    ``False``.  On success, resets the TTL and returns ``True``.

    The TTL is the safety net for OOM-killed workers: the key expires
    automatically after ``_DOMAIN_SLOT_TTL_S`` seconds if never released.

    Parameters
    ----------
    redis:
        An active async Redis client.
    domain:
        Target domain (e.g. ``"example.com"``).

    Returns
    -------
    bool
        ``True`` if the slot was granted; ``False`` if the domain is at capacity.
    """
    if MAX_CONCURRENT_PER_DOMAIN <= 0:
        return True

    key = f"{_DOMAIN_SLOT_PREFIX}{domain}"
    current: int = await redis.incr(key)

    if current > MAX_CONCURRENT_PER_DOMAIN:
        await redis.decr(key)
        return False

    await redis.expire(key, _DOMAIN_SLOT_TTL_S)
    return True


async def release_domain_slot(redis: aioredis.Redis, domain: str) -> None:
    """
    Release one concurrency slot for ``domain``.

    Decrements ``throttle:domain:{domain}``.  Deletes the key when the
    counter reaches zero.  Resets the TTL when peers are still holding slots
    to prevent mid-scrape expiry from corrupting the semaphore counter.

    Parameters
    ----------
    redis:
        An active async Redis client.
    domain:
        Target domain.
    """
    if MAX_CONCURRENT_PER_DOMAIN <= 0:
        return

    key = f"{_DOMAIN_SLOT_PREFIX}{domain}"
    remaining: int = await redis.decr(key)
    if remaining <= 0:
        await redis.delete(key)
    else:
        # Reset TTL: a peer still holds a slot; without this refresh a long
        # Playwright scrape (>90 s) lets the key expire and corrupts the counter.
        await redis.expire(key, _DOMAIN_SLOT_TTL_S)

"""
app/utils/flush_state.py
========================
Redis state reset utilities for the Enterprise AI Audit Crawler.

Two modes:

``flush_crawl(crawl_id)``
    Deletes all Redis keys scoped to the given ``crawl_id``
    (``crawl:{id}:*``).  Safe to run while other crawls are active.

``flush_all()``
    Full scorched-earth reset.  Deletes every key matching ``crawl:*``,
    removing state for ALL crawls.  Requires explicit administrative intent --
    this operation is irreversible.
"""

from __future__ import annotations

import logging
from typing import Optional

import redis.asyncio as aioredis

from app.redis_client import (
    create_redis_pool,
    tasks_key,
    results_key,
    telemetry_key,
    dlq_key,
    visited_key,
    queued_key,
    budget_key,
)

log: logging.Logger = logging.getLogger("redis_flusher")


async def flush_crawl(crawl_id: str, redis: Optional[aioredis.Redis] = None) -> None:
    """
    Delete all Redis state scoped to a single crawl.

    Deletes in this order:
    1. Named streams and sets.
    2. All ``crawl:{id}:lock:processing:*`` keys (in-flight locks).
    3. All ``crawl:{id}:throttle:domain:*`` keys (domain counters).

    Safe to call while other crawls are running -- only keys matching
    ``crawl:{crawl_id}:`` are affected.

    Parameters
    ----------
    crawl_id:
        The crawl namespace identifier to flush.
    redis:
        Optional existing Redis client.  A new pool is created if omitted.
    """
    _owns_pool = redis is None
    if _owns_pool:
        redis = create_redis_pool()

    log.info("[FLUSH] Flushing state for crawl_id='%s'", crawl_id)

    try:
        named_keys = [
            tasks_key(crawl_id),
            results_key(crawl_id),
            telemetry_key(crawl_id),
            dlq_key(crawl_id),
            visited_key(crawl_id),
            queued_key(crawl_id),
            budget_key(crawl_id),
        ]

        deleted_named = await redis.delete(*named_keys)
        log.info("[FLUSH] Deleted %d named key(s).", deleted_named)

        # Sweep processing locks
        lock_pattern = f"crawl:{crawl_id}:lock:processing:*"
        locks_deleted = await _scan_and_delete(redis, lock_pattern)
        log.info("[FLUSH] Deleted %d processing lock(s).", locks_deleted)

        # Sweep domain throttle counters
        throttle_pattern = f"crawl:{crawl_id}:throttle:domain:*"
        throttle_deleted = await _scan_and_delete(redis, throttle_pattern)
        log.info("[FLUSH] Deleted %d domain throttle counter(s).", throttle_deleted)

        log.info(
            "[FLUSH] crawl_id='%s' state is clean. "
            "Total keys removed: %d.",
            crawl_id,
            deleted_named + locks_deleted + throttle_deleted,
        )

    except Exception as exc:
        log.error("[FLUSH] Failed to flush crawl '%s': %s", crawl_id, exc)
        raise

    finally:
        if _owns_pool:
            await redis.aclose()


async def flush_all(redis: Optional[aioredis.Redis] = None) -> None:
    """
    Full scorched-earth reset: delete ALL crawl state across every crawl_id.

    Scans for every key matching ``crawl:*`` and deletes it.  This is an
    irreversible administrative operation.  Never call this during normal
    operation -- it will destroy state for all concurrent crawls.

    Parameters
    ----------
    redis:
        Optional existing Redis client.  A new pool is created if omitted.
    """
    _owns_pool = redis is None
    if _owns_pool:
        redis = create_redis_pool()

    log.warning(
        "[FLUSH-ALL] Performing full scorched-earth reset (all crawl:* keys)."
    )

    try:
        total_deleted = await _scan_and_delete(redis, "crawl:*")
        log.info("[FLUSH-ALL] Removed %d key(s) total.", total_deleted)

    except Exception as exc:
        log.error("[FLUSH-ALL] Failed: %s", exc)
        raise

    finally:
        if _owns_pool:
            await redis.aclose()


async def _scan_and_delete(redis: aioredis.Redis, pattern: str) -> int:
    """
    SCAN ``pattern`` across all Redis keyspace pages and DELETE each batch.

    Uses cursor-based iteration to handle arbitrarily large key sets without
    blocking Redis.

    Parameters
    ----------
    redis:
        Active async Redis client.
    pattern:
        Glob pattern passed to ``SCAN MATCH``.

    Returns
    -------
    int
        Total number of keys deleted.
    """
    cursor: int = 0
    total_deleted: int = 0

    while True:
        cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=100)
        if keys:
            await redis.delete(*keys)
            total_deleted += len(keys)
        if cursor == 0:
            break

    return total_deleted


if __name__ == "__main__":
    import argparse
    import asyncio
    import sys
    from app.config import settings

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s]:(%(name)s) %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Flush Redis crawl state.")
    parser.add_argument("--crawl-id", default=settings.CRAWL_ID, help="Crawl ID to flush.")
    parser.add_argument("--all", action="store_true", help="Flush all crawl states.")

    args = parser.parse_args()

    async def _run():
        if args.all:
            await flush_all()
        else:
            await flush_crawl(args.crawl_id)

    asyncio.run(_run())

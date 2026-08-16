"""
app/utils/flush_state.py
========================
Redis state reset utility for the Enterprise AI Audit Crawler.

- Full wipe (``keys_to_delete=None``): deletes all streams, fingerprint sets,
  budget counter, all ``lock:processing:*`` keys, and all ``throttle:domain:*``
  semaphore counters.
- Targeted wipe: deletes only the specified keys; lock/throttle sweeps are skipped.
"""

import asyncio
import logging
from typing import Union, Iterable

from app.redis_client import (
    create_redis_pool,
    STREAM_TASKS,
    STREAM_DLQ,
    SET_VISITED,
    STREAM_RESULTS,
    STREAM_TELEMETRY,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("redis_flusher")


async def flush_crawler_state(keys_to_delete: Union[str, Iterable[str], None] = None):
    """
    Wipe crawler state from Redis.

    Parameters
    ----------
    keys_to_delete:
        ``None`` for a full scorched-earth reset.
        A string or iterable of strings for targeted key deletion.
    """
    log.info("Connecting to Redis to flush state...")
    redis = create_redis_pool()

    is_full_wipe = keys_to_delete is None
    if is_full_wipe:
        keys_to_delete = [
            STREAM_TASKS,
            STREAM_DLQ,
            SET_VISITED,
            "set:queued_fingerprints",
            STREAM_RESULTS,
            STREAM_TELEMETRY,
            "global_budget:tickets_dispensed",
        ]
    elif isinstance(keys_to_delete, str):
        keys_to_delete = [keys_to_delete]
    else:
        keys_to_delete = list(keys_to_delete)

    try:
        if keys_to_delete:
            deleted_count = await redis.delete(*keys_to_delete)
            log.info(f"Destroyed {deleted_count} core structure(s) (Streams/Sets).")

        if is_full_wipe:
            cursor = 0
            locks_deleted = 0
            while True:
                cursor, keys = await redis.scan(
                    cursor=cursor, match="lock:processing:*", count=100
                )
                if keys:
                    await redis.delete(*keys)
                    locks_deleted += len(keys)
                if cursor == 0:
                    break

            log.info(f"Destroyed {locks_deleted} lock(s).")

            # Sweep throttle counters: stale throttle:domain:* keys stall new workers
            # for up to 90 seconds on the next crawl run.
            cursor = 0
            throttle_deleted = 0
            while True:
                cursor, keys = await redis.scan(
                    cursor=cursor, match="throttle:domain:*", count=100
                )
                if keys:
                    await redis.delete(*keys)
                    throttle_deleted += len(keys)
                if cursor == 0:
                    break

            log.info(f"Destroyed {throttle_deleted} domain throttle counter(s).")

        log.info("=====================================================")
        log.info("Redis state is completely clean. Ready for a fresh seed!")
        log.info("=====================================================")

    except Exception as e:
        log.error(f"Failed to flush Redis: {e}")
    finally:
        await redis.aclose()

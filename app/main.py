"""
app/main.py
===========
Distributed worker loop for the Enterprise AI Audit Crawler.

Each worker reads one task from ``stream:audit_tasks`` via XREADGROUP,
evaluates six sequential gates, and runs ``AuditSpider`` as a single-page
fetcher.  All crawl state is held in Redis; the worker itself is stateless
across tasks.

Environment variables
---------------------
REDIS_URL       Redis connection URL.  Default: redis://localhost:6379/0
WORKER_COUNT    Concurrent worker coroutines.  Default: 2
"""

from __future__ import annotations

import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import asyncio
import datetime
import json
import logging
import os
import socket
from typing import Any, Dict, Optional

import anyio
import redis.asyncio as aioredis
import redis.exceptions as redis_exceptions

from app.logger import LOG_FILE_PATH, get_pipeline_logger
from app.redis_client import (
    CONSUMER_GROUP,
    MAX_DEPTH,
    MAX_PAGES_PER_RUN,
    MAX_RETRIES,
    SET_VISITED,
    STREAM_DLQ,
    STREAM_RESULTS,
    STREAM_TASKS,
    acquire_domain_slot,
    create_redis_pool,
    ensure_consumer_group,
    release_domain_slot,
)
from app.spider import AuditSpider
from app.utils.utilities import get_fingerprint, _route_to_dlq
from app.config import settings

log: logging.Logger = get_pipeline_logger("audit_crawler.main")

WORKER_COUNT = settings.WORKER_COUNT
GLOBAL_MAX_PAGES = settings.GLOBAL_MAX_PAGES
_XREAD_BLOCK_MS: int = 5_000

CONTAINER_ID = settings.HOSTNAME or socket.gethostname()


async def worker_loop(worker_id: str, redis: aioredis.Redis) -> None:
    """
    Single stateless worker coroutine.  Runs until cancelled.

    Parameters
    ----------
    worker_id:
        Unique consumer identifier within the Redis consumer group.
    redis:
        Shared async Redis connection pool.
    """
    log.info("[WORKER:%s] Started. Listening on %s", worker_id, STREAM_TASKS)
    pages_processed: int = 0

    while True:
        try:
            raw_messages: Optional[list] = await redis.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=worker_id,
                streams={STREAM_TASKS: ">"},
                count=1,
                block=_XREAD_BLOCK_MS,
            )
        except redis_exceptions.TimeoutError:
            log.error(
                "[WORKER:%s] Redis timeout while reading from stream -- "
                "check Redis server availability and performance.",
                worker_id,
            )
            continue

        if not raw_messages:
            continue

        stream_name, entries = raw_messages[0]
        if not entries:
            continue

        message_id: str
        task_fields: Dict[str, str]
        message_id, task_fields = entries[0]

        url: str = task_fields.get("url", "")
        depth: int = int(task_fields.get("depth", "0"))
        retry_count: int = int(task_fields.get("retry_count", "0"))
        domain: str = task_fields.get("domain", "")

        if not url or not domain:
            log.warning(
                "[WORKER:%s] Malformed task (missing url/domain): %s -- "
                "routing to DLQ.",
                worker_id,
                task_fields,
            )
            await _route_to_dlq(
                redis, task_fields, "malformed_task_missing_url_or_domain", log=log
            )
            await redis.xack(STREAM_TASKS, CONSUMER_GROUP, message_id)
            continue

        log.info(
            "[WORKER:%s] Task claimed: msg_id=%s url=%s depth=%d retry=%d",
            worker_id,
            message_id,
            url,
            depth,
            retry_count,
        )

        # --- Gate 1: Global Deduplication -----------------------------------
        url_hash: str = get_fingerprint(url)
        already_visited: bool = bool(await redis.sismember(SET_VISITED, url_hash))

        if already_visited:
            log.info(
                "[WORKER:%s] [DEDUP] Already visited -- XACK and skip: %s",
                worker_id,
                url,
            )
            await redis.xack(STREAM_TASKS, CONSUMER_GROUP, message_id)
            continue

        lock_key: str = f"lock:processing:{url_hash}"
        lock_acquired: bool = bool(await redis.set(lock_key, "1", ex=600, nx=True))

        if not lock_acquired:
            log.info(
                "[WORKER:%s] [DEDUP] Currently being processed by another worker -- "
                "XACK and skip: %s",
                worker_id,
                url,
            )
            await redis.xack(STREAM_TASKS, CONSUMER_GROUP, message_id)
            continue

        # --- Gate 2: Depth --------------------------------------------------
        if depth > MAX_DEPTH:
            log.info(
                "[WORKER:%s] [DEPTH] depth=%d > MAX_DEPTH=%d -- dropping: %s",
                worker_id,
                depth,
                MAX_DEPTH,
                url,
            )
            await redis.delete(lock_key)
            await redis.xack(STREAM_TASKS, CONSUMER_GROUP, message_id)
            continue

        # --- Gate 3: Global Budget Ticket Dispenser -------------------------
        # is_first_attempt is keyed on throttle_count, not retry_count, so
        # that throttle-cycled tasks cannot bypass the budget check.
        is_first_attempt: bool = (
            retry_count == 0
            and int(task_fields.get("throttle_count", "0")) == 0
        )

        if GLOBAL_MAX_PAGES > 0 and is_first_attempt:
            current_tickets: int = int(
                await redis.get("global_budget:tickets_dispensed") or 0
            )
            if current_tickets >= GLOBAL_MAX_PAGES:
                log.info(
                    "[WORKER:%s] [GLOBAL BUDGET] Limit reached (%d/%d) -- "
                    "dropping task and deleting lock: %s",
                    worker_id,
                    current_tickets,
                    GLOBAL_MAX_PAGES,
                    url,
                )
                await redis.delete(lock_key)
                await redis.xack(STREAM_TASKS, CONSUMER_GROUP, message_id)
                continue

            ticket_number: int = await redis.incr("global_budget:tickets_dispensed")
            log.debug(
                "[WORKER:%s] [GLOBAL BUDGET] Ticket #%d dispensed for: %s",
                worker_id,
                ticket_number,
                url,
            )

        # --- Gate 4: Domain Throttle ----------------------------------------
        domain_slot_acquired: bool = False
        slot_denied: bool = False
        try:
            domain_slot_acquired = await acquire_domain_slot(redis, domain)
        except Exception as exc:
            log.warning(
                "[WORKER:%s] [THROTTLE] Domain slot acquire failed (%s) -- "
                "proceeding without throttle for %s.",
                worker_id,
                exc,
                domain,
            )
            domain_slot_acquired = True  # Fail open: don't freeze the crawl

        if not domain_slot_acquired:
            throttle_count: int = int(task_fields.get("throttle_count", "0")) + 1
            log.info(
                "[WORKER:%s] [THROTTLE] Domain '%s' at capacity -- "
                "re-queuing (throttle #%d): %s",
                worker_id,
                domain,
                throttle_count,
                url,
            )
            await redis.delete(lock_key)

            throttle_payload: Dict[str, str] = dict(task_fields)
            throttle_payload["retry_count"] = str(
                max(1, int(task_fields.get("retry_count", "0")))
            )
            throttle_payload["throttle_count"] = str(throttle_count)

            await redis.xadd(STREAM_TASKS, throttle_payload)
            await redis.xack(STREAM_TASKS, CONSUMER_GROUP, message_id)

            await asyncio.sleep(2.0)
            slot_denied = True

        if slot_denied:
            continue

        # --- Gates 5 & 6: Per-run cap + Spider (inside try/finally) ---------
        try:
            # Gate 5: Per-process page cap
            if MAX_PAGES_PER_RUN > 0 and pages_processed >= MAX_PAGES_PER_RUN:
                log.warning(
                    "[WORKER:%s] [BUDGET] MAX_PAGES_PER_RUN=%d reached -- "
                    "re-queuing task and stopping.",
                    worker_id,
                    MAX_PAGES_PER_RUN,
                )
                await redis.xadd(STREAM_TASKS, task_fields)
                break

            # Gate 6: Spider execution
            await _run_spider(
                redis=redis,
                worker_id=worker_id,
                url=url,
                domain=domain,
                depth=depth,
            )

            # Marked visited only after a successful fetch so failures remain retryable.
            await redis.sadd(SET_VISITED, url_hash)
            pages_processed += 1

            log.info(
                "[WORKER:%s] Successfully processed page #%d: %s",
                worker_id,
                pages_processed,
                url,
            )

        except Exception as exc:
            log.error(
                "[WORKER:%s] Spider error for %s: %s: %s",
                worker_id,
                url,
                type(exc).__name__,
                exc,
                exc_info=True,
            )

            retry_count += 1
            if retry_count >= MAX_RETRIES:
                log.error(
                    "[WORKER:%s] [DLQ] retry_count=%d >= MAX_RETRIES=%d "
                    "for %s -- routing to DLQ.",
                    worker_id,
                    retry_count,
                    MAX_RETRIES,
                    url,
                )
                await _route_to_dlq(
                    redis,
                    task_fields,
                    f"spider_error: {type(exc).__name__}: {exc}",
                    log=log,
                )
            else:
                log.warning(
                    "[WORKER:%s] Re-queuing %s (retry %d/%d).",
                    worker_id,
                    url,
                    retry_count,
                    MAX_RETRIES - 1,
                )
                retry_payload: Dict[str, str] = {
                    "url": url,
                    "depth": str(depth),
                    "retry_count": str(retry_count),
                    "domain": domain,
                    "published_at": datetime.datetime.now(datetime.UTC).isoformat(),
                }
                await redis.xadd(STREAM_TASKS, retry_payload)

        finally:
            if domain_slot_acquired and not slot_denied:
                try:
                    await release_domain_slot(redis, domain)
                except Exception as exc:
                    log.warning(
                        "[WORKER:%s] [THROTTLE] Domain slot release failed: %s",
                        worker_id,
                        exc,
                    )

            await redis.delete(lock_key)

            await redis.xack(STREAM_TASKS, CONSUMER_GROUP, message_id)
            log.debug(
                "[WORKER:%s] XACK: %s (message_id=%s)",
                worker_id,
                url,
                message_id,
            )


async def _run_spider(
    redis: aioredis.Redis,
    worker_id: str,
    url: str,
    domain: str,
    depth: int,
) -> None:
    """
    Instantiate ``AuditSpider`` and run it for a single URL.

    Parameters
    ----------
    redis:
        Shared Redis client injected into the spider for stream writes.
    worker_id:
        Worker identifier for logging.
    url:
        URL to fetch and process.
    domain:
        Domain fence for ``allowed_domains``.
    depth:
        Hop depth from the seed URL.
    """
    log.info("[WORKER:%s] Running spider for: %s (depth=%d)", worker_id, url, depth)

    spider = AuditSpider(
        redis_client=redis,
        task_depth=depth,
        task_domain=domain,
    )
    spider.start_urls = [url]
    spider.allowed_domains = {domain}

    items_yielded: int = 0
    async for _item in spider.stream():
        items_yielded += 1

    try:
        session_manager = getattr(spider, "_session_manager", None)
        if session_manager is not None:
            await session_manager.close()
    except Exception as _sm_exc:
        log.warning(
            "[WORKER:%s] SessionManager close error for %s: %s",
            worker_id,
            url,
            _sm_exc,
        )
    finally:
        del spider

    log.info(
        "[WORKER:%s] Spider finished for %s -- %d item(s) yielded to Scrapling.",
        worker_id,
        url,
        items_yielded,
    )


async def main() -> None:
    """
    Async entry point.

    Creates the shared Redis pool, ensures the consumer group exists, then
    launches ``WORKER_COUNT`` concurrent worker coroutines via
    ``asyncio.gather()``.
    """
    log.info("=" * 70)
    log.info("Enterprise AI Audit Crawler -- Distributed Worker")
    log.info("=" * 70)
    log.info("Redis URL:        %s", settings.REDIS_URL)
    log.info("Worker count:     %d", WORKER_COUNT)
    log.info("Max depth:        %d", MAX_DEPTH)
    log.info("Max Pages Crawl:  %d", GLOBAL_MAX_PAGES)
    log.info("Max pages/run:    %d", MAX_PAGES_PER_RUN)
    log.info("Log file:         %s", LOG_FILE_PATH)
    log.info("=" * 70)

    redis: aioredis.Redis = create_redis_pool(max_connections=WORKER_COUNT * 5)

    try:
        await ensure_consumer_group(redis)
        log.info(
            "[BOOTSTRAP] Consumer group '%s' ready on '%s'.",
            CONSUMER_GROUP,
            STREAM_TASKS,
        )

        worker_coroutines = [
            worker_loop(worker_id=f"{CONTAINER_ID}-worker-{i}", redis=redis)
            for i in range(WORKER_COUNT)
        ]

        log.info("[BOOTSTRAP] Launching %d worker(s)...", WORKER_COUNT)
        await asyncio.gather(*worker_coroutines)

    except KeyboardInterrupt:
        log.warning("KeyboardInterrupt received -- shutting down workers.")

    except Exception as exc:
        log.critical("Unhandled exception in main: %s", exc, exc_info=True)
        raise

    finally:
        await redis.aclose()
        log.info("[SHUTDOWN] Redis connection pool closed.")


if __name__ == "__main__":
    anyio.run(main)

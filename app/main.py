"""
app/main.py
===========
Distributed worker loop for the Enterprise AI Audit Crawler.

Each worker reads one task from the crawl-scoped task stream via XREADGROUP,
evaluates six sequential gates, and runs ``AuditSpider`` as a single-page
fetcher.  All crawl state is held in Redis; the worker itself is stateless
across tasks.

Environment variables
---------------------
REDIS_URL           Redis connection URL.  Default: redis://localhost:6379/0
CRAWL_ID            Crawl namespace identifier.  Default: default
WORKER_COUNT        Concurrent worker coroutines.  Default: 2
GLOBAL_MAX_PAGES    Hard page budget.  Default: 50
MAX_PAGES_PER_RUN   Per-process page cap.  Default: 100
MAX_DEPTH           Maximum crawl depth from the seed URL.  Default: 2
"""

from __future__ import annotations

import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import asyncio
import datetime
import logging
import socket
from typing import Any, Dict, Optional

import anyio
import redis.asyncio as aioredis
import redis.exceptions as redis_exceptions

from app.logger import LOG_FILE_PATH, get_pipeline_logger
from app.redis_client import (
    MAX_DEPTH,
    MAX_PAGES_PER_RUN,
    MAX_RETRIES,
    acquire_domain_slot,
    consumer_group_name,
    create_redis_pool,
    dlq_key,
    ensure_consumer_group,
    lock_key,
    queued_key,
    release_domain_slot,
    reserve_page_ticket,
    tasks_key,
    visited_key,
)
from app.spider import AuditSpider
from app.utils.utilities import get_fingerprint, _route_to_dlq
from app.config import settings

log: logging.Logger = get_pipeline_logger("audit_crawler.main")

WORKER_COUNT: int = settings.WORKER_COUNT
GLOBAL_MAX_PAGES: int = settings.GLOBAL_MAX_PAGES
CRAWL_ID: str = settings.CRAWL_ID
CONTAINER_ID: str = settings.HOSTNAME or socket.gethostname()

_XREAD_BLOCK_MS: int = 5_000


async def worker_loop(
    worker_id: str,
    redis: aioredis.Redis,
    crawl_id: str,
    auto_exit_on_drain: bool = False,
) -> None:
    """
    Single stateless worker coroutine. Runs until cancelled or queue is drained.
    """
    _tasks_stream: str = tasks_key(crawl_id)
    _consumer_group: str = consumer_group_name(crawl_id)
    _visited_set: str = visited_key(crawl_id)
    _queued_set: str = queued_key(crawl_id)
    _dlq_stream: str = dlq_key(crawl_id)

    log.info("[WORKER:%s] Started. Listening on %s", worker_id, _tasks_stream)
    pages_processed: int = 0
    consecutive_idle: int = 0

    while True:
        # --- Lifecycle Control Checks -----------------------------------------
        try:
            cancelled = await redis.get(f"crawl:{crawl_id}:control:cancelled")
            if cancelled:
                log.info("[WORKER:%s] Crawl %s marked CANCELLED. Exiting worker.", worker_id, crawl_id)
                break

            paused = await redis.get(f"crawl:{crawl_id}:control:paused")
            if paused:
                log.debug("[WORKER:%s] Crawl %s is PAUSED. Holding state...", worker_id, crawl_id)
                await asyncio.sleep(2.0)
                continue
        except Exception as ctrl_err:
            log.debug("[WORKER:%s] Control check error: %s", worker_id, ctrl_err)

        # --- Consume one task -------------------------------------------------
        try:
            raw_messages: Optional[list] = await redis.xreadgroup(
                groupname=_consumer_group,
                consumername=worker_id,
                streams={_tasks_stream: ">"},
                count=1,
                block=_XREAD_BLOCK_MS,
            )

        except redis_exceptions.TimeoutError:
            consecutive_idle += 1
            if auto_exit_on_drain and consecutive_idle >= 2:
                log.info("[WORKER:%s] Queue drained (idle timeout reached). Exiting.", worker_id)
                break
            continue

        if not raw_messages:
            consecutive_idle += 1
            if auto_exit_on_drain and consecutive_idle >= 2:
                log.info("[WORKER:%s] Queue drained (no messages). Exiting.", worker_id)
                break
            continue

        consecutive_idle = 0


        _stream_name, entries = raw_messages[0]
        if not entries:
            continue

        message_id: str
        task_fields: Dict[str, str]
        message_id, task_fields = entries[0]

        url: str = task_fields.get("url", "")
        depth: int = int(task_fields.get("depth", "0"))
        retry_count: int = int(task_fields.get("retry_count", "0"))
        throttle_count: int = int(task_fields.get("throttle_count", "0"))
        domain: str = task_fields.get("domain", "")

        # --- Malformed task check --------------------------------------------
        if not url or not domain:
            log.warning(
                "[WORKER:%s] Malformed task (missing url/domain): %s -- "
                "routing to DLQ.",
                worker_id,
                task_fields,
            )
            await _route_to_dlq(
                redis,
                task_fields,
                "malformed_task_missing_url_or_domain",
                dlq_key=_dlq_stream,
                log=log,
            )
            await redis.xack(_tasks_stream, _consumer_group, message_id)
            continue

        log.info(
            "[WORKER:%s] Task claimed: msg_id=%s url=%s depth=%d retry=%d throttle=%d",
            worker_id,
            message_id,
            url,
            depth,
            retry_count,
            throttle_count,
        )

        # --- Gate 1a: Visited fingerprint check ------------------------------
        url_hash: str = get_fingerprint(url)
        already_visited: bool = bool(await redis.sismember(_visited_set, url_hash))

        if already_visited:
            log.info(
                "[WORKER:%s] [DEDUP] Already visited -- XACK and skip: %s",
                worker_id,
                url,
            )
            await redis.xack(_tasks_stream, _consumer_group, message_id)
            continue

        # --- Gate 1b: In-flight processing lock (SETNX) ----------------------
        _lock_key: str = lock_key(crawl_id, url_hash)
        lock_acquired: bool = bool(
            await redis.set(_lock_key, worker_id, ex=600, nx=True)
        )

        if not lock_acquired:
            log.info(
                "[WORKER:%s] [DEDUP] Currently being processed by another worker -- "
                "XACK and skip: %s",
                worker_id,
                url,
            )
            await redis.xack(_tasks_stream, _consumer_group, message_id)
            continue

        # --- Gate 2: Depth ---------------------------------------------------
        if depth > MAX_DEPTH:
            log.info(
                "[WORKER:%s] [DEPTH] depth=%d > MAX_DEPTH=%d -- dropping: %s",
                worker_id,
                depth,
                MAX_DEPTH,
                url,
            )
            await redis.delete(_lock_key)
            await redis.xack(_tasks_stream, _consumer_group, message_id)
            continue

        # --- Gate 3: Atomic global budget (Lua script) -----------------------
        # Only first-attempt tasks pay a budget ticket.  A task is a first
        # attempt when neither retry_count nor throttle_count has been
        # incremented, i.e. it has never been through the spider or the
        # throttle re-queue path.
        is_first_attempt: bool = retry_count == 0 and throttle_count == 0

        if GLOBAL_MAX_PAGES > 0 and is_first_attempt:
            ticket: int = await reserve_page_ticket(redis, crawl_id, GLOBAL_MAX_PAGES)

            if ticket == 0:
                log.info(
                    "[WORKER:%s] [GLOBAL BUDGET] Limit reached (%d pages) -- "
                    "dropping task and deleting lock: %s",
                    worker_id,
                    GLOBAL_MAX_PAGES,
                    url,
                )
                await redis.delete(_lock_key)
                await redis.xack(_tasks_stream, _consumer_group, message_id)
                continue

            log.debug(
                "[WORKER:%s] [GLOBAL BUDGET] Ticket #%d dispensed for: %s",
                worker_id,
                ticket,
                url,
            )

        # --- Gate 4: Domain concurrency throttle (Lua script) ----------------
        domain_slot_acquired: bool = False
        slot_denied: bool = False

        try:
            domain_slot_acquired = await acquire_domain_slot(redis, crawl_id, domain)
        except Exception as exc:
            log.warning(
                "[WORKER:%s] [THROTTLE] Domain slot acquire failed (%s) -- "
                "proceeding without throttle for %s.",
                worker_id,
                exc,
                domain,
            )
            domain_slot_acquired = True  # Fail open: do not freeze the crawl

        if not domain_slot_acquired:
            throttle_count += 1
            log.info(
                "[WORKER:%s] [THROTTLE] Domain '%s' at capacity -- "
                "re-queuing (throttle #%d): %s",
                worker_id,
                domain,
                throttle_count,
                url,
            )
            await redis.delete(_lock_key)

            throttle_payload: Dict[str, str] = dict(task_fields)
            throttle_payload["schema_version"] = "1"
            throttle_payload["throttle_count"] = str(throttle_count)
            # Preserve retry_count as-is — this is NOT a spider failure.

            await redis.xadd(_tasks_stream, throttle_payload)
            await redis.xack(_tasks_stream, _consumer_group, message_id)
            await asyncio.sleep(2.0)
            slot_denied = True

        if slot_denied:
            continue

        # --- Gates 5 & 6: Per-run cap + Spider (inside try/finally) ----------
        try:
            # Gate 5: Per-process page cap
            if MAX_PAGES_PER_RUN > 0 and pages_processed >= MAX_PAGES_PER_RUN:
                log.warning(
                    "[WORKER:%s] [BUDGET] MAX_PAGES_PER_RUN=%d reached -- "
                    "re-queuing task and stopping.",
                    worker_id,
                    MAX_PAGES_PER_RUN,
                )
                # Increment throttle_count so Gate 3 does not charge a second
                # budget ticket when the next worker picks this task up.
                requeue_payload: Dict[str, str] = dict(task_fields)
                requeue_payload["schema_version"] = "1"
                requeue_payload["throttle_count"] = str(throttle_count + 1)
                await redis.xadd(_tasks_stream, requeue_payload)
                break

            # Gate 6: Spider execution
            items_yielded = await _run_spider(
                redis=redis,
                crawl_id=crawl_id,
                worker_id=worker_id,
                url=url,
                domain=domain,
                depth=depth,
            )

            if items_yielded == 0:
                log.warning(
                    "[WORKER:%s] Spider yielded 0 items for %s (blocked or unparseable) -- routing to DLQ.",
                    worker_id,
                    url,
                )
                await _route_to_dlq(
                    redis,
                    task_fields,
                    "spider_yielded_zero_items_or_blocked",
                    dlq_key=_dlq_stream,
                    log=log,
                )
            else:
                # Mark visited only after successful fetch so failures remain retryable.
                await redis.sadd(_visited_set, url_hash)
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
                    dlq_key=_dlq_stream,
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
                    "schema_version": "1",
                    "crawl_id": crawl_id,
                    "url": url,
                    "depth": str(depth),
                    "retry_count": str(retry_count),
                    "throttle_count": "0",
                    "domain": domain,
                    "published_at": datetime.datetime.now(datetime.UTC).isoformat(),
                }
                await redis.xadd(_tasks_stream, retry_payload)

        finally:
            if domain_slot_acquired and not slot_denied:
                try:
                    await release_domain_slot(redis, crawl_id, domain)
                except Exception as exc:
                    log.warning(
                        "[WORKER:%s] [THROTTLE] Domain slot release failed: %s",
                        worker_id,
                        exc,
                    )

            await redis.delete(_lock_key)
            await redis.xack(_tasks_stream, _consumer_group, message_id)
            log.debug(
                "[WORKER:%s] XACK: %s (message_id=%s)",
                worker_id,
                url,
                message_id,
            )


async def _run_spider(
    redis: aioredis.Redis,
    crawl_id: str,
    worker_id: str,
    url: str,
    domain: str,
    depth: int,
) -> None:
    """
    Instantiate ``AuditSpider`` and run it for a single URL.

    The spider and its Playwright session (if launched) are unconditionally
    closed in the ``finally`` block — even if ``spider.stream()`` raises —
    to prevent Chromium zombie process accumulation.

    Parameters
    ----------
    redis:
        Shared Redis client injected into the spider for stream writes.
    crawl_id:
        The crawl namespace identifier passed through to the spider.
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
        crawl_id=crawl_id,
        task_depth=depth,
        task_domain=domain,
    )
    spider.start_urls = [url]
    spider.allowed_domains = {domain}

    items_yielded: int = 0

    try:
        async for _item in spider.stream():
            items_yielded += 1
    finally:
        # Always close the session manager, even if stream() raised.
        session_manager = getattr(spider, "_session_manager", None)
        if session_manager is not None:
            try:
                await session_manager.close()
            except Exception as _sm_exc:
                if "Cannot exit invalid session" not in str(_sm_exc):
                    log.warning(
                        "[WORKER:%s] SessionManager close error for %s: %s",
                        worker_id,
                        url,
                        _sm_exc,
                    )
        del spider

    log.info(
        "[WORKER:%s] Spider finished for %s -- %d item(s) yielded.",
        worker_id,
        url,
        items_yielded,
    )
    return items_yielded


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
    log.info("Crawl ID:         %s", CRAWL_ID)
    log.info("Worker count:     %d", WORKER_COUNT)
    log.info("Max depth:        %d", MAX_DEPTH)
    log.info("Max pages (crawl):%d", GLOBAL_MAX_PAGES)
    log.info("Max pages (run):  %d", MAX_PAGES_PER_RUN)
    log.info("Log file:         %s", LOG_FILE_PATH)
    log.info("=" * 70)

    redis: aioredis.Redis = create_redis_pool(max_connections=WORKER_COUNT * 5)

    try:
        await ensure_consumer_group(redis, CRAWL_ID)
        log.info(
            "[BOOTSTRAP] Consumer group '%s' ready on '%s'.",
            consumer_group_name(CRAWL_ID),
            tasks_key(CRAWL_ID),
        )

        worker_coroutines = [
            worker_loop(
                worker_id=f"{CONTAINER_ID}-worker-{i}",
                redis=redis,
                crawl_id=CRAWL_ID,
            )
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


async def run_workers_for_crawl(crawl_id: str, worker_count: int = 2) -> None:
    """
    In-process background worker launcher for a specific crawl_id.
    Runs until the task queue is drained, then cleans up gracefully.
    """
    log.info("[WORKER_SPAWNER] Starting %d in-process worker(s) for crawl %s...", worker_count, crawl_id)
    redis: aioredis.Redis = create_redis_pool(max_connections=worker_count * 3)
    try:
        await ensure_consumer_group(redis, crawl_id)
        worker_coroutines = [
            worker_loop(
                worker_id=f"inproc-{i}",
                redis=redis,
                crawl_id=crawl_id,
                auto_exit_on_drain=True,
            )
            for i in range(min(worker_count, 4))
        ]
        await asyncio.gather(*worker_coroutines)
        log.info("[WORKER_SPAWNER] In-process workers finished for crawl %s.", crawl_id)
    except asyncio.CancelledError:
        log.info("[WORKER_SPAWNER] In-process workers cancelled for crawl %s.", crawl_id)
    except Exception as exc:
        log.error("[WORKER_SPAWNER] In-process worker error for %s: %s", crawl_id, exc, exc_info=True)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    anyio.run(main)


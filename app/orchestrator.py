"""
app/orchestrator.py
===================
Pipeline management tools for the distributed Redis task queue.

- ``publish_seed_url()``  -- enqueue a starting URL into the crawl task stream.
- ``janitor_loop()``      -- PEL watchdog; recovers stale tasks from dead workers.

CLI usage
---------
Publish a seed::

    python -m app.orchestrator --seed https://example.com --domain example.com --crawl-id my-crawl

Flush state for a specific crawl::

    python -m app.orchestrator --flush --crawl-id my-crawl

Full administrative reset (ALL crawls)::

    python -m app.orchestrator --flush-all

Run the janitor standalone::

    CRAWL_ID=my-crawl python -m app.orchestrator --janitor
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import sys
import uuid
from typing import Optional

import anyio
import redis.asyncio as aioredis
from sqlalchemy import select

from app.redis_client import (
    MAX_RETRIES,
    PEL_TIMEOUT_MS,
    consumer_group_name,
    persist_consumer_group_name,
    create_redis_pool,
    dlq_key,
    ensure_consumer_group,
    queued_key,
    tasks_key,
    results_key,
    telemetry_key,
)
from app.utils.utilities import _route_to_dlq, get_fingerprint, canonicalize_url
from app.utils.flush_state import flush_crawl, flush_all
from app.consolidation import consolidate_crawl
from app.db.engine import async_session_factory, close_engine
from app.models.schema import Crawl
from app.config import settings

log: logging.Logger = logging.getLogger("audit_crawler.orchestrator")


async def publish_seed_url(
    redis: aioredis.Redis,
    url: str,
    domain: str,
    crawl_id: str,
    depth: int = 0,
) -> str:
    """
    Enqueue a seed URL into the crawl-scoped task stream.

    Pre-registers the seed in the discovery ledger (``set:queued_fingerprints``)
    so that workers parsing pages that link back to the seed do not re-queue it.

    Parameters
    ----------
    redis:
        Active async Redis client.
    url:
        Absolute URL to crawl.
    domain:
        Domain fence for this crawl job (e.g. ``"example.com"``).
    crawl_id:
        Crawl namespace identifier.
    depth:
        Hop depth from seed.  Seeds always start at 0.

    Returns
    -------
    str
        Redis stream message ID.
    """
    canonical: str = canonicalize_url(url)
    url_hash: str = get_fingerprint(canonical)

    # Pre-register seed in discovery ledger to prevent nav-bar re-queuing.
    await redis.sadd(queued_key(crawl_id), url_hash)

    task_payload: dict[str, str] = {
        "schema_version": "1",
        "crawl_id": crawl_id,
        "url": canonical,
        "depth": str(depth),
        "retry_count": "0",
        "throttle_count": "0",
        "domain": domain,
        "published_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }

    message_id: str = await redis.xadd(tasks_key(crawl_id), task_payload)

    log.info(
        "[PUBLISHER] Enqueued seed URL: %s (depth=%d crawl_id=%s) -> message_id=%s",
        canonical,
        depth,
        crawl_id,
        message_id,
    )
    return message_id


async def janitor_loop(
    redis: aioredis.Redis,
    crawl_id: str,
    interval_s: float = 60.0,
    batch_size: int = 100,
) -> None:
    """
    Continuously monitor the PEL and re-publish tasks from dead workers.

    Tasks read via ``XREADGROUP`` stay in the worker's PEL until ``XACK``.
    If a worker crashes, its claimed messages remain in the PEL indefinitely.
    The janitor:
    1. Detects messages idle longer than ``PEL_TIMEOUT_MS``.
    2. Claims them via ``XCLAIM``.
    3. Re-publishes them via ``XADD`` (so they return to the ``>`` delivery path).
    4. Acknowledges the old PEL entry via ``XACK``.

    Tasks that have exhausted ``MAX_RETRIES`` are routed to the DLQ instead.

    Parameters
    ----------
    redis:
        Active async Redis client.
    crawl_id:
        Crawl namespace identifier.
    interval_s:
        PEL poll interval in seconds.  Default: 60.
    batch_size:
        Maximum PEL entries to inspect per iteration.
    """
    log.info(
        "[JANITOR] Starting PEL watchdog for crawl_id='%s'. "
        "Poll interval: %.0fs | Idle threshold: %dms (%.0f min)",
        crawl_id,
        interval_s,
        PEL_TIMEOUT_MS,
        PEL_TIMEOUT_MS / 60_000,
    )

    while True:
        await asyncio.sleep(interval_s)

        try:
            await _reclaim_stale_tasks(redis, crawl_id, batch_size)
        except Exception as exc:
            log.error("[JANITOR] Error during PEL scan: %s", exc, exc_info=True)

        # Lifecycle Watchdog: Check if crawl has completed traversal and persistence
        try:
            is_complete = await evaluate_crawl_completion(redis, crawl_id)
            if is_complete:
                log.info("[JANITOR] Crawl '%s' meets all completion conditions. Triggering consolidation...", crawl_id)
                summary = await run_crawl_consolidation(crawl_id)
                if summary:
                    log.info("[JANITOR] Crawl '%s' successfully consolidated and marked 'finished'.", crawl_id)
                    break
        except Exception as exc:
            log.error("[JANITOR] Error during crawl completion evaluation: %s", exc, exc_info=True)


async def _reclaim_stale_tasks(
    redis: aioredis.Redis,
    crawl_id: str,
    batch_size: int,
) -> None:
    """
    Inspect the PEL, claim stale entries, and re-publish them for workers.

    Stale tasks are re-published via ``XADD`` (not left in the janitor's PEL)
    so that they return to the ``>`` delivery path and are visible to healthy
    workers on their next ``XREADGROUP ">"`` call.

    Parameters
    ----------
    redis:
        Active async Redis client.
    crawl_id:
        Crawl namespace identifier.
    batch_size:
        Maximum number of PEL entries to inspect.
    """
    _tasks_stream = tasks_key(crawl_id)
    _consumer_group = consumer_group_name(crawl_id)
    _dlq_stream = dlq_key(crawl_id)

    try:
        pending: list = await redis.xpending_range(
            name=_tasks_stream,
            groupname=_consumer_group,
            min="-",
            max="+",
            count=batch_size,
        )
    except aioredis.ResponseError as exc:
        if "NOGROUP" in str(exc):
            log.warning(
                "[JANITOR] Stream or consumer group not yet initialised "
                "(NOGROUP) -- skipping PEL scan. Start a worker first. "
                "Detail: %s",
                exc,
            )
        else:
            log.error(
                "[JANITOR] Unexpected Redis error during XPENDING: %s",
                exc,
                exc_info=True,
            )
        return

    if not pending:
        log.debug("[JANITOR] PEL is empty -- nothing to reclaim.")
        return

    stale_ids: list[str] = []
    for entry in pending:
        idle_ms: int = entry.get("time_since_delivered", 0)
        message_id: str = entry.get("message_id", "")
        consumer: str = entry.get("consumer", "<unknown>")
        deliveries: int = entry.get("times_delivered", 0)

        if idle_ms >= PEL_TIMEOUT_MS:
            log.warning(
                "[JANITOR] Stale task detected: message_id=%s consumer=%s "
                "idle=%.1fmin deliveries=%d -- reclaiming.",
                message_id,
                consumer,
                idle_ms / 60_000,
                deliveries,
            )
            stale_ids.append(message_id)

    if not stale_ids:
        log.debug(
            "[JANITOR] Inspected %d PEL entries -- none exceeded timeout.",
            len(pending),
        )
        return

    try:
        claimed = await redis.xclaim(
            name=_tasks_stream,
            groupname=_consumer_group,
            consumername="janitor",
            min_idle_time=PEL_TIMEOUT_MS,
            message_ids=stale_ids,
        )
    except aioredis.ResponseError as exc:
        log.error(
            "[JANITOR] XCLAIM failed for %d stale message(s): %s",
            len(stale_ids),
            exc,
            exc_info=True,
        )
        return

    log.info(
        "[JANITOR] Claimed %d / %d stale task(s).",
        len(claimed),
        len(stale_ids),
    )

    for message in claimed:
        message_id = message[0]
        fields: dict = message[1] if len(message) > 1 else {}
        retry_count: int = int(fields.get("retry_count", "0"))

        if retry_count >= MAX_RETRIES:
            log.error(
                "[JANITOR] Task %s has been retried %d times -- routing to DLQ.",
                message_id,
                retry_count,
            )
            try:
                await _route_to_dlq(
                    redis,
                    fields,
                    "max_retries_exceeded_in_pel",
                    dlq_key=_dlq_stream,
                    log=log,
                )
                await redis.xack(_tasks_stream, _consumer_group, message_id)
            except aioredis.ResponseError as exc:
                log.error(
                    "[JANITOR] Failed to route task %s to DLQ: %s",
                    message_id,
                    exc,
                    exc_info=True,
                )
        else:
            # Re-publish as a fresh stream message so workers receive it via ">".
            requeue_payload: dict = dict(fields)
            requeue_payload["schema_version"] = "1"
            requeue_payload["crawl_id"] = crawl_id
            requeue_payload["retry_count"] = str(retry_count + 1)
            requeue_payload["published_at"] = datetime.datetime.now(datetime.UTC).isoformat()

            try:
                new_msg_id = await redis.xadd(_tasks_stream, requeue_payload)
                await redis.xack(_tasks_stream, _consumer_group, message_id)
                log.info(
                    "[JANITOR] Re-published stale task: old=%s new=%s url=%s retry=%d",
                    message_id,
                    new_msg_id,
                    fields.get("url", "<unknown>"),
                    retry_count + 1,
                )
            except aioredis.ResponseError as exc:
                log.error(
                    "[JANITOR] Failed to re-publish task %s: %s",
                    message_id,
                    exc,
                    exc_info=True,
                )


async def evaluate_crawl_completion(redis: aioredis.Redis, crawl_id: str) -> bool:
    """
    Evaluate whether a crawl run has completed traversal and persistence.

    Conditions:
    1. Redis task stream has no pending tasks (PEL == 0 and unread tasks == 0).
    2. Active processing locks for the crawl are 0.
    3. Persistence consumer groups for results, DLQ, and telemetry have 0 pending lag.
    """
    _tasks_stream = tasks_key(crawl_id)
    _group = consumer_group_name(crawl_id)

    # 1. Check task stream
    try:
        pending_info = await redis.xpending(_tasks_stream, _group)
        pending_count = pending_info.get("pending", 0) if isinstance(pending_info, dict) else 0
        if pending_count > 0:
            return False
    except Exception:
        # Group not created yet or no stream
        return False

    # 2. Check active domain processing locks
    lock_keys = [k async for k in redis.scan_iter(f"crawl:{crawl_id}:lock:processing:*")]
    if lock_keys:
        return False

    # 3. Check persistence consumer lag
    persist_group = persist_consumer_group_name(crawl_id)
    for stream_name in (results_key(crawl_id), dlq_key(crawl_id), telemetry_key(crawl_id)):
        try:
            p_info = await redis.xpending(stream_name, persist_group)
            if isinstance(p_info, dict) and p_info.get("pending", 0) > 0:
                return False
        except Exception:
            pass

    return True


async def run_crawl_consolidation(crawl_id: str) -> Optional[dict]:
    """
    Execute consolidation on the Postgres database for the given crawl_id.
    """
    async with async_session_factory() as session:
        # Determine target UUID deterministically
        try:
            target_uuid = uuid.UUID(crawl_id)
        except ValueError:
            target_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, crawl_id)

        res = await session.execute(
            select(Crawl).where(Crawl.id == target_uuid)
        )
        crawl = res.scalar_one_or_none()

        if not crawl:
            # Fallback to scanning config dictionary
            res_all = await session.execute(
                select(Crawl).order_by(Crawl.started_at.desc())
            )
            for c in res_all.scalars().all():
                if c.config and (c.config.get("crawl_id") == crawl_id or c.config.get("crawl_id_alias") == crawl_id):
                    crawl = c
                    break

        if not crawl:
            log.warning("[CONSOLIDATION] No Postgres Crawl record found with config.crawl_id='%s'", crawl_id)
            return None

        if crawl.status == "finished":
            log.info("[CONSOLIDATION] Crawl '%s' is already marked 'finished'.", crawl_id)
            return crawl.config.get("consolidation") if crawl.config else {}

        # Set status to consolidating
        crawl.status = "consolidating"
        await session.commit()

        # Run consolidation
        report = await consolidate_crawl(session=session, crawl_uuid=crawl.id, crawl_id=crawl_id)
        return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


async def _cli_main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m app.orchestrator",
        description=(
            "Manage the distributed Redis audit-crawler pipeline.\n\n"
            "Modes:\n"
            "  --seed / --domain / --crawl-id   Enqueue a seed URL.\n"
            "  --flush --crawl-id <id>           Wipe state for one crawl.\n"
            "  --flush-all                       Wipe ALL crawl state (admin).\n"
            "  --janitor                         Run PEL watchdog loop.\n"
            "  --consolidate                     Run crawl rollup and finalize."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)

    mode_group.add_argument(
        "--seed",
        metavar="URL",
        help="Seed URL to enqueue (e.g. https://example.com).",
    )

    mode_group.add_argument(
        "--flush",
        action="store_true",
        default=False,
        help=(
            "Wipe Redis state for the crawl specified by --crawl-id. "
            "Deletes all streams, sets, budget counter, processing locks, "
            "and domain throttle counters for that crawl."
        ),
    )

    mode_group.add_argument(
        "--flush-all",
        action="store_true",
        default=False,
        dest="flush_all",
        help=(
            "ADMINISTRATIVE: Delete ALL Redis state for ALL crawls "
            "(matches crawl:* pattern). Irreversible."
        ),
    )

    mode_group.add_argument(
        "--janitor",
        action="store_true",
        default=False,
        help="Run the PEL watchdog loop for --crawl-id.",
    )

    mode_group.add_argument(
        "--consolidate",
        action="store_true",
        default=False,
        help="Execute site-wide consolidation and finalize the crawl in Postgres.",
    )

    parser.add_argument(
        "--domain",
        metavar="DOMAIN",
        help="Domain fence for the crawl job. Required with --seed.",
    )

    parser.add_argument(
        "--crawl-id",
        metavar="CRAWL_ID",
        default=settings.CRAWL_ID,
        dest="crawl_id",
        help=(
            "Crawl namespace identifier. "
            f"Defaults to CRAWL_ID env var (currently: '{settings.CRAWL_ID}')."
        ),
    )

    args = parser.parse_args()

    if args.seed and not args.domain:
        parser.error("--domain is required when --seed is used.")

    if args.flush and not args.crawl_id:
        parser.error("--crawl-id is required when --flush is used.")

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s]:(%(name)s) %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    crawl_id: str = args.crawl_id

    if args.flush_all:
        log.warning(
            "[FLUSH-ALL] Deleting ALL crawl state. "
            "This affects every crawl in Redis."
        )
        await flush_all()
        return

    if args.flush:
        log.info("[FLUSH] Flushing state for crawl_id='%s'.", crawl_id)
        await flush_crawl(crawl_id)
        return

    if args.consolidate:
        log.info("[CONSOLIDATE] Running consolidation for crawl_id='%s'...", crawl_id)
        report = await run_crawl_consolidation(crawl_id)
        await close_engine()
        if report:
            print(f"Consolidation complete: {report}")
        return

    redis = create_redis_pool()

    try:
        if args.janitor:
            await ensure_consumer_group(redis, crawl_id)
            await janitor_loop(redis, crawl_id)
            return

        # --seed mode
        await ensure_consumer_group(redis, crawl_id)
        msg_id = await publish_seed_url(redis, args.seed, args.domain, crawl_id)
        print(f"Published: {msg_id}")

    finally:
        await redis.aclose()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    anyio.run(_cli_main)

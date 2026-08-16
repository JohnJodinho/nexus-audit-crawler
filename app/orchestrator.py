"""
app/orchestrator.py
===================
Pipeline management tools for the distributed Redis task queue.

- ``publish_seed_url()``  -- enqueue a starting URL into ``stream:audit_tasks``.
- ``janitor_loop()``      -- PEL watchdog; recovers tasks from dead workers.

CLI usage
---------
Publish a seed::

    python -m app.orchestrator --seed https://example.com --domain example.com

Full state reset (deletes all streams, visited set, budget counter, and all locks)::

    python -m app.orchestrator --flush

Targeted deletion (lock sweep is skipped)::

    python -m app.orchestrator --flush stream:audit_tasks set:visited_fingerprints

Run the janitor standalone::

    import anyio
    from app.orchestrator import janitor_loop
    from app.redis_client import create_redis_pool
    anyio.run(lambda: janitor_loop(create_redis_pool()))
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import sys

import anyio
import redis.asyncio as aioredis

from app.redis_client import (
    CONSUMER_GROUP,
    MAX_RETRIES,
    PEL_TIMEOUT_MS,
    STREAM_DLQ,
    STREAM_TASKS,
    create_redis_pool,
    ensure_consumer_group,
)
from app.utils.utilities import _route_to_dlq, get_fingerprint
from app.utils.flush_state import flush_crawler_state

log: logging.Logger = logging.getLogger("audit_crawler.orchestrator")


async def publish_seed_url(
    redis: aioredis.Redis,
    url: str,
    domain: str,
    depth: int = 0,
) -> str:
    """
    Enqueue a seed URL into ``stream:audit_tasks``.

    Also pre-registers the seed URL in ``set:queued_fingerprints`` so that
    workers parsing pages that link back to the seed do not re-queue it.

    Parameters
    ----------
    redis:
        Active async Redis client.
    url:
        Absolute URL to crawl.
    domain:
        Domain fence for this crawl job (e.g. ``"example.com"``).
    depth:
        Hop depth from seed.  Seeds always start at 0.

    Returns
    -------
    str
        Redis stream message ID.
    """
    task_payload: dict[str, str] = {
        "url": url,
        "depth": str(depth),
        "retry_count": "0",
        "domain": domain,
        "published_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }

    # Pre-register seed in discovery ledger to prevent nav-bar re-queuing.
    url_hash: str = get_fingerprint(url)
    await redis.sadd("set:queued_fingerprints", url_hash)

    message_id: str = await redis.xadd(STREAM_TASKS, task_payload)

    log.info(
        "[PUBLISHER] Enqueued seed URL: %s (depth=%d) -> message_id=%s",
        url,
        depth,
        message_id,
    )
    return message_id


async def janitor_loop(
    redis: aioredis.Redis,
    interval_s: float = 60.0,
    batch_size: int = 100,
) -> None:
    """
    Continuously monitor the PEL and reclaim tasks from dead workers.

    Tasks read via ``XREADGROUP`` stay in the worker's PEL until ``XACK``.
    If the worker crashes, those tasks are stuck until this janitor transfers
    them back to the group via ``XCLAIM``.

    Parameters
    ----------
    redis:
        Active async Redis client.
    interval_s:
        PEL poll interval in seconds.  Default: 60.
    batch_size:
        Maximum PEL entries to inspect per iteration.
    """
    log.info(
        "[JANITOR] Starting PEL watchdog. Poll interval: %.0fs | "
        "Idle threshold: %dms (%.0f min)",
        interval_s,
        PEL_TIMEOUT_MS,
        PEL_TIMEOUT_MS / 60_000,
    )

    while True:
        await asyncio.sleep(interval_s)

        try:
            await _reclaim_stale_tasks(redis, batch_size)
        except Exception as exc:
            log.error("[JANITOR] Error during PEL scan: %s", exc, exc_info=True)


async def _reclaim_stale_tasks(
    redis: aioredis.Redis,
    batch_size: int,
) -> None:
    """
    Inspect the PEL and ``XCLAIM`` entries idle longer than ``PEL_TIMEOUT_MS``.

    ``NOGROUP`` errors are logged as WARNING (normal before any worker starts).
    Tasks that exceed ``MAX_RETRIES`` deliveries are routed to the DLQ.

    Parameters
    ----------
    redis:
        Active async Redis client.
    batch_size:
        Maximum number of PEL entries to inspect.
    """
    try:
        pending: list = await redis.xpending_range(
            name=STREAM_TASKS,
            groupname=CONSUMER_GROUP,
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
    entry: dict[str, any]
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
            name=STREAM_TASKS,
            groupname=CONSUMER_GROUP,
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
        "[JANITOR] Reclaimed %d / %d stale tasks back into the pending queue.",
        len(claimed),
        len(stale_ids),
    )

    for message in claimed:
        message_id = message[0]
        fields = message[1] if len(message) > 1 else {}
        retry_count: int = int(fields.get("retry_count", "0"))

        if retry_count >= MAX_RETRIES:
            log.error(
                "[JANITOR] Task %s has been retried %d times -- routing to DLQ.",
                message_id,
                retry_count,
            )
            try:
                dlq_payload = dict(fields)
                await _route_to_dlq(
                    redis, dlq_payload, "max_retries_exceeded_in_pel", log=log
                )
                await redis.xack(STREAM_TASKS, CONSUMER_GROUP, message_id)
            except aioredis.ResponseError as exc:
                log.error(
                    "[JANITOR] Failed to route task %s to DLQ: %s",
                    message_id,
                    exc,
                    exc_info=True,
                )


_KNOWN_KEYS: tuple[str, ...] = (
    "stream:audit_tasks",
    "stream:audit_results",
    "stream:dropped_telemetry",
    "stream:dlq",
    "set:visited_fingerprints",
    "set:queued_fingerprints",
    "global_budget:tickets_dispensed",
)


async def _cli_main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m app.orchestrator",
        description=(
            "Manage the distributed Redis audit-crawler pipeline.\n\n"
            "Modes:\n"
            "  --seed / --domain   Enqueue a seed URL and start crawling.\n"
            "  --flush             Wipe Redis state before a fresh run.\n"
            "                      Bare --flush = full scorched-earth reset.\n"
            "                      --flush KEY [KEY ...] = targeted deletion."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)

    mode_group.add_argument(
        "--flush",
        nargs="*",
        metavar="KEY",
        dest="flush_keys",
        help=(
            "Wipe Redis state for a fresh crawl run. "
            "With no arguments, performs a full scorched-earth reset: deletes "
            "all streams, the visited-fingerprints set, the budget counter, "
            "and every lock:processing:* key. "
            "Pass one or more explicit key names to delete only those keys "
            "(lock sweep is skipped for targeted deletions). "
            f"Known keys: {', '.join(_KNOWN_KEYS)}"
        ),
    )

    mode_group.add_argument(
        "--seed",
        metavar="URL",
        help="Seed URL to enqueue (e.g. https://example.com).",
    )

    parser.add_argument(
        "--domain",
        metavar="DOMAIN",
        help=(
            "Domain fence for the crawl job (e.g. example.com). "
            "Required when --seed is used."
        ),
    )

    args = parser.parse_args()

    if args.seed and not args.domain:
        parser.error("--domain is required when --seed is used.")

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s]:(%(name)s) %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.flush_keys is not None or args.seed is None:
        keys_arg = args.flush_keys if args.flush_keys else None

        if keys_arg is None:
            log.info(
                "[FLUSH] No keys specified -- performing full scorched-earth reset "
                "(all streams + visited set + budget counter + all locks)."
            )
        else:
            log.info(
                "[FLUSH] Targeted deletion -- keys: %s  (lock sweep skipped).",
                ", ".join(keys_arg),
            )

        await flush_crawler_state(keys_arg)
        return

    redis = create_redis_pool()
    await ensure_consumer_group(redis)
    try:
        msg_id = await publish_seed_url(redis, args.seed, args.domain)
        print(f"Published: {msg_id}")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    anyio.run(_cli_main)

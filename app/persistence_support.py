"""Idempotency, crawl-drain, and event helpers for persistence consumers."""

from __future__ import annotations

import datetime
import hashlib
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, Dict

import redis.asyncio as aioredis
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import resolve_crawl_uuid
from app.api.event_log import append_crawl_event
from app.models.schema import (
    Crawl,
    DeadLetterTask,
    DroppedTelemetry,
    ProcessedStreamMessage,
)
from app.redis_client import consumer_group_name

log = logging.getLogger("audit_crawler.persist")


async def ensure_crawl_record(
    session,
    crawl_id_str: str,
    domain: str = "",
    target_url: str = "",
) -> uuid.UUID:
    """Ensure a row in `crawls` exists for this crawl_id."""
    try:
        crawl_uuid = uuid.UUID(crawl_id_str)
    except ValueError:
        crawl_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, crawl_id_str)

    stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    result = await session.execute(stmt)
    crawl = result.scalar_one_or_none()

    if crawl is None:
        crawl = Crawl(
            id=crawl_uuid,
            target_url=target_url or f"https://{domain or 'example.com'}",
            target_domain=domain or "example.com",
            status="running",
            started_at=datetime.datetime.now(datetime.UTC),
            config={"crawl_id": crawl_id_str, "crawl_id_alias": crawl_id_str},
        )
        session.add(crawl)
        await session.flush()

    return crawl_uuid


async def process_telemetry_message(session, crawl_id: str, payload: Dict[str, Any]) -> None:
    """Process a link drop telemetry message."""
    crawl_uuid = await ensure_crawl_record(session, crawl_id)
    session.add(
        DroppedTelemetry(
            crawl_id=crawl_uuid,
            source_url=payload.get("source_url"),
            target_url=payload.get("target_url"),
            drop_reason=payload.get("drop_reason", "UNKNOWN"),
            timestamp_utc=datetime.datetime.now(datetime.UTC),
        )
    )


async def process_dlq_message(session, crawl_id: str, payload: Dict[str, Any]) -> None:
    """Process a dead-letter queue message."""
    crawl_uuid = await ensure_crawl_record(session, crawl_id)
    session.add(
        DeadLetterTask(
            crawl_id=crawl_uuid,
            url=payload.get("url"),
            dlq_reason=payload.get("dlq_reason", "unknown_error"),
            task_fields=payload,
            failed_at_utc=datetime.datetime.now(datetime.UTC),
        )
    )
    await session.execute(
        update(Crawl)
        .where(Crawl.id == crawl_uuid)
        .values(pages_failed=Crawl.pages_failed + 1)
    )


async def claim_stream_message(
    session: AsyncSession,
    stream_name: str,
    message_id: str,
) -> bool:
    """Claim a stream message once, in the same transaction as its persisted effects."""
    statement = (
        insert(ProcessedStreamMessage)
        .values(stream_name=stream_name, message_id=message_id)
        .on_conflict_do_nothing(
            index_elements=[
                ProcessedStreamMessage.stream_name,
                ProcessedStreamMessage.message_id,
            ]
        )
        .returning(ProcessedStreamMessage.message_id)
    )
    result = await session.execute(statement)
    return result.scalar_one_or_none() is not None


async def persist_stream_message(
    session: AsyncSession,
    crawl_id: str,
    stream_name: str,
    message_id: str,
    payload: dict[str, Any],
    result_stream: str,
    telemetry_stream: str,
    dead_letter_stream: str,
    result_processor: Callable[..., Awaitable[None]],
    telemetry_processor: Callable[..., Awaitable[None]],
    dead_letter_processor: Callable[..., Awaitable[None]],
    redis: aioredis.Redis | None = None,
) -> None:
    """Persist one new stream item and its matching durable UI event."""
    if not await claim_stream_message(session, stream_name, message_id):
        return

    crawl_uuid = resolve_crawl_uuid(crawl_id)
    if stream_name == result_stream:
        await result_processor(session, crawl_id, message_id, payload)
        if payload.get("url"):
            # Compute current progress metrics for rich real-time SSE updates
            crawl_obj = None
            try:
                crawl_res = await session.execute(select(Crawl).where(Crawl.id == crawl_uuid))
                if hasattr(crawl_res, "scalar_one_or_none"):
                    crawl_obj = crawl_res.scalar_one_or_none()
            except Exception:
                crawl_obj = None

            processed_count = getattr(crawl_obj, "pages_processed", 1) or 1
            config_dict = getattr(crawl_obj, "config", {}) or {}
            max_pages_cfg = config_dict.get("max_pages", 15) if isinstance(config_dict, dict) else 15
            calc_progress = min(0.95, round(processed_count / max_pages_cfg, 2)) if max_pages_cfg > 0 else 0.5

            await append_crawl_event(
                session,
                crawl_uuid,
                "page.available",
                {
                    "crawl_id": crawl_id,
                    "findings_updated": True,
                    "url": payload.get("url"),
                    "status": "running",
                    "pages_processed": processed_count,
                    "progress": calc_progress,
                },
                dedupe_key=persistence_event_key(stream_name, message_id),
                redis=redis,
            )
    elif stream_name == telemetry_stream:
        await telemetry_processor(session, crawl_id, payload)
        await append_crawl_event(
            session,
            crawl_uuid,
            "telemetry.updated",
            {"crawl_id": crawl_id},
            dedupe_key=persistence_event_key(stream_name, message_id),
        )
    elif stream_name == dead_letter_stream:
        await dead_letter_processor(session, crawl_id, payload)
        await append_crawl_event(
            session,
            crawl_uuid,
            "dlq.updated",
            {"crawl_id": crawl_id},
            dedupe_key=persistence_event_key(stream_name, message_id),
        )


def persistence_event_key(stream_name: str, message_id: str) -> str:
    """Return a bounded stable key so Redis redelivery cannot duplicate API events."""
    digest = hashlib.sha256(f"{stream_name}:{message_id}".encode("utf-8")).hexdigest()
    return f"redis:{digest}"


async def crawl_streams_drained(
    redis: aioredis.Redis,
    crawl_id: str,
    tasks_stream: str,
) -> bool:
    """Check the worker completion signal and task consumer-group backlog."""
    if await redis.get(f"crawl:{crawl_id}:control:workers_done"):
        return True

    try:
        worker_group = consumer_group_name(crawl_id)
        group_info = await redis.xinfo_groups(tasks_stream)
        for group in group_info:
            if group.get("name") == worker_group:
                return group.get("pending", 1) == 0 and group.get("lag", 1) == 0
    except Exception:
        return False
    return False


async def persist_stream_batch(
    redis: aioredis.Redis,
    session_factory,
    crawl_id: str,
    group: str,
    stream_name: str,
    messages,
    result_stream: str,
    telemetry_stream: str,
    dead_letter_stream: str,
    result_processor: Callable[..., Awaitable[None]],
    telemetry_processor: Callable[..., Awaitable[None]],
    dead_letter_processor: Callable[..., Awaitable[None]],
) -> None:
    """Commit a batch before acknowledging its Redis messages."""
    acknowledgements: list[str] = []
    async with session_factory() as session:
        async with session.begin():
            for message_id, payload in messages:
                try:
                    await persist_stream_message(
                        session,
                        crawl_id,
                        stream_name,
                        message_id,
                        payload,
                        result_stream,
                        telemetry_stream,
                        dead_letter_stream,
                        result_processor,
                        telemetry_processor,
                        dead_letter_processor,
                        redis=redis,
                    )
                    acknowledgements.append(message_id)
                except Exception as msg_exc:
                    log.error(
                        "[PERSIST] Error processing msg %s from %s: %s",
                        message_id,
                        stream_name,
                        msg_exc,
                        exc_info=True,
                    )
                    raise

    for message_id in acknowledgements:
        try:
            await redis.xack(stream_name, group, message_id)
        except Exception:
            log.error(
                "[PERSIST] DB commit succeeded but XACK failed for msg %s from %s; redelivery is safe.",
                message_id,
                stream_name,
                exc_info=True,
            )
            raise


async def auto_consolidate_if_drained(
    redis: aioredis.Redis,
    session_factory,
    crawl_id: str,
    tasks_stream: str,
) -> bool:
    """Consolidate once workers and the task stream have both drained."""
    try:
        if not await crawl_streams_drained(redis, crawl_id, tasks_stream):
            return False

        from app.consolidation import consolidate_crawl

        crawl_uuid = uuid.UUID(crawl_id) if len(crawl_id) == 36 else uuid.uuid5(uuid.NAMESPACE_DNS, crawl_id)
        async with session_factory() as session:
            await consolidate_crawl(session, crawl_uuid, crawl_id)
        log.info(
            "[PERSIST] Crawl %s tasks and results drained. Auto-consolidated successfully.",
            crawl_id,
        )
        return True
    except Exception as drain_exc:
        log.debug("[PERSIST] Drain check: %s", drain_exc)
        return False


async def handle_empty_read(
    redis: aioredis.Redis,
    session_factory,
    crawl_id: str,
    tasks_stream: str,
    auto_exit_on_drain: bool,
    idle_count: int,
) -> tuple[int, bool]:
    """Advance the drain grace period and report when the consumer should exit."""
    if not auto_exit_on_drain:
        return idle_count, False

    idle_count += 1
    if idle_count < 2:
        return idle_count, False

    should_exit = await auto_consolidate_if_drained(
        redis,
        session_factory,
        crawl_id,
        tasks_stream,
    )
    return idle_count, should_exit

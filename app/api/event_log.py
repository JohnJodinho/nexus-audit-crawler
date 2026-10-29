"""Transactional persistence helpers for replayable crawl change events."""

from __future__ import annotations

import logging
from typing import Any
import uuid

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import CrawlApiEvent

log = logging.getLogger("audit_crawler.event_log")

NOTIFY_CHANNEL = "nexus_crawl_events"


async def append_crawl_event(
    session: AsyncSession,
    crawl_id: uuid.UUID | None,
    event_type: str,
    payload: dict[str, Any],
    *,
    dedupe_key: str | None = None,
    redis=None,
) -> int | None:
    """Insert a durable event, queue its wake-up notification in this transaction, and optionally publish via Redis."""
    statement = (
        insert(CrawlApiEvent)
        .values(
            crawl_id=crawl_id,
            event_type=event_type,
            payload=payload,
            dedupe_key=dedupe_key,
        )
        .on_conflict_do_nothing(index_elements=[CrawlApiEvent.dedupe_key])
        .returning(CrawlApiEvent.id)
    )
    result = await session.execute(statement)
    event_id = result.scalar_one_or_none()
    if event_id is None:
        return None

    # 1. PostgreSQL transaction-level notification (for direct DB listeners)
    await session.execute(select(func.pg_notify(NOTIFY_CHANNEL, str(event_id))))

    # 2. Redis pub/sub broadcast fallback (for instant multi-tenant wakeups without transaction pooler issues)
    if redis is not None:
        try:
            from app.redis_client import events_channel
            import json
            channel = events_channel(str(crawl_id) if crawl_id else None)
            global_channel = events_channel(None)
            msg = json.dumps({
                "event_id": event_id,
                "event_type": event_type,
                "crawl_id": str(crawl_id) if crawl_id else None,
            })
            await redis.publish(channel, msg)
            if channel != global_channel:
                await redis.publish(global_channel, msg)
        except Exception as redis_exc:
            log.debug("Redis pub/sub event publish failed: %s", redis_exc)

    return event_id

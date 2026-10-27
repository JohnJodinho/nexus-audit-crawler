"""Transactional persistence helpers for replayable crawl change events."""

from __future__ import annotations

from typing import Any
import uuid

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import CrawlApiEvent

NOTIFY_CHANNEL = "nexus_crawl_events"


async def append_crawl_event(
    session: AsyncSession,
    crawl_id: uuid.UUID | None,
    event_type: str,
    payload: dict[str, Any],
    *,
    dedupe_key: str | None = None,
) -> int | None:
    """Insert a durable event and queue its wake-up notification in this transaction."""
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

    await session.execute(select(func.pg_notify(NOTIFY_CHANNEL, str(event_id))))
    return event_id

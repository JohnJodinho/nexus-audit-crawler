"""Authenticated SSE endpoint for durable crawl change events with multi-transport wakeups."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import re
from collections.abc import AsyncIterator, Callable
from typing import Any

import asyncpg
from fastapi import APIRouter, Header, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from app.api.deps import resolve_crawl_uuid
from app.api.event_log import NOTIFY_CHANNEL
from app.config import settings
from app.db.engine import get_sessionmaker
from app.models.schema import CrawlApiEvent
from app.redis_client import create_redis_pool, events_channel

log = logging.getLogger("audit_crawler.api.events")

router = APIRouter()

_EVENT_RETENTION = datetime.timedelta(days=30)
_HEARTBEAT_SECONDS = 15
_BATCH_SIZE = 200
_EVENT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def _listener_dsn() -> str:
    raw_url = settings.DATABASE_LISTEN_URL or settings.DATABASE_URL
    if not raw_url:
        raise ValueError("A PostgreSQL listener URL is not configured.")

    from sqlalchemy.engine import make_url

    url = make_url(raw_url)
    if not url.drivername.startswith("postgresql"):
        raise ValueError("The SSE listener requires PostgreSQL.")
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def _sse_message(event_id: int, event_name: str, data: dict[str, Any]) -> str:
    safe_name = event_name if _EVENT_NAME_PATTERN.fullmatch(event_name) else "message"
    payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    return f"id: {event_id}\nevent: {safe_name}\ndata: {payload}\n\n"


def _initial_frame(
    after_id: int,
    ready_cursor: int | None,
    resync_required: bool,
) -> tuple[str | None, int]:
    if resync_required:
        cursor = ready_cursor if ready_cursor is not None else after_id
        return (
            _sse_message(cursor, "resync_required", {"reason": "event_cursor_expired"}),
            cursor,
        )
    if ready_cursor is not None:
        return _sse_message(ready_cursor, "ready", {"cursor": ready_cursor}), after_id
    return None, after_id


def _event_frames(events: list[CrawlApiEvent]) -> list[tuple[str, int]]:
    return [
        (
            _sse_message(
                int(event.id),
                event.event_type,
                {
                    "crawl_id": str(event.crawl_id) if event.crawl_id else None,
                    "payload": event.payload or {},
                },
            ),
            int(event.id),
        )
        for event in events
    ]


async def _wait_for_event(wake_event: asyncio.Event) -> bool:
    try:
        await asyncio.wait_for(wake_event.wait(), timeout=_HEARTBEAT_SECONDS)
        return False
    except TimeoutError:
        return True


async def _redis_event_listener(redis_client, crawl_id: str | None, wake_event: asyncio.Event) -> None:
    """Listen for real-time events published via Redis Pub/Sub."""
    try:
        pubsub = redis_client.pubsub()
        channels = [events_channel(None)]
        if crawl_id:
            channels.append(events_channel(crawl_id))
        await pubsub.subscribe(*channels)
        async for _ in pubsub.listen():
            wake_event.set()
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        log.debug("Redis SSE listener ended: %s", exc)


async def _close_event_stream(
    connection: asyncpg.Connection | None,
    listener_callback: Callable[..., None] | None,
    redis_task: asyncio.Task | None = None,
    redis_client = None,
) -> None:
    if redis_task is not None and not redis_task.done():
        redis_task.cancel()
    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception:
            pass

    if connection is not None and listener_callback is not None:
        try:
            await connection.remove_listener(NOTIFY_CHANNEL, listener_callback)
        except Exception:
            pass
        finally:
            await connection.close()


async def _read_events(
    session_factory,
    after_id: int,
    crawl_uuid,
) -> list[CrawlApiEvent]:
    retention_start = datetime.datetime.now(datetime.UTC) - _EVENT_RETENTION
    statement = (
        select(CrawlApiEvent)
        .where(CrawlApiEvent.id > after_id, CrawlApiEvent.created_at >= retention_start)
        .order_by(CrawlApiEvent.id)
        .limit(_BATCH_SIZE)
    )
    if crawl_uuid is not None:
        statement = statement.where(CrawlApiEvent.crawl_id == crawl_uuid)

    async with session_factory() as session:
        result = await session.execute(statement)
        return list(result.scalars().all())


async def _event_stream(
    connection: asyncpg.Connection | None,
    wake_event: asyncio.Event,
    listener_callback: Callable[..., None] | None,
    session_factory,
    crawl_uuid,
    after_id: int,
    *,
    ready_cursor: int | None,
    resync_required: bool,
    redis_task: asyncio.Task | None = None,
    redis_client = None,
) -> AsyncIterator[str]:
    try:
        initial_frame, after_id = _initial_frame(after_id, ready_cursor, resync_required)
        if initial_frame is not None:
            yield initial_frame

        while True:
            wake_event.clear()
            try:
                events = await _read_events(session_factory, after_id, crawl_uuid)
            except SQLAlchemyError as exc:
                log.error("SSE event query failed (%s).", type(exc).__name__)
                yield "event: stream_error\ndata: {\"retryable\":true}\n\n"
                return

            frames = _event_frames(events)
            if frames:
                for frame, after_id in frames:
                    yield frame
                continue

            if await _wait_for_event(wake_event):
                yield ": keep-alive\n\n"
    finally:
        await _close_event_stream(connection, listener_callback, redis_task, redis_client)


def _parse_event_cursor(last_event_id: str | None) -> int | None:
    if not last_event_id:
        return None
    try:
        cursor = int(last_event_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Last-Event-ID must be a non-negative integer.",
        ) from exc
    if cursor < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Last-Event-ID must be a non-negative integer.",
        )
    return cursor


async def _close_listener(connection: asyncpg.Connection | None) -> None:
    if connection is not None:
        await connection.close()


async def _event_cursor_window(session_factory) -> tuple[int | None, int | None]:
    retention_start = datetime.datetime.now(datetime.UTC) - _EVENT_RETENTION
    async with session_factory() as session:
        result = await session.execute(
            select(func.min(CrawlApiEvent.id), func.max(CrawlApiEvent.id)).where(
                CrawlApiEvent.created_at >= retention_start
            )
        )
        earliest_id, latest_id = result.one()
    return earliest_id, latest_id


def _cursor_state(
    cursor: int | None,
    earliest_id: int | None,
    latest_id: int | None,
) -> tuple[int, int | None, bool]:
    _validate_cursor(cursor, latest_id)
    resync_required = _cursor_expired(cursor, earliest_id)
    current_cursor = int(latest_id or 0)
    after_id = current_cursor if cursor is None or resync_required else cursor
    ready_cursor = current_cursor if cursor is None else None
    return after_id, ready_cursor, resync_required


def _validate_cursor(cursor: int | None, latest_id: int | None) -> None:
    if cursor is not None and latest_id is not None and cursor > latest_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Last-Event-ID is ahead of the current event stream.",
        )


def _cursor_expired(cursor: int | None, earliest_id: int | None) -> bool:
    return cursor is not None and cursor > 0 and (
        earliest_id is None or cursor < earliest_id
    )


@router.get("/events", tags=["Events"])
@router.get("/crawls/{crawl_id}/events", tags=["Events"])
@router.get("/crawls/{crawl_id}/events/stream", tags=["Events"])
async def stream_crawl_events(
    crawl_id: str | None = None,
    last_event_id: str | None = Header(None, alias="Last-Event-ID"),
):
    """Stream durable crawl-change notifications and replay events after a cursor."""
    cursor = _parse_event_cursor(last_event_id)
    crawl_uuid = resolve_crawl_uuid(crawl_id) if crawl_id else None
    wake_event = asyncio.Event()

    def notify_callback(*_args: Any) -> None:
        wake_event.set()

    connection: asyncpg.Connection | None = None
    redis_client = None
    redis_task: asyncio.Task | None = None

    try:
        # 1. Try PostgreSQL LISTEN
        try:
            connection = await asyncpg.connect(dsn=_listener_dsn(), timeout=5)
            await connection.add_listener(NOTIFY_CHANNEL, notify_callback)
        except Exception as db_listen_exc:
            log.warning("PostgreSQL LISTEN not available (e.g. transaction pooler): %s", db_listen_exc)
            connection = None

        # 2. Redis pub/sub listener (works seamlessly across all environments)
        try:
            redis_client = create_redis_pool()
            redis_task = asyncio.create_task(_redis_event_listener(redis_client, crawl_id, wake_event))
        except Exception as redis_exc:
            log.warning("Redis SSE listener setup: %s", redis_exc)

        if connection is None and redis_client is None:
            raise RuntimeError("No event notification transport available.")

        session_factory = get_sessionmaker()
        earliest_id, latest_id = await _event_cursor_window(session_factory)
        after_id, ready_cursor, resync_required = _cursor_state(
            cursor,
            earliest_id,
            latest_id,
        )
    except HTTPException:
        await _close_event_stream(connection, notify_callback if connection else None, redis_task, redis_client)
        raise
    except (asyncpg.PostgresError, OSError, TimeoutError, SQLAlchemyError, ValueError, RuntimeError) as exc:
        await _close_event_stream(connection, notify_callback if connection else None, redis_task, redis_client)
        log.error("Unable to initialize SSE event stream (%s).", type(exc).__name__)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="The event stream is temporarily unavailable.",
        ) from exc

    return StreamingResponse(
        _event_stream(
            connection,
            wake_event,
            notify_callback if connection else None,
            session_factory,
            crawl_uuid,
            after_id,
            ready_cursor=ready_cursor,
            resync_required=resync_required,
            redis_task=redis_task,
            redis_client=redis_client,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )

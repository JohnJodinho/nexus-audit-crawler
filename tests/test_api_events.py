from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError

import app.api.routes.events as event_route
from app.api.event_log import append_crawl_event


class FakeListener:
    def __init__(self):
        self.added_channel = None
        self.removed = False
        self.closed = False

    async def add_listener(self, channel, callback):
        self.added_channel = (channel, callback)

    async def remove_listener(self, channel, callback):
        self.removed = (channel, callback) == self.added_channel

    async def close(self):
        self.closed = True


class FakeDatabaseSession:
    def __init__(self, earliest_id, latest_id):
        self.earliest_id = earliest_id
        self.latest_id = latest_id

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def execute(self, _statement):
        result = MagicMock()
        result.one.return_value = (self.earliest_id, self.latest_id)
        return result


class FakeSessionFactory:
    def __init__(self, earliest_id, latest_id):
        self.earliest_id = earliest_id
        self.latest_id = latest_id

    def __call__(self):
        return FakeDatabaseSession(self.earliest_id, self.latest_id)


async def _prepare_stream(monkeypatch, *, earliest=10, latest=12):
    listener = FakeListener()
    async_connect = AsyncMock(return_value=listener)
    monkeypatch.setattr(event_route.asyncpg, "connect", async_connect)
    monkeypatch.setattr(
        event_route.settings,
        "DATABASE_URL",
        "postgresql+asyncpg://test:test@example.invalid/test",
    )
    monkeypatch.setattr(event_route.settings, "DATABASE_LISTEN_URL", "")
    monkeypatch.setattr(
        event_route,
        "get_sessionmaker",
        lambda: FakeSessionFactory(earliest, latest),
    )
    return listener, async_connect


async def _collect(response) -> list[str]:
    return [
        chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
        async for chunk in response.body_iterator
    ]


@pytest.mark.asyncio
async def test_sse_initial_connection_sends_ready_cursor(monkeypatch):
    listener, async_connect = await _prepare_stream(monkeypatch)

    async def fail_event_read(*_args):
        raise SQLAlchemyError("test stream stop")

    monkeypatch.setattr(event_route, "_read_events", fail_event_read)
    response = await event_route.stream_crawl_events(crawl_id=None, last_event_id=None)
    chunks = await _collect(response)

    assert "id: 12\nevent: ready\ndata: {\"cursor\":12}" in chunks[0]
    assert "event: stream_error" in chunks[1]
    async_connect.assert_awaited_once()
    assert listener.removed is True
    assert listener.closed is True


@pytest.mark.asyncio
async def test_sse_expired_cursor_requests_snapshot_resync(monkeypatch):
    listener, _ = await _prepare_stream(monkeypatch, earliest=10, latest=12)

    async def fail_event_read(*_args):
        raise SQLAlchemyError("test stream stop")

    monkeypatch.setattr(event_route, "_read_events", fail_event_read)
    response = await event_route.stream_crawl_events(crawl_id=None, last_event_id="4")
    chunks = await _collect(response)

    assert "id: 12\nevent: resync_required" in chunks[0]
    assert '"reason":"event_cursor_expired"' in chunks[0]
    assert listener.closed is True


@pytest.mark.asyncio
async def test_sse_replays_ordered_events_after_cursor(monkeypatch):
    listener, _ = await _prepare_stream(monkeypatch, earliest=10, latest=12)
    crawl_uuid = uuid.uuid4()
    pending = [
        SimpleNamespace(id=11, event_type="page.available", crawl_id=crawl_uuid, payload={"findings_updated": True}),
        SimpleNamespace(id=12, event_type="crawl.completed", crawl_id=crawl_uuid, payload={"status": "finished"}),
    ]
    calls = 0

    async def read_events(*_args):
        nonlocal calls
        calls += 1
        if calls == 1:
            return pending
        raise SQLAlchemyError("test stream stop")

    monkeypatch.setattr(event_route, "_read_events", read_events)
    response = await event_route.stream_crawl_events(crawl_id=None, last_event_id="10")
    chunks = await _collect(response)

    assert "id: 11\nevent: page.available" in chunks[0]
    assert "id: 12\nevent: crawl.completed" in chunks[1]
    assert str(crawl_uuid) in chunks[0]
    assert listener.closed is True


@pytest.mark.asyncio
async def test_sse_rejects_malformed_event_cursor_without_connecting(monkeypatch):
    async_connect = AsyncMock()
    monkeypatch.setattr(event_route.asyncpg, "connect", async_connect)

    with pytest.raises(HTTPException) as response:
        await event_route.stream_crawl_events(crawl_id=None, last_event_id="not-an-id")

    assert response.value.status_code == 400
    async_connect.assert_not_awaited()


@pytest.mark.asyncio
async def test_event_insert_queues_notification_only_for_new_event():
    session = AsyncMock()
    inserted = MagicMock()
    inserted.scalar_one_or_none.return_value = 27
    session.execute.side_effect = [inserted, MagicMock()]

    event_id = await append_crawl_event(
        session,
        uuid.uuid4(),
        "page.available",
        {"findings_updated": True},
        dedupe_key="redis:test-event",
    )

    assert event_id == 27
    assert session.execute.await_count == 2
    assert "pg_notify" in str(session.execute.await_args_list[1].args[0])


@pytest.mark.asyncio
async def test_duplicate_event_does_not_queue_another_notification():
    session = AsyncMock()
    inserted = MagicMock()
    inserted.scalar_one_or_none.return_value = None
    session.execute.return_value = inserted

    event_id = await append_crawl_event(
        session,
        uuid.uuid4(),
        "page.available",
        {"findings_updated": True},
        dedupe_key="redis:already-processed",
    )

    assert event_id is None
    session.execute.assert_awaited_once()

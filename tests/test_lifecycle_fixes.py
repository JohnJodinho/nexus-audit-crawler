"""
tests/test_lifecycle_fixes.py
==============================
Unit and integration tests for SPA detection, worker queue drain exit,
multi-tenant persistence consumption, and real-time lifecycle status.
"""

from __future__ import annotations

import asyncio
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.api.app import create_app
from app.api.deps import get_db, get_redis, resolve_crawl_uuid
from app.models.schema import Crawl
from app.persistence_worker import persistence_loop
from app.main import worker_loop
from app.spider import AuditSpider


class MockResponse:
    def __init__(self, url: str, status: int, body: bytes, headers: dict = None, request=None):
        self.url = url
        self.status = status
        self.body = body
        self.headers = headers or {}
        self.request = request or MagicMock(sid="primary_http")


@pytest_asyncio.fixture
async def test_redis():
    """Provide a FakeRedis client."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def mock_db_session() -> AsyncMock:
    """Provide a mocked AsyncSession for API routing tests."""
    session = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.flush = AsyncMock()
    return session


@pytest_asyncio.fixture
async def client(mock_db_session: AsyncMock, test_redis) -> AsyncGenerator[AsyncClient, None]:
    """Provide an AsyncClient wired with mocked DB and Redis."""
    app = create_app()

    async def override_get_db():
        yield mock_db_session

    async def override_get_redis():
        yield test_redis

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_redis] = override_get_redis

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ---------------------------------------------------------------------------
# 1. SPA Detection and Stealth Browser Auto-Pivot Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_spa_detection_triggers_stealth_pivot():
    """Verify that an SPA skeleton with <div id='root'></div> triggers pivot to stealth browser."""
    mock_redis = AsyncMock()
    spider = AuditSpider(crawl_id="test-spa", redis_client=mock_redis)

    spa_html = b"""<!DOCTYPE html>
    <html>
    <head><title>SPA App</title><script src="/assets/index.js"></script></head>
    <body>
        <div id="root"></div>
    </body>
    </html>"""

    response = MockResponse(
        url="https://example.com/",
        status=200,
        body=spa_html,
        request=MagicMock(sid="primary_http"),
    )

    is_blocked = await spider.is_blocked(response)
    assert is_blocked is True

    # Check retry pivots to stealth
    orig_req = MagicMock(sid="primary_http", url="https://example.com/", kwargs={})
    pivoted_req = await spider.retry_blocked_request(orig_req, response)
    assert pivoted_req.sid == spider._SID_STEALTH


@pytest.mark.asyncio
async def test_normal_html_does_not_trigger_spa_pivot():
    """Verify that standard HTML with links and content does not falsely trigger SPA pivot."""
    mock_redis = AsyncMock()
    spider = AuditSpider(crawl_id="test-normal", redis_client=mock_redis)

    normal_html = b"""<!DOCTYPE html>
    <html>
    <head><title>Normal Site</title></head>
    <body>
        <h1>Welcome</h1>
        <p>This is standard content with plenty of paragraphs and text that does not require client-side SPA rendering.</p>
        <a href="/about">About Us</a>
        <a href="/pricing">Pricing</a>
    </body>
    </html>"""

    response = MockResponse(
        url="https://example.com/",
        status=200,
        body=normal_html,
        request=MagicMock(sid="primary_http"),
    )

    is_blocked = await spider.is_blocked(response)
    assert is_blocked is False


# ---------------------------------------------------------------------------
# 2. Worker Auto-Exit on Queue Drain Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_auto_exits_on_empty_queue():
    """Verify that worker_loop terminates when queue is empty after idle timeout."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None
    mock_redis.xreadgroup.return_value = []

    # Run worker_loop with auto_exit_on_drain=True
    # Should exit cleanly after 2 consecutive idle polls without hanging
    await asyncio.wait_for(
        worker_loop(
            worker_id="test-worker-0",
            redis=mock_redis,
            crawl_id="test-drain",
            auto_exit_on_drain=True,
        ),
        timeout=2.0,
    )
    assert mock_redis.xreadgroup.call_count >= 2


# ---------------------------------------------------------------------------
# 3. Multi-Tenant Persistence Consumer Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_persistence_consumes_result_message():
    """Verify that persistence_loop consumes messages from results stream and calls XACK."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    crawl_id = "test-persist-dyn"
    result_stream = f"crawl:{crawl_id}:stream:audit_results"
    mock_redis.get.return_value = "1"

    fake_msg = (
        "1788000000000-0",
        {
            "schema_version": "1",
            "url": "https://example.com/test",
            "canonical_url": "https://example.com/test",
            "status_code": "200",
            "raw_markdown": "# Test Page",
            "metadata": "{}",
            "contacts": "{\"emails\":[],\"phones\":[]}",
        },
    )

    mock_redis.xreadgroup.side_effect = [
        [(result_stream, [fake_msg])],
        [],
        [],
        [],
    ]
    mock_redis.xlen.return_value = 0

    class AsyncContextManagerMock:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_session = AsyncMock()
    mock_session.begin = MagicMock(return_value=AsyncContextManagerMock())

    class SessionFactoryMock:
        def __call__(self):
            return self
        async def __aenter__(self):
            return mock_session
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    with patch("app.persistence_worker.process_result_message", new_callable=AsyncMock) as mock_process:
        with patch("app.persistence_worker.get_sessionmaker", return_value=SessionFactoryMock()):
            with patch("app.consolidation.consolidate_crawl", new_callable=AsyncMock) as mock_consolidate:
                await asyncio.wait_for(
                    persistence_loop(
                        redis=mock_redis,
                        crawl_id=crawl_id,
                        worker_id="test-persist",
                        auto_exit_on_drain=True,
                    ),
                    timeout=5.0,
                )

                mock_process.assert_called_once()
                mock_redis.xack.assert_called_once()
                mock_consolidate.assert_called_once()


# ---------------------------------------------------------------------------
# 4. Status Route Terminal State Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_crawl_status_completed(client: AsyncClient, mock_db_session: AsyncMock):
    """Verify that status 'completed' sets progress=1.0 and next_action='retrieve'."""
    crawl_id = "test-completed-status"
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    mock_crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com/",
        target_domain="example.com",
        status="completed",
        pages_processed=5,
        pages_failed=0,
        config={"max_pages": 5},
    )

    res_crawl = MagicMock()
    res_crawl.scalar_one_or_none.return_value = mock_crawl

    res_findings = MagicMock()
    res_findings.scalar_one.return_value = 12

    mock_db_session.execute.side_effect = [res_crawl, res_findings]

    resp = await client.get(f"/api/crawls/{crawl_id}/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "completed"
    assert data["progress"] == 1.0
    assert data["next_recommended_action"] == "retrieve"
    assert data["findings_count"] == 12
    assert data["pages_processed"] == 5

"""
tests/test_api.py
=================
Unit & integration tests for the Nexus Query API (Phase 3).
Tests all REST endpoints with mocked database sessions and FakeRedis.
"""

from __future__ import annotations

import datetime
import uuid
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
import fakeredis.aioredis

from app.api.app import create_app
from app.api.deps import get_db, get_redis, resolve_crawl_uuid
from app.models.schema import AuditFinding, Crawl, Page, PageContact


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
# Test Cases
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["service"] == "nexus-audit-crawler-api"


@pytest.mark.asyncio
async def test_create_crawl_success(client: AsyncClient, mock_db_session: AsyncMock, test_redis):
    # Mock no active in-flight crawl
    mock_res = MagicMock()
    mock_res.scalars.return_value.first.return_value = None
    mock_db_session.execute.return_value = mock_res

    payload = {
        "url": "https://audit-target.com/",
        "crawl_id": "test-new-crawl",
        "config": {"max_pages": 10, "max_depth": 1, "worker_count": 2},
    }
    resp = await client.post("/api/crawls", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["crawl_id"] == "test-new-crawl"
    assert data["status"] == "queued"
    assert data["is_duplicate"] is False
    mock_db_session.add.assert_called_once()
    mock_db_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_create_crawl_duplicate_deduplication(client: AsyncClient, mock_db_session: AsyncMock, test_redis):
    """Verify that duplicate in-flight requests return the existing crawl idempotently."""
    existing_crawl = Crawl(
        id=uuid.uuid4(),
        target_url="https://audit-target.com/",
        target_domain="audit-target.com",
        status="running",
        config={"crawl_id": "existing-crawl-123"},
    )
    mock_res = MagicMock()
    mock_res.scalars.return_value.first.return_value = existing_crawl
    mock_db_session.execute.return_value = mock_res

    payload = {
        "url": "https://audit-target.com/",
        "config": {"max_pages": 10, "max_depth": 1, "worker_count": 2},
    }
    resp = await client.post("/api/crawls", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["is_duplicate"] is True
    assert data["crawl_id"] == "existing-crawl-123"
    assert data["status"] == "running"


@pytest.mark.asyncio
async def test_create_crawl_invalid_url(client: AsyncClient):
    resp = await client.post("/api/crawls", json={"url": "not-a-valid-url"})
    assert resp.status_code in (422, 400)



@pytest.mark.asyncio
async def test_create_crawl_worker_count_exceeds_limit(client: AsyncClient):
    """Verify that specifying worker_count > 4 fails validation with HTTP 422."""
    payload = {
        "url": "https://example.com/",
        "crawl_id": "test-limit-exceeded",
        "config": {"max_pages": 10, "max_depth": 1, "worker_count": 8},
    }
    resp = await client.post("/api/crawls", json=payload)
    assert resp.status_code == 422
    assert "exceed" in resp.text.lower() or "less than or equal to 4" in resp.text.lower()



@pytest.mark.asyncio
async def test_get_crawl_found(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-crawl-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    mock_crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com/",
        target_domain="example.com",
        status="finished",
        started_at=datetime.datetime.now(datetime.UTC),
        finished_at=datetime.datetime.now(datetime.UTC),
        worker_count=2,
        pages_discovered=5,
        pages_processed=3,
        pages_failed=0,
        config={"crawl_id": crawl_id, "max_pages": 15},
    )
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = mock_crawl
    mock_db_session.execute.return_value = mock_result

    resp = await client.get(f"/api/crawls/{crawl_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["target_url"] == "https://example.com/"
    assert data["target_domain"] == "example.com"
    assert data["status"] == "finished"


@pytest.mark.asyncio
async def test_get_crawl_not_found(client: AsyncClient, mock_db_session: AsyncMock):
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db_session.execute.return_value = mock_result

    resp = await client.get("/api/crawls/nonexistent-crawl")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_crawl_status(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-crawl-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    mock_crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com/",
        target_domain="example.com",
        status="finished",
        pages_processed=15,
        pages_discovered=20,
        pages_failed=0,
        config={"max_pages": 15},
    )

    # First call for Crawl, second call for findings count
    mock_crawl_res = MagicMock()
    mock_crawl_res.scalar_one_or_none.return_value = mock_crawl

    mock_count_res = MagicMock()
    mock_count_res.scalar_one.return_value = 8

    mock_db_session.execute.side_effect = [mock_crawl_res, mock_count_res]

    resp = await client.get(f"/api/crawls/{crawl_id}/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["crawl_id"] == crawl_id
    assert data["status"] == "finished"
    assert data["progress"] == 1.0
    assert data["next_recommended_action"] == "retrieve"
    assert data["findings_count"] == 8


@pytest.mark.asyncio
async def test_get_findings_paginated(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-crawl-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    f1 = AuditFinding(
        id="1:seo.missing_h1",
        rule_id="seo.missing_h1",
        crawl_id=crawl_uuid,
        page_id=1,
        url="https://example.com",
        category="seo",
        severity="critical",
        canvas_zone="head",
        explanation="No H1 found",
        evidence={"selector": "body"},
        remediation={"proposed": "<h1>Title</h1>", "confidence": 0.9},
        status="open",
        detected_at=datetime.datetime.now(datetime.UTC),
    )

    # 1st call for total count, 2nd call for findings list
    mock_count_res = MagicMock()
    mock_count_res.scalar_one.return_value = 1

    mock_list_res = MagicMock()
    mock_list_res.scalars.return_value.all.return_value = [f1]

    mock_db_session.execute.side_effect = [mock_count_res, mock_list_res]

    resp = await client.get(f"/api/crawls/{crawl_id}/findings?category=seo")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["page"] == 1
    assert len(data["findings"]) == 1
    assert data["findings"][0]["id"] == "1:seo.missing_h1"
    assert data["findings"][0]["canvas_zone"] == "head"


@pytest.mark.asyncio
async def test_get_single_finding(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-crawl-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    f1 = AuditFinding(
        id="1:seo.missing_h1",
        rule_id="seo.missing_h1",
        crawl_id=crawl_uuid,
        page_id=1,
        url="https://example.com",
        category="seo",
        severity="critical",
        canvas_zone="head",
        explanation="No H1 found",
        evidence={"selector": "body"},
        remediation={"proposed": "<h1>Title</h1>", "confidence": 0.9},
        status="open",
        detected_at=datetime.datetime.now(datetime.UTC),
    )

    mock_res = MagicMock()
    mock_res.scalar_one_or_none.return_value = f1
    mock_db_session.execute.return_value = mock_res

    resp = await client.get(f"/api/crawls/{crawl_id}/findings/1:seo.missing_h1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "1:seo.missing_h1"
    assert data["explanation"] == "No H1 found"


@pytest.mark.asyncio
async def test_patch_finding_status(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-crawl-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    f1 = AuditFinding(
        id="1:seo.missing_h1",
        rule_id="seo.missing_h1",
        crawl_id=crawl_uuid,
        page_id=1,
        url="https://example.com",
        category="seo",
        severity="critical",
        canvas_zone="head",
        explanation="No H1 found",
        evidence={"selector": "body"},
        remediation={"proposed": "<h1>Title</h1>", "confidence": 0.9},
        status="approved",
        detected_at=datetime.datetime.now(datetime.UTC),
    )

    mock_res = MagicMock()
    mock_res.scalar_one_or_none.return_value = f1
    mock_db_session.execute.return_value = mock_res

    resp = await client.patch(
        f"/api/crawls/{crawl_id}/findings/1:seo.missing_h1",
        json={"status": "approved"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "approved"
    mock_db_session.commit.assert_called_once()


@pytest.mark.asyncio
async def test_list_pages(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-crawl-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    p1 = Page(
        id=1,
        crawl_id=crawl_uuid,
        url="https://example.com/",
        canonical_url="https://example.com",
        status_code=200,
        markdown_file_id="md_123",
        markdown_byte_size=3000,
        markdown_token_count=750,
    )
    f1 = AuditFinding(
        id="1:seo.missing_h1",
        rule_id="seo.missing_h1",
        category="seo",
        severity="critical",
        canvas_zone="head",
        explanation="Missing H1",
        evidence={},
        status="open",
    )
    p1.findings = [f1]

    # 1st call count, 2nd call pages
    mock_count_res = MagicMock()
    mock_count_res.scalar_one.return_value = 1

    mock_pages_res = MagicMock()
    mock_pages_res.scalars.return_value.all.return_value = [p1]

    mock_db_session.execute.side_effect = [mock_count_res, mock_pages_res]

    resp = await client.get(f"/api/crawls/{crawl_id}/pages")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["pages"][0]["id"] == 1
    assert data["pages"][0]["finding_counts"]["total"] == 1
    assert data["pages"][0]["finding_counts"]["critical"] == 1


@pytest.mark.asyncio
async def test_get_page_detail(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-crawl-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    p1 = Page(
        id=1,
        crawl_id=crawl_uuid,
        url="https://example.com/",
        canonical_url="https://example.com",
        status_code=200,
        markdown_file_id="md_123",
        markdown_byte_size=3000,
        markdown_token_count=750,
        fetched_at=datetime.datetime.now(datetime.UTC),
    )
    f1 = AuditFinding(
        id="1:seo.missing_h1",
        rule_id="seo.missing_h1",
        crawl_id=crawl_uuid,
        page_id=1,
        url="https://example.com",
        category="seo",
        severity="critical",
        canvas_zone="head",
        explanation="Missing H1",
        evidence={},
        status="open",
        detected_at=datetime.datetime.now(datetime.UTC),
    )
    c1 = PageContact(id=1, page_id=1, kind="email", value="info@example.com")
    p1.findings = [f1]
    p1.contacts = [c1]

    mock_res = MagicMock()
    mock_res.scalar_one_or_none.return_value = p1
    mock_db_session.execute.return_value = mock_res

    resp = await client.get(f"/api/crawls/{crawl_id}/pages/1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["page"]["id"] == 1
    assert data["page"]["contacts"][0]["value"] == "info@example.com"
    assert len(data["findings"]) == 1


@pytest.mark.asyncio
async def test_get_summary(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-crawl-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    mock_crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com/",
        target_domain="example.com",
        status="finished",
    )

    f1 = AuditFinding(
        id="1:seo.missing_h1",
        rule_id="seo.missing_h1",
        crawl_id=crawl_uuid,
        page_id=1,
        url="https://example.com",
        category="seo",
        severity="critical",
        canvas_zone="head",
        explanation="Missing H1",
        evidence={},
        status="open",
        detected_at=datetime.datetime.now(datetime.UTC),
    )

    # side_effect queries:
    # 1. crawl
    res_crawl = MagicMock()
    res_crawl.scalar_one_or_none.return_value = mock_crawl

    # 2. pages_total
    res_ptotal = MagicMock()
    res_ptotal.scalar_one.return_value = 5

    # 3. pages_with_issues
    res_pissues = MagicMock()
    res_pissues.scalar_one.return_value = 3

    # 4. category counts
    res_cat = MagicMock()
    res_cat.all.return_value = [("seo", 4), ("security", 2)]

    # 5. severity counts
    res_sev = MagicMock()
    res_sev.all.return_value = [("critical", 3), ("warning", 3)]

    # 6. top findings
    res_top = MagicMock()
    res_top.scalars.return_value.all.return_value = [f1]

    mock_db_session.execute.side_effect = [
        res_crawl,
        res_ptotal,
        res_pissues,
        res_cat,
        res_sev,
        res_top,
    ]

    resp = await client.get(f"/api/crawls/{crawl_id}/summary")
    assert resp.status_code == 200
    data = resp.json()
    assert data["crawl_id"] == crawl_id
    assert data["pages_total"] == 5
    assert data["pages_with_issues"] == 3
    assert data["finding_counts_by_category"]["seo"] == 4
    assert data["finding_counts_by_category"]["security"] == 2
    assert data["finding_counts_by_severity"]["critical"] == 3
    assert len(data["top_findings"]) == 1


# ---------------------------------------------------------------------------
# Lifecycle Control & Observability Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_crawls(client: AsyncClient, mock_db_session: AsyncMock):
    mock_crawl = Crawl(
        id=uuid.uuid4(),
        target_url="https://example.com/",
        target_domain="example.com",
        status="running",
        pages_processed=0,
        pages_failed=0,
        started_at=datetime.datetime.now(datetime.UTC),
    )
    res_count = MagicMock()
    res_count.scalar_one.return_value = 1
    res_crawls = MagicMock()
    res_crawls.scalars.return_value.all.return_value = [mock_crawl]
    res_fcount = MagicMock()
    res_fcount.scalar_one.return_value = 5

    mock_db_session.execute.side_effect = [res_count, res_crawls, res_fcount]

    resp = await client.get("/api/crawls?limit=10&offset=0")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert len(data["crawls"]) == 1
    assert data["crawls"][0]["target_domain"] == "example.com"



@pytest.mark.asyncio
async def test_cancel_crawl(client: AsyncClient, mock_db_session: AsyncMock, test_redis):
    crawl_id = "test-cancel-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    mock_crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com",
        target_domain="example.com",
        status="running",
    )
    mock_res = MagicMock()
    mock_res.scalar_one_or_none.return_value = mock_crawl
    mock_db_session.execute.return_value = mock_res

    resp = await client.post(f"/api/crawls/{crawl_id}/cancel")
    assert resp.status_code == 200
    data = resp.json()
    assert data["action"] == "cancelled"
    assert data["current_status"] == "cancelled"
    assert mock_crawl.status == "cancelled"


@pytest.mark.asyncio
async def test_pause_and_resume_crawl(client: AsyncClient, mock_db_session: AsyncMock, test_redis):
    crawl_id = "test-pause-resume"
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    mock_crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com",
        target_domain="example.com",
        status="running",
    )
    mock_res = MagicMock()
    mock_res.scalar_one_or_none.return_value = mock_crawl
    mock_db_session.execute.return_value = mock_res

    # 1. Pause
    resp = await client.post(f"/api/crawls/{crawl_id}/pause")
    assert resp.status_code == 200
    assert resp.json()["action"] == "paused"
    assert mock_crawl.status == "paused"

    # 2. Resume
    resp = await client.post(f"/api/crawls/{crawl_id}/resume")
    assert resp.status_code == 200
    assert resp.json()["action"] == "resumed"
    assert mock_crawl.status == "running"


@pytest.mark.asyncio
async def test_get_telemetry(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-telemetry-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    mock_crawl = Crawl(id=crawl_uuid, target_url="https://example.com", status="running")

    res_crawl = MagicMock()
    res_crawl.scalar_one_or_none.return_value = mock_crawl
    res_total = MagicMock()
    res_total.scalar_one.return_value = 10
    res_reasons = MagicMock()
    res_reasons.all.return_value = [("OFF_DOMAIN", 7), ("ALREADY_VISITED", 3)]
    res_events = MagicMock()
    res_events.scalars.return_value.all.return_value = []

    mock_db_session.execute.side_effect = [res_crawl, res_total, res_reasons, res_events]

    resp = await client.get(f"/api/crawls/{crawl_id}/telemetry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_dropped"] == 10
    assert len(data["dropped_reasons"]) == 2


@pytest.mark.asyncio
async def test_get_graph(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-graph-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    mock_crawl = Crawl(id=crawl_uuid, target_url="https://example.com", status="completed")

    p1 = Page(
        id=1,
        crawl_id=crawl_uuid,
        url="https://example.com",
        canonical_url="https://example.com",
        status_code=200,
        metadata_={"seo": {"title": "Home"}, "links": [{"url": "https://example.com/about", "text": "About"}]},
    )
    p2 = Page(
        id=2,
        crawl_id=crawl_uuid,
        url="https://example.com/about",
        canonical_url="https://example.com/about",
        status_code=200,
        metadata_={"seo": {"title": "About"}, "links": []},
    )

    res_crawl = MagicMock()
    res_crawl.scalar_one_or_none.return_value = mock_crawl
    res_pages = MagicMock()
    res_pages.scalars.return_value.all.return_value = [p1, p2]
    res_f1 = MagicMock()
    res_f1.scalars.return_value.all.return_value = []
    res_f2 = MagicMock()
    res_f2.scalars.return_value.all.return_value = []

    mock_db_session.execute.side_effect = [res_crawl, res_pages, res_f1, res_f2]

    resp = await client.get(f"/api/crawls/{crawl_id}/graph")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_nodes"] == 2
    assert data["total_edges"] == 1
    assert data["edges"][0]["source"] == "https://example.com"
    assert data["edges"][0]["target"] == "https://example.com/about"


@pytest.mark.asyncio
async def test_export_report_json_and_markdown(client: AsyncClient, mock_db_session: AsyncMock):
    crawl_id = "test-export-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    mock_crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com",
        target_domain="example.com",
        status="completed",
        pages_processed=2,
        pages_failed=0,
        config={"consolidation": {"health_scorecard": {"h1_coverage_pct": 100, "average_security_score": 95}}},
    )


    res_crawl = MagicMock()
    res_crawl.scalar_one_or_none.return_value = mock_crawl
    res_pages = MagicMock()
    res_pages.scalars.return_value.all.return_value = []
    res_findings = MagicMock()
    res_findings.scalars.return_value.all.return_value = []

    mock_db_session.execute.side_effect = [
        res_crawl, res_pages, res_findings,  # for json
        res_crawl, res_pages, res_findings,  # for markdown
    ]

    # 1. JSON Export
    resp = await client.get(f"/api/crawls/{crawl_id}/export?format=json")
    assert resp.status_code == 200
    assert resp.json()["format"] == "json"

    # 2. Markdown Export
    resp_md = await client.get(f"/api/crawls/{crawl_id}/export?format=markdown")
    assert resp_md.status_code == 200
    assert resp_md.json()["format"] == "markdown"
    assert "# Nexus Audit Report" in resp_md.json()["content"]


@pytest.mark.asyncio
async def test_delete_crawl(client: AsyncClient, mock_db_session: AsyncMock, test_redis):
    crawl_id = "test-delete-01"
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    mock_crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com",
        target_domain="example.com",
        status="finished",
    )
    res_crawl = MagicMock()
    res_crawl.scalar_one_or_none.return_value = mock_crawl
    res_pages = MagicMock()
    res_pages.scalars.return_value.all.return_value = []

    mock_db_session.execute.side_effect = [res_crawl, res_pages]

    resp = await client.delete(f"/api/crawls/{crawl_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["action"] == "deleted"
    mock_db_session.delete.assert_called_once_with(mock_crawl)


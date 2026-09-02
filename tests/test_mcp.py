"""
tests/test_mcp.py
=================
Tests for the MCP adapter tools (Phase 4).
Verifies that all 6 tools correctly invoke the API and return the expected contract shapes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch
import pytest

from app.mcp.server import (
    get_audit_findings,
    get_audit_status,
    get_audit_summary,
    get_finding,
    get_page_audit,
    start_audit,
)


@pytest.mark.asyncio
async def test_mcp_start_audit():
    mock_response = {
        "crawl_id": "test-crawl-123",
        "status": "queued",
        "message": "Crawl enqueued",
        "estimated_scope": {"max_pages": 10, "max_depth": 1},
    }
    with patch("app.mcp.server.api_start_audit", new_callable=AsyncMock) as mock_fn:
        mock_fn.return_value = mock_response
        res = await start_audit("https://example.com", max_pages=10, max_depth=1)
        assert res["crawl_id"] == "test-crawl-123"
        assert res["status"] == "queued"
        mock_fn.assert_called_once_with(
            url="https://example.com",
            max_pages=10,
            max_depth=1,
            worker_count=2,
        )


@pytest.mark.asyncio
async def test_mcp_get_audit_status():
    mock_status = {
        "crawl_id": "test-crawl-123",
        "status": "running",
        "progress": 0.6,
        "pages_processed": 6,
        "findings_count": 4,
        "next_recommended_action": "wait",
    }
    with patch("app.mcp.server.api_get_audit_status", new_callable=AsyncMock) as mock_fn:
        mock_fn.return_value = mock_status
        res = await get_audit_status("test-crawl-123")
        assert res["status"] == "running"
        assert res["progress"] == 0.6
        assert res["next_recommended_action"] == "wait"
        mock_fn.assert_called_once_with(crawl_id="test-crawl-123")


@pytest.mark.asyncio
async def test_mcp_get_audit_summary():
    mock_summary = {
        "crawl_id": "test-crawl-123",
        "target_url": "https://example.com",
        "status": "finished",
        "pages_total": 5,
        "pages_with_issues": 3,
        "finding_counts_by_category": {"seo": 3, "security": 2, "performance": 0, "accessibility": 0},
        "finding_counts_by_severity": {"critical": 2, "warning": 2, "info": 1},
        "top_findings": [{"id": "1:seo.missing_h1", "severity": "critical"}],
    }
    with patch("app.mcp.server.api_get_audit_summary", new_callable=AsyncMock) as mock_fn:
        mock_fn.return_value = mock_summary
        res = await get_audit_summary("test-crawl-123")
        assert res["pages_total"] == 5
        assert res["finding_counts_by_category"]["seo"] == 3
        assert len(res["top_findings"]) == 1
        mock_fn.assert_called_once_with(crawl_id="test-crawl-123")


@pytest.mark.asyncio
async def test_mcp_get_audit_findings():
    mock_findings = {
        "total": 1,
        "page": 1,
        "per_page": 50,
        "total_pages": 1,
        "findings": [
            {
                "id": "1:seo.missing_h1",
                "rule_id": "seo.missing_h1",
                "category": "seo",
                "severity": "critical",
                "canvas_zone": "head",
                "explanation": "No H1 found",
            }
        ],
    }
    with patch("app.mcp.server.api_get_audit_findings", new_callable=AsyncMock) as mock_fn:
        mock_fn.return_value = mock_findings
        res = await get_audit_findings(
            crawl_id="test-crawl-123",
            category="seo",
            severity="critical",
            canvas_zone="head",
            page=1,
            per_page=20,
        )
        assert res["total"] == 1
        assert res["findings"][0]["id"] == "1:seo.missing_h1"
        mock_fn.assert_called_once_with(
            crawl_id="test-crawl-123",
            category="seo",
            severity="critical",
            canvas_zone="head",
            status=None,
            page=1,
            per_page=20,
        )


@pytest.mark.asyncio
async def test_mcp_get_finding():
    mock_finding = {
        "id": "1:seo.missing_h1",
        "rule_id": "seo.missing_h1",
        "explanation": "No H1 found",
        "evidence": {"selector": "body"},
        "remediation": {"proposed": "<h1>Title</h1>", "confidence": 0.9},
        "status": "open",
    }
    with patch("app.mcp.server.api_get_finding", new_callable=AsyncMock) as mock_fn:
        mock_fn.return_value = mock_finding
        res = await get_finding("test-crawl-123", "1:seo.missing_h1")
        assert res["id"] == "1:seo.missing_h1"
        assert res["remediation"]["confidence"] == 0.9
        mock_fn.assert_called_once_with(crawl_id="test-crawl-123", finding_id="1:seo.missing_h1")


@pytest.mark.asyncio
async def test_mcp_get_page_audit():
    mock_page = {
        "page": {
            "id": 1,
            "url": "https://example.com/",
            "status_code": 200,
            "contacts": [{"kind": "email", "value": "info@example.com"}],
        },
        "findings": [{"id": "1:seo.missing_h1"}],
    }
    with patch("app.mcp.server.api_get_page_audit", new_callable=AsyncMock) as mock_fn:
        mock_fn.return_value = mock_page
        res = await get_page_audit("test-crawl-123", 1)
        assert res["page"]["id"] == 1
        assert len(res["findings"]) == 1
        mock_fn.assert_called_once_with(crawl_id="test-crawl-123", page_id=1)

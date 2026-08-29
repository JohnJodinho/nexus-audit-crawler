"""
tests/test_consolidation.py
===========================
Unit tests for the Crawl Consolidation Engine and Lifecycle State Machine (Phase 4).
"""

import uuid
from unittest.mock import AsyncMock, MagicMock
import pytest

from app.models.schema import Crawl
from app.consolidation import consolidate_crawl


@pytest.mark.asyncio
async def test_consolidate_crawl_full_rollup():
    mock_session = AsyncMock()
    crawl_uuid = uuid.uuid4()
    crawl_id = "test-crawl-01"

    # Mock DB query results
    mock_pages_res = MagicMock()
    mock_pages_res.scalar_one.return_value = 2

    mock_failed_res = MagicMock()
    mock_failed_res.scalar_one.return_value = 0

    mock_dropped_res = MagicMock()
    mock_dropped_res.scalar_one.return_value = 1

    mock_contacts_res = MagicMock()
    mock_contacts_res.all.return_value = [
        ("email", "info@example.com", 2),
        ("phone", "+1234567890", 1),
    ]

    mock_pages_data_res = MagicMock()
    meta1 = {
        "seo": {
            "title": "Home",
            "meta_description": "Welcome home",
            "schema_types": ["Organization"],
            "headings": {"h1_count": 1},
            "images": {"total": 4, "missing_alt": 1},
        },
        "security": {
            "security_score": 80,
            "missing_headers": ["Content-Security-Policy"],
        },
    }
    meta2 = {
        "seo": {
            "title": "About",
            "meta_description": "About us",
            "schema_types": ["LegalService"],
            "headings": {"h1_count": 1},
            "images": {"total": 2, "missing_alt": 0},
        },
        "security": {
            "security_score": 100,
            "missing_headers": [],
        },
    }
    mock_pages_data_res.all.return_value = [
        (1, "https://example.com", meta1),
        (2, "https://example.com/about", meta2),
    ]

    mock_links_res = MagicMock()
    mock_links_res.all.return_value = [(1, "https://example.com/#section")]

    mock_crawl_res = MagicMock()
    mock_crawl_obj = Crawl(
        id=crawl_uuid,
        target_url="https://example.com",
        status="running",
        config={"crawl_id": crawl_id},
    )
    mock_crawl_res.scalar_one_or_none.return_value = mock_crawl_obj

    # Configure session.execute to return the mocked queries sequentially
    mock_session.execute.side_effect = [
        mock_pages_res,      # func.count(Page.id)
        mock_failed_res,     # func.count(DeadLetterTask.id)
        mock_dropped_res,    # func.count(DroppedTelemetry.id)
        mock_contacts_res,   # contacts group_by
        mock_pages_data_res, # pages metadata
        mock_links_res,      # page_links anchor links
        mock_crawl_res,      # select Crawl
        MagicMock(),         # update Crawl
    ]

    # Execute consolidation
    report = await consolidate_crawl(mock_session, crawl_uuid, crawl_id)

    assert report["pages_processed"] == 2
    assert report["pages_failed"] == 0
    assert report["telemetry_dropped"] == 1
    assert report["contacts"]["total_unique_emails"] == 1
    assert report["contacts"]["total_unique_phones"] == 1
    assert report["health_scorecard"]["h1_coverage_pct"] == 100
    assert report["health_scorecard"]["meta_description_coverage_pct"] == 100
    assert report["health_scorecard"]["image_alt_coverage_pct"] == 83  # (6-1)/6 * 100
    assert report["health_scorecard"]["average_security_score"] == 90  # (80+100)/2
    assert report["links"]["anchor_links_evaluated"] == 1

    # Verify session commit was called
    mock_session.commit.assert_called_once()

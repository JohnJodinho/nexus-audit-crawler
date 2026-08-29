"""
tests/test_persistence.py
=========================
Unit tests for the Phase 2 persistence consumer, message processors, and schema mappings.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import uuid
import pytest

from app.models.schema import Crawl, Page, PageContact, DroppedTelemetry, DeadLetterTask
from app.persistence_worker import (
    ensure_crawl_record,
    process_result_message,
    process_telemetry_message,
    process_dlq_message,
)


@pytest.mark.asyncio
async def test_ensure_crawl_record_with_mock_session():
    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_result = MagicMock()
    # First call: crawl does not exist, return None
    mock_result.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_result

    test_crawl_id = "test-run-123"
    crawl_uuid = await ensure_crawl_record(mock_session, test_crawl_id, domain="example.com", target_url="https://example.com")

    assert isinstance(crawl_uuid, uuid.UUID)
    mock_session.add.assert_called_once()
    added_obj = mock_session.add.call_args[0][0]
    assert isinstance(added_obj, Crawl)
    assert added_obj.target_domain == "example.com"


@pytest.mark.asyncio
async def test_process_result_message_with_mock_session():
    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_result = MagicMock()
    # Mock crawl lookup
    mock_result.scalar_one_or_none.return_value = Crawl(
        id=uuid.uuid4(),
        target_url="https://example.com",
        target_domain="example.com",
    )
    # Mock page upsert returning page id 42
    mock_result.scalar_one.return_value = 42
    mock_session.execute.return_value = mock_result

    payload = {
        "schema_version": "1",
        "crawl_id": "test-crawl",
        "url": "https://example.com/about/",
        "status_code": "200",
        "raw_markdown": "# About Us\n\nContact: info@example.com, Phone: +1 555 123 4567",
        "contacts": '{"emails": ["info@example.com"], "phones": ["+1 555 123 4567"]}',
        "extraction_methods": '["markitdown"]',
        "domain": "example.com",
    }

    with patch("app.persistence_worker.storage_client.upload_markdown", new_callable=AsyncMock) as mock_upload:
        mock_upload.return_value = "appwrite_md_file_123"

        await process_result_message(mock_session, "test-crawl", "msg-1", payload)

        mock_upload.assert_called_once()
        # Verify page contacts added
        added_objs = [call[0][0] for call in mock_session.add.call_args_list]
        contacts = [o for o in added_objs if isinstance(o, PageContact)]
        assert len(contacts) == 2
        emails = [c.value for c in contacts if c.kind == "email"]
        phones = [c.value for c in contacts if c.kind == "phone"]
        assert "info@example.com" in emails
        assert "+1 555 123 4567" in phones


@pytest.mark.asyncio
async def test_process_telemetry_message_with_mock_session():
    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = Crawl(id=uuid.uuid4(), target_url="https://example.com", target_domain="example.com")
    mock_session.execute.return_value = mock_result

    payload = {
        "source_url": "https://example.com/home",
        "target_url": "https://example.com/login",
        "drop_reason": "DENY_LIST",
    }

    await process_telemetry_message(mock_session, "test-crawl", payload)

    mock_session.add.assert_called_once()
    added_obj = mock_session.add.call_args[0][0]
    assert isinstance(added_obj, DroppedTelemetry)
    assert added_obj.drop_reason == "DENY_LIST"


@pytest.mark.asyncio
async def test_process_dlq_message_with_mock_session():
    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = Crawl(id=uuid.uuid4(), target_url="https://example.com", target_domain="example.com")
    mock_session.execute.return_value = mock_result

    payload = {
        "url": "https://example.com/broken",
        "dlq_reason": "max_retries_exceeded_in_pel",
        "retry_count": "3",
    }

    await process_dlq_message(mock_session, "test-crawl", payload)

    mock_session.add.assert_called_once()
    added_obj = mock_session.add.call_args[0][0]
    assert isinstance(added_obj, DeadLetterTask)
    assert added_obj.dlq_reason == "max_retries_exceeded_in_pel"

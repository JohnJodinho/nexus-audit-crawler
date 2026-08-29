"""
tests/test_storage.py
=====================
Tests for Appwrite Blob Storage client.
"""

from unittest.mock import MagicMock, patch
import pytest

from app.storage.appwrite_client import AppwriteStorageClient


class TestAppwriteStorageClient:
    def test_client_init_defaults(self):
        client = AppwriteStorageClient(
            endpoint="https://test.appwrite.io/v1",
            project_id="test_proj",
            api_key="test_key",
            bucket_id="test_bucket",
        )
        assert client.endpoint == "https://test.appwrite.io/v1"
        assert client.project_id == "test_proj"
        assert client.bucket_id == "test_bucket"

    @pytest.mark.asyncio
    async def test_upload_markdown_skips_when_no_credentials(self):
        client = AppwriteStorageClient(
            endpoint="",
            project_id="",
            api_key="",
            bucket_id="",
        )
        result = await client.upload_markdown("crawl-1", "fingerprint123", "# Hello")
        assert result == ""

    @pytest.mark.asyncio
    async def test_upload_markdown_calls_sync_upload(self):
        client = AppwriteStorageClient(
            endpoint="https://test.appwrite.io/v1",
            project_id="proj",
            api_key="key",
            bucket_id="bucket123",
        )

        with patch.object(client, "_sync_upload", return_value="file_abc_123") as mock_sync:
            file_id = await client.upload_markdown("crawl-1", "0123456789abcdef0123456789abcdef", "# Title\n\nContent")
            assert file_id == "file_abc_123"
            mock_sync.assert_called_once()
            args = mock_sync.call_args[0]
            assert "0123456789abcdef" in args[0]  # file_id contains fingerprint prefix
            assert args[3] == "text/markdown"

    def test_extract_file_id(self):
        # 1. Dictionary with $id
        assert AppwriteStorageClient._extract_file_id({"$id": "file_123"}, "default") == "file_123"

        # 2. Object with .id attribute
        class MockFile:
            id = "file_model_456"

        assert AppwriteStorageClient._extract_file_id(MockFile(), "default") == "file_model_456"

        # 3. Fallback
        assert AppwriteStorageClient._extract_file_id(None, "default") == "default"

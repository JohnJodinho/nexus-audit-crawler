"""
app/storage/appwrite_client.py
==============================
Appwrite Blob Storage client for the Enterprise AI Audit Crawler.

Stores large unstructured artifacts (Markdown documents, screenshots,
raw HTML dumps) in Appwrite Storage buckets, keeping the Postgres database
lean, fast, and free of heavy text/binary bloat.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from app.config import settings

log = logging.getLogger("audit_crawler.storage")


class AppwriteStorageClient:
    """
    Asynchronous-friendly client wrapper for Appwrite Storage.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        project_id: Optional[str] = None,
        api_key: Optional[str] = None,
        bucket_id: Optional[str] = None,
    ) -> None:
        self.endpoint = endpoint if endpoint is not None else settings.APP_WRITE_API_ENDPOINT
        self.project_id = project_id if project_id is not None else settings.APP_WRITE_PROJECT_ID
        self.api_key = api_key if api_key is not None else settings.APP_WRITE_API_KEY
        self.bucket_id = bucket_id if bucket_id is not None else settings.APP_WRITE_BUCKET_ID

        self._client = None
        self._storage = None

    def _get_storage(self):
        """Lazy-initialize the Appwrite Storage service."""
        if self._storage is None:
            from appwrite.client import Client
            from appwrite.services.storage import Storage

            client = Client()
            if self.endpoint:
                client.set_endpoint(self.endpoint)
            if self.project_id:
                client.set_project(self.project_id)
            if self.api_key:
                client.set_key(self.api_key)

            self._client = client
            self._storage = Storage(client)
        return self._storage

    async def upload_markdown(
        self,
        crawl_id: str,
        fingerprint: str,
        markdown_text: str,
    ) -> str:
        """
        Upload raw Markdown text to the Appwrite Storage bucket.

        Parameters
        ----------
        crawl_id:
            Crawl namespace identifier.
        fingerprint:
            64-char SHA-256 fingerprint of the canonical URL.
        markdown_text:
            The raw Markdown text content to persist.

        Returns
        -------
        str
            The Appwrite File ID for the stored object.
        """
        if not self.bucket_id or not self.api_key or not self.project_id:
            log.warning("[STORAGE] Appwrite credentials not configured; skipping blob upload.")
            return ""

        # Construct a safe Appwrite file ID (<= 36 chars, alphanumeric/._-)
        file_id = f"md_{fingerprint[:32]}"
        filename = f"{fingerprint[:16]}.md"
        data_bytes = markdown_text.encode("utf-8")

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._sync_upload,
            file_id,
            filename,
            data_bytes,
            "text/markdown",
        )

    async def upload_screenshot(
        self,
        crawl_id: str,
        fingerprint: str,
        image_bytes: bytes,
    ) -> str:
        """
        Upload screenshot PNG bytes to Appwrite Storage.
        """
        if not self.bucket_id or not self.api_key or not self.project_id:
            return ""

        file_id = f"shot_{fingerprint[:30]}"
        filename = f"{fingerprint[:16]}.png"

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._sync_upload,
            file_id,
            filename,
            image_bytes,
            "image/png",
        )

    async def upload_json_payload(
        self,
        crawl_id: str,
        fingerprint: str,
        suffix: str,
        data: Any,
    ) -> str:
        """
        Upload oversized JSON payload (hydration / XHR dump > 16 KB) to Appwrite Storage.
        """
        if not self.bucket_id or not self.api_key or not self.project_id:
            return ""

        import json

        # e.g., "hyd_" + 30 chars or "xhr_" + 30 chars
        prefix = suffix[:3] + "_"
        file_id = f"{prefix}{fingerprint[:31]}"
        filename = f"{fingerprint[:16]}_{suffix}.json"
        json_bytes = json.dumps(data, default=str).encode("utf-8")

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._sync_upload,
            file_id,
            filename,
            json_bytes,
            "application/json",
        )

    async def upload_bytes(
        self,
        file_id: str,
        filename: str,
        data_bytes: bytes,
        mime_type: str = "application/octet-stream",
    ) -> str:
        """
        Upload arbitrary binary data to Appwrite Storage.
        """
        if not self.bucket_id or not self.api_key or not self.project_id:
            return ""

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._sync_upload,
            file_id,
            filename,
            data_bytes,
            mime_type,
        )

    def _sync_upload(
        self,
        file_id: str,
        filename: str,
        data_bytes: bytes,
        mime_type: str,
    ) -> str:
        """Synchronous worker executed in executor thread."""
        from appwrite.input_file import InputFile
        from appwrite.exception import AppwriteException

        storage = self._get_storage()
        input_file = InputFile.from_bytes(data_bytes, filename=filename, mime_type=mime_type)

        try:
            result = storage.create_file(
                bucket_id=self.bucket_id,
                file_id=file_id,
                file=input_file,
            )
            return self._extract_file_id(result, file_id)
        except AppwriteException as exc:
            # If file already exists (HTTP 409), delete and recreate idempotently
            if exc.code == 409:
                try:
                    storage.delete_file(bucket_id=self.bucket_id, file_id=file_id)
                    result = storage.create_file(
                        bucket_id=self.bucket_id,
                        file_id=file_id,
                        file=input_file,
                    )
                    return self._extract_file_id(result, file_id)
                except Exception as retry_exc:
                    log.error("[STORAGE] Overwrite failed for %s: %s", file_id, retry_exc)
                    raise
            log.error("[STORAGE] Appwrite upload error for %s: %s", file_id, exc)
            raise

    @staticmethod
    def _extract_file_id(result: Any, default_id: str) -> str:
        """Extract file ID safely across dictionary or Pydantic File model responses."""
        if hasattr(result, "id") and getattr(result, "id"):
            return getattr(result, "id")
        if isinstance(result, dict) and "$id" in result:
            return result["$id"]
        if hasattr(result, "get") and callable(result.get):
            try:
                return result.get("$id", default_id)
            except Exception:
                pass
        return default_id


# Global singleton storage client instance
storage_client = AppwriteStorageClient()

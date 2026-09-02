"""
app/mcp/client.py
=================
Async Query API client for MCP tools.
Decoupled HTTP REST client communicating strictly over network endpoints.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional
from httpx import AsyncClient

# Base URL for Nexus Query API (defaults to local standard port 8000)
_BASE_URL: str = os.getenv("NEXUS_API_URL", "http://localhost:8000")


def get_api_client() -> AsyncClient:
    """Return an AsyncClient configured for Nexus Query API over HTTP."""
    return AsyncClient(base_url=_BASE_URL, timeout=30.0)


async def api_start_audit(url: str, max_pages: int = 15, max_depth: int = 2, worker_count: int = 2) -> Dict[str, Any]:
    """Trigger a new crawl audit."""
    async with get_api_client() as client:
        resp = await client.post(
            "/api/crawls",
            json={
                "url": url,
                "config": {
                    "max_pages": max_pages,
                    "max_depth": max_depth,
                    "worker_count": worker_count,
                },
            },
        )
        resp.raise_for_status()
        data = resp.json()
        data["estimated_scope"] = {
            "max_pages": max_pages,
            "max_depth": max_depth,
        }
        return data


async def api_get_audit_status(crawl_id: str) -> Dict[str, Any]:
    """Retrieve runtime progress and next recommended action for an audit."""
    async with get_api_client() as client:
        resp = await client.get(f"/api/crawls/{crawl_id}/status")
        resp.raise_for_status()
        return resp.json()


async def api_get_audit_summary(crawl_id: str) -> Dict[str, Any]:
    """Retrieve aggregated finding metrics rollup and top priority issues."""
    async with get_api_client() as client:
        resp = await client.get(f"/api/crawls/{crawl_id}/summary")
        resp.raise_for_status()
        return resp.json()


async def api_get_audit_findings(
    crawl_id: str,
    category: Optional[str] = None,
    severity: Optional[str] = None,
    canvas_zone: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    per_page: int = 50,
) -> Dict[str, Any]:
    """Retrieve paginated audit findings with optional category/severity/zone filters."""
    params: Dict[str, Any] = {"page": page, "per_page": per_page}
    if category:
        params["category"] = category
    if severity:
        params["severity"] = severity
    if canvas_zone:
        params["canvas_zone"] = canvas_zone
    if status:
        params["status"] = status

    async with get_api_client() as client:
        resp = await client.get(f"/api/crawls/{crawl_id}/findings", params=params)
        resp.raise_for_status()
        return resp.json()


async def api_get_finding(crawl_id: str, finding_id: str) -> Dict[str, Any]:
    """Retrieve a single finding with full evidence selectors and proposed remediation."""
    async with get_api_client() as client:
        resp = await client.get(f"/api/crawls/{crawl_id}/findings/{finding_id}")
        resp.raise_for_status()
        return resp.json()


async def api_get_page_audit(crawl_id: str, page_id: int) -> Dict[str, Any]:
    """Retrieve details and findings for a specific crawled page."""
    async with get_api_client() as client:
        resp = await client.get(f"/api/crawls/{crawl_id}/pages/{page_id}")
        resp.raise_for_status()
        return resp.json()

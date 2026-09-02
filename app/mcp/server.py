"""
app/mcp/server.py
=================
FastMCP Server exposing deterministic Nexus audit tools to ChatGPT and MCP-compatible agents.
"""

from __future__ import annotations

import sys
from typing import Any, Dict, Optional

try:
    from mcp.server.fastmcp import FastMCP
except (ImportError, ModuleNotFoundError):
    try:
        from mcp.server.mcpserver import MCPServer as FastMCP
    except (ImportError, ModuleNotFoundError):
        FastMCP = None

mcp = FastMCP("Nexus Audit Server") if FastMCP else None

from app.mcp.client import (
    api_get_audit_findings,
    api_get_audit_status,
    api_get_audit_summary,
    api_get_finding,
    api_get_page_audit,
    api_start_audit,
)




@mcp.tool()
async def start_audit(url: str, max_pages: int = 15, max_depth: int = 2, worker_count: int = 2) -> Dict[str, Any]:
    """
    Trigger a new deterministic audit crawl for a target URL.

    Parameters:
    - url: The starting seed URL (e.g. 'https://example.com')
    - max_pages: Maximum number of pages to crawl (default: 15)
    - max_depth: Maximum link hop depth (default: 2)
    - worker_count: Number of concurrent workers (default: 2)

    Returns:
    - crawl_id: The unique crawl identifier
    - status: 'queued'
    - estimated_scope: Scope configuration
    """
    return await api_start_audit(
        url=url,
        max_pages=max_pages,
        max_depth=max_depth,
        worker_count=worker_count,
    )


@mcp.tool()
async def get_audit_status(crawl_id: str) -> Dict[str, Any]:
    """
    Check runtime progress of an audit crawl and get the recommended agent action.

    Parameters:
    - crawl_id: The crawl ID returned by start_audit

    Returns:
    - status: 'queued' | 'running' | 'draining' | 'consolidating' | 'finished' | 'failed'
    - progress: Float between 0.0 and 1.0
    - pages_processed: Number of pages crawled and audited so far
    - findings_count: Number of issues detected
    - next_recommended_action: 'wait' (keep polling) | 'retrieve' (audit complete, ready to inspect) | 'none'
    """
    return await api_get_audit_status(crawl_id=crawl_id)


@mcp.tool()
async def get_audit_summary(crawl_id: str) -> Dict[str, Any]:
    """
    Retrieve structured rollup summary of an audit crawl:
    Total pages with issues, category breakdowns (SEO, security, performance),
    severity breakdown (critical, warning, info), and top 5 priority findings.

    Parameters:
    - crawl_id: The crawl ID to inspect
    """
    return await api_get_audit_summary(crawl_id=crawl_id)


@mcp.tool()
async def get_audit_findings(
    crawl_id: str,
    category: Optional[str] = None,
    severity: Optional[str] = None,
    canvas_zone: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    per_page: int = 50,
) -> Dict[str, Any]:
    """
    Retrieve paginated audit findings with optional category, severity, canvas zone, or status filters.

    Parameters:
    - crawl_id: The crawl ID
    - category: Optional filter ('seo' | 'security' | 'performance' | 'accessibility')
    - severity: Optional filter ('critical' | 'warning' | 'info')
    - canvas_zone: Optional filter ('head' | 'content' | 'nav' | 'footer' | 'server')
    - status: Optional filter ('open' | 'approved' | 'rejected' | 'pending_review')
    - page: Page number (1-indexed, default: 1)
    - per_page: Findings per page (default: 50, max: 200)
    """
    return await api_get_audit_findings(
        crawl_id=crawl_id,
        category=category,
        severity=severity,
        canvas_zone=canvas_zone,
        status=status,
        page=page,
        per_page=per_page,
    )


@mcp.tool()
async def get_finding(crawl_id: str, finding_id: str) -> Dict[str, Any]:
    """
    Retrieve complete details for a single finding, including DOM selector,
    observed vs expected evidence, and proposed remediation diff.

    Parameters:
    - crawl_id: The crawl ID
    - finding_id: The unique finding identifier (e.g. '456:seo.missing_h1')
    """
    return await api_get_finding(crawl_id=crawl_id, finding_id=finding_id)


@mcp.tool()
async def get_page_audit(crawl_id: str, page_id: int) -> Dict[str, Any]:
    """
    Retrieve full audit results for a specific page, including all page-level findings,
    extracted contacts (emails, phone numbers), and Appwrite blob storage pointers.

    Parameters:
    - crawl_id: The crawl ID
    - page_id: The integer page ID
    """
    return await api_get_page_audit(crawl_id=crawl_id, page_id=page_id)


def main():
    """CLI entry point for running the MCP server over stdio."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    mcp.run()


if __name__ == "__main__":
    main()

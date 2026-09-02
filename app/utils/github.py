"""
app/utils/github.py
===================
Automated GitHub Actions Workflow Dispatcher (Phase 8).
Triggers ephemeral Scrapling crawler matrix runs on GitHub Actions runners via the GitHub REST API.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
import httpx

from app.config import settings

log = logging.getLogger("audit_crawler.github_dispatch")


async def dispatch_github_crawler(
    crawl_id: str,
    seed_url: str,
    max_pages: int = 15,
    max_depth: int = 2,
    worker_count: int = 2,
) -> bool:
    """
    Dispatch an ephemeral crawl run to GitHub Actions.

    Returns:
        bool: True if GitHub accepted the dispatch request (204 No Content), False otherwise.
    """
    token = settings.GITHUB_TOKEN.strip()
    repo = settings.GITHUB_REPO.strip()
    workflow = settings.GITHUB_WORKFLOW_FILE.strip()

    if not token:
        log.info("[GH_DISPATCH] GITHUB_TOKEN not configured; skipping automatic cloud runner dispatch.")
        return False

    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    payload: Dict[str, Any] = {
        "ref": "main",
        "inputs": {
            "crawl_id": crawl_id,
            "seed_url": seed_url,
            "max_pages": str(max_pages),
            "max_depth": str(max_depth),
            "worker_count": str(min(worker_count, 4)),
        },
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
            if resp.status_code == 204:
                log.info(
                    "[GH_DISPATCH] Successfully triggered GitHub Actions crawler workflow for crawl %s (workers=%d).",
                    crawl_id,
                    min(worker_count, 4),
                )
                return True
            else:
                log.error(
                    "[GH_DISPATCH] GitHub dispatch failed with status %d: %s",
                    resp.status_code,
                    resp.text,
                )
                return False
    except Exception as exc:
        log.error("[GH_DISPATCH] Error dispatching to GitHub Actions: %s", exc, exc_info=True)
        return False

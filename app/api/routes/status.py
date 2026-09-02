"""
app/api/routes/status.py
========================
Endpoint for polling crawl progress and determining next agent action.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, resolve_crawl_uuid
from app.api.schemas import CrawlStatusResponse
from app.models.schema import AuditFinding, Crawl

router = APIRouter()


@router.get("/{crawl_id}/status", response_model=CrawlStatusResponse)
async def get_crawl_status(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get runtime progress of an audit crawl with next recommended action for agents.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    # Fetch Crawl record
    crawl_stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    res = await db.execute(crawl_stmt)
    crawl = res.scalar_one_or_none()

    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    # Count audit findings
    f_stmt = select(func.count(AuditFinding.id)).where(AuditFinding.crawl_id == crawl_uuid)
    f_res = await db.execute(f_stmt)
    findings_count = f_res.scalar_one() or 0

    # Calculate progress and next action
    config = crawl.config or {}
    max_pages = config.get("max_pages", 15) or 15
    processed = crawl.pages_processed or 0

    if crawl.status == "finished":
        progress = 1.0
        next_action = "retrieve"
    elif crawl.status == "failed":
        progress = 1.0 if processed > 0 else 0.0
        next_action = "none"
    elif crawl.status in ("running", "queued", "draining", "consolidating"):
        # Rough progress estimate
        progress = min(0.95, round(processed / max_pages, 2)) if max_pages > 0 else 0.5
        next_action = "wait"
    else:
        progress = 0.0
        next_action = "none"

    return CrawlStatusResponse(
        crawl_id=crawl_id,
        status=crawl.status,
        pages_discovered=crawl.pages_discovered or 0,
        pages_processed=processed,
        pages_failed=crawl.pages_failed or 0,
        findings_count=findings_count,
        progress=progress,
        next_recommended_action=next_action,
    )

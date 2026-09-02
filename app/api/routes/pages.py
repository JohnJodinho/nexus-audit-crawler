"""
app/api/routes/pages.py
=======================
Endpoints for listing crawled pages and retrieving per-page audit findings.
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.deps import get_db, resolve_crawl_uuid
from app.api.schemas import (
    AuditFindingResponse,
    PageDetailResponse,
    PageSummaryItem,
    PaginatedPagesResponse,
)
from app.models.schema import AuditFinding, Page

router = APIRouter()


@router.get("/{crawl_id}/pages", response_model=PaginatedPagesResponse)
async def list_pages(
    crawl_id: str,
    has_findings: Optional[bool] = Query(None, description="Filter pages that have at least one audit finding"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    per_page: int = Query(50, ge=1, le=200, description="Items per page"),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieve paginated crawled pages with finding counts per severity tier.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    # Base query for pages
    stmt = select(Page).where(Page.crawl_id == crawl_uuid)

    if has_findings is True:
        stmt = stmt.where(Page.findings.any())
    elif has_findings is False:
        stmt = stmt.where(~Page.findings.any())

    # Count total
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_res = await db.execute(count_stmt)
    total = total_res.scalar_one() or 0

    # Paginate pages
    offset = (page - 1) * per_page
    paginated_stmt = (
        stmt.options(selectinload(Page.findings))
        .order_by(Page.id.asc())
        .offset(offset)
        .limit(per_page)
    )
    pages_res = await db.execute(paginated_stmt)
    pages = pages_res.scalars().all()

    page_items: List[PageSummaryItem] = []
    for p in pages:
        findings = p.findings or []
        counts: Dict[str, int] = {
            "total": len(findings),
            "critical": sum(1 for f in findings if f.severity == "critical"),
            "warning": sum(1 for f in findings if f.severity == "warning"),
            "info": sum(1 for f in findings if f.severity == "info"),
        }
        sample = [
            {
                "id": f.id,
                "rule_id": f.rule_id,
                "category": f.category,
                "severity": f.severity,
                "canvas_zone": f.canvas_zone,
                "explanation": f.explanation,
            }
            for f in findings[:5]
        ]
        page_items.append(
            PageSummaryItem(
                id=p.id,
                url=p.url,
                canonical_url=p.canonical_url,
                status_code=p.status_code,
                markdown_file_id=p.markdown_file_id,
                markdown_byte_size=p.markdown_byte_size,
                markdown_token_count=p.markdown_token_count,
                finding_counts=counts,
                findings_sample=sample,
            )
        )

    total_pages = math.ceil(total / per_page) if total > 0 else 1

    return PaginatedPagesResponse(
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        pages=page_items,
    )


@router.get("/{crawl_id}/pages/{page_id}", response_model=PageDetailResponse)
async def get_page_detail(
    crawl_id: str,
    page_id: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieve comprehensive details for a single page including all findings and contacts.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    stmt = (
        select(Page)
        .where(Page.crawl_id == crawl_uuid, Page.id == page_id)
        .options(selectinload(Page.findings), selectinload(Page.contacts))
    )
    res = await db.execute(stmt)
    page = res.scalar_one_or_none()

    if not page:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Page {page_id} not found for crawl '{crawl_id}'.",
        )

    page_dict = {
        "id": page.id,
        "crawl_id": str(page.crawl_id),
        "url": page.url,
        "canonical_url": page.canonical_url,
        "path": page.path,
        "status_code": page.status_code,
        "extraction_methods": page.extraction_methods or [],
        "markdown_file_id": page.markdown_file_id,
        "markdown_byte_size": page.markdown_byte_size,
        "markdown_token_count": page.markdown_token_count,
        "summary": page.summary,
        "screenshot_file_id": page.screenshot_file_id,
        "fetched_at": page.fetched_at.isoformat() if page.fetched_at else None,
        "contacts": [
            {"kind": c.kind, "value": c.value} for c in (page.contacts or [])
        ],
        "metadata": page.metadata_ or {},
    }

    findings = [
        AuditFindingResponse(
            id=f.id,
            rule_id=f.rule_id,
            crawl_id=str(f.crawl_id),
            page_id=f.page_id,
            url=f.url,
            category=f.category,
            severity=f.severity,
            canvas_zone=f.canvas_zone,
            explanation=f.explanation,
            evidence=f.evidence or {},
            remediation=f.remediation,
            status=f.status,
            detected_at=f.detected_at,
        )
        for f in (page.findings or [])
    ]

    return PageDetailResponse(
        page=page_dict,
        findings=findings,
    )

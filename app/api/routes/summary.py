"""
app/api/routes/summary.py
=========================
Endpoint for aggregated audit metrics and top findings summary.
"""

from __future__ import annotations

from typing import Dict
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, resolve_crawl_uuid
from app.api.schemas import AuditFindingResponse, CrawlSummaryResponse
from app.models.schema import AuditFinding, Crawl, Page

router = APIRouter()


@router.get("/{crawl_id}/summary", response_model=CrawlSummaryResponse)
async def get_crawl_summary(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieve structured rollup summary of an audit crawl:
    aggregated category/severity breakdowns, pages affected, and top priority findings.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    # 1. Fetch Crawl
    c_stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    c_res = await db.execute(c_stmt)
    crawl = c_res.scalar_one_or_none()

    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    # 2. Count total pages
    p_total_stmt = select(func.count(Page.id)).where(Page.crawl_id == crawl_uuid)
    p_total_res = await db.execute(p_total_stmt)
    pages_total = p_total_res.scalar_one() or 0

    # 3. Count pages with at least 1 finding
    p_issues_stmt = select(func.count(func.distinct(AuditFinding.page_id))).where(
        AuditFinding.crawl_id == crawl_uuid
    )
    p_issues_res = await db.execute(p_issues_stmt)
    pages_with_issues = p_issues_res.scalar_one() or 0

    # 4. Count findings by category
    cat_stmt = (
        select(AuditFinding.category, func.count(AuditFinding.id))
        .where(AuditFinding.crawl_id == crawl_uuid)
        .group_by(AuditFinding.category)
    )
    cat_res = await db.execute(cat_stmt)
    by_category: Dict[str, int] = {cat: count for cat, count in cat_res.all()}

    # Ensure standard categories exist
    for c in ("seo", "security", "performance", "accessibility"):
        by_category.setdefault(c, 0)

    # 5. Count findings by severity
    sev_stmt = (
        select(AuditFinding.severity, func.count(AuditFinding.id))
        .where(AuditFinding.crawl_id == crawl_uuid)
        .group_by(AuditFinding.severity)
    )
    sev_res = await db.execute(sev_stmt)
    by_severity: Dict[str, int] = {sev: count for sev, count in sev_res.all()}

    for s in ("critical", "warning", "info"):
        by_severity.setdefault(s, 0)

    # 6. Fetch top 5 findings (prioritizing critical severity)
    top_stmt = (
        select(AuditFinding)
        .where(AuditFinding.crawl_id == crawl_uuid)
        .order_by(
            # Ordering: critical -> warning -> info
            case(
                (AuditFinding.severity == "critical", 1),
                (AuditFinding.severity == "warning", 2),
                (AuditFinding.severity == "info", 3),
                else_=4,
            ),
            AuditFinding.detected_at.asc(),
        )
        .limit(5)
    )
    top_res = await db.execute(top_stmt)
    top_findings = top_res.scalars().all()

    return CrawlSummaryResponse(
        crawl_id=crawl_id,
        target_url=crawl.target_url,
        status=crawl.status,
        pages_total=pages_total,
        pages_with_issues=pages_with_issues,
        finding_counts_by_category=by_category,
        finding_counts_by_severity=by_severity,
        top_findings=[
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
            for f in top_findings
        ],
    )

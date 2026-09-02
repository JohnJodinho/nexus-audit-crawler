"""
app/api/routes/findings.py
==========================
Endpoints for listing, filtering, viewing, and updating audit findings.
"""

from __future__ import annotations

import math
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, resolve_crawl_uuid
from app.api.schemas import (
    AuditFindingResponse,
    FindingUpdateRequest,
    PaginatedFindingsResponse,
)
from app.models.schema import AuditFinding

router = APIRouter()


@router.get("/{crawl_id}/findings", response_model=PaginatedFindingsResponse)
async def list_findings(
    crawl_id: str,
    category: Optional[str] = Query(None, description="Filter by category (seo, security, performance, accessibility)"),
    severity: Optional[str] = Query(None, description="Filter by severity (critical, warning, info)"),
    canvas_zone: Optional[str] = Query(None, description="Filter by canvas zone (nav, head, content, footer, server)"),
    finding_status: Optional[str] = Query(None, alias="status", description="Filter by finding status (open, approved, rejected, pending_review)"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    per_page: int = Query(50, ge=1, le=200, description="Items per page"),
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieve paginated audit findings for a specific crawl with optional classification filters.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    # Base query
    stmt = select(AuditFinding).where(AuditFinding.crawl_id == crawl_uuid)

    if category:
        stmt = stmt.where(AuditFinding.category == category.lower().strip())
    if severity:
        stmt = stmt.where(AuditFinding.severity == severity.lower().strip())
    if canvas_zone:
        stmt = stmt.where(AuditFinding.canvas_zone == canvas_zone.lower().strip())
    if finding_status:
        stmt = stmt.where(AuditFinding.status == finding_status.lower().strip())

    # Count total
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total_res = await db.execute(count_stmt)
    total = total_res.scalar_one() or 0

    # Paginate
    offset = (page - 1) * per_page
    paginated_stmt = stmt.order_by(AuditFinding.detected_at.asc(), AuditFinding.id.asc()).offset(offset).limit(per_page)
    findings_res = await db.execute(paginated_stmt)
    findings = findings_res.scalars().all()

    total_pages = math.ceil(total / per_page) if total > 0 else 1

    return PaginatedFindingsResponse(
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        findings=[
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
            for f in findings
        ],
    )


@router.get("/{crawl_id}/findings/{finding_id}", response_model=AuditFindingResponse)
async def get_finding(
    crawl_id: str,
    finding_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Retrieve full details of a specific finding by ID."""
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    stmt = select(AuditFinding).where(
        AuditFinding.crawl_id == crawl_uuid,
        AuditFinding.id == finding_id,
    )
    res = await db.execute(stmt)
    f = res.scalar_one_or_none()

    if not f:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Finding '{finding_id}' not found for crawl '{crawl_id}'.",
        )

    return AuditFindingResponse(
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


@router.patch("/{crawl_id}/findings/{finding_id}", response_model=AuditFindingResponse)
async def update_finding_status(
    crawl_id: str,
    finding_id: str,
    payload: FindingUpdateRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Update finding review status (Phase 5 decision write path: open, approved, rejected, pending_review).
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    update_stmt = (
        update(AuditFinding)
        .where(AuditFinding.crawl_id == crawl_uuid, AuditFinding.id == finding_id)
        .values(status=payload.status)
        .returning(AuditFinding)
    )
    res = await db.execute(update_stmt)
    await db.commit()
    f = res.scalar_one_or_none()

    if not f:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Finding '{finding_id}' not found for crawl '{crawl_id}'.",
        )

    return AuditFindingResponse(
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

"""
app/api/routes/telemetry.py
===========================
Observability endpoints for inspecting dropped links and dead letter queue tasks.
"""

from __future__ import annotations

import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, resolve_crawl_uuid
from app.api.schemas import (
    CrawlDLQResponse,
    CrawlTelemetryResponse,
    DLQItem,
    TelemetryReasonCount,
)
from app.models.schema import Crawl, DeadLetterTask, DroppedTelemetry

log = logging.getLogger("audit_crawler.api.telemetry")

router = APIRouter()


@router.get("/{crawl_id}/telemetry", response_model=CrawlTelemetryResponse)
async def get_crawl_telemetry(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Retrieve dropped link telemetry metrics and reason breakdowns."""
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    # Verify crawl exists
    crawl_stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    crawl_res = await db.execute(crawl_stmt)
    if not crawl_res.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    # 1. Total dropped links
    total_stmt = select(func.count(DroppedTelemetry.id)).where(DroppedTelemetry.crawl_id == crawl_uuid)
    total_res = await db.execute(total_stmt)
    total_dropped = total_res.scalar_one()

    # 2. Breakdown by drop reason
    reason_stmt = (
        select(DroppedTelemetry.drop_reason, func.count(DroppedTelemetry.id))
        .where(DroppedTelemetry.crawl_id == crawl_uuid)
        .group_by(DroppedTelemetry.drop_reason)
    )
    reason_res = await db.execute(reason_stmt)
    reasons = [
        TelemetryReasonCount(reason=row[0], count=row[1])
        for row in reason_res.all()
    ]

    # 3. Recent 50 dropped events
    events_stmt = (
        select(DroppedTelemetry)
        .where(DroppedTelemetry.crawl_id == crawl_uuid)
        .order_by(desc(DroppedTelemetry.timestamp_utc))
        .limit(50)
    )
    events_res = await db.execute(events_stmt)
    recent_events = [
        {
            "id": e.id,
            "source_url": e.source_url,
            "target_url": e.target_url,
            "drop_reason": e.drop_reason,
            "timestamp": e.timestamp_utc.isoformat() if e.timestamp_utc else None,
        }
        for e in events_res.scalars().all()
    ]

    return CrawlTelemetryResponse(
        crawl_id=crawl_id,
        total_dropped=total_dropped,
        dropped_reasons=reasons,
        recent_events=recent_events,
    )


@router.get("/{crawl_id}/dlq", response_model=CrawlDLQResponse)
async def get_crawl_dlq(
    crawl_id: str,
    limit: int = Query(50, ge=1, le=200, description="Items limit"),
    db: AsyncSession = Depends(get_db),
):
    """Retrieve dead letter queue items (failed tasks that exhausted retries)."""
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    # Verify crawl exists
    crawl_stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    crawl_res = await db.execute(crawl_stmt)
    if not crawl_res.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    # Total DLQ count
    count_stmt = select(func.count(DeadLetterTask.id)).where(DeadLetterTask.crawl_id == crawl_uuid)
    count_res = await db.execute(count_stmt)
    total_dlq = count_res.scalar_one()

    # DLQ items
    items_stmt = (
        select(DeadLetterTask)
        .where(DeadLetterTask.crawl_id == crawl_uuid)
        .order_by(desc(DeadLetterTask.failed_at_utc))
        .limit(limit)
    )
    items_res = await db.execute(items_stmt)
    dlq_rows = items_res.scalars().all()

    items = [
        DLQItem(
            id=row.id,
            url=row.url,
            dlq_reason=row.dlq_reason,
            failed_at_utc=row.failed_at_utc,
            task_fields=row.task_fields or {},
        )
        for row in dlq_rows
    ]

    return CrawlDLQResponse(
        crawl_id=crawl_id,
        total_failed_tasks=total_dlq,
        items=items,
    )

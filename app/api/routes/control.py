"""
app/api/routes/control.py
=========================
Lifecycle state machine controllers: Cancel, Pause, Resume, Consolidate, and DLQ Replay.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any, Dict

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_redis, resolve_crawl_uuid
from app.api.schemas import CrawlControlResponse
from app.consolidation import consolidate_crawl
from app.models.schema import Crawl, DeadLetterTask
from app.redis_client import (
    dlq_key,
    tasks_key,
)
from app.utils.utilities import canonicalize_url, get_fingerprint

log = logging.getLogger("audit_crawler.api.control")

router = APIRouter()


@router.post("/{crawl_id}/cancel", response_model=CrawlControlResponse)
async def cancel_crawl(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    Cancel an in-flight crawl job.
    Sets Redis cancellation flag and transitions database state to 'cancelled'.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    res = await db.execute(stmt)
    crawl = res.scalar_one_or_none()

    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    prev_status = crawl.status
    if prev_status in ("completed", "cancelled", "finished"):
        return CrawlControlResponse(
            crawl_id=crawl_id,
            action="cancelled",
            previous_status=prev_status,
            current_status=prev_status,
            message=f"Crawl '{crawl_id}' is already in terminal state '{prev_status}'.",
        )

    # 1. Set Redis cancel flag
    await redis.set(f"crawl:{crawl_id}:control:cancelled", "1", ex=86400)

    # 2. Release seed lock
    seed_fp = get_fingerprint(canonicalize_url(crawl.target_url))
    await redis.delete(f"lock:crawl:seed:{seed_fp}")

    # 3. Update DB status
    crawl.status = "cancelled"
    crawl.finished_at = datetime.datetime.now(datetime.UTC)
    await db.commit()

    log.info("[LIFECYCLE] Cancelled crawl %s (was: %s)", crawl_id, prev_status)

    return CrawlControlResponse(
        crawl_id=crawl_id,
        action="cancelled",
        previous_status=prev_status,
        current_status="cancelled",
        message=f"Crawl '{crawl_id}' was cancelled successfully. Workers will abort remaining tasks.",
    )


@router.post("/{crawl_id}/pause", response_model=CrawlControlResponse)
async def pause_crawl(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    Pause an active crawl job.
    Workers will pause claiming tasks from Redis stream without losing state.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    res = await db.execute(stmt)
    crawl = res.scalar_one_or_none()

    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    prev_status = crawl.status
    if prev_status not in ("running", "queued"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot pause crawl in '{prev_status}' state (must be 'queued' or 'running').",
        )

    # 1. Set Redis pause flag
    await redis.set(f"crawl:{crawl_id}:control:paused", "1", ex=86400)

    # 2. Update DB status
    crawl.status = "paused"
    await db.commit()

    log.info("[LIFECYCLE] Paused crawl %s", crawl_id)

    return CrawlControlResponse(
        crawl_id=crawl_id,
        action="paused",
        previous_status=prev_status,
        current_status="paused",
        message=f"Crawl '{crawl_id}' paused successfully. Workers will hold current state.",
    )


@router.post("/{crawl_id}/resume", response_model=CrawlControlResponse)
async def resume_crawl(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    Resume a paused crawl job.
    Clears the pause flag in Redis and restores active crawling.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    res = await db.execute(stmt)
    crawl = res.scalar_one_or_none()

    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    prev_status = crawl.status
    if prev_status != "paused":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot resume crawl in '{prev_status}' state (must be 'paused').",
        )

    # 1. Clear Redis pause flag
    await redis.delete(f"crawl:{crawl_id}:control:paused")

    # 2. Update DB status
    crawl.status = "running"
    await db.commit()

    log.info("[LIFECYCLE] Resumed crawl %s", crawl_id)

    return CrawlControlResponse(
        crawl_id=crawl_id,
        action="resumed",
        previous_status=prev_status,
        current_status="running",
        message=f"Crawl '{crawl_id}' resumed successfully.",
    )


@router.post("/{crawl_id}/consolidate", response_model=CrawlControlResponse)
async def trigger_consolidation(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    Explicitly trigger post-crawl consolidation and health scorecard rollup.
    Computes site scorecard metrics and transitions status to 'completed'.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    res = await db.execute(stmt)
    crawl = res.scalar_one_or_none()

    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    prev_status = crawl.status

    # Run consolidation engine
    report = await consolidate_crawl(db, crawl_uuid, crawl_id)

    # Release seed lock
    seed_fp = get_fingerprint(canonicalize_url(crawl.target_url))
    await redis.delete(f"lock:crawl:seed:{seed_fp}")

    return CrawlControlResponse(
        crawl_id=crawl_id,
        action="consolidated",
        previous_status=prev_status,
        current_status="completed",
        message=f"Crawl '{crawl_id}' consolidated successfully.",
        details=report,
    )


@router.post("/{crawl_id}/retry-failed", response_model=CrawlControlResponse)
async def retry_failed_tasks(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    Replay failed tasks from the Dead Letter Queue (DLQ) back into the active task stream.
    Resets retry counters to 0.
    """
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    res = await db.execute(stmt)
    crawl = res.scalar_one_or_none()

    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    # 1. Fetch all DLQ tasks for this crawl from Postgres
    dlq_stmt = select(DeadLetterTask).where(DeadLetterTask.crawl_id == crawl_uuid)
    dlq_res = await db.execute(dlq_stmt)
    failed_tasks = dlq_res.scalars().all()

    if not failed_tasks:
        return CrawlControlResponse(
            crawl_id=crawl_id,
            action="retried_failed",
            previous_status=crawl.status,
            current_status=crawl.status,
            message=f"No failed tasks found in DLQ for crawl '{crawl_id}'.",
            details={"requeued_count": 0},
        )

    _tasks_stream = tasks_key(crawl_id)
    requeued = 0

    for ft in failed_tasks:
        task_data = dict(ft.task_fields or {})
        task_data["retry_count"] = "0"
        task_data["url"] = ft.url
        task_data["domain"] = crawl.target_domain

        # Re-publish to active task stream
        await redis.xadd(_tasks_stream, task_data)
        requeued += 1
        await db.delete(ft)

    # Adjust crawl counters and status
    crawl.pages_failed = max(0, crawl.pages_failed - requeued)
    if crawl.status in ("completed", "cancelled"):
        crawl.status = "running"

    await db.commit()

    log.info("[LIFECYCLE] Requeued %d DLQ tasks for crawl %s", requeued, crawl_id)

    return CrawlControlResponse(
        crawl_id=crawl_id,
        action="retried_failed",
        previous_status=crawl.status,
        current_status=crawl.status,
        message=f"Successfully re-enqueued {requeued} failed tasks from DLQ back into the task stream.",
        details={"requeued_count": requeued},
    )

"""
app/api/routes/crawls.py
========================
Endpoints for creating and inspecting crawl jobs.
"""

from __future__ import annotations

import datetime
import uuid
from urllib.parse import urlparse

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_redis, resolve_crawl_uuid
from app.api.schemas import (
    CrawlCreateRequest,
    CrawlCreateResponse,
    CrawlResponse,
)
from app.models.schema import Crawl
from app.orchestrator import publish_seed_url
from app.redis_client import ensure_consumer_group, ensure_persist_consumer_groups

router = APIRouter()


@router.post("", response_model=CrawlCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_crawl(
    payload: CrawlCreateRequest,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    Trigger a new deterministic audit crawl.
    Creates the database record and publishes the seed URL into the Redis task stream.
    """
    raw_url = str(payload.url).strip()
    if not raw_url.startswith(("http://", "https://")):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="URL must start with http:// or https://",
        )

    domain = urlparse(raw_url).netloc.lower()
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Could not determine domain from URL.",
        )

    crawl_id_str = payload.crawl_id.strip() if payload.crawl_id else f"crawl-{uuid.uuid4().hex[:12]}"
    crawl_uuid = resolve_crawl_uuid(crawl_id_str)

    cfg = payload.config.model_dump() if payload.config else {}
    cfg["crawl_id"] = crawl_id_str

    # 1. Ensure consumer groups exist in Redis
    await ensure_consumer_group(redis, crawl_id_str)
    await ensure_persist_consumer_groups(redis, crawl_id_str)

    # 2. Persist Crawl row in Postgres
    crawl = Crawl(
        id=crawl_uuid,
        target_url=raw_url,
        target_domain=domain,
        status="queued",
        started_at=datetime.datetime.now(datetime.UTC),
        worker_count=cfg.get("worker_count", 2),
        config=cfg,
    )
    db.add(crawl)
    await db.commit()

    # 3. Publish seed URL to Redis
    await publish_seed_url(
        redis=redis,
        url=raw_url,
        domain=domain,
        crawl_id=crawl_id_str,
        depth=0,
    )

    return CrawlCreateResponse(
        crawl_id=crawl_id_str,
        status="queued",
        message=f"Crawl {crawl_id_str} enqueued successfully for {raw_url}",
    )


@router.get("/{crawl_id}", response_model=CrawlResponse)
async def get_crawl(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Retrieve top-level details of a specific crawl job."""
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    res = await db.execute(stmt)
    crawl = res.scalar_one_or_none()

    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    return CrawlResponse(
        id=str(crawl.id),
        target_url=crawl.target_url,
        target_domain=crawl.target_domain,
        status=crawl.status,
        started_at=crawl.started_at,
        finished_at=crawl.finished_at,
        worker_count=crawl.worker_count,
        pages_discovered=crawl.pages_discovered,
        pages_processed=crawl.pages_processed,
        pages_failed=crawl.pages_failed,
        config=crawl.config or {},
        error=crawl.error,
    )

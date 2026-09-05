"""
app/api/routes/crawls.py
========================
Endpoints for creating, listing, inspecting, and deleting crawl jobs.
Includes atomic distributed deduplication locks to prevent duplicate in-flight crawls.
"""

from __future__ import annotations

import datetime
import logging
import uuid
from typing import Optional
from urllib.parse import urlparse

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_redis, resolve_crawl_uuid
from app.api.schemas import (
    CrawlControlResponse,
    CrawlCreateRequest,
    CrawlCreateResponse,
    CrawlListItem,
    CrawlListResponse,
    CrawlResponse,
)
from app.models.schema import (
    AuditFinding,
    Crawl,
    DeadLetterTask,
    DroppedTelemetry,
    Page,
    PageContact,
)
from app.orchestrator import publish_seed_url
from app.redis_client import (
    ensure_consumer_group,
    ensure_persist_consumer_groups,
)
from app.utils.flush_state import flush_crawl
from app.utils.github import dispatch_github_crawler
from app.utils.utilities import canonicalize_url, get_fingerprint



log = logging.getLogger("audit_crawler.api.crawls")

router = APIRouter()


@router.get("", response_model=CrawlListResponse)
async def list_crawls(
    domain: Optional[str] = Query(None, description="Filter by target domain"),
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by crawl status"),
    limit: int = Query(20, ge=1, le=100, description="Page limit"),
    offset: int = Query(0, ge=0, description="Page offset"),
    db: AsyncSession = Depends(get_db),
):
    """List historical crawls with filtering, sorting, and pagination."""
    query = select(Crawl)
    count_query = select(func.count(Crawl.id))

    if domain:
        clean_domain = domain.strip().lower()
        query = query.where(Crawl.target_domain.ilike(f"%{clean_domain}%"))
        count_query = count_query.where(Crawl.target_domain.ilike(f"%{clean_domain}%"))

    if status_filter:
        clean_status = status_filter.strip().lower()
        query = query.where(Crawl.status == clean_status)
        count_query = count_query.where(Crawl.status == clean_status)

    total_res = await db.execute(count_query)
    total = total_res.scalar_one()

    query = query.order_by(desc(Crawl.started_at)).limit(limit).offset(offset)
    res = await db.execute(query)
    crawls = res.scalars().all()

    items = []
    for c in crawls:
        # Findings count rollup
        f_count_stmt = select(func.count(AuditFinding.id)).where(AuditFinding.crawl_id == c.id)
        f_count_res = await db.execute(f_count_stmt)
        findings_count = f_count_res.scalar_one()

        items.append(
            CrawlListItem(
                id=str(c.id),
                target_url=c.target_url,
                target_domain=c.target_domain,
                status=c.status,
                started_at=c.started_at,
                finished_at=c.finished_at,
                pages_processed=c.pages_processed,
                pages_failed=c.pages_failed,
                findings_count=findings_count,
            )
        )

    return CrawlListResponse(total=total, limit=limit, offset=offset, crawls=items)


@router.post("", response_model=CrawlCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_crawl(
    payload: CrawlCreateRequest,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    Trigger a new deterministic audit crawl.
    Guarantees end-to-end idempotency via distributed Redis seed locks and active crawl checks.
    """
    raw_url = str(payload.url).strip()
    if not raw_url.startswith(("http://", "https://")):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="URL must start with http:// or https://",
        )

    canonical = canonicalize_url(raw_url)
    domain = urlparse(canonical).netloc.lower()
    if not domain:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Could not determine domain from URL.",
        )

    seed_fp = get_fingerprint(canonical)
    lock_key = f"lock:crawl:seed:{seed_fp}"

    cfg = payload.config.model_dump() if payload.config else {}
    worker_count = cfg.get("worker_count", 2)

    if worker_count > 4:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"worker_count ({worker_count}) exceeds the maximum allowed limit of 4 to stay within the free runner concurrency cap.",
        )

    # 1. Concurrency & Deduplication Protection
    # Check if an active crawl is already in-flight for this domain
    active_stmt = select(Crawl).where(
        Crawl.target_domain == domain,
        Crawl.status.in_(["queued", "running", "paused"]),
    ).order_by(desc(Crawl.started_at))
    active_res = await db.execute(active_stmt)
    existing_active_crawl = active_res.scalars().first()

    if existing_active_crawl:
        existing_id_alias = existing_active_crawl.config.get("crawl_id", str(existing_active_crawl.id))
        log.info(
            "[DEDUP] Intercepted duplicate crawl request for %s. Active crawl ID: %s (status: %s)",
            canonical,
            existing_id_alias,
            existing_active_crawl.status,
        )
        return CrawlCreateResponse(
            crawl_id=existing_id_alias,
            status=existing_active_crawl.status,
            is_duplicate=True,
            existing_crawl_id=str(existing_active_crawl.id),
            message=f"An active crawl ({existing_id_alias}) is already in-flight for {domain}. Returning existing crawl instance.",
        )

    # 2. Acquire short-lived atomic distributed lock during creation
    crawl_id_str = payload.crawl_id.strip() if payload.crawl_id else f"crawl-{uuid.uuid4().hex[:12]}"
    acquired = await redis.set(lock_key, crawl_id_str, nx=True, ex=300)

    if not acquired:
        active_val = await redis.get(lock_key)
        log.warning("[DEDUP] Concurrent crawl initialization locked for %s: %s", canonical, active_val)
        return CrawlCreateResponse(
            crawl_id=str(active_val or crawl_id_str),
            status="queued",
            is_duplicate=True,
            existing_crawl_id=str(active_val),
            message=f"Crawl initialization currently in progress for {domain}.",
        )

    crawl_uuid = resolve_crawl_uuid(crawl_id_str)
    cfg["crawl_id"] = crawl_id_str

    # 3. Ensure consumer groups exist in Redis
    await ensure_consumer_group(redis, crawl_id_str)
    await ensure_persist_consumer_groups(redis, crawl_id_str)

    # 4. Persist Crawl row in Postgres
    crawl = Crawl(
        id=crawl_uuid,
        target_url=canonical,
        target_domain=domain,
        status="queued",
        started_at=datetime.datetime.now(datetime.UTC),
        worker_count=worker_count,
        config=cfg,
    )
    db.add(crawl)
    await db.commit()

    # 5. Publish seed URL to Redis task stream
    await publish_seed_url(
        redis=redis,
        url=canonical,
        domain=domain,
        crawl_id=crawl_id_str,
        depth=0,
    )

    # 6. Immediately attach persistence consumer on Render to listen for results
    from app.api.app import spawn_persistence_consumer_for_crawl
    spawn_persistence_consumer_for_crawl(crawl_id_str)

    # 7. Automatically dispatch ephemeral crawler workers to GitHub Actions
    dispatched = await dispatch_github_crawler(
        crawl_id=crawl_id_str,
        seed_url=canonical,
        max_pages=cfg.get("max_pages", 15),
        max_depth=cfg.get("max_depth", 2),
        worker_count=worker_count,
    )

    # 8. Fallback / Immediate Processing: If GitHub Actions was not dispatched (e.g. no GITHUB_TOKEN configured),
    # launch the background worker coroutine directly inside this container so it starts crawling immediately!
    if not dispatched:
        import asyncio
        from app.main import run_workers_for_crawl
        log.info("[CRAWL_API] Spawning in-process background workers for %s...", crawl_id_str)
        asyncio.create_task(run_workers_for_crawl(crawl_id=crawl_id_str, worker_count=worker_count))

    msg = (
        f"Crawl {crawl_id_str} enqueued and auto-dispatched to GitHub Actions for {canonical}"
        if dispatched
        else f"Crawl {crawl_id_str} enqueued and active background workers spawned for {canonical}"
    )


    return CrawlCreateResponse(
        crawl_id=crawl_id_str,
        status="queued",
        message=msg,
        is_duplicate=False,
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
        consolidated_report=crawl.consolidated_report,
        error=crawl.error,
    )


@router.delete("/{crawl_id}", response_model=CrawlControlResponse)
async def delete_crawl(
    crawl_id: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    Purge a crawl job and perform cascading deletion across all DB records,
    Redis stream keys, and Appwrite storage assets.
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

    # 1. Cascade delete all DB children
    await db.execute(select(Page).where(Page.crawl_id == crawl_uuid))
    # ORM cascade takes care of Page, AuditFinding, PageContact, DeadLetterTask, DroppedTelemetry
    await db.delete(crawl)
    await db.commit()

    # 2. Flush all Redis keys scoped to this crawl ID
    await flush_crawl(crawl_id, redis=redis)

    # 3. Clean up lock keys
    seed_fp = get_fingerprint(canonicalize_url(crawl.target_url))
    await redis.delete(f"lock:crawl:seed:{seed_fp}")

    log.info("[PURGE] Deleted crawl %s (%s) and purged Redis stream keys.", crawl_id, crawl_uuid)

    return CrawlControlResponse(
        crawl_id=crawl_id,
        action="deleted",
        previous_status=prev_status,
        current_status="deleted",
        message=f"Crawl '{crawl_id}' and all associated pages, findings, telemetry, and Redis keys were permanently purged.",
        details={"status": "purged"},
    )


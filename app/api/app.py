"""
app/api/app.py
==============
FastAPI application factory for the Nexus Audit Query API (Phase 3).
Includes full lifecycle state machine routes, graph visualizer, and RFC 7807 error formatting.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Dict

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import SQLAlchemyError

from app.api.auth import require_authenticated_principal
from app.api.audit import write_action_audit
from app.api.maintenance import run_api_retention_cleanup
from app.api.routes import (
    auth,
    control,
    crawls,
    events,
    findings,
    graph,
    pages,
    status as status_route,
    summary,
    telemetry,
)
from app.config import settings
from app.db.engine import close_engine
from app.models.schema import Crawl, init_db
from app.persistence_worker import run_persistence_worker_for_crawl
from app.redis_client import create_redis_pool
from app.db.engine import get_db_session
from sqlalchemy import select

log = logging.getLogger("audit_crawler.api")

# Active persistence consumer tasks registry: crawl_id -> asyncio.Task
_active_persistence_tasks: Dict[str, asyncio.Task] = {}


def _is_auditable_mutation(request: Request, principal: object | None) -> bool:
    return bool(
        principal is not None
        and request.method in {"POST", "PUT", "PATCH", "DELETE"}
        and request.url.path.startswith("/api/")
    )


def _audit_resource_ids(request: Request) -> tuple[str | None, str | None]:
    segments = [segment for segment in request.url.path.split("/") if segment]
    crawl_id = segments[2] if len(segments) > 2 and segments[1] == "crawls" else None
    finding_id = None
    if "findings" in segments:
        finding_index = segments.index("findings")
        if finding_index + 1 < len(segments):
            finding_id = segments[finding_index + 1]
    return crawl_id, finding_id


def spawn_persistence_consumer_for_crawl(crawl_id: str) -> None:
    """Register and start a background persistence consumer for a specific crawl_id."""
    existing_task = _active_persistence_tasks.get(crawl_id)
    if existing_task is None or existing_task.done():
        log.info("[PERSIST_MANAGER] Attaching persistence consumer for crawl: %s", crawl_id)
        task = asyncio.create_task(run_persistence_worker_for_crawl(crawl_id, auto_exit_on_drain=True))
        _active_persistence_tasks[crawl_id] = task


async def run_multitenant_persistence_manager():
    """
    Background manager dynamically attaching persistence consumers to all active crawl streams.
    Discovers active crawls from Postgres and active Redis streams.
    """
    log.info("[PERSIST_MANAGER] Starting multi-tenant persistence manager...")
    redis = create_redis_pool()

    try:
        while True:
            try:
                # 1. Discover all non-terminal crawls from Postgres
                async with get_db_session() as session:
                    stmt = select(Crawl).where(Crawl.status.in_(["queued", "running", "paused"]))
                    res = await session.execute(stmt)
                    active_crawls = res.scalars().all()
                    for c in active_crawls:
                        cid = (c.config or {}).get("crawl_id") or str(c.id)
                        spawn_persistence_consumer_for_crawl(cid)

                # 2. Also check Redis for any active audit_results streams
                async for key in redis.scan_iter("crawl:*:stream:audit_results"):
                    key_str = key.decode("utf-8") if isinstance(key, bytes) else str(key)
                    parts = key_str.split(":")
                    if len(parts) >= 4:
                        cid = parts[1]
                        spawn_persistence_consumer_for_crawl(cid)

                # 3. Prune finished tasks
                done_cids = [cid for cid, t in _active_persistence_tasks.items() if t.done()]
                for cid in done_cids:
                    _active_persistence_tasks.pop(cid, None)

            except Exception as loop_err:
                log.debug("[PERSIST_MANAGER] Discovery loop error: %s", loop_err)

            await asyncio.sleep(8.0)

    except asyncio.CancelledError:
        log.info("[PERSIST_MANAGER] Manager received cancel signal.")
        for task in _active_persistence_tasks.values():
            if not task.done():
                task.cancel()
    finally:
        await redis.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager to initialize DB schema and dispose engine on shutdown."""
    # 1. Initialize database tables
    await init_db()

    # 2. Start multi-tenant persistence manager
    manager_task: asyncio.Task | None = None
    retention_task = asyncio.create_task(run_api_retention_cleanup())
    should_run_worker = os.getenv("RUN_EMBEDDED_WORKER", "true").lower() in ("true", "1", "yes")

    if should_run_worker:
        manager_task = asyncio.create_task(run_multitenant_persistence_manager())

    yield

    # Shutdown sequence
    if manager_task and not manager_task.done():
        manager_task.cancel()
        try:
            await asyncio.wait_for(manager_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    if not retention_task.done():
        retention_task.cancel()
        try:
            await asyncio.wait_for(retention_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    for task in _active_persistence_tasks.values():
        if not task.done():
            task.cancel()

    await close_engine()



def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance."""
    app = FastAPI(
        title="Nexus Audit Query API",
        description="Deterministic REST API powering AuditMorph Studio and MCP Adapters with complete lifecycle management.",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.audit_action_writer = write_action_audit

    # Browser access is limited to the configured console origins.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.api_cors_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Last-Event-ID"],
    )

    # Every crawl-data and action route shares the same API-side authorization.
    protected_router_options = {
        "prefix": "/api/crawls",
        "dependencies": [Depends(require_authenticated_principal)],
    }
    app.include_router(crawls.router, tags=["Crawls"], **protected_router_options)
    app.include_router(status_route.router, tags=["Status"], **protected_router_options)
    app.include_router(control.router, tags=["Lifecycle Control"], **protected_router_options)
    app.include_router(telemetry.router, tags=["Telemetry & DLQ"], **protected_router_options)
    app.include_router(graph.router, tags=["Graph & Export"], **protected_router_options)
    app.include_router(findings.router, tags=["Findings"], **protected_router_options)
    app.include_router(pages.router, tags=["Pages"], **protected_router_options)
    app.include_router(summary.router, tags=["Summary"], **protected_router_options)
    app.include_router(
        events.router,
        prefix="/api",
        tags=["Events"],
        dependencies=[Depends(require_authenticated_principal)],
    )
    app.include_router(auth.router)

    async def write_request_audit(request: Request, response_status: int) -> None:
        principal = getattr(request.state, "principal", None)
        if not _is_auditable_mutation(request, principal):
            return

        crawl_id, finding_id = _audit_resource_ids(request)
        try:
            await request.app.state.audit_action_writer(
                principal,
                f"{request.method} {request.url.path}",
                crawl_id,
                finding_id,
                response_status,
            )
        except (SQLAlchemyError, OSError, ValueError) as exc:
            log.error(
                "Could not persist API action audit (%s) for %s.",
                type(exc).__name__,
                request.method,
            )

    @app.middleware("http")
    async def audit_mutating_requests(request: Request, call_next):
        try:
            response = await call_next(request)
        except Exception:
            await write_request_audit(request, 500)
            raise

        await write_request_audit(request, response.status_code)
        return response

    @app.get("/", tags=["Health"])
    @app.get("/health", tags=["Health"])
    async def health_check():
        return {
            "status": "healthy",
            "service": "nexus-audit-crawler-api",
            "version": "1.0.0",
            "embedded_worker": os.getenv("RUN_EMBEDDED_WORKER", "false").lower() in ("true", "1", "yes"),
        }

    return app


app = create_app()

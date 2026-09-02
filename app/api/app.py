"""
app/api/app.py
==============
FastAPI application factory for the Nexus Audit Query API (Phase 3).
Includes optional embedded persistence worker for single-container free-tier deployment.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import crawls, findings, pages, status, summary
from app.config import settings
from app.db.engine import close_engine
from app.models.schema import init_db
from app.persistence_worker import persistence_loop
from app.redis_client import create_redis_pool, ensure_persist_consumer_groups

log = logging.getLogger("audit_crawler.api")


async def run_embedded_persistence_worker():
    """Background task to run persistence consumer concurrently within the web server."""
    log.info("[EMBEDDED_WORKER] Starting embedded persistence consumer loop...")
    redis = create_redis_pool()
    crawl_id = settings.CRAWL_ID or "default"
    try:
        await ensure_persist_consumer_groups(redis, crawl_id)
        await persistence_loop(redis, crawl_id, worker_id="embedded-persist-0")
    except asyncio.CancelledError:
        log.info("[EMBEDDED_WORKER] Persistence worker received cancel signal.")
    except Exception as exc:
        log.error("[EMBEDDED_WORKER] Persistence loop error: %s", exc, exc_info=True)
    finally:
        await redis.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager to initialize DB schema and dispose engine on shutdown."""
    # 1. Initialize database tables
    await init_db()

    # 2. Optionally start embedded persistence worker (Free-Tier single-container deployment)
    worker_task: asyncio.Task | None = None
    should_run_worker = os.getenv("RUN_EMBEDDED_WORKER", "false").lower() in ("true", "1", "yes")

    if should_run_worker:
        worker_task = asyncio.create_task(run_embedded_persistence_worker())

    yield

    # Shutdown sequence
    if worker_task and not worker_task.done():
        worker_task.cancel()
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    await close_engine()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance."""
    app = FastAPI(
        title="Nexus Audit Query API",
        description="Deterministic REST API powering AuditMorph Studio and MCP Adapters.",
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS configuration -- allowing frontend dev environments
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Mount API routers
    app.include_router(crawls.router, prefix="/api/crawls", tags=["Crawls"])
    app.include_router(status.router, prefix="/api/crawls", tags=["Status"])
    app.include_router(findings.router, prefix="/api/crawls", tags=["Findings"])
    app.include_router(pages.router, prefix="/api/crawls", tags=["Pages"])
    app.include_router(summary.router, prefix="/api/crawls", tags=["Summary"])

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

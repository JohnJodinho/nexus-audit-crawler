"""
app/api/app.py
==============
FastAPI application factory for the Nexus Audit Query API (Phase 3).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import crawls, findings, pages, status, summary
from app.db.engine import close_engine, get_engine
from app.models.schema import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager to initialize DB schema and dispose engine on shutdown."""
    # Ensure tables exist
    await init_db()
    yield
    await close_engine()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance."""
    app = FastAPI(
        title="Nexus Audit Query API",
        description="Deterministic REST API powering AuditMorph Studio and MCP Adapters.",
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS configuration -- allowing localhost frontend dev environments
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Mount API routers
    # All routes are scoped under /api/crawls
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
        }

    return app


app = create_app()

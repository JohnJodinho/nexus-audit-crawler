"""
app/db/engine.py
================
Async SQLAlchemy database engine and session factory for the distributed crawler.
"""

from __future__ import annotations

import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings

log = logging.getLogger("audit_crawler.db")

_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """Return the global AsyncEngine instance (lazy-created)."""
    global _engine
    if _engine is None:
        db_url = settings.DATABASE_URL
        if not db_url:
            raise ValueError("DATABASE_URL is not configured in settings / .env.")

        # Ensure asyncpg dialect is used if postgresql:// is provided
        if db_url.startswith("postgresql://"):
            db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

        _engine = create_async_engine(
            db_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False,
        )
        log.info("[DB] Initialized async engine.")
    return _engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    """Return the global sessionmaker instance."""
    global _sessionmaker
    if _sessionmaker is None:
        engine = get_engine()
        _sessionmaker = async_sessionmaker(
            engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )
    return _sessionmaker


def async_session_factory() -> AsyncSession:
    """Return a new async session from the global sessionmaker."""
    return get_sessionmaker()()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async database session."""
    session_factory = get_sessionmaker()
    async with session_factory() as session:
        yield session


async def close_engine() -> None:
    """Dispose of the database engine and connection pool."""
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _sessionmaker = None
        log.info("[DB] Engine and connection pool disposed.")

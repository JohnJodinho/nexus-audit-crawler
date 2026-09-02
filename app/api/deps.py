"""
app/api/deps.py
===============
FastAPI dependencies for database sessions, Redis connections, and crawl ID resolution.
"""

from __future__ import annotations

import uuid
from typing import AsyncGenerator

import redis.asyncio as aioredis
from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_sessionmaker
from app.redis_client import create_redis_pool


def resolve_crawl_uuid(crawl_id_str: str) -> uuid.UUID:
    """Resolve a string or UUID crawl identifier to a deterministic UUID."""
    try:
        return uuid.UUID(crawl_id_str)
    except ValueError:
        return uuid.uuid5(uuid.NAMESPACE_DNS, crawl_id_str)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency that provides an async SQLAlchemy session."""
    session_factory = get_sessionmaker()
    async with session_factory() as session:
        yield session


async def get_redis() -> AsyncGenerator[aioredis.Redis, None]:
    """Dependency that provides an async Redis client."""
    client = create_redis_pool()
    try:
        yield client
    finally:
        await client.aclose()

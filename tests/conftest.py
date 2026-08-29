"""
tests/conftest.py
=================
Shared pytest fixtures for the audit crawler test suite.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
import fakeredis.aioredis as fake_aioredis


@pytest.fixture
def fake_redis():
    """
    Return a synchronous fakeredis client.

    Use this fixture when testing synchronous code or when you need to
    inspect Redis state after async operations.
    """
    return fake_aioredis.FakeRedis(decode_responses=True)


@pytest_asyncio.fixture
async def async_fake_redis():
    """
    Return an async fakeredis client for use in async test functions.

    Supports all redis.asyncio commands including Lua scripting (eval).
    """
    client = fake_aioredis.FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def crawl_id() -> str:
    """Return a fixed crawl_id for testing."""
    return "test-crawl-001"

"""
tests/test_orchestrator.py
==========================
Tests for orchestrator utilities: seed publication, flush operations, and PEL recovery.
"""

import pytest
from app.orchestrator import publish_seed_url, _reclaim_stale_tasks
from app.utils.flush_state import flush_crawl, flush_all
from app.redis_client import (
    tasks_key,
    queued_key,
    visited_key,
    budget_key,
    lock_key,
    domain_throttle_key,
    dlq_key,
    consumer_group_name,
    ensure_consumer_group,
)


class TestPublishSeedUrl:
    @pytest.mark.asyncio
    async def test_publish_seed_registers_queued_and_stream(self, async_fake_redis, crawl_id):
        await ensure_consumer_group(async_fake_redis, crawl_id)
        msg_id = await publish_seed_url(
            async_fake_redis,
            url="https://Example.COM/about/#section",
            domain="example.com",
            crawl_id=crawl_id,
            depth=0,
        )

        assert msg_id is not None

        # Check queued set contains fingerprint of canonical URL
        members = await async_fake_redis.smembers(queued_key(crawl_id))
        assert len(members) == 1

        # Check stream contains task with schema_version and crawl_id
        messages = await async_fake_redis.xrange(tasks_key(crawl_id), "-", "+")
        assert len(messages) == 1
        _id, fields = messages[0]
        assert fields["url"] == "https://example.com/about"
        assert fields["crawl_id"] == crawl_id
        assert fields["schema_version"] == "1"
        assert fields["retry_count"] == "0"
        assert fields["throttle_count"] == "0"


class TestFlushOperations:
    @pytest.mark.asyncio
    async def test_flush_crawl_isolated(self, async_fake_redis):
        cid_a = "crawl-a"
        cid_b = "crawl-b"

        # Populate keys for crawl-a
        await async_fake_redis.set(budget_key(cid_a), "10")
        await async_fake_redis.sadd(visited_key(cid_a), "hash1")
        await async_fake_redis.set(lock_key(cid_a, "hash1"), "worker-1")
        await async_fake_redis.set(domain_throttle_key(cid_a, "example.com"), "1")

        # Populate keys for crawl-b
        await async_fake_redis.set(budget_key(cid_b), "5")
        await async_fake_redis.sadd(visited_key(cid_b), "hash2")
        await async_fake_redis.set(lock_key(cid_b, "hash2"), "worker-2")

        # Flush crawl-a only
        await flush_crawl(cid_a, redis=async_fake_redis)

        # Assert crawl-a keys deleted
        assert await async_fake_redis.get(budget_key(cid_a)) is None
        assert await async_fake_redis.scard(visited_key(cid_a)) == 0
        assert await async_fake_redis.get(lock_key(cid_a, "hash1")) is None
        assert await async_fake_redis.get(domain_throttle_key(cid_a, "example.com")) is None

        # Assert crawl-b keys untouched
        assert await async_fake_redis.get(budget_key(cid_b)) == "5"
        assert await async_fake_redis.scard(visited_key(cid_b)) == 1
        assert await async_fake_redis.get(lock_key(cid_b, "hash2")) == "worker-2"

    @pytest.mark.asyncio
    async def test_flush_all(self, async_fake_redis):
        await async_fake_redis.set("crawl:1:budget", "1")
        await async_fake_redis.set("crawl:2:budget", "2")
        await async_fake_redis.set("unrelated:key", "keep")

        await flush_all(redis=async_fake_redis)

        assert await async_fake_redis.get("crawl:1:budget") is None
        assert await async_fake_redis.get("crawl:2:budget") is None
        assert await async_fake_redis.get("unrelated:key") == "keep"


class TestJanitorPELReclaim:
    @pytest.mark.asyncio
    async def test_pel_reclaim_republishes_stale_tasks(self, async_fake_redis, crawl_id):
        await ensure_consumer_group(async_fake_redis, crawl_id)

        # Push task and read via consumer to put in PEL
        stream = tasks_key(crawl_id)
        group = consumer_group_name(crawl_id)
        msg_id = await async_fake_redis.xadd(
            stream,
            {
                "schema_version": "1",
                "crawl_id": crawl_id,
                "url": "https://example.com/stale",
                "depth": "0",
                "retry_count": "0",
                "domain": "example.com",
            },
        )

        # Worker reads message (moves to PEL)
        await async_fake_redis.xreadgroup(
            groupname=group,
            consumername="dead-worker",
            streams={stream: ">"},
            count=1,
        )

        # Inspect pending list
        pending = await async_fake_redis.xpending_range(stream, group, "-", "+", 10)
        assert len(pending) == 1

        # Simulate PEL claim by directly calling _reclaim_stale_tasks with 0 timeout / mocking
        # With fakeredis, time_since_delivered will be 0ms unless we simulate claim or patch PEL_TIMEOUT_MS
        # Let's test _reclaim_stale_tasks when idle threshold is met
        import app.orchestrator as orch
        orig_timeout = orch.PEL_TIMEOUT_MS
        try:
            orch.PEL_TIMEOUT_MS = 0  # Reclaim anything idle >= 0ms
            await _reclaim_stale_tasks(async_fake_redis, crawl_id, batch_size=10)

            # Old message should be XACKed
            pending_after = await async_fake_redis.xpending_range(stream, group, "-", "+", 10)
            assert len(pending_after) == 0

            # New message should be published to the stream with retry_count incremented
            all_msgs = await async_fake_redis.xrange(stream, "-", "+")
            assert len(all_msgs) == 2  # Original (xacked) + Re-published
            new_id, new_fields = all_msgs[1]
            assert new_fields["url"] == "https://example.com/stale"
            assert new_fields["retry_count"] == "1"

        finally:
            orch.PEL_TIMEOUT_MS = orig_timeout

    @pytest.mark.asyncio
    async def test_pel_reclaim_routes_to_dlq_on_max_retries(self, async_fake_redis, crawl_id):
        await ensure_consumer_group(async_fake_redis, crawl_id)
        stream = tasks_key(crawl_id)
        group = consumer_group_name(crawl_id)

        # Task already at MAX_RETRIES (3)
        await async_fake_redis.xadd(
            stream,
            {
                "schema_version": "1",
                "crawl_id": crawl_id,
                "url": "https://example.com/failed",
                "depth": "0",
                "retry_count": "3",
                "domain": "example.com",
            },
        )

        await async_fake_redis.xreadgroup(
            groupname=group,
            consumername="dead-worker",
            streams={stream: ">"},
            count=1,
        )

        import app.orchestrator as orch
        orig_timeout = orch.PEL_TIMEOUT_MS
        try:
            orch.PEL_TIMEOUT_MS = 0
            await _reclaim_stale_tasks(async_fake_redis, crawl_id, batch_size=10)

            # Message routed to DLQ
            dlq_msgs = await async_fake_redis.xrange(dlq_key(crawl_id), "-", "+")
            assert len(dlq_msgs) == 1
            assert dlq_msgs[0][1]["url"] == "https://example.com/failed"
            assert dlq_msgs[0][1]["dlq_reason"] == "max_retries_exceeded_in_pel"

        finally:
            orch.PEL_TIMEOUT_MS = orig_timeout

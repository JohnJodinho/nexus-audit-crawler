"""
tests/test_redis_client.py
==========================
Tests for Redis key naming, Lua budget reservation, and domain concurrency semaphores.
"""

import pytest
from app.redis_client import (
    tasks_key,
    results_key,
    telemetry_key,
    dlq_key,
    visited_key,
    queued_key,
    budget_key,
    lock_key,
    domain_throttle_key,
    consumer_group_name,
    reserve_page_ticket,
    acquire_domain_slot,
    release_domain_slot,
    ensure_consumer_group,
)


class TestKeyGeneration:
    def test_keys_scoped_to_crawl_id(self):
        cid = "crawl-xyz-123"
        assert tasks_key(cid) == f"crawl:{cid}:stream:audit_tasks"
        assert results_key(cid) == f"crawl:{cid}:stream:audit_results"
        assert telemetry_key(cid) == f"crawl:{cid}:stream:dropped_telemetry"
        assert dlq_key(cid) == f"crawl:{cid}:stream:dlq"
        assert visited_key(cid) == f"crawl:{cid}:set:visited_fingerprints"
        assert queued_key(cid) == f"crawl:{cid}:set:queued_fingerprints"
        assert budget_key(cid) == f"crawl:{cid}:budget:tickets_dispensed"
        assert lock_key(cid, "abc") == f"crawl:{cid}:lock:processing:abc"
        assert domain_throttle_key(cid, "example.com") == f"crawl:{cid}:throttle:domain:example.com"
        assert consumer_group_name(cid) == f"audit_workers:{cid}"


class TestReservePageTicket:
    @pytest.mark.asyncio
    async def test_reserve_within_limit(self, async_fake_redis, crawl_id):
        t1 = await reserve_page_ticket(async_fake_redis, crawl_id, global_max_pages=2)
        assert t1 == 1

        t2 = await reserve_page_ticket(async_fake_redis, crawl_id, global_max_pages=2)
        assert t2 == 2

    @pytest.mark.asyncio
    async def test_reserve_exceeds_limit(self, async_fake_redis, crawl_id):
        t1 = await reserve_page_ticket(async_fake_redis, crawl_id, global_max_pages=1)
        assert t1 == 1

        t2 = await reserve_page_ticket(async_fake_redis, crawl_id, global_max_pages=1)
        assert t2 == 0  # rejected, budget exhausted

        # Verify counter was decremented back to 1 (not leaking)
        val = await async_fake_redis.get(budget_key(crawl_id))
        assert int(val) == 1

    @pytest.mark.asyncio
    async def test_reserve_zero_limit_unbounded(self, async_fake_redis, crawl_id):
        ticket = await reserve_page_ticket(async_fake_redis, crawl_id, global_max_pages=0)
        assert ticket == 1


class TestDomainSemaphore:
    @pytest.mark.asyncio
    async def test_acquire_and_release_slot(self, async_fake_redis, crawl_id):
        domain = "example.com"
        # Acquire slot 1
        s1 = await acquire_domain_slot(async_fake_redis, crawl_id, domain)
        assert s1 is True

        # Acquire slot 2 (MAX_CONCURRENT_PER_DOMAIN is 2 by default)
        s2 = await acquire_domain_slot(async_fake_redis, crawl_id, domain)
        assert s2 is True

        # Acquire slot 3 (should fail when max is 2)
        s3 = await acquire_domain_slot(async_fake_redis, crawl_id, domain)
        assert s3 is False

        # Release one slot
        await release_domain_slot(async_fake_redis, crawl_id, domain)

        # Now slot 3 should succeed
        s3_retry = await acquire_domain_slot(async_fake_redis, crawl_id, domain)
        assert s3_retry is True

        # Clean up
        await release_domain_slot(async_fake_redis, crawl_id, domain)
        await release_domain_slot(async_fake_redis, crawl_id, domain)


class TestEnsureConsumerGroup:
    @pytest.mark.asyncio
    async def test_ensure_consumer_group_idempotent(self, async_fake_redis, crawl_id):
        # First call creates stream and group
        await ensure_consumer_group(async_fake_redis, crawl_id)

        # Second call should not raise BUSYGROUP
        await ensure_consumer_group(async_fake_redis, crawl_id)

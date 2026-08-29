"""
scripts/verify_phase34.py
=========================
Live end-to-end verification of Phase 3 (Audit Signals) and Phase 4 (Consolidation).
"""

import asyncio
import json
import logging
import os
import sys

from app.config import settings
from app.db.engine import async_session_factory, close_engine
from app.models.schema import Crawl, Page, PageContact, PageLink
from app.redis_client import create_redis_pool, ensure_consumer_group, ensure_persist_consumer_groups
from app.orchestrator import publish_seed_url, run_crawl_consolidation
from app.utils.flush_state import flush_crawl
from app.persistence_worker import persistence_loop
from app.main import worker_loop
from sqlalchemy import select

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s]:(%(name)s) %(levelname)s: %(message)s",
)
log = logging.getLogger("verify_phase34")


async def run_live_test():
    crawl_id = "setia-audit-01"
    seed_url = "https://www.setialaw.com"
    domain = "setialaw.com"

    # Configure run constraints
    settings.CRAWL_ID = crawl_id
    settings.GLOBAL_MAX_PAGES = 3
    settings.MAX_DEPTH = 1
    settings.WORKER_COUNT = 1

    redis = create_redis_pool()

    try:
        log.info("1. Flushing any previous Redis state for %s...", crawl_id)
        await flush_crawl(crawl_id)

        log.info("2. Initializing Redis consumer groups...")
        await ensure_consumer_group(redis, crawl_id)
        await ensure_persist_consumer_groups(redis, crawl_id)

        log.info("3. Publishing seed: %s...", seed_url)
        await publish_seed_url(redis, seed_url, domain, crawl_id)

        log.info("4. Starting background persistence consumer...")
        persist_task = asyncio.create_task(
            persistence_loop(redis=redis, crawl_id=crawl_id, worker_id="test-persist-1")
        )

        log.info("5. Starting crawler worker...")
        worker_task = asyncio.create_task(
            worker_loop(redis=redis, worker_id="test-worker-1", crawl_id=crawl_id)
        )

        # Wait for worker to finish
        try:
            await asyncio.wait_for(worker_task, timeout=45.0)
        except asyncio.TimeoutError:
            log.warning("Worker timed out after 45s.")

        # Let persistence worker drain results
        await asyncio.sleep(4.0)
        persist_task.cancel()

        log.info("6. Running Consolidation Engine...")
        report = await run_crawl_consolidation(crawl_id)
        log.info("Consolidation Report:\n%s", json.dumps(report, indent=2))

        log.info("7. Verifying PostgreSQL persistence...")
        async with async_session_factory() as session:
            # Query crawl
            res = await session.execute(
                select(Crawl).order_by(Crawl.started_at.desc())
            )
            crawls = res.scalars().all()
            target_crawl = next((c for c in crawls if c.config and c.config.get("crawl_id") == crawl_id), None)
            if target_crawl:
                log.info("Crawl Record: ID=%s, Status=%s, Processed=%d, Failed=%d",
                         target_crawl.id, target_crawl.status, target_crawl.pages_processed, target_crawl.pages_failed)

                # Query pages
                p_res = await session.execute(
                    select(Page).where(Page.crawl_id == target_crawl.id)
                )
                pages = p_res.scalars().all()
                for p in pages:
                    log.info("PAGE: %s | Status: %d | Methods: %s | Markdown ID: %s",
                             p.url, p.status_code, p.extraction_methods, p.markdown_file_id)
                    log.info("PAGE AUDIT METADATA: %s", json.dumps(p.metadata_, indent=2))

                # Query contacts
                c_res = await session.execute(
                    select(PageContact).join(Page, PageContact.page_id == Page.id).where(Page.crawl_id == target_crawl.id)
                )
                contacts = c_res.scalars().all()
                log.info("TOTAL CONTACTS PERSISTED: %d", len(contacts))
                for c in contacts:
                    log.info("CONTACT: [%s] %s", c.kind, c.value)

    finally:
        await redis.aclose()
        await close_engine()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    asyncio.run(run_live_test())

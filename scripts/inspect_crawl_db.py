"""
scripts/inspect_crawl_db.py
===========================
Inspects Postgres database for crawl metadata and audit records.
"""

import asyncio
import json
import logging
import sys

from app.db.engine import async_session_factory, close_engine
from app.models.schema import Crawl, Page, PageContact, PageLink, DeadLetterTask, DroppedTelemetry
from sqlalchemy import select

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s]:(%(name)s) %(levelname)s: %(message)s",
)
log = logging.getLogger("inspect_db")


async def inspect_db():
    async with async_session_factory() as session:
        log.info("--- RECENT CRAWLS ---")
        crawls_res = await session.execute(select(Crawl).order_by(Crawl.started_at.desc()).limit(5))
        crawls = crawls_res.scalars().all()
        for c in crawls:
            log.info("CRAWL: ID=%s | URL=%s | Status=%s | Processed=%d | Failed=%d | Config=%s",
                     c.id, c.target_url, c.status, c.pages_processed, c.pages_failed, json.dumps(c.config, default=str))

        if crawls:
            target_crawl = crawls[0]
            log.info("\n--- PAGES FOR CRAWL %s ---", target_crawl.id)
            pages_res = await session.execute(
                select(Page).where(Page.crawl_id == target_crawl.id)
            )
            pages = pages_res.scalars().all()
            for p in pages:
                log.info("PAGE: ID=%s | URL=%s | Code=%d | Markdown File=%s | Extracted By=%s",
                         p.id, p.url, p.status_code, p.markdown_file_id, p.extraction_methods)
                log.info("METADATA JSON:\n%s\n", json.dumps(p.metadata_, indent=2))

            log.info("--- EXTRACTED CONTACTS FOR CRAWL %s ---", target_crawl.id)
            contacts_res = await session.execute(
                select(PageContact).join(Page, PageContact.page_id == Page.id).where(Page.crawl_id == target_crawl.id)
            )
            contacts = contacts_res.scalars().all()
            for ct in contacts:
                log.info("CONTACT: Kind=%s | Value=%s | PageID=%s", ct.kind, ct.value, ct.page_id)

    await close_engine()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    asyncio.run(inspect_db())

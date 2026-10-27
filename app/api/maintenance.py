"""Scheduled retention cleanup for API events and action audit records."""

from __future__ import annotations

import asyncio
import datetime
import logging

from sqlalchemy import delete
from sqlalchemy.exc import SQLAlchemyError

from app.db.engine import get_sessionmaker
from app.models.schema import ApiActionAudit, CrawlApiEvent

log = logging.getLogger("audit_crawler.api.maintenance")

_AUDIT_RETENTION = datetime.timedelta(days=365)
_EVENT_RETENTION = datetime.timedelta(days=30)
_CLEANUP_INTERVAL_SECONDS = 24 * 60 * 60


async def cleanup_expired_api_records() -> None:
    """Delete expired audit and event rows in one database transaction."""
    now = datetime.datetime.now(datetime.UTC)
    session_factory = get_sessionmaker()
    async with session_factory() as session:
        async with session.begin():
            await session.execute(
                delete(ApiActionAudit).where(
                    ApiActionAudit.created_at < now - _AUDIT_RETENTION
                )
            )
            await session.execute(
                delete(CrawlApiEvent).where(
                    CrawlApiEvent.created_at < now - _EVENT_RETENTION
                )
            )


async def run_api_retention_cleanup() -> None:
    """Run retention cleanup at startup and once per day."""
    while True:
        try:
            await cleanup_expired_api_records()
        except SQLAlchemyError as exc:
            log.error("API retention cleanup failed (%s).", type(exc).__name__)
        await asyncio.sleep(_CLEANUP_INTERVAL_SECONDS)

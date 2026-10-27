"""Persistence helpers for authenticated API action auditing."""

from __future__ import annotations

import logging

from app.api.auth import Principal
from app.api.deps import resolve_crawl_uuid
from app.api.event_log import append_crawl_event
from app.db.engine import get_sessionmaker
from app.models.schema import ApiActionAudit

log = logging.getLogger("audit_crawler.api.audit")


async def write_action_audit(
    principal: Principal,
    action: str,
    crawl_id: str | None,
    finding_id: str | None,
    response_status: int,
) -> None:
    """Persist a mutation outcome without recording request bodies or credentials."""
    outcome = "success" if response_status < 400 else "failure"
    session_factory = get_sessionmaker()
    async with session_factory() as session:
        session.add(
            ApiActionAudit(
                actor_subject=principal.subject,
                actor_type=principal.actor_type,
                action=action,
                crawl_id=crawl_id,
                finding_id=finding_id,
                outcome=outcome,
                response_status=response_status,
            )
        )
        await append_crawl_event(
            session,
            resolve_crawl_uuid(crawl_id) if crawl_id else None,
            "action.outcome",
            {
                "action": action,
                "outcome": outcome,
                "response_status": response_status,
                "finding_id": finding_id,
            },
        )
        await session.commit()

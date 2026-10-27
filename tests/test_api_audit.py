from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import app.api.audit as audit
from app.api.auth import Principal
from app.models.schema import ApiActionAudit


@pytest.mark.asyncio
async def test_action_audit_persists_identity_target_and_outcome(monkeypatch):
    session = AsyncMock()
    session.add = MagicMock()
    inserted = MagicMock()
    inserted.scalar_one_or_none.return_value = 27
    session.execute.side_effect = [inserted, MagicMock()]

    class SessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return session

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(audit, "get_sessionmaker", lambda: SessionFactory())

    await audit.write_action_audit(
        Principal(subject="operator", actor_type="operator"),
        "PATCH /api/crawls/{crawl_id}/findings/{finding_id}",
        "crawl-123",
        "finding-456",
        200,
    )

    record = session.add.call_args.args[0]
    assert isinstance(record, ApiActionAudit)
    assert record.actor_subject == "operator"
    assert record.actor_type == "operator"
    assert record.crawl_id == "crawl-123"
    assert record.finding_id == "finding-456"
    assert record.outcome == "success"
    assert record.response_status == 200
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_action_audit_marks_rejected_action_as_failure(monkeypatch):
    session = AsyncMock()
    session.add = MagicMock()
    inserted = MagicMock()
    inserted.scalar_one_or_none.return_value = 28
    session.execute.side_effect = [inserted, MagicMock()]

    class SessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return session

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(audit, "get_sessionmaker", lambda: SessionFactory())

    await audit.write_action_audit(
        Principal(subject="operator", actor_type="operator"),
        "DELETE /api/crawls/{crawl_id}",
        "missing",
        None,
        404,
    )

    assert session.add.call_args.args[0].outcome == "failure"
    assert session.add.call_args.args[0].response_status == 404

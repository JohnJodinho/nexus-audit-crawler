from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import app.api.maintenance as maintenance


@pytest.mark.asyncio
async def test_retention_cleanup_deletes_expired_audit_and_event_rows(monkeypatch):
    session = AsyncMock()

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    session.begin = MagicMock(return_value=Transaction())

    class SessionFactory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return session

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(maintenance, "get_sessionmaker", lambda: SessionFactory())

    await maintenance.cleanup_expired_api_records()

    assert session.execute.await_count == 2
    table_names = [statement.table.name for statement in (
        call.args[0] for call in session.execute.await_args_list
    )]
    assert table_names == ["api_action_audit", "crawl_api_events"]

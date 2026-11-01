"""Versioned database migrations for the API and crawler schema."""

from __future__ import annotations

import datetime
import logging
from collections.abc import Callable

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    insert,
    select,
    text,
)
from sqlalchemy.engine import Connection

from app.db.engine import get_engine

log = logging.getLogger("audit_crawler.db.migrations")

_metadata = MetaData()
_schema_migrations = Table(
    "schema_migrations",
    _metadata,
    # A single integer version keeps deployment checks cheap and deterministic.
    # Migration names remain visible for operators in the same table.
    Column("version", Integer, primary_key=True),
    Column("name", String(128), nullable=False),
    Column(
        "applied_at", DateTime(timezone=True), nullable=False
    ),
)


def _apply_baseline(connection: Connection) -> None:
    """Create all SQLAlchemy-managed tables for a new or legacy database."""
    from app.models.schema import Base

    Base.metadata.create_all(connection)


_MIGRATIONS: tuple[tuple[int, str, Callable[[Connection], None]], ...] = (
    (1, "baseline_schema", _apply_baseline),
)


def _lock_migrations(connection: Connection) -> None:
    """Serialize migration runners when multiple API instances start together."""
    if connection.dialect.name == "postgresql":
        connection.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": 481923})


async def run_migrations(engine=None) -> None:
    """Apply pending migrations atomically before the API accepts requests."""
    target_engine = engine or get_engine()
    async with target_engine.begin() as connection:
        await connection.run_sync(_lock_migrations)
        await connection.run_sync(_metadata.create_all)
        applied = dict(
            (
                await connection.execute(
                    select(_schema_migrations.c.version, _schema_migrations.c.name)
                )
            ).all()
        )

        for version, name, migration in _MIGRATIONS:
            applied_name = applied.get(version)
            if applied_name is not None and applied_name != name:
                raise RuntimeError(
                    f"Migration version {version} is recorded as {applied_name!r}, "
                    f"not {name!r}."
                )
            if applied_name is not None:
                continue
            await connection.run_sync(migration)
            await connection.execute(
                insert(_schema_migrations).values(
                    version=version,
                    name=name,
                    applied_at=datetime.datetime.now(datetime.timezone.utc),
                )
            )
            log.info("[DB] Applied migration %s (%s).", version, name)

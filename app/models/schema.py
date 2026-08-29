"""
app/models/schema.py
====================
SQLAlchemy async models for Phase 2 durable crawl state and dual-tier storage.
"""

from __future__ import annotations

import datetime
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Crawl(Base):
    """
    Represents an overarching crawl execution job.
    """
    __tablename__ = "crawls"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    target_url: Mapped[str] = mapped_column(Text, nullable=False)
    target_domain: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    started_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    finished_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    worker_count: Mapped[int] = mapped_column(Integer, default=1)
    pages_discovered: Mapped[int] = mapped_column(Integer, default=0)
    pages_processed: Mapped[int] = mapped_column(Integer, default=0)
    pages_failed: Mapped[int] = mapped_column(Integer, default=0)
    config: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    pages: Mapped[List[Page]] = relationship("Page", back_populates="crawl", cascade="all, delete-orphan")


class Page(Base):
    """
    Represents a successfully scraped page with Appwrite blob storage pointers.
    """
    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crawl_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("crawls.id", ondelete="CASCADE"), nullable=False, index=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status_code: Mapped[int] = mapped_column(Integer, default=200)
    extraction_methods: Mapped[List[str]] = mapped_column(ARRAY(String), default=list)

    # Blob pointers & metadata (Appwrite Storage)
    markdown_file_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    markdown_byte_size: Mapped[int] = mapped_column(Integer, default=0)
    markdown_token_count: Mapped[int] = mapped_column(Integer, default=0)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Structured JSON Extractions
    json_state: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    hydration_state: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    xhr_payloads: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB, nullable=True)

    screenshot_file_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fetched_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    metadata_: Mapped[Dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)

    crawl: Mapped[Crawl] = relationship("Crawl", back_populates="pages")
    contacts: Mapped[List[PageContact]] = relationship("PageContact", back_populates="page", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("crawl_id", "canonical_url", name="uq_crawl_canonical_url"),
    )


class PageContact(Base):
    """
    Extracted contact entity (email, phone) linked to a page.
    """
    __tablename__ = "page_contacts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    page_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), nullable=False, index=True)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)  # 'email' or 'phone'
    value: Mapped[str] = mapped_column(Text, nullable=False, index=True)

    page: Mapped[Page] = relationship("Page", back_populates="contacts")


class PageLink(Base):
    """
    Discovered link connection forming the crawl link graph.
    """
    __tablename__ = "page_links"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crawl_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("crawls.id", ondelete="CASCADE"), nullable=False, index=True)
    from_page_id: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), nullable=True)
    to_url: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_to_url: Mapped[str] = mapped_column(Text, nullable=False)
    link_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_internal: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        UniqueConstraint("crawl_id", "from_page_id", "canonical_to_url", name="uq_crawl_link"),
    )


class DroppedTelemetry(Base):
    """
    Log of links dropped by boundary gates or duplicate filters.
    """
    __tablename__ = "dropped_telemetry"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crawl_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("crawls.id", ondelete="CASCADE"), nullable=False, index=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    target_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    drop_reason: Mapped[str] = mapped_column(String(64), nullable=False)
    timestamp_utc: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=func.now())


class DeadLetterTask(Base):
    """
    Tasks that permanently failed after MAX_RETRIES.
    """
    __tablename__ = "dead_letter_queue"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    crawl_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("crawls.id", ondelete="CASCADE"), nullable=False, index=True)
    url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dlq_reason: Mapped[str] = mapped_column(Text, nullable=False)
    task_fields: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    failed_at_utc: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=func.now())


async def init_db(engine=None) -> None:
    """Create all tables idempotently if they do not already exist."""
    from app.db.engine import get_engine
    target_engine = engine or get_engine()
    async with target_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

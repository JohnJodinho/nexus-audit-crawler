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
    findings: Mapped[List["AuditFinding"]] = relationship("AuditFinding", back_populates="page", cascade="all, delete-orphan")

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


class AuditFinding(Base):
    """
    A single deterministic audit finding for a crawled page.

    ``id`` is the globally-unique instance key composed as ``"{page_id}:{rule_id}"``
    (e.g. ``"456:seo.missing_h1"``).  This is the key all WebMCP tools key off.

    ``rule_id`` is the reusable taxonomy code (``"seo.missing_h1"``) that can
    appear across many pages; ``id`` uniquely identifies this specific instance.

    ``canvas_zone`` is a **closed enum**: ``"nav"`` | ``"head"`` | ``"content"``
    | ``"footer"`` | ``"server"``.  ``"server"`` is used for response-level
    findings (security headers) that have no DOM position.

    ``status`` is seeded as ``"open"`` at write time.  Runtime state mutation
    (``pending_review``, ``approved``, ``rejected``) lives in the AuditMorph
    browser ``findingState`` dict and is **not** written back here until Phase 5.
    """
    __tablename__ = "audit_findings"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    # rule_id is the reusable taxonomy code; id is the unique per-page instance
    rule_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    crawl_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("crawls.id", ondelete="CASCADE"), nullable=False, index=True
    )
    page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), nullable=False, index=True
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)

    # Classification
    category: Mapped[str] = mapped_column(String(32), nullable=False)   # seo|security|performance|accessibility
    severity: Mapped[str] = mapped_column(String(16), nullable=False)   # critical|warning|info
    canvas_zone: Mapped[str] = mapped_column(String(16), nullable=False) # nav|head|content|footer|server

    # Human-readable description
    explanation: Mapped[str] = mapped_column(Text, nullable=False)

    # Evidence: {selector, observed, expected}
    evidence: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)

    # Optional proposed fix: {proposed, confidence}
    remediation: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    # Seed state — only "open" is written here; runtime transitions live in AuditMorph findingState
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open")

    detected_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=func.now()
    )
    metadata_: Mapped[Dict[str, Any]] = mapped_column("finding_metadata", JSONB, default=dict)

    page: Mapped["Page"] = relationship("Page", back_populates="findings")

    __table_args__ = (
        UniqueConstraint("page_id", "rule_id", name="uq_page_rule"),
    )


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

"""URL inventory normalization and persistence helpers."""

from __future__ import annotations

import datetime
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Select, select

from app.models.schema import UrlInventory
from app.utils.utilities import canonicalize_url


def inventory_values(
    *,
    crawl_uuid,
    original_url: str,
    discovery_status: str,
    processing_status: str,
    exclusion_reason: str | None = None,
    matched_rule_id: str | None = None,
    page_id: int | None = None,
    http_status: int | None = None,
    content_type: str | None = None,
) -> dict[str, Any]:
    """Build the normalized columns shared by all inventory writes."""
    normalized_url = canonicalize_url(original_url)
    parsed = urlsplit(normalized_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    return {
        "crawl_id": crawl_uuid,
        "original_url": original_url,
        "normalized_url": normalized_url,
        "origin": origin,
        "normalized_path": parsed.path or "/",
        "query_parameters": dict(parse_qsl(parsed.query, keep_blank_values=True)),
        "discovery_status": discovery_status,
        "processing_status": processing_status,
        "exclusion_reason": exclusion_reason,
        "matched_rule_id": matched_rule_id,
        "page_id": page_id,
        "http_status": http_status,
        "content_type": content_type,
        "processed_at": (
            datetime.datetime.now(datetime.UTC)
            if processing_status in {"processed", "failed", "unavailable"}
            else None
        ),
    }


async def upsert_inventory(
    session,
    *,
    crawl_uuid,
    original_url: str,
    discovery_status: str,
    processing_status: str,
    exclusion_reason: str | None = None,
    matched_rule_id: str | None = None,
    page_id: int | None = None,
    http_status: int | None = None,
    content_type: str | None = None,
) -> None:
    """Insert or advance one crawl URL without duplicating normalized URLs."""
    values = inventory_values(
        crawl_uuid=crawl_uuid,
        original_url=original_url,
        discovery_status=discovery_status,
        processing_status=processing_status,
        exclusion_reason=exclusion_reason,
        matched_rule_id=matched_rule_id,
        page_id=page_id,
        http_status=http_status,
        content_type=content_type,
    )
    statement = insert(UrlInventory).values(**values).on_conflict_do_update(
        constraint="uq_inventory_crawl_url",
        set_={
            key: value
            for key, value in values.items()
            if key not in {"crawl_id", "normalized_url", "original_url", "discovered_at"}
        },
    )
    await session.execute(statement)


def candidate_inventory_query(
    *,
    crawl_uuid,
    origin: str | None = None,
    path_prefix: str | None = None,
) -> Select:
    """Build an indexed-first inventory query for later selection services."""
    statement = select(UrlInventory).where(UrlInventory.crawl_id == crawl_uuid)
    if origin:
        statement = statement.where(UrlInventory.origin == origin)
    if path_prefix:
        statement = statement.where(UrlInventory.normalized_path.startswith(path_prefix))
    return statement

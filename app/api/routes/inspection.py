"""Authenticated crawl inspection read models."""

from __future__ import annotations

from collections import defaultdict
from typing import Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, resolve_crawl_uuid
from app.api.url_patterns import compile_patterns, evaluate_url
from app.models.schema import (
    Crawl,
    DeadLetterTask,
    Page,
    PageContact,
    PageLink,
    UrlInventory,
)

router = APIRouter()
url_rules_router = APIRouter()


async def _crawl(db: AsyncSession, crawl_id: str) -> Crawl:
    return (
        await db.execute(select(Crawl).where(Crawl.id == resolve_crawl_uuid(crawl_id)))
    ).scalar_one()


@router.get("/{crawl_id}/paths")
async def paths(crawl_id: str, db: AsyncSession = Depends(get_db)):
    crawl = await _crawl(db, crawl_id)
    rows = (
        await db.execute(
            select(UrlInventory.normalized_path, func.count(UrlInventory.id))
            .where(UrlInventory.crawl_id == crawl.id)
            .group_by(UrlInventory.normalized_path)
            .order_by(UrlInventory.normalized_path)
        )
    ).all()
    nodes: dict[str, dict[str, Any]] = {
        path: {
            "path": path,
            "url": f"{crawl.target_url.rstrip('/')}{path}",
            "page_count": count,
            "children": [],
        }
        for path, count in rows
    }
    roots: list[dict[str, Any]] = []
    for path in sorted(nodes, key=lambda value: (value.count("/"), value)):
        parent = path.rsplit("/", 1)[0] or "/" if path != "/" else None
        if parent and parent in nodes and parent != path:
            nodes[parent]["children"].append(nodes[path])
        else:
            roots.append(nodes[path])
    return {"crawl_id": crawl_id, "paths": roots}


@router.get("/{crawl_id}/contacts")
async def contacts(
    crawl_id: str,
    type: str = Query("all", pattern="^(all|email|phone)$"),
    search: str | None = None,
    unique: bool = False,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    source_page_id: int | None = None,
    db: AsyncSession = Depends(get_db),
):
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    query = select(PageContact).join(Page).where(Page.crawl_id == crawl_uuid)
    if type != "all":
        query = query.where(PageContact.kind == type)
    if search:
        query = query.where(PageContact.value.ilike(f"%{search}%"))
    if source_page_id is not None:
        query = query.where(PageContact.page_id == source_page_id)
    query = query.order_by(PageContact.value, PageContact.id).offset((page - 1) * limit).limit(limit)
    rows = (await db.execute(query)).scalars().all()
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        grouped.setdefault(row.value, {"value": row.value, "source_pages": []})
        grouped[row.value]["source_pages"].append(
            {"page_id": row.page_id}
        )
    values = list(grouped.values())
    if unique:
        values = values[:]
    return {
        "crawl_id": crawl_id,
        "total": len(values),
        "contacts": values,
        "emails": [item for item in values if type in {"all", "email"}],
        "phones": [item for item in values if type in {"all", "phone"}],
        "page": page,
        "limit": limit,
    }


@router.get("/{crawl_id}/search")
async def search_pages(
    crawl_id: str,
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    term = f"%{q}%"
    query = select(Page).where(
        Page.crawl_id == crawl_uuid,
        or_(Page.url.ilike(term), Page.canonical_url.ilike(term), Page.summary.ilike(term)),
    )
    rows = (await db.execute(query.order_by(Page.url).offset((page - 1) * limit).limit(limit))).scalars().all()
    return {
        "query": q,
        "total": len(rows),
        "results": [
            {
                "page_id": row.id,
                "url": row.canonical_url,
                "title": (row.metadata_ or {}).get("title"),
                "snippet": row.summary,
                "matched_in": "metadata",
            }
            for row in rows
        ],
    }


@router.get("/{crawl_id}/pages/{page_id}/links")
async def page_links(
    crawl_id: str,
    page_id: int,
    direction: str = Query("all", pattern="^(incoming|outgoing|all)$"),
    db: AsyncSession = Depends(get_db),
):
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    outgoing = []
    incoming = []
    if direction in {"outgoing", "all"}:
        outgoing = (
            await db.execute(
                select(PageLink).where(PageLink.crawl_id == crawl_uuid, PageLink.from_page_id == page_id)
            )
        ).scalars().all()
    if direction in {"incoming", "all"}:
        incoming = (
            await db.execute(
                select(PageLink).where(
                    PageLink.crawl_id == crawl_uuid,
                    PageLink.canonical_to_url.in_(
                        select(Page.canonical_url).where(Page.id == page_id)
                    ),
                )
            )
        ).scalars().all()

    def serialize(row: PageLink) -> dict[str, Any]:
        return {
            "url": row.to_url,
            "link_text": row.link_text,
            "is_internal": row.is_internal,
            "page_id": row.from_page_id,
        }

    return {
        "page_id": page_id,
        "counts": {"incoming": len(incoming), "outgoing": len(outgoing)},
        "incoming": [serialize(row) for row in incoming],
        "outgoing": [serialize(row) for row in outgoing],
    }


@router.get("/{crawl_id}/failures")
async def failures(crawl_id: str, db: AsyncSession = Depends(get_db)):
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    rows = (await db.execute(select(DeadLetterTask).where(DeadLetterTask.crawl_id == crawl_uuid))).scalars().all()
    return {
        "total": len(rows),
        "failures": [
            {
                "url": row.url,
                "error": row.dlq_reason,
                "retry_count": (row.task_fields or {}).get("retry_count", 0),
                "last_attempt": row.failed_at_utc,
                "retryable": False,
            }
            for row in rows
        ],
    }


@router.get("/{crawl_id}/statistics")
async def statistics(crawl_id: str, db: AsyncSession = Depends(get_db)):
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    inventory = (await db.execute(select(UrlInventory).where(UrlInventory.crawl_id == crawl_uuid))).scalars().all()
    pages = (await db.execute(select(Page).where(Page.crawl_id == crawl_uuid))).scalars().all()
    return {
        "pages": {
            "discovered": len(inventory),
            "processed": sum(row.processing_status == "processed" for row in inventory),
            "failed": sum(row.processing_status == "failed" for row in inventory),
            "excluded": sum(row.processing_status == "excluded" for row in inventory),
            "pending": sum(row.processing_status in {"queued", "processing"} for row in inventory),
        },
        "http_statuses": _counts(row.status_code for row in pages),
        "content_types": _counts(row.content_type for row in inventory if row.content_type),
        "contacts": {
            "emails": await _contact_count(db, crawl_uuid, "email"),
            "phones": await _contact_count(db, crawl_uuid, "phone"),
        },
    }


def _counts(values) -> dict[str, int]:
    result: dict[str, int] = defaultdict(int)
    for value in values:
        result[str(value)] += 1
    return dict(result)


async def _contact_count(db: AsyncSession, crawl_uuid, kind: str) -> int:
    return int(
        (
            await db.execute(
                select(func.count(PageContact.id))
                .join(Page)
                .where(Page.crawl_id == crawl_uuid, PageContact.kind == kind)
            )
        ).scalar_one()
    )


@router.get("/{crawl_id}/pages/{page_id}/metadata")
async def page_metadata(crawl_id: str, page_id: int, db: AsyncSession = Depends(get_db)):
    row = (
        await db.execute(
            select(Page).where(Page.crawl_id == resolve_crawl_uuid(crawl_id), Page.id == page_id)
        )
    ).scalar_one()
    html_available = bool((row.metadata_ or {}).get("html_file_id"))
    return {
        "page_id": row.id,
        "url": row.url,
        "canonical_url": row.canonical_url,
        "status_code": row.status_code,
        "fetched_at": row.fetched_at,
        "extraction_methods": row.extraction_methods or [],
        "available_formats": {"markdown": bool(row.markdown_file_id), "html": html_available, "text": bool(row.markdown_file_id)},
        "markdown_byte_size": row.markdown_byte_size,
        "screenshot_available": bool(row.screenshot_file_id),
        "metadata": row.metadata_ or {},
    }


@router.get("/{crawl_id}/artifacts")
async def artifacts(crawl_id: str, type: str | None = None, page_id: int | None = None, db: AsyncSession = Depends(get_db)):
    query = select(Page).where(Page.crawl_id == resolve_crawl_uuid(crawl_id))
    if page_id is not None:
        query = query.where(Page.id == page_id)
    pages = (await db.execute(query)).scalars().all()
    result = []
    for row in pages:
        values = [
            ("markdown", row.markdown_file_id),
            ("html", (row.metadata_ or {}).get("html_file_id")),
            ("screenshot", row.screenshot_file_id),
        ]
        result.extend(
            {"page_id": row.id, "type": artifact_type, "available": True}
            for artifact_type, file_id in values
            if file_id and (type is None or type == artifact_type)
        )
    return {"crawl_id": crawl_id, "artifacts": result}


@url_rules_router.post("/url-rules/preview")
async def preview_rules(payload: dict[str, Any], db: AsyncSession = Depends(get_db)):
    target_url = payload.get("target_url", "")
    target = urlsplit(target_url)
    if not target.scheme or not target.netloc:
        raise HTTPException(
            status_code=422,
            detail={"code": "INVALID_TARGET_URL", "message": "target_url must be an absolute URL."},
        )
    origin = f"{target.scheme.lower()}://{target.netloc.lower()}"
    try:
        include = compile_patterns(
            payload.get("include_patterns", []),
            rule_type="include",
            target_origin=origin,
        )
        exclude = compile_patterns(
            payload.get("exclude_patterns", []),
            rule_type="exclude",
            target_origin=origin,
        )
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=422,
            detail={"code": "INVALID_URL_PATTERN", "message": str(exc)},
        ) from exc
    results = []
    for url in payload.get("sample_urls", []):
        decision = evaluate_url(
            url,
            target_origin=origin,
            include_rules=include,
            exclude_rules=exclude,
        )
        results.append(
            {
                "url": url,
                "decision": decision.decision,
                "matched_pattern": decision.matched_rule.source if decision.matched_rule else None,
            }
        )
    return {"valid": True, "results": results}

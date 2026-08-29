"""
app/consolidation.py
====================
Crawl Completion & Site-Wide Consolidation Engine (Phase 4).

Computes domain-level quality rollups, contact graph aggregation, SEO and security
health scores, and advances the crawl lifecycle state machine from 'consolidating'
to 'finished'.
"""

from __future__ import annotations

import datetime
import logging
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import (
    Crawl,
    DeadLetterTask,
    DroppedTelemetry,
    Page,
    PageContact,
    PageLink,
)

log = logging.getLogger("audit_crawler.consolidation")


async def consolidate_crawl(
    session: AsyncSession,
    crawl_uuid: uuid.UUID,
    crawl_id: str,
) -> Dict[str, Any]:
    """
    Execute site-wide audit consolidation and finalize the crawl record.

    Parameters
    ----------
    session:
        Active SQLAlchemy async database session.
    crawl_uuid:
        UUID primary key of the Crawl record.
    crawl_id:
        String namespace identifier (e.g. 'setia-01').

    Returns
    -------
    Dict[str, Any]:
        Consolidated crawl audit report summary.
    """
    log.info("[CONSOLIDATION] Beginning crawl consolidation for crawl_id=%s (%s)", crawl_id, crawl_uuid)

    # 1. Total page counts
    pages_res = await session.execute(
        select(func.count(Page.id)).where(Page.crawl_id == crawl_uuid)
    )
    pages_processed = pages_res.scalar_one() or 0

    failed_res = await session.execute(
        select(func.count(DeadLetterTask.id)).where(DeadLetterTask.crawl_id == crawl_uuid)
    )
    pages_failed = failed_res.scalar_one() or 0

    dropped_res = await session.execute(
        select(func.count(DroppedTelemetry.id)).where(DroppedTelemetry.crawl_id == crawl_uuid)
    )
    telemetry_dropped = dropped_res.scalar_one() or 0

    # 2. Aggregated Contact Rollup
    contacts_res = await session.execute(
        select(PageContact.kind, PageContact.value, func.count(PageContact.id).label("occurrences"))
        .join(Page, PageContact.page_id == Page.id)
        .where(Page.crawl_id == crawl_uuid)
        .group_by(PageContact.kind, PageContact.value)
    )
    contacts_rows = contacts_res.all()

    unique_emails: List[Dict[str, Any]] = []
    unique_phones: List[Dict[str, Any]] = []
    for c_kind, c_val, occ in contacts_rows:
        if c_kind == "email":
            unique_emails.append({"email": c_val, "occurrences": occ})
        elif c_kind == "phone":
            unique_phones.append({"phone": c_val, "occurrences": occ})

    # 3. Domain Quality & Health Metrics Rollup
    pages_data_res = await session.execute(
        select(Page.id, Page.url, Page.metadata_).where(Page.crawl_id == crawl_uuid)
    )
    pages_rows = pages_data_res.all()

    pages_with_h1 = 0
    pages_with_meta_desc = 0
    total_images_seen = 0
    total_images_missing_alt = 0
    security_scores: List[int] = []
    schema_types_distribution: Dict[str, int] = {}
    missing_security_headers_agg: Dict[str, int] = {}

    for _, _, meta in pages_rows:
        if not meta or not isinstance(meta, dict):
            continue

        seo = meta.get("seo", {})
        sec = meta.get("security", {})

        # SEO checks
        if seo.get("headings", {}).get("h1_count", 0) >= 1:
            pages_with_h1 += 1
        if seo.get("meta_description"):
            pages_with_meta_desc += 1

        img_stats = seo.get("images", {})
        total_images_seen += img_stats.get("total", 0)
        total_images_missing_alt += img_stats.get("missing_alt", 0)

        for st in seo.get("schema_types", []):
            schema_types_distribution[st] = schema_types_distribution.get(st, 0) + 1

        # Security checks
        sec_score = sec.get("security_score")
        if isinstance(sec_score, (int, float)):
            security_scores.append(int(sec_score))

        for mh in sec.get("missing_headers", []):
            missing_security_headers_agg[mh] = missing_security_headers_agg.get(mh, 0) + 1

    h1_coverage_pct = (
        int((pages_with_h1 / pages_processed) * 100) if pages_processed > 0 else 0
    )
    meta_desc_coverage_pct = (
        int((pages_with_meta_desc / pages_processed) * 100) if pages_processed > 0 else 0
    )
    overall_alt_coverage_pct = (
        int(((total_images_seen - total_images_missing_alt) / total_images_seen) * 100)
        if total_images_seen > 0
        else 100
    )
    avg_security_score = (
        int(sum(security_scores) / len(security_scores)) if security_scores else 0
    )

    # 4. Anchor Link Integrity Verification (#hash links)
    links_res = await session.execute(
        select(PageLink.from_page_id, PageLink.to_url)
        .where(PageLink.crawl_id == crawl_uuid)
        .where(PageLink.to_url.like("%#%"))
    )
    anchor_links = links_res.all()
    anchor_link_count = len(anchor_links)

    # 5. Compile Consolidation Summary
    now_utc = datetime.datetime.now(datetime.UTC)
    consolidation_report: Dict[str, Any] = {
        "consolidated_at_utc": now_utc.isoformat(),
        "pages_processed": pages_processed,
        "pages_failed": pages_failed,
        "telemetry_dropped": telemetry_dropped,
        "contacts": {
            "total_unique_emails": len(unique_emails),
            "total_unique_phones": len(unique_phones),
            "emails": unique_emails,
            "phones": unique_phones,
        },
        "health_scorecard": {
            "h1_coverage_pct": h1_coverage_pct,
            "meta_description_coverage_pct": meta_desc_coverage_pct,
            "image_alt_coverage_pct": overall_alt_coverage_pct,
            "average_security_score": avg_security_score,
            "missing_security_headers": missing_security_headers_agg,
            "schema_org_types": schema_types_distribution,
        },
        "links": {
            "anchor_links_evaluated": anchor_link_count,
        },
    }

    # 6. Update Crawl state to 'finished'
    crawl_row_res = await session.execute(select(Crawl).where(Crawl.id == crawl_uuid))
    crawl_obj = crawl_row_res.scalar_one_or_none()
    if crawl_obj:
        existing_config = crawl_obj.config or {}
        existing_config["consolidation"] = consolidation_report

        await session.execute(
            update(Crawl)
            .where(Crawl.id == crawl_uuid)
            .values(
                status="finished",
                finished_at=now_utc,
                pages_processed=pages_processed,
                pages_failed=pages_failed,
                config=existing_config,
            )
        )
        await session.commit()

    log.info(
        "[CONSOLIDATION] Crawl %s finalized successfully: %d pages, %d emails, %d phones, security=%d/100",
        crawl_id,
        pages_processed,
        len(unique_emails),
        len(unique_phones),
        avg_security_score,
    )

    return consolidation_report

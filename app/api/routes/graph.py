"""
app/api/routes/graph.py
=======================
Graph topology visualizer and audit deliverable export handlers (JSON / Markdown).
"""

from __future__ import annotations

import datetime
import json
import logging
from typing import Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, resolve_crawl_uuid
from app.api.schemas import (
    CrawlExportResponse,
    CrawlGraphResponse,
    GraphEdge,
    GraphNode,
)
from app.models.schema import AuditFinding, Crawl, Page, PageContact

log = logging.getLogger("audit_crawler.api.graph")

router = APIRouter()


@router.get("/{crawl_id}/graph", response_model=CrawlGraphResponse)
async def get_crawl_graph(
    crawl_id: str,
    limit: int = Query(200, ge=1, le=1000, description="Max nodes limit"),
    db: AsyncSession = Depends(get_db),
):
    """Retrieve site topology graph (nodes and internal link edges) for architecture visualization."""
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    # Verify crawl exists
    crawl_stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    crawl_res = await db.execute(crawl_stmt)
    crawl = crawl_res.scalar_one_or_none()
    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    # 1. Fetch crawled pages
    pages_stmt = (
        select(Page)
        .where(Page.crawl_id == crawl_uuid)
        .order_by(Page.id.asc())
        .limit(limit)
    )
    pages_res = await db.execute(pages_stmt)
    pages = pages_res.scalars().all()

    nodes: List[GraphNode] = []
    edges: List[GraphEdge] = []
    page_url_set = {p.canonical_url for p in pages}

    for p in pages:
        meta = p.metadata_ or {}
        seo_meta = meta.get("seo", {}) if isinstance(meta, dict) else {}
        title = seo_meta.get("title") if isinstance(seo_meta, dict) else None

        # Count findings for this page
        f_stmt = select(AuditFinding).where(AuditFinding.page_id == p.id)
        f_res = await db.execute(f_stmt)
        findings = f_res.scalars().all()

        nodes.append(
            GraphNode(
                id=str(p.id),
                url=p.canonical_url,
                title=title,
                status_code=p.status_code,
                findings_count=len(findings),
            )
        )

        # Internal link edges
        links = meta.get("links", []) if isinstance(meta, dict) else []
        for lk in links:
            target_url = lk.get("url") if isinstance(lk, dict) else str(lk)
            if target_url and target_url in page_url_set and target_url != p.canonical_url:
                edges.append(
                    GraphEdge(
                        source=p.canonical_url,
                        target=target_url,
                        anchor_text=lk.get("text") if isinstance(lk, dict) else None,
                    )
                )

    return CrawlGraphResponse(
        crawl_id=crawl_id,
        total_nodes=len(nodes),
        total_edges=len(edges),
        nodes=nodes,
        edges=edges,
    )


@router.get("/{crawl_id}/export", response_model=CrawlExportResponse)
async def export_crawl_report(
    crawl_id: str,
    format_type: Literal["json", "markdown"] = Query("json", alias="format", description="Export format: json or markdown"),
    db: AsyncSession = Depends(get_db),
):
    """Export the complete audit deliverable in structured JSON or executive Markdown format."""
    crawl_uuid = resolve_crawl_uuid(crawl_id)

    crawl_stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    crawl_res = await db.execute(crawl_stmt)
    crawl = crawl_res.scalar_one_or_none()
    if not crawl:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Crawl '{crawl_id}' not found.",
        )

    # 1. Fetch pages, contacts, and findings
    pages_stmt = select(Page).where(Page.crawl_id == crawl_uuid).order_by(Page.id.asc())
    pages_res = await db.execute(pages_stmt)
    pages = pages_res.scalars().all()

    findings_stmt = select(AuditFinding).where(AuditFinding.crawl_id == crawl_uuid).order_by(AuditFinding.severity.asc())
    findings_res = await db.execute(findings_stmt)
    findings = findings_res.scalars().all()

    contacts_stmt = select(PageContact).where(PageContact.page_id.in_([p.id for p in pages])) if pages else None
    contacts = []
    if contacts_stmt is not None:
        contacts_res = await db.execute(contacts_stmt)
        contacts = contacts_res.scalars().all()

    now_utc = datetime.datetime.now(datetime.UTC)

    if format_type == "json":
        export_dict = {
            "crawl_id": crawl_id,
            "target_url": crawl.target_url,
            "target_domain": crawl.target_domain,
            "status": crawl.status,
            "started_at": crawl.started_at.isoformat() if crawl.started_at else None,
            "finished_at": crawl.finished_at.isoformat() if crawl.finished_at else None,
            "metrics": {
                "pages_processed": crawl.pages_processed,
                "pages_failed": crawl.pages_failed,
                "total_findings": len(findings),
                "total_contacts": len(contacts),
            },
            "consolidated_report": crawl.consolidated_report or {},
            "findings": [
                {
                    "id": f.id,
                    "rule_id": f.rule_id,
                    "url": f.url,
                    "category": f.category,
                    "severity": f.severity,
                    "canvas_zone": f.canvas_zone,
                    "explanation": f.explanation,
                    "evidence": f.evidence,
                    "remediation": f.remediation,
                    "status": f.status,
                }
                for f in findings
            ],
            "pages": [
                {
                    "id": p.id,
                    "url": p.url,
                    "canonical_url": p.canonical_url,
                    "status_code": p.status_code,
                    "markdown_file_id": p.markdown_file_id,
                    "markdown_token_count": p.markdown_token_count,
                }
                for p in pages
            ],
            "contacts": [
                {"page_id": c.page_id, "kind": c.kind, "value": c.value}
                for c in contacts
            ],
        }
        content_str = json.dumps(export_dict, indent=2)

    else:
        # Markdown executive summary deliverable
        report = crawl.consolidated_report or {}
        scorecard = report.get("health_scorecard", {})
        lines = [
            f"# Nexus Audit Report — {crawl.target_domain}",
            f"\n- **Target URL**: {crawl.target_url}",
            f"- **Crawl ID**: `{crawl_id}`",
            f"- **Status**: `{crawl.status.upper()}`",
            f"- **Audited On**: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"- **Pages Processed**: {crawl.pages_processed}",
            f"- **Total Findings**: {len(findings)}",
            "\n## Executive Scorecard",
            f"- **H1 Tag Coverage**: {scorecard.get('h1_coverage_pct', 'N/A')}%",
            f"- **Meta Description Coverage**: {scorecard.get('meta_description_coverage_pct', 'N/A')}%",
            f"- **Image Alt Text Coverage**: {scorecard.get('image_alt_coverage_pct', 'N/A')}%",
            f"- **Average Security Header Score**: {scorecard.get('average_security_score', 'N/A')}/100",
            "\n## Findings Breakdown",
            "| ID | Rule | Category | Severity | Zone | URL |",
            "| :--- | :--- | :--- | :--- | :--- | :--- |",
        ]
        for f in findings:
            lines.append(f"| `{f.id}` | `{f.rule_id}` | {f.category} | **{f.severity}** | {f.canvas_zone} | [{f.url}]({f.url}) |")

        if contacts:
            lines.extend([
                "\n## Extracted Contacts",
                "| Kind | Value | Page URL |",
                "| :--- | :--- | :--- |",
            ])
            page_map = {p.id: p.canonical_url for p in pages}
            for c in contacts:
                p_url = page_map.get(c.page_id, "")
                lines.append(f"| {c.kind} | `{c.value}` | [{p_url}]({p_url}) |")

        content_str = "\n".join(lines)

    return CrawlExportResponse(
        crawl_id=crawl_id,
        format=format_type,
        generated_at=now_utc,
        content=content_str,
    )

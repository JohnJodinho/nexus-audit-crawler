"""Authenticated content export and persisted preview job routes."""

from __future__ import annotations

import asyncio
import datetime
import io
import json
import logging
import re
import uuid
import zipfile
from collections.abc import AsyncIterator
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, resolve_crawl_uuid
from app.api.url_patterns import compile_patterns
from app.api.schemas import ExportRequest, ExportStatusResponse, JobResponse, PreviewStatusResponse
from app.db.engine import get_sessionmaker
from app.export_selection import SelectionResult, select_inventory
from app.models.schema import Crawl, ExportJob, Page, PreviewEvent, PreviewJob
from app.storage.appwrite_client import storage_client
from app.utils.utilities import canonicalize_url

log = logging.getLogger("audit_crawler.api.exports")
router = APIRouter()
_background_jobs: set[asyncio.Task] = set()
_job_tasks: dict[str, asyncio.Task] = {}
_MAX_ARCHIVE_BYTES = 50 * 1024 * 1024


def _export_file_id(job_id: str) -> str:
    """Return a stable Appwrite file ID within its 36-character limit."""
    identifier = job_id.removeprefix("export-")
    return f"exp_{identifier[:32]}"


def _track(task: asyncio.Task) -> None:
    _background_jobs.add(task)
    task.add_done_callback(_background_jobs.discard)


def _archive_name(url: str, format_type: str, used: set[str]) -> str:
    path = PurePosixPath(url.split("?", 1)[0].split("#", 1)[0])
    pieces = [piece for piece in path.parts if piece not in {"/", ".", ".."}]
    base = "/".join(pieces) or "index"
    if base.endswith("/"):
        base = f"{base}index"
    extension = {"markdown": ".md", "html": ".html", "text": ".txt"}[format_type]
    if not base.lower().endswith(extension):
        base += extension
    candidate = base
    counter = 2
    while candidate in used:
        stem, suffix = candidate.rsplit(".", 1)
        candidate = f"{stem}-{counter}.{suffix}"
        counter += 1
    used.add(candidate)
    return candidate


def _selection_result(result: SelectionResult) -> dict[str, Any]:
    return {
        **result.counts,
        "grammar_version": result.grammar_version,
        "items": [
            {
                "url": item.inventory.normalized_url,
                "page_id": item.inventory.page_id,
                "decision": item.decision,
                "reason": item.reason,
                "matched_rule": item.matched_rule,
                "content_status": item.content_status,
            }
            for item in result.items
        ],
    }


def _markdown_to_text(content: bytes) -> bytes:
    text = content.decode("utf-8", errors="replace")
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"```(?:[^\n]*)\n?", "", text)
    text = re.sub(r"[*_`>#-]", "", text)
    return text.encode("utf-8")


async def _load_crawl(session, crawl_id: str) -> Crawl:
    crawl = (
        await session.execute(select(Crawl).where(Crawl.id == resolve_crawl_uuid(crawl_id)))
    ).scalar_one_or_none()
    if crawl is None:
        raise HTTPException(status_code=404, detail=f"Crawl '{crawl_id}' not found.")
    return crawl


async def _run_export(job_id: str, crawl_id: str) -> None:
    session_factory = get_sessionmaker()
    async with session_factory() as session:
        job = (await session.execute(select(ExportJob).where(ExportJob.id == job_id))).scalar_one()
        crawl = (await session.execute(select(Crawl).where(Crawl.id == job.crawl_id))).scalar_one()
        job.status = "processing"
        await session.commit()
        try:
            request = job.request
            selection = await select_inventory(
                session,
                crawl=crawl,
                scope=request.get("scope"),
                format_type=request["format"],
                include_patterns=request.get("include_patterns", []),
                exclude_patterns=request.get("exclude_patterns", []),
                exclude_resource_types=request.get("exclude_resource_types", []),
            )
            archive_bytes, manifest = await _build_archive(selection, request, job_id)
            await _finish_export(job, archive_bytes, manifest)
            await session.commit()
        except asyncio.CancelledError:
            job.status = "cancelled"
            await session.commit()
            raise
        except Exception as exc:
            log.error("Export job %s failed: %s", job_id, exc, exc_info=True)
            job.status = "failed"
            job.error_code = "EXPORT_FAILED"
            job.error_message = str(exc)
            job.completed_at = datetime.datetime.now(datetime.UTC)
            await session.commit()


async def _content_bytes(page, format_type: str) -> bytes | None:
    if page is None:
        return None
    file_id = (
        (page.metadata_ or {}).get("html_file_id")
        if format_type == "html"
        else page.markdown_file_id
    )
    if not file_id:
        return None
    content = await storage_client.download_bytes(file_id)
    if format_type == "markdown":
        return content
    if format_type == "text":
        return _markdown_to_text(content)
    return content if format_type == "html" else None


async def _build_archive(selection: SelectionResult, request: dict[str, Any], job_id: str) -> tuple[bytes, dict[str, Any]]:
    buffer = io.BytesIO()
    used: set[str] = set()
    manifest = _selection_result(selection)
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for item in selection.items:
            if item.decision != "included" or item.content_status == "unavailable":
                continue
            content = await _content_bytes(item.page, request["format"])
            if content is None:
                continue
            archive.writestr(
                _archive_name(item.inventory.normalized_url, request["format"], used),
                content,
            )
            if request.get("include_assets") and item.page and item.page.screenshot_file_id:
                try:
                    archive.writestr(
                        f"assets/screenshots/page-{item.page.id}.png",
                        await storage_client.download_bytes(item.page.screenshot_file_id),
                    )
                except Exception as exc:
                    manifest.setdefault("asset_failures", []).append(
                        {"page_id": item.page.id, "error": str(exc)}
                    )
        if request.get("include_manifest", True):
            archive.writestr("manifest.json", json.dumps(manifest, indent=2))
        if request.get("include_metadata", True):
            archive.writestr(
                "metadata.json",
                json.dumps(
                    {
                        "crawl_id": selection.items[0].inventory.crawl_id
                        if selection.items
                        else None,
                        "format": request["format"],
                        "include_assets": request.get("include_assets", False),
                    },
                    default=str,
                    indent=2,
                ),
            )
    archive_bytes = buffer.getvalue()
    if len(archive_bytes) > _MAX_ARCHIVE_BYTES:
        raise ValueError("Export archive exceeds the configured size limit.")
    return archive_bytes, manifest


async def _finish_export(job: ExportJob, archive_bytes: bytes, manifest: dict[str, Any]) -> None:
    job.status = "completed_with_errors" if manifest["unavailable_content"] else "completed"
    job.result = manifest
    job.archive_file_id = await storage_client.upload_bytes(
        _export_file_id(job.id),
        f"{job.id}.zip",
        archive_bytes,
        "application/zip",
    ) or None
    job.completed_at = datetime.datetime.now(datetime.UTC)


async def _validate_export_request(crawl: Crawl, payload: ExportRequest) -> None:
    target = canonicalize_url(payload.scope or crawl.target_url)
    target_parts = urlsplit(target)
    crawl_parts = urlsplit(canonicalize_url(crawl.target_url))
    target_origin = f"{target_parts.scheme}://{target_parts.netloc}"
    crawl_origin = f"{crawl_parts.scheme}://{crawl_parts.netloc}"
    try:
        if target_origin != crawl_origin:
            raise ValueError("Scope must use the crawl origin.")
        compile_patterns(
            payload.include_patterns,
            rule_type="include",
            target_origin=crawl_origin,
        )
        compile_patterns(
            payload.exclude_patterns,
            rule_type="exclude",
            target_origin=crawl_origin,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": "INVALID_EXPORT_CONFIGURATION", "message": str(exc)},
        ) from exc


async def _emit_preview_event(session, preview_id: str, event_type: str, payload: dict[str, Any]) -> None:
    session.add(PreviewEvent(preview_id=preview_id, event_type=event_type, payload=payload))
    await session.commit()


async def _run_preview(preview_id: str) -> None:
    session_factory = get_sessionmaker()
    async with session_factory() as session:
        job = (await session.execute(select(PreviewJob).where(PreviewJob.id == preview_id))).scalar_one()
        crawl = (await session.execute(select(Crawl).where(Crawl.id == job.crawl_id))).scalar_one()
        job.status = "processing"
        await session.commit()
        try:
            request = job.request
            selection = await select_inventory(
                session,
                crawl=crawl,
                scope=request.get("scope"),
                format_type=request["format"],
                include_patterns=request.get("include_patterns", []),
                exclude_patterns=request.get("exclude_patterns", []),
                exclude_resource_types=request.get("exclude_resource_types", []),
            )
            await _emit_preview_event(
                session, preview_id, "preview.started",
                {"preview_id": preview_id, "total_candidates": selection.counts["total_candidates"]},
            )
            for item in selection.items:
                await _emit_preview_event(
                    session,
                    preview_id,
                    "url.evaluated",
                    {
                        "url": item.inventory.normalized_url,
                        "scope_decision": "in_scope" if item.decision != "outside_scope" else "outside_scope",
                        "decision": item.decision,
                        "matched_pattern": item.matched_rule,
                        "content_status": item.content_status,
                        "reason": item.reason,
                    },
                )
            result = _selection_result(selection)
            job.status = "completed"
            job.result = result
            job.completed_at = datetime.datetime.now(datetime.UTC)
            await session.commit()
            await _emit_preview_event(
                session,
                preview_id,
                "preview.completed",
                {"preview_id": preview_id, "status": job.status, **selection.counts},
            )
        except asyncio.CancelledError:
            job.status = "cancelled"
            await session.commit()
            await _emit_preview_event(session, preview_id, "preview.cancelled", {"preview_id": preview_id})
            raise
        except Exception as exc:
            log.error("Preview job %s failed: %s", preview_id, exc, exc_info=True)
            job.status = "failed"
            job.error_code = "PREVIEW_FAILED"
            job.error_message = str(exc)
            await session.commit()
            await _emit_preview_event(
                session, preview_id, "preview.failed",
                {"preview_id": preview_id, "error_code": "PREVIEW_FAILED", "message": str(exc)},
            )


@router.get("/{crawl_id}/pages/{page_id}/content")
async def get_page_content(
    crawl_id: str,
    page_id: int,
    format_type: str = Query("markdown", alias="format"),
    download: bool = Query(False),
    include_metadata: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    """Return stored page content without exposing storage credentials."""
    if format_type not in {"markdown", "html", "text"}:
        raise HTTPException(status_code=422, detail="format must be markdown, html, or text.")
    crawl_uuid = resolve_crawl_uuid(crawl_id)
    page = (
        await db.execute(
            select(Page).where(Page.crawl_id == crawl_uuid, Page.id == page_id)
        )
    ).scalar_one_or_none()
    if page is None:
        raise HTTPException(status_code=404, detail="Page not found.")
    if format_type == "html":
        file_id = (page.metadata_ or {}).get("html_file_id")
    else:
        file_id = page.markdown_file_id
    if not file_id:
        raise HTTPException(
            status_code=404,
            detail={"code": "CONTENT_UNAVAILABLE", "format": format_type},
        )
    try:
        content = await storage_client.download_bytes(file_id)
    except Exception as exc:
        log.error("Content download failed for page %s: %s", page_id, exc, exc_info=True)
        raise HTTPException(status_code=503, detail="Content storage is temporarily unavailable.") from exc
    media_type = {"markdown": "text/markdown", "html": "text/html", "text": "text/plain"}[format_type]
    if format_type == "text":
        content = _markdown_to_text(content)
    headers = {}
    if download:
        headers["Content-Disposition"] = f'attachment; filename="page-{page_id}.{format_type}"'
    if include_metadata:
        return JSONResponse(
            {
                "page_id": page.id,
                "crawl_id": crawl_id,
                "url": page.canonical_url,
                "format": format_type,
                "content": content.decode("utf-8", errors="replace"),
                "content_type": media_type,
                "content_length": len(content),
                "truncated": False,
                "fetched_at": page.fetched_at.isoformat() if page.fetched_at else None,
            }
        )
    return StreamingResponse(iter([content]), media_type=media_type, headers=headers)


@router.post("/{crawl_id}/exports", response_model=JobResponse, status_code=202)
async def create_export(crawl_id: str, payload: ExportRequest, db: AsyncSession = Depends(get_db)):
    crawl = await _load_crawl(db, crawl_id)
    await _validate_export_request(crawl, payload)
    job_id = f"export-{uuid.uuid4().hex}"
    job = ExportJob(id=job_id, crawl_id=crawl.id, request=payload.model_dump())
    db.add(job)
    await db.commit()
    task = asyncio.create_task(_run_export(job_id, crawl_id))
    _job_tasks[job_id] = task
    task.add_done_callback(lambda _: _job_tasks.pop(job_id, None))
    _track(task)
    return JobResponse(job_id=job_id, crawl_id=crawl_id, status="queued")


@router.get("/{crawl_id}/exports/{export_id}/download")
async def download_export(crawl_id: str, export_id: str, db: AsyncSession = Depends(get_db)):
    response = await get_export(crawl_id, export_id, db)
    if response.status not in {"completed", "completed_with_errors"}:
        raise HTTPException(status_code=409, detail="Export is not ready for download.")
    job = (await db.execute(select(ExportJob).where(ExportJob.id == export_id))).scalar_one()
    if not job.archive_file_id:
        raise HTTPException(status_code=404, detail="Export archive is unavailable.")
    try:
        content = await storage_client.download_bytes(job.archive_file_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Export storage is temporarily unavailable.") from exc
    return StreamingResponse(
        iter([content]),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{export_id}.zip"'},
    )


@router.post("/{crawl_id}/exports/preview", response_model=JobResponse, status_code=202)
async def create_preview(crawl_id: str, payload: ExportRequest, db: AsyncSession = Depends(get_db)):
    crawl = await _load_crawl(db, crawl_id)
    await _validate_export_request(crawl, payload)
    preview_id = f"preview-{uuid.uuid4().hex}"
    job = PreviewJob(id=preview_id, crawl_id=crawl.id, request=payload.model_dump())
    db.add(job)
    await db.commit()
    task = asyncio.create_task(_run_preview(preview_id))
    _job_tasks[preview_id] = task
    task.add_done_callback(lambda _: _job_tasks.pop(preview_id, None))
    _track(task)
    return JobResponse(job_id=preview_id, crawl_id=crawl_id, status="queued")


@router.get("/{crawl_id}/exports/previews/{preview_id}", response_model=PreviewStatusResponse)
async def get_preview(crawl_id: str, preview_id: str, db: AsyncSession = Depends(get_db)):
    job = (
        await db.execute(
            select(PreviewJob).where(
                PreviewJob.id == preview_id,
                PreviewJob.crawl_id == resolve_crawl_uuid(crawl_id),
            )
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Preview job not found.")
    return PreviewStatusResponse(
        job_id=job.id, crawl_id=crawl_id, status=job.status, result=job.result or {},
        error_code=job.error_code, error_message=job.error_message,
    )


@router.get("/{crawl_id}/exports/previews/{preview_id}/events")
async def preview_events(
    crawl_id: str,
    preview_id: str,
    last_event_id: int = Query(0, ge=0, alias="last_event_id"),
    last_event_header: str | None = Header(None, alias="Last-Event-ID"),
    db: AsyncSession = Depends(get_db),
):
    preview = (
        await db.execute(
            select(PreviewJob).where(
                PreviewJob.id == preview_id,
                PreviewJob.crawl_id == resolve_crawl_uuid(crawl_id),
            )
        )
    ).scalar_one_or_none()
    if preview is None:
        raise HTTPException(status_code=404, detail="Preview job not found.")
    if last_event_header is not None:
        try:
            last_event_id = int(last_event_header)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Last-Event-ID must be an integer.") from exc
        if last_event_id < 0:
            raise HTTPException(status_code=400, detail="Last-Event-ID must be non-negative.")
    session_factory = get_sessionmaker()

    async def stream() -> AsyncIterator[str]:
        cursor = last_event_id
        while True:
            async with session_factory() as session:
                events = list(
                    (
                        await session.execute(
                            select(PreviewEvent)
                            .where(PreviewEvent.preview_id == preview_id, PreviewEvent.id > cursor)
                            .order_by(PreviewEvent.id.asc())
                        )
                    ).scalars().all()
                )
                job = (await session.execute(select(PreviewJob).where(PreviewJob.id == preview_id))).scalar_one()
            for event in events:
                cursor = event.id
                yield f"id: {event.id}\nevent: {event.event_type}\ndata: {json.dumps(event.payload)}\n\n"
            if job.status in {"completed", "failed", "cancelled"} and not events:
                return
            if not events:
                yield ": keep-alive\n\n"
                await asyncio.sleep(1)

    return StreamingResponse(stream(), media_type="text/event-stream")


async def _cancel_job(
    job_id: str,
    crawl_id: str,
    db: AsyncSession,
    model,
) -> JobResponse:
    job = (
        await db.execute(
            select(model).where(
                model.id == job_id,
                model.crawl_id == resolve_crawl_uuid(crawl_id),
            )
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job.status in {"completed", "completed_with_errors", "failed", "cancelled"}:
        return JobResponse(job_id=job_id, crawl_id=crawl_id, status=job.status)
    task = _job_tasks.get(job_id)
    if task is not None and not task.done():
        task.cancel()
    job.status = "cancelled"
    job.completed_at = datetime.datetime.now(datetime.UTC)
    await db.commit()
    return JobResponse(job_id=job_id, crawl_id=crawl_id, status="cancelled")


@router.post("/{crawl_id}/exports/{export_id}/cancel", response_model=JobResponse)
async def cancel_export(crawl_id: str, export_id: str, db: AsyncSession = Depends(get_db)):
    return await _cancel_job(export_id, crawl_id, db, ExportJob)


@router.post("/{crawl_id}/exports/previews/{preview_id}/cancel", response_model=JobResponse)
async def cancel_preview(crawl_id: str, preview_id: str, db: AsyncSession = Depends(get_db)):
    return await _cancel_job(preview_id, crawl_id, db, PreviewJob)


@router.get("/{crawl_id}/exports/{export_id}", response_model=ExportStatusResponse)
async def get_export(crawl_id: str, export_id: str, db: AsyncSession = Depends(get_db)):
    job = (
        await db.execute(
            select(ExportJob).where(
                ExportJob.id == export_id,
                ExportJob.crawl_id == resolve_crawl_uuid(crawl_id),
            )
        )
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Export job not found.")
    return ExportStatusResponse(
        job_id=job.id, crawl_id=crawl_id, status=job.status, result=job.result or {},
        error_code=job.error_code, error_message=job.error_message,
    )

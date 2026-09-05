"""
app/persistence_worker.py
=========================
Independent, idempotent dual-tier persistence consumer for the distributed crawler.

Consumes from:
- ``crawl:{crawl_id}:stream:audit_results``
- ``crawl:{crawl_id}:stream:dropped_telemetry``
- ``crawl:{crawl_id}:stream:dlq``

Dual-Tier Storage Workflow
--------------------------
1. Extracts result payload and validates schema.
2. Uploads raw Markdown text to Appwrite Storage Bucket ({crawl_id}/{fingerprint}.md).
3. Upserts page metadata, contacts, and Appwrite storage pointer into Supabase Postgres.
4. Updates crawl statistics in Postgres.
5. Acknowledges Redis stream message via ``XACK`` only after successful DB commit.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import logging
import sys
import uuid
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from app.config import settings
from app.db.engine import get_sessionmaker
from app.models.schema import (
    AuditFinding,
    Crawl,
    DeadLetterTask,
    DroppedTelemetry,
    Page,
    PageContact,
    init_db,
)
from app.redis_client import (
    create_redis_pool,
    dlq_key,
    ensure_persist_consumer_groups,
    persist_consumer_group_name,
    results_key,
    tasks_key,
    telemetry_key,
)

from app.storage.appwrite_client import storage_client
from app.utils.utilities import canonicalize_url, get_fingerprint
from app.audit.rules import evaluate_findings

log = logging.getLogger("audit_crawler.persist")

_BATCH_SIZE: int = 10
_BLOCK_MS: int = 2_000


async def ensure_crawl_record(session, crawl_id_str: str, domain: str = "", target_url: str = "") -> uuid.UUID:
    """Ensure a row in `crawls` exists for this crawl_id."""
    try:
        crawl_uuid = uuid.UUID(crawl_id_str)
    except ValueError:
        # Fallback to deterministic UUID from string namespace if string is not valid UUID
        crawl_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, crawl_id_str)

    stmt = select(Crawl).where(Crawl.id == crawl_uuid)
    result = await session.execute(stmt)
    crawl = result.scalar_one_or_none()

    if crawl is None:
        crawl = Crawl(
            id=crawl_uuid,
            target_url=target_url or f"https://{domain or 'example.com'}",
            target_domain=domain or "example.com",
            status="running",
            started_at=datetime.datetime.now(datetime.UTC),
            config={"crawl_id": crawl_id_str, "crawl_id_alias": crawl_id_str},
        )
        session.add(crawl)
        await session.flush()

    return crawl_uuid


async def process_result_message(
    session,
    crawl_id: str,
    message_id: str,
    payload: Dict[str, Any],
) -> None:
    """
    Process a single scraped page result message.

    Uploads Markdown to Appwrite Storage, then writes metadata and child contacts
    to Postgres in a single atomic transaction.
    """
    raw_url: str = payload.get("url", "")
    if not raw_url:
        log.warning("[PERSIST] Result missing 'url'; skipping.")
        return

    canonical = canonicalize_url(raw_url)
    fingerprint = get_fingerprint(canonical)
    domain = payload.get("domain", "")

    # Ensure parent Crawl entity exists
    crawl_uuid = await ensure_crawl_record(session, crawl_id, domain=domain, target_url=raw_url)

    # 1. Dual-Tier Storage: Upload raw Markdown to Appwrite Storage
    raw_markdown: str = payload.get("raw_markdown", "") or ""
    markdown_file_id: Optional[str] = None
    markdown_bytes: int = len(raw_markdown.encode("utf-8")) if raw_markdown else 0
    # Rough token count estimate (~4 chars per token)
    markdown_tokens: int = len(raw_markdown) // 4 if raw_markdown else 0

    if raw_markdown:
        try:
            markdown_file_id = await storage_client.upload_markdown(
                crawl_id=crawl_id,
                fingerprint=fingerprint,
                markdown_text=raw_markdown,
            )
        except Exception as storage_exc:
            log.error("[PERSIST] Storage upload failed for %s: %s", canonical, storage_exc)
            # Re-raise so the message is not XACKed and will be retried
    # 2. Parse JSON states and audit metadata
    json_state = payload.get("json_state")
    if isinstance(json_state, str):
        try:
            json_state = json.loads(json_state)
        except Exception:
            json_state = None

    xhr_payloads = payload.get("xhr_payloads")
    if isinstance(xhr_payloads, str):
        try:
            xhr_payloads = json.loads(xhr_payloads)
        except Exception:
            xhr_payloads = None

    # Enforce 16 KB decoupling rule on XHR dumps
    if xhr_payloads:
        try:
            xhr_bytes_len = len(json.dumps(xhr_payloads, default=str).encode("utf-8"))
            if xhr_bytes_len > 16384:
                xhr_file_id = await storage_client.upload_json_payload(
                    crawl_id=crawl_id,
                    fingerprint=fingerprint,
                    suffix="xhr",
                    data=xhr_payloads,
                )
                xhr_payloads = [
                    {
                        "_appwrite_file_id": xhr_file_id,
                        "_byte_size": xhr_bytes_len,
                        "offloaded": True,
                    }
                ]
        except Exception as exc:
            log.warning("[PERSIST] Failed to offload oversized XHR to Appwrite: %s", exc)

    hydration_state = payload.get("hydration_state")
    if isinstance(hydration_state, str):
        try:
            hydration_state = json.loads(hydration_state)
        except Exception:
            hydration_state = None

    # Enforce 16 KB decoupling rule on Hydration trees
    if hydration_state:
        try:
            hyd_bytes_len = len(json.dumps(hydration_state, default=str).encode("utf-8"))
            if hyd_bytes_len > 16384:
                hyd_file_id = await storage_client.upload_json_payload(
                    crawl_id=crawl_id,
                    fingerprint=fingerprint,
                    suffix="hydration",
                    data=hydration_state,
                )
                hydration_state = {
                    "_appwrite_file_id": hyd_file_id,
                    "_byte_size": hyd_bytes_len,
                    "offloaded": True,
                }
        except Exception as exc:
            log.warning("[PERSIST] Failed to offload oversized hydration to Appwrite: %s", exc)

    extraction_methods = payload.get("extraction_methods", [])
    if isinstance(extraction_methods, str):
        try:
            extraction_methods = json.loads(extraction_methods)
        except Exception:
            extraction_methods = [extraction_methods]

    contacts_data = payload.get("contacts", {})
    if isinstance(contacts_data, str):
        try:
            contacts_data = json.loads(contacts_data)
        except Exception:
            contacts_data = {}

    metadata = payload.get("metadata", {})
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}

    screenshot_file_id = payload.get("screenshot_file_id") or None

    # 3. Upsert Page row in Postgres
    page_stmt = insert(Page).values(
        crawl_id=crawl_uuid,
        url=raw_url,
        canonical_url=canonical,
        path=payload.get("path"),
        status_code=int(payload.get("status_code", 200)),
        extraction_methods=extraction_methods,
        markdown_file_id=markdown_file_id,
        markdown_byte_size=markdown_bytes,
        markdown_token_count=markdown_tokens,
        summary=payload.get("summary"),
        json_state=json_state,
        hydration_state=hydration_state,
        xhr_payloads=xhr_payloads,
        screenshot_file_id=screenshot_file_id,
        metadata_=metadata,
        fetched_at=datetime.datetime.now(datetime.UTC),
    ).on_conflict_do_update(
        constraint="uq_crawl_canonical_url",
        set_={
            "status_code": int(payload.get("status_code", 200)),
            "extraction_methods": extraction_methods,
            "markdown_file_id": markdown_file_id,
            "markdown_byte_size": markdown_bytes,
            "markdown_token_count": markdown_tokens,
            "json_state": json_state,
            "hydration_state": hydration_state,
            "xhr_payloads": xhr_payloads,
            "screenshot_file_id": screenshot_file_id,
            "metadata": metadata,
            "fetched_at": datetime.datetime.now(datetime.UTC),
        },
    ).returning(Page.id)

    page_res = await session.execute(page_stmt)
    page_id = page_res.scalar_one()

    # 4. Insert extracted contacts
    emails = contacts_data.get("emails", [])
    phones = contacts_data.get("phones", [])

    for email in emails:
        session.add(PageContact(page_id=page_id, kind="email", value=email))
    for phone in phones:
        session.add(PageContact(page_id=page_id, kind="phone", value=phone))

    # 5. Increment crawl pages_processed statistic
    await session.execute(
        update(Crawl)
        .where(Crawl.id == crawl_uuid)
        .values(pages_processed=Crawl.pages_processed + 1)
    )

    # 6. Run deterministic audit rules and bulk-upsert findings (Phase 2)
    audit_metadata = metadata.get("audit") if isinstance(metadata, dict) else None
    if not audit_metadata:
        # Older payloads store audit keys at the top level of metadata
        audit_metadata = metadata if isinstance(metadata, dict) else {}

    findings_to_write = evaluate_findings(
        page_id=page_id,
        crawl_id=crawl_id,
        url=canonical,
        audit_metadata=audit_metadata,
        status_code=int(payload.get("status_code", 200)),
    )

    if findings_to_write:
        for finding in findings_to_write:
            finding_stmt = insert(AuditFinding).values(
                id=finding["id"],
                rule_id=finding["rule_id"],
                crawl_id=crawl_uuid,
                page_id=page_id,
                url=finding["url"],
                category=finding["category"],
                severity=finding["severity"],
                canvas_zone=finding["canvas_zone"],
                explanation=finding["explanation"],
                evidence=finding["evidence"],
                remediation=finding.get("remediation"),
                status="open",
                finding_metadata=finding.get("finding_metadata") or {},
            ).on_conflict_do_update(
                constraint="uq_page_rule",
                set_={
                    "explanation": finding["explanation"],
                    "evidence": finding["evidence"],
                    "remediation": finding.get("remediation"),
                    "severity": finding["severity"],
                },
            )
            await session.execute(finding_stmt)

        log.info(
            "[PERSIST] Wrote %d finding(s) for page %s (page_id=%s)",
            len(findings_to_write),
            canonical,
            page_id,
        )

    log.info(
        "[PERSIST] Saved page: id=%s url=%s md_file=%s contacts=(%d e, %d p)",
        page_id,
        canonical,
        markdown_file_id or "none",
        len(emails),
        len(phones),
    )


async def process_telemetry_message(session, crawl_id: str, payload: Dict[str, Any]) -> None:
    """Process a link drop telemetry message."""
    crawl_uuid = await ensure_crawl_record(session, crawl_id)
    session.add(
        DroppedTelemetry(
            crawl_id=crawl_uuid,
            source_url=payload.get("source_url"),
            target_url=payload.get("target_url"),
            drop_reason=payload.get("drop_reason", "UNKNOWN"),
            timestamp_utc=datetime.datetime.now(datetime.UTC),
        )
    )


async def process_dlq_message(session, crawl_id: str, payload: Dict[str, Any]) -> None:
    """Process a dead-letter queue message."""
    crawl_uuid = await ensure_crawl_record(session, crawl_id)
    session.add(
        DeadLetterTask(
            crawl_id=crawl_uuid,
            url=payload.get("url"),
            dlq_reason=payload.get("dlq_reason", "unknown_error"),
            task_fields=payload,
            failed_at_utc=datetime.datetime.now(datetime.UTC),
        )
    )
    await session.execute(
        update(Crawl)
        .where(Crawl.id == crawl_uuid)
        .values(pages_failed=Crawl.pages_failed + 1)
    )


async def persistence_loop(
    redis: aioredis.Redis,
    crawl_id: str,
    worker_id: str = "persist-0",
    auto_exit_on_drain: bool = False,
) -> None:
    """
    Continuous persistence loop consuming results, telemetry, and DLQ streams.
    When auto_exit_on_drain is True, detects when tasks and results streams are empty
    and triggers automatic consolidation.
    """
    group = persist_consumer_group_name(crawl_id)
    _res_stream = results_key(crawl_id)
    _tel_stream = telemetry_key(crawl_id)
    _dlq_stream = dlq_key(crawl_id)
    _tasks_stream = tasks_key(crawl_id)

    streams = {_res_stream: ">", _tel_stream: ">", _dlq_stream: ">"}
    session_factory = get_sessionmaker()

    log.info("[PERSIST] Listening on streams: %s (group: %s)", list(streams.keys()), group)
    idle_count = 0

    while True:
        try:
            raw_data = await redis.xreadgroup(
                groupname=group,
                consumername=worker_id,
                streams=streams,
                count=_BATCH_SIZE,
                block=_BLOCK_MS,
            )
        except Exception as exc:
            log.debug("[PERSIST] Stream poll: %s", exc)
            await asyncio.sleep(0.5)
            continue

        if not raw_data:
            if auto_exit_on_drain:
                idle_count += 1
                if idle_count >= 3:
                    try:
                        tasks_len = await redis.xlen(_tasks_stream)
                        if tasks_len == 0:
                            from app.consolidation import consolidate_crawl
                            crawl_uuid = uuid.UUID(crawl_id) if len(crawl_id) == 36 else uuid.uuid5(uuid.NAMESPACE_DNS, crawl_id)
                            async with session_factory() as session:
                                await consolidate_crawl(session, crawl_uuid, crawl_id)
                            log.info("[PERSIST] Crawl %s tasks and results drained. Auto-consolidated successfully.", crawl_id)
                            break
                    except Exception as drain_exc:
                        log.debug("[PERSIST] Drain check: %s", drain_exc)
            continue

        idle_count = 0

        for stream_name, messages in raw_data:
            if not messages:
                continue

            async with session_factory() as session:
                async with session.begin():
                    for message_id, payload in messages:
                        try:
                            if stream_name == _res_stream:
                                await process_result_message(session, crawl_id, message_id, payload)
                            elif stream_name == _tel_stream:
                                await process_telemetry_message(session, crawl_id, payload)
                            elif stream_name == _dlq_stream:
                                await process_dlq_message(session, crawl_id, payload)

                            # XACK on message success after commit
                            await redis.xack(stream_name, group, message_id)
                        except Exception as msg_exc:
                            log.error(
                                "[PERSIST] Error processing msg %s from %s: %s",
                                message_id,
                                stream_name,
                                msg_exc,
                                exc_info=True,
                            )
                            # Rollback transaction for this batch; uncommitted messages will remain in PEL
                            raise


async def run_persistence_worker_for_crawl(
    crawl_id: str,
    worker_id: Optional[str] = None,
    auto_exit_on_drain: bool = True,
) -> None:
    """
    Dedicated background persistence consumer for a specific crawl_id.
    Ensures persist consumer groups exist, consumes results, updates Postgres,
    and auto-consolidates when tasks and results are drained.
    """
    w_id = worker_id or f"persist-{crawl_id[:8]}"
    log.info("[PERSIST_SPAWNER] Starting persistence consumer %s for crawl %s...", w_id, crawl_id)
    redis = create_redis_pool(max_connections=5)
    try:
        await ensure_persist_consumer_groups(redis, crawl_id)
        await persistence_loop(
            redis=redis,
            crawl_id=crawl_id,
            worker_id=w_id,
            auto_exit_on_drain=auto_exit_on_drain,
        )
        log.info("[PERSIST_SPAWNER] Persistence consumer finished for crawl %s.", crawl_id)
    except asyncio.CancelledError:
        log.info("[PERSIST_SPAWNER] Persistence consumer cancelled for crawl %s.", crawl_id)
    except Exception as exc:
        log.error("[PERSIST_SPAWNER] Error in persistence consumer for crawl %s: %s", crawl_id, exc, exc_info=True)
    finally:
        await redis.aclose()


async def main() -> None:

    parser = argparse.ArgumentParser(description="Durable Persistence Consumer for Audit Crawler.")
    parser.add_argument(
        "--crawl-id",
        default=settings.CRAWL_ID,
        help=f"Crawl ID to consume for (default: {settings.CRAWL_ID})",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize database tables and exit.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s]:(%(name)s) %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.init_db:
        log.info("[DB] Initializing database tables...")
        from app.db.engine import close_engine
        try:
            await init_db()
            log.info("[DB] Database tables initialized successfully.")
        finally:
            await close_engine()
        return

    # Ensure DB tables exist
    await init_db()

    redis = create_redis_pool()
    try:
        await ensure_persist_consumer_groups(redis, args.crawl_id)
        await persistence_loop(redis, args.crawl_id)
    finally:
        await redis.aclose()
        from app.db.engine import close_engine
        await close_engine()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    asyncio.run(main())

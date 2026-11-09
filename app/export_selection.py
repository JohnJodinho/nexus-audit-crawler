"""Shared persisted-inventory selection for exports and previews."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from sqlalchemy import select

from app.api.url_patterns import (
    CompiledPattern,
    PatternDecision,
    compile_patterns,
    evaluate_url,
)
from app.models.schema import Crawl, Page, UrlInventory
from app.utils.utilities import canonicalize_url


@dataclass(frozen=True)
class SelectionItem:
    """One deterministic export or preview decision."""

    inventory: UrlInventory
    page: Page | None
    decision: str
    reason: str
    matched_rule: str | None
    content_status: str


@dataclass(frozen=True)
class SelectionResult:
    """Complete selection snapshot shared by preview and export."""

    items: tuple[SelectionItem, ...]
    grammar_version: str

    @property
    def counts(self) -> dict[str, int]:
        return {
            "total_candidates": len(self.items),
            "included": sum(item.decision == "included" for item in self.items),
            "excluded": sum(item.decision == "excluded" for item in self.items),
            "outside_scope": sum(item.decision == "outside_scope" for item in self.items),
            "unavailable_content": sum(
                item.content_status == "unavailable" for item in self.items
            ),
        }


def _scope_contains(scope_path: str, candidate_path: str) -> bool:
    if scope_path == "/":
        return candidate_path.startswith("/")
    return candidate_path == scope_path or candidate_path.startswith(f"{scope_path}/")


def _content_status(page: Page | None, format_type: str) -> str:
    if page is None:
        return "unavailable"
    if format_type == "markdown":
        return "available" if page.markdown_file_id else "unavailable"
    if format_type == "html":
        return "available" if (page.metadata_ or {}).get("html_file_id") else "unavailable"
    if format_type == "text":
        return "available" if page.markdown_file_id else "unavailable"
    return "unavailable"


def _candidate_query(crawl_uuid, origin: str, scope_path: str):
    return select(UrlInventory).where(
        UrlInventory.crawl_id == crawl_uuid,
        UrlInventory.origin == origin,
        UrlInventory.normalized_path.startswith(scope_path),
    ).order_by(UrlInventory.id.asc())


async def select_inventory(
    session,
    *,
    crawl: Crawl,
    scope: str | None,
    format_type: str,
    include_patterns: list[str],
    exclude_patterns: list[str],
    exclude_resource_types: list[str] | None = None,
) -> SelectionResult:
    """Select persisted URLs without fetching or enqueueing any crawl work."""
    target = canonicalize_url(scope or crawl.target_url)
    target_parts = urlsplit(target)
    target_origin = f"{target_parts.scheme}://{target_parts.netloc}"
    crawl_parts = urlsplit(canonicalize_url(crawl.target_url))
    crawl_origin = f"{crawl_parts.scheme}://{crawl_parts.netloc}"
    if target_origin != crawl_origin:
        raise ValueError("Scope must use the crawl origin.")

    scope_path = target_parts.path or "/"
    include_rules = compile_patterns(
        include_patterns,
        rule_type="include",
        target_origin=crawl_origin,
    )
    exclude_rules = compile_patterns(
        exclude_patterns,
        rule_type="exclude",
        target_origin=crawl_origin,
    )
    inventories = list(
        (await session.execute(_candidate_query(crawl.id, crawl_origin, scope_path)))
        .scalars()
        .all()
    )
    if not inventories:
        raise ValueError("CRAWL_INVENTORY_UNAVAILABLE: no persisted URL inventory exists.")
    page_ids = [item.page_id for item in inventories if item.page_id is not None]
    pages = {}
    if page_ids:
        pages = {
            page.id: page
            for page in (
                await session.execute(select(Page).where(Page.id.in_(page_ids)))
            ).scalars().all()
        }

    items: list[SelectionItem] = []
    for inventory in inventories:
        page = pages.get(inventory.page_id)
        if not _scope_contains(scope_path, inventory.normalized_path):
            items.append(
                SelectionItem(
                    inventory, page, "outside_scope", "outside_scope", None,
                    _content_status(page, format_type),
                )
            )
            continue
        if inventory.processing_status in {"excluded", "failed"}:
            items.append(
                SelectionItem(
                    inventory, page, "excluded", inventory.exclusion_reason or "crawl_restriction",
                    inventory.matched_rule_id, _content_status(page, format_type),
                )
            )
            continue
        decision: PatternDecision = evaluate_url(
            inventory.normalized_url,
            target_origin=crawl_origin,
            include_rules=include_rules,
            exclude_rules=exclude_rules,
            exclude_resource_types=tuple(exclude_resource_types or ()),
        )
        items.append(
            SelectionItem(
                inventory,
                page,
                decision.decision,
                decision.reason,
                decision.matched_rule.rule_id if decision.matched_rule else None,
                _content_status(page, format_type),
            )
        )
    return SelectionResult(tuple(items), "1")

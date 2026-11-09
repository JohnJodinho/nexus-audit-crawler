import datetime
import uuid

import pytest
from sqlalchemy.dialects import postgresql

from app.export_selection import select_inventory
from app.export_selection import _candidate_query
from app.models.schema import Crawl, Page, UrlInventory


class _Result:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return self._values


class _Session:
    def __init__(self, values):
        self.values = values

    async def execute(self, _statement):
        return _Result(self.values.pop(0))


@pytest.mark.asyncio
async def test_selection_reuses_persisted_inventory_and_pattern_engine():
    crawl_uuid = uuid.uuid4()
    crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com",
        target_domain="example.com",
    )
    page = Page(
        id=7,
        crawl_id=crawl_uuid,
        url="https://example.com/courses/python",
        canonical_url="https://example.com/courses/python",
        markdown_file_id="md-7",
        fetched_at=datetime.datetime.now(datetime.UTC),
    )
    inventory = UrlInventory(
        id=1,
        crawl_id=crawl_uuid,
        original_url=page.url,
        normalized_url=page.canonical_url,
        origin="https://example.com",
        normalized_path="/courses/python",
        query_parameters={},
        page_id=7,
        discovery_status="processed",
        processing_status="processed",
    )
    result = await select_inventory(
        _Session([[inventory], [page]]),
        crawl=crawl,
        scope="https://example.com/courses",
        format_type="markdown",
        include_patterns=["/courses/**"],
        exclude_patterns=[],
    )

    assert result.counts["included"] == 1
    assert result.items[0].content_status == "available"
    assert result.items[0].matched_rule is not None


@pytest.mark.asyncio
async def test_root_scope_includes_every_path_on_the_crawl_origin():
    crawl_uuid = uuid.uuid4()
    crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com/",
        target_domain="example.com",
    )
    paths = ["/about", "/courses", "/facilities", "legacy-path-without-leading-slash"]
    inventories = [
        UrlInventory(
            id=index,
            crawl_id=crawl_uuid,
            original_url=f"https://example.com/{path.lstrip('/')}",
            normalized_url=f"https://example.com/{path.lstrip('/')}",
            origin="https://example.com",
            normalized_path=path,
            query_parameters={},
            page_id=None,
            discovery_status="processed",
            processing_status="processed",
        )
        for index, path in enumerate(paths, start=1)
    ]

    result = await select_inventory(
        _Session([inventories, []]),
        crawl=crawl,
        scope="https://example.com",
        format_type="markdown",
        include_patterns=[],
        exclude_patterns=[],
    )

    assert result.counts["included"] == 4
    assert result.counts["outside_scope"] == 0
    assert [item.decision for item in result.items] == ["included"] * 4


@pytest.mark.asyncio
async def test_root_scope_includes_persisted_pages_despite_stale_inventory_status():
    crawl_uuid = uuid.uuid4()
    crawl = Crawl(
        id=crawl_uuid,
        target_url="https://example.com/",
        target_domain="example.com",
    )
    paths = ["/", "/about", "/courses"]
    pages = [
        Page(
            id=index,
            crawl_id=crawl_uuid,
            url=f"https://example.com{path}",
            canonical_url=f"https://example.com{path}",
            markdown_file_id=f"md-{index}",
            fetched_at=datetime.datetime.now(datetime.UTC),
        )
        for index, path in enumerate(paths, start=1)
    ]
    inventories = [
        UrlInventory(
            id=index,
            crawl_id=crawl_uuid,
            original_url=page.url,
            normalized_url=page.canonical_url,
            origin="https://example.com",
            normalized_path=paths[index - 1],
            query_parameters={},
            page_id=page.id,
            discovery_status="processed",
            processing_status="excluded" if index > 1 else "processed",
        )
        for index, page in enumerate(pages, start=1)
    ]

    result = await select_inventory(
        _Session([inventories, pages]),
        crawl=crawl,
        scope="https://example.com",
        format_type="markdown",
        include_patterns=[],
        exclude_patterns=[],
    )

    assert result.counts["included"] == 3
    assert result.counts["excluded"] == 0
    assert [item.content_status for item in result.items] == ["available"] * 3


def test_root_scope_candidate_query_does_not_filter_by_path_prefix():
    statement = _candidate_query(uuid.uuid4(), "https://example.com", "/")
    sql = str(statement.compile(dialect=postgresql.dialect()))

    assert "url_inventory.normalized_path LIKE" not in sql
    assert "url_inventory.crawl_id" in sql
    assert "url_inventory.origin" in sql

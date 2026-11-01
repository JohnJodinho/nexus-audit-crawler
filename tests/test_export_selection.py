import datetime
import uuid

import pytest

from app.export_selection import select_inventory
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

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.spider import AuditSpider


@pytest.mark.asyncio
async def test_dynamic_interaction_is_bounded_and_returns_rendered_html(monkeypatch):
    page = MagicMock()
    page.evaluate = AsyncMock()
    page.content = AsyncMock(return_value="<html><body>revealed</body></html>")
    response = MagicMock(url="https://example.com/about", page=page)

    spider = AuditSpider(
        redis_client=MagicMock(),
        crawl_id="test",
        task_domain="example.com",
    )
    monkeypatch.setattr(
        "app.spider.settings.DYNAMIC_INTERACTION_ENABLED",
        True,
    )

    html = await spider._explore_rendered_page(response)

    assert html == b"<html><body>revealed</body></html>"
    page.evaluate.assert_awaited_once()
    page.content.assert_awaited_once()


@pytest.mark.asyncio
async def test_dynamic_interaction_is_opt_out(monkeypatch):
    page = MagicMock()
    page.evaluate = AsyncMock()
    response = MagicMock(url="https://example.com/about", page=page)
    spider = AuditSpider(
        redis_client=MagicMock(),
        crawl_id="test",
        task_domain="example.com",
    )
    monkeypatch.setattr(
        "app.spider.settings.DYNAMIC_INTERACTION_ENABLED",
        False,
    )

    assert await spider._explore_rendered_page(response) is None
    page.evaluate.assert_not_awaited()

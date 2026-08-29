"""
tests/test_utils.py
===================
Tests for canonicalize_url(), get_fingerprint(), and _route_to_dlq().
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from app.utils.utilities import canonicalize_url, get_fingerprint, _route_to_dlq
from app.redis_client import dlq_key


# ---------------------------------------------------------------------------
# canonicalize_url
# ---------------------------------------------------------------------------

class TestCanonicalizeUrl:
    def test_lowercase_scheme(self):
        assert canonicalize_url("HTTPS://example.com/") == "https://example.com/"

    def test_lowercase_hostname(self):
        assert canonicalize_url("https://Example.COM/about") == "https://example.com/about"

    def test_strips_trailing_slash(self):
        assert canonicalize_url("https://example.com/about/") == "https://example.com/about"

    def test_preserves_root_slash(self):
        assert canonicalize_url("https://example.com/") == "https://example.com/"

    def test_strips_fragment(self):
        assert canonicalize_url("https://example.com/about#section") == "https://example.com/about"

    def test_strips_default_https_port(self):
        assert canonicalize_url("https://example.com:443/page") == "https://example.com/page"

    def test_strips_default_http_port(self):
        assert canonicalize_url("http://example.com:80/page") == "http://example.com/page"

    def test_preserves_non_default_port(self):
        result = canonicalize_url("https://example.com:8443/page")
        assert "8443" in result

    def test_preserves_query_string(self):
        url = "https://example.com/search?q=audit&page=2"
        assert "q=audit" in canonicalize_url(url)

    def test_equivalent_urls_canonicalize_to_same(self):
        variants = [
            "HTTPS://Example.COM/about/",
            "https://example.com:443/about/",
            "https://example.com/about/#top",
            "https://example.com/about",
        ]
        canonical_forms = {canonicalize_url(u) for u in variants}
        assert len(canonical_forms) == 1, f"Expected 1 canonical form, got: {canonical_forms}"


# ---------------------------------------------------------------------------
# get_fingerprint
# ---------------------------------------------------------------------------

class TestGetFingerprint:
    def test_returns_64_char_hex(self):
        fp = get_fingerprint("https://example.com/about")
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)

    def test_deterministic(self):
        url = "https://example.com/about"
        assert get_fingerprint(url) == get_fingerprint(url)

    def test_equivalent_urls_same_fingerprint(self):
        assert (
            get_fingerprint("https://Example.COM/about/")
            == get_fingerprint("https://example.com/about")
        )

    def test_different_urls_different_fingerprints(self):
        assert get_fingerprint("https://example.com/about") != get_fingerprint(
            "https://example.com/contact"
        )

    def test_fragment_ignored(self):
        assert (
            get_fingerprint("https://example.com/about#section1")
            == get_fingerprint("https://example.com/about#section2")
        )

    def test_default_port_ignored(self):
        assert (
            get_fingerprint("https://example.com:443/about")
            == get_fingerprint("https://example.com/about")
        )


# ---------------------------------------------------------------------------
# _route_to_dlq
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_route_to_dlq_publishes_event(async_fake_redis, crawl_id):
    task_fields = {
        "schema_version": "1",
        "crawl_id": crawl_id,
        "url": "https://example.com/broken",
        "depth": "1",
        "retry_count": "2",
    }
    _dlq = dlq_key(crawl_id)

    await _route_to_dlq(async_fake_redis, task_fields, "test_reason", dlq_key=_dlq)

    messages = await async_fake_redis.xrange(_dlq, "-", "+")
    assert len(messages) == 1, "Expected exactly one DLQ message"

    _msg_id, payload = messages[0]
    assert payload["url"] == "https://example.com/broken"
    assert payload["dlq_reason"] == "test_reason"
    assert payload["schema_version"] == "1"
    assert "dlq_at_utc" in payload


@pytest.mark.asyncio
async def test_route_to_dlq_preserves_original_fields(async_fake_redis, crawl_id):
    task_fields = {
        "url": "https://example.com/page",
        "retry_count": "3",
        "domain": "example.com",
        "crawl_id": crawl_id,
    }
    _dlq = dlq_key(crawl_id)

    await _route_to_dlq(async_fake_redis, task_fields, "spider_error", dlq_key=_dlq)

    messages = await async_fake_redis.xrange(_dlq, "-", "+")
    _msg_id, payload = messages[0]
    assert payload["retry_count"] == "3"
    assert payload["domain"] == "example.com"

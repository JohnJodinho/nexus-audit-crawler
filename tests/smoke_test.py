"""
tests/smoke_test.py
===================
Migrated smoke tests, now running under pytest.

These tests replaced the original script-style runner that crashed on import
due to the stale ALLOW_PATTERN reference (removed when the URL filtering
model was updated from an allow-list to a deny-list in Phase 0).
"""

from __future__ import annotations

import logging

import pytest

from app.spider import DENY_PATTERN
from app.extraction import extract_from_hydration, extract_from_xhr, extract_via_markitdown
from app.logger import LOG_FILE_PATH, get_pipeline_logger


# ---------------------------------------------------------------------------
# Extraction smoke tests
# ---------------------------------------------------------------------------

class FakeXHR:
    def __init__(self, url: str, payload):
        self.url = url
        self._payload = payload

    def json(self):
        return self._payload


def test_xhr_extraction_valid_response():
    """XHR extraction should collect valid non-tracking responses."""
    xhr = FakeXHR("https://api.example.com/products", {"products": [{"name": "Widget", "price": 99}], "total": 1})
    result = extract_from_xhr([xhr])
    assert len(result) == 1
    assert "products" in result[0]


def test_xhr_extraction_tracking_url_rejected():
    """XHR extraction must reject known analytics/tracking URLs."""
    xhr = FakeXHR("https://www.google-analytics.com/collect", {"cid": "123", "t": "pageview"})
    result = extract_from_xhr([xhr])
    assert result == [], f"Expected empty list, got: {result}"


def test_hydration_next_data():
    """Hydration extraction should detect __NEXT_DATA__ embedded JSON."""
    html = (
        '<html><script id="__NEXT_DATA__" type="application/json">'
        '{"props":{"pageProps":{"title":"Test","pricing":{"monthly":99}}}}'
        "</script></html>"
    )
    result = extract_from_hydration(html)
    assert result is not None
    assert "props" in result


def test_markitdown_converts_html():
    """MarkItDown should convert HTML bytes to readable Markdown."""
    html_bytes = b"<html><body><h1>Enterprise AI</h1><p>Our pricing starts at $99/mo.</p></body></html>"
    result = extract_via_markitdown(html_bytes)
    assert result is not None
    assert "Enterprise AI" in result


# ---------------------------------------------------------------------------
# URL boundary pattern smoke tests (deny-list only, no allow-list)
# ---------------------------------------------------------------------------

def test_deny_pattern_matches_blog():
    assert DENY_PATTERN.search("/blog/post-1")


def test_deny_pattern_matches_privacy():
    assert DENY_PATTERN.search("/privacy-policy")


def test_deny_pattern_matches_login():
    assert DENY_PATTERN.search("/login")


def test_deny_pattern_matches_careers():
    assert DENY_PATTERN.search("/careers/senior-engineer")


def test_deny_pattern_does_not_match_about():
    assert not DENY_PATTERN.search("/about")


def test_deny_pattern_does_not_match_pricing():
    assert not DENY_PATTERN.search("/pricing")


def test_deny_pattern_does_not_match_services():
    assert not DENY_PATTERN.search("/services")


# ---------------------------------------------------------------------------
# Logger module smoke tests
# ---------------------------------------------------------------------------

def test_log_file_path_not_empty():
    assert LOG_FILE_PATH


def test_pipeline_logger_returns_logger():
    logger = get_pipeline_logger("smoke.test.pipeline")
    assert isinstance(logger, logging.Logger)

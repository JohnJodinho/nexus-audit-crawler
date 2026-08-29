"""
tests/test_extraction.py
========================
Tests for the cumulative Triple-Threat extraction pipeline.

Covers:
- XHR tracking deny-list
- XHR accumulation (all valid responses collected)
- Hydration pattern detection
- MarkItDown conversion
- run_triple_threat accumulation (all three fields present)
- Drop condition (raw_markdown empty + no JSON)
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from app.extraction import (
    _is_tracking_xhr,
    extract_from_hydration,
    extract_from_xhr,
    extract_via_markitdown,
    run_triple_threat,
    _determine_methods,
)


# ---------------------------------------------------------------------------
# XHR tracking deny-list tests
# ---------------------------------------------------------------------------

class TestIsTrackingXhr:
    def test_google_analytics_blocked(self):
        assert _is_tracking_xhr("https://www.google-analytics.com/collect")

    def test_googletagmanager_blocked(self):
        assert _is_tracking_xhr("https://www.googletagmanager.com/gtag/js?id=G-XXXX")

    def test_hotjar_blocked(self):
        assert _is_tracking_xhr("https://insights.hotjar.com/api/v1/event")

    def test_segment_blocked(self):
        assert _is_tracking_xhr("https://api.segment.io/v1/track")

    def test_facebook_pixel_blocked(self):
        assert _is_tracking_xhr("https://www.facebook.com/tr?id=123")

    def test_sentry_blocked(self):
        assert _is_tracking_xhr("https://o123.ingest.sentry.io/api/event")

    def test_beacon_blocked(self):
        assert _is_tracking_xhr("https://example.com/beacon?data=xyz")

    def test_telemetry_blocked(self):
        assert _is_tracking_xhr("https://example.com/api/telemetry/report")

    def test_legitimate_api_allowed(self):
        assert not _is_tracking_xhr("https://api.example.com/products")

    def test_internal_api_allowed(self):
        assert not _is_tracking_xhr("https://example.com/api/v2/pricing")

    def test_cdn_content_allowed(self):
        assert not _is_tracking_xhr("https://cdn.example.com/data/config.json")


# ---------------------------------------------------------------------------
# XHR extraction tests
# ---------------------------------------------------------------------------

class FakeXHR:
    """Minimal XHR response stub."""
    def __init__(self, url: str, payload: Any):
        self.url = url
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class TestExtractFromXhr:
    def test_collects_all_valid_responses(self):
        xhrs = [
            FakeXHR("https://api.example.com/products", {"products": [1, 2], "total": 2}),
            FakeXHR("https://api.example.com/pricing", {"plans": ["basic", "pro"], "currency": "USD"}),
        ]
        result = extract_from_xhr(xhrs)
        assert len(result) == 2

    def test_filters_tracking_urls(self):
        xhrs = [
            FakeXHR("https://www.google-analytics.com/collect", {"cid": "123", "t": "pageview"}),
            FakeXHR("https://api.example.com/products", {"products": [1, 2], "total": 2}),
        ]
        result = extract_from_xhr(xhrs)
        assert len(result) == 1
        assert result[0].get("products") is not None

    def test_filters_trivial_heartbeats(self):
        xhrs = [
            FakeXHR("https://api.example.com/ping", {"ok": True}),  # 1 key: rejected
            FakeXHR("https://api.example.com/data", {"items": [1], "count": 1}),  # 2 keys: accepted
        ]
        result = extract_from_xhr(xhrs)
        assert len(result) == 1
        assert "count" in result[0]

    def test_empty_list_returns_empty(self):
        assert extract_from_xhr([]) == []

    def test_none_list_returns_empty(self):
        assert extract_from_xhr(None) == []

    def test_non_dict_responses_skipped(self):
        xhrs = [
            FakeXHR("https://api.example.com/list", [1, 2, 3]),  # list, not dict
            FakeXHR("https://api.example.com/data", {"items": [1], "count": 1}),
        ]
        result = extract_from_xhr(xhrs)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Hydration extraction tests
# ---------------------------------------------------------------------------

class TestExtractFromHydration:
    def test_next_data_detected(self):
        html = (
            '<html><script id="__NEXT_DATA__" type="application/json">'
            '{"props":{"pageProps":{"title":"Test","price":99}}}'
            "</script></html>"
        )
        result = extract_from_hydration(html)
        assert result is not None
        assert "props" in result

    def test_nuxt_state_detected(self):
        html = 'window.__NUXT__ = {"state":{"product":"widget"},"data":{"version":1}};'
        result = extract_from_hydration(html)
        assert result is not None

    def test_no_hydration_returns_none(self):
        html = "<html><body><h1>Plain page</h1></body></html>"
        result = extract_from_hydration(html)
        assert result is None

    def test_empty_string_returns_none(self):
        assert extract_from_hydration("") is None

    def test_invalid_json_returns_none(self):
        html = '<script id="__NEXT_DATA__" type="application/json">{broken json}</script>'
        result = extract_from_hydration(html)
        assert result is None


# ---------------------------------------------------------------------------
# MarkItDown extraction tests
# ---------------------------------------------------------------------------

class TestExtractViaMarkitdown:
    def test_converts_html_to_markdown(self):
        html_bytes = b"<html><body><h1>Enterprise AI</h1><p>Price: $99/mo.</p></body></html>"
        result = extract_via_markitdown(html_bytes)
        assert result is not None
        assert "Enterprise AI" in result

    def test_empty_bytes_returns_none(self):
        assert extract_via_markitdown(b"") is None

    def test_none_returns_none(self):
        assert extract_via_markitdown(None) is None

    def test_whitespace_only_returns_none(self):
        result = extract_via_markitdown(b"   ")
        # MarkItDown on whitespace only produces empty output
        assert result is None or not result.strip()


# ---------------------------------------------------------------------------
# _determine_methods tests
# ---------------------------------------------------------------------------

class TestDetermineMethods:
    def test_all_present(self):
        methods = _determine_methods([{"a": 1}], {"b": 2}, "# Heading")
        assert set(methods) == {"xhr", "hydration", "markitdown"}

    def test_only_markitdown(self):
        assert _determine_methods([], None, "# Some text") == ["markitdown"]

    def test_none_returns_none_list(self):
        assert _determine_methods([], None, "") == ["none"]

    def test_xhr_only(self):
        methods = _determine_methods([{"a": 1}], None, "")
        assert "xhr" in methods
        assert "markitdown" not in methods


# ---------------------------------------------------------------------------
# run_triple_threat integration tests
# ---------------------------------------------------------------------------

class FakeResponse:
    """Minimal Scrapling Response stub."""
    def __init__(
        self,
        url: str = "https://example.com",
        body: bytes = b"<html><body><h1>Hello</h1></body></html>",
        captured_xhr: Optional[List[Any]] = None,
        encoding: str = "utf-8",
        status: int = 200,
    ):
        self.url = url
        self.body = body
        self.captured_xhr = captured_xhr or []
        self.encoding = encoding
        self.status = status


class TestRunTripleThreat:
    def test_always_runs_markitdown(self):
        """MarkItDown should always run even if XHR and Hydration are absent."""
        response = FakeResponse(body=b"<html><body><h1>Test</h1></body></html>")
        result = run_triple_threat(response)
        assert result["raw_markdown"] != ""
        assert "Test" in result["raw_markdown"]

    def test_always_returns_all_keys(self):
        """Result must always have all four extraction fields."""
        response = FakeResponse()
        result = run_triple_threat(response)
        assert "url" in result
        assert "xhr_payloads" in result
        assert "hydration_state" in result
        assert "raw_markdown" in result
        assert "extraction_methods" in result

    def test_xhr_and_markitdown_both_present(self):
        """When XHR succeeds, MarkItDown should also have been run."""
        xhrs = [FakeXHR("https://api.example.com/data", {"products": [1, 2], "count": 2})]
        response = FakeResponse(
            body=b"<html><body><h1>Products</h1></body></html>",
            captured_xhr=xhrs,
        )
        result = run_triple_threat(response)
        assert len(result["xhr_payloads"]) > 0
        assert result["raw_markdown"].strip() != ""
        assert "xhr" in result["extraction_methods"]
        assert "markitdown" in result["extraction_methods"]

    def test_hydration_and_markitdown_both_present(self):
        """When Hydration succeeds, MarkItDown should also have been run."""
        body = (
            b'<html><script id="__NEXT_DATA__" type="application/json">'
            b'{"props":{"title":"Pricing"},"data":{"price":99}}'
            b"</script><body><h1>Pricing</h1></body></html>"
        )
        response = FakeResponse(body=body)
        result = run_triple_threat(response)
        assert result["hydration_state"] is not None
        assert result["raw_markdown"].strip() != ""
        assert "hydration" in result["extraction_methods"]
        assert "markitdown" in result["extraction_methods"]

    def test_tracking_xhr_excluded(self):
        """Tracking XHR responses must not appear in xhr_payloads."""
        xhrs = [
            FakeXHR("https://www.google-analytics.com/collect", {"cid": "123", "hit": "pv"}),
            FakeXHR("https://api.example.com/products", {"items": [1, 2], "count": 2}),
        ]
        response = FakeResponse(captured_xhr=xhrs)
        result = run_triple_threat(response)
        # Only the products response should appear
        assert len(result["xhr_payloads"]) == 1
        assert "items" in result["xhr_payloads"][0]

"""
tests/test_audits.py
====================
Tests for the deterministic audit rules engine (Phase 2).
Verifies that each rule fires correctly and that the AuditFinding id/shape is correct.
"""

from __future__ import annotations

import pytest

from app.audit.rules import evaluate_findings


# ---------------------------------------------------------------------------
# Helper: build a minimal compile_full_audit()-shaped dict
# ---------------------------------------------------------------------------

def _make_metadata(
    h1_count: int = 1,
    title: str = "My Page",
    meta_desc: str = "A description.",
    canonical_status: str = "matches_url",
    word_count: int = 500,
    missing_alt: int = 0,
    json_ld_schemas: list | None = None,
    missing_headers: list | None = None,
    response_time_ms: float = 200.0,
    dom_complete_ms: float | None = None,
) -> dict:
    return {
        "seo": {
            "title": title,
            "meta_description": meta_desc,
            "canonical_status": canonical_status,
            "headings": {
                "h1_count": h1_count,
                "h1_texts": ["Heading"] * h1_count,
                "h2_count": 2,
                "h3_count": 0,
                "heading_order_valid": True,
            },
            "images": {"total": 3, "missing_alt": missing_alt, "alt_coverage_pct": 100},
            "content": {"word_count": word_count, "character_count": word_count * 5, "is_thin_content": word_count < 150},
            "json_ld_schemas": json_ld_schemas if json_ld_schemas is not None else [{"@type": "Organization"}],
            "schema_types": ["Organization"],
        },
        "security": {
            "security_score": 100 if not missing_headers else 0,
            "missing_headers": missing_headers or [],
            "present_headers": {},
            "insecure_cookies": [],
        },
        "runtime": {
            "response_time_ms": response_time_ms,
            "dom_complete_ms": dom_complete_ms,
            "js_error_count": 0,
        },
    }


_PAGE_ID = 456
_CRAWL_ID = "test-crawl-uuid"
_URL = "https://example.com/about"


class TestFindingIdScheme:
    def test_id_is_page_id_colon_rule_id(self):
        findings = evaluate_findings(
            page_id=_PAGE_ID,
            crawl_id=_CRAWL_ID,
            url=_URL,
            audit_metadata=_make_metadata(h1_count=0),
        )
        h1_finding = next((f for f in findings if f["rule_id"] == "seo.missing_h1"), None)
        assert h1_finding is not None
        assert h1_finding["id"] == f"{_PAGE_ID}:seo.missing_h1"

    def test_page_id_2_has_different_id(self):
        f1 = evaluate_findings(1, _CRAWL_ID, _URL, _make_metadata(h1_count=0))
        f2 = evaluate_findings(2, _CRAWL_ID, _URL, _make_metadata(h1_count=0))
        assert f1[0]["id"] != f2[0]["id"]
        assert f1[0]["rule_id"] == f2[0]["rule_id"]  # same taxonomy code

    def test_status_is_always_open(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(h1_count=0))
        assert all(f["status"] == "open" for f in findings)


class TestSEORules:
    def test_missing_h1_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(h1_count=0))
        rule_ids = [f["rule_id"] for f in findings]
        assert "seo.missing_h1" in rule_ids

    def test_no_missing_h1_when_present(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(h1_count=1))
        assert "seo.missing_h1" not in [f["rule_id"] for f in findings]

    def test_multiple_h1_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(h1_count=3))
        rule_ids = [f["rule_id"] for f in findings]
        assert "seo.multiple_h1" in rule_ids
        assert "seo.missing_h1" not in rule_ids

    def test_missing_title_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(title=""))
        assert "seo.missing_title" in [f["rule_id"] for f in findings]

    def test_missing_meta_description_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(meta_desc=""))
        assert "seo.missing_meta_description" in [f["rule_id"] for f in findings]

    def test_missing_canonical_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(canonical_status="missing"))
        assert "seo.missing_canonical" in [f["rule_id"] for f in findings]

    def test_thin_content_fires_under_300_words(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(word_count=150), status_code=200)
        assert "seo.thin_content" in [f["rule_id"] for f in findings]

    def test_thin_content_does_not_fire_for_404(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(word_count=50), status_code=404)
        assert "seo.thin_content" not in [f["rule_id"] for f in findings]

    def test_images_missing_alt_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(missing_alt=3))
        assert "seo.images_missing_alt" in [f["rule_id"] for f in findings]

    def test_missing_schema_org_fires_when_no_json_ld(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(json_ld_schemas=[]))
        assert "seo.missing_schema_org" in [f["rule_id"] for f in findings]

    def test_no_missing_schema_org_when_present(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(json_ld_schemas=[{"@type": "Organization"}]))
        assert "seo.missing_schema_org" not in [f["rule_id"] for f in findings]


class TestSecurityRules:
    def test_missing_hsts_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(missing_headers=["HSTS"]))
        assert "security.missing_hsts" in [f["rule_id"] for f in findings]

    def test_missing_csp_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(missing_headers=["CSP"]))
        assert "security.missing_csp" in [f["rule_id"] for f in findings]

    def test_missing_x_frame_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(missing_headers=["X-Frame-Options"]))
        assert "security.missing_x_frame" in [f["rule_id"] for f in findings]

    def test_missing_x_content_type_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(missing_headers=["X-Content-Type-Options"]))
        assert "security.missing_x_content_type" in [f["rule_id"] for f in findings]

    def test_missing_referrer_policy_fires(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(missing_headers=["Referrer-Policy"]))
        assert "security.missing_referrer_policy" in [f["rule_id"] for f in findings]

    def test_all_security_headers_present_no_security_findings(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(missing_headers=[]))
        security_findings = [f for f in findings if f["category"] == "security"]
        assert security_findings == []


class TestPerformanceRules:
    def test_slow_response_fires_above_800ms(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(response_time_ms=1200.0))
        assert "performance.slow_response" in [f["rule_id"] for f in findings]

    def test_slow_response_does_not_fire_at_799ms(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(response_time_ms=799.0))
        assert "performance.slow_response" not in [f["rule_id"] for f in findings]

    def test_slow_load_fires_above_3000ms(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(dom_complete_ms=4500.0))
        assert "performance.slow_load" in [f["rule_id"] for f in findings]

    def test_slow_load_not_fire_when_none(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(dom_complete_ms=None))
        assert "performance.slow_load" not in [f["rule_id"] for f in findings]


class TestStructureRules:
    def test_broken_link_fires_on_404(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(), status_code=404)
        assert "structure.broken_link" in [f["rule_id"] for f in findings]

    def test_broken_link_fires_on_410(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(), status_code=410)
        assert "structure.broken_link" in [f["rule_id"] for f in findings]

    def test_no_broken_link_on_200(self):
        findings = evaluate_findings(_PAGE_ID, _CRAWL_ID, _URL, _make_metadata(), status_code=200)
        assert "structure.broken_link" not in [f["rule_id"] for f in findings]


class TestCanvasZoneEnum:
    def test_canvas_zones_are_only_valid_values(self):
        valid_zones = {"nav", "head", "content", "footer", "server"}
        all_headers_missing = ["HSTS", "CSP", "X-Frame-Options", "X-Content-Type-Options", "Referrer-Policy"]
        findings = evaluate_findings(
            _PAGE_ID, _CRAWL_ID, _URL,
            _make_metadata(h1_count=0, title="", meta_desc="", canonical_status="missing",
                           word_count=50, missing_alt=2, json_ld_schemas=[],
                           missing_headers=all_headers_missing,
                           response_time_ms=1000.0, dom_complete_ms=5000.0),
            status_code=404,
        )
        for f in findings:
            assert f["canvas_zone"] in valid_zones, f"Invalid canvas_zone: {f['canvas_zone']} in {f['rule_id']}"

    def test_no_findings_for_perfect_page(self):
        findings = evaluate_findings(
            _PAGE_ID, _CRAWL_ID, _URL,
            _make_metadata(
                h1_count=1, title="Good title", meta_desc="Good description.", canonical_status="matches_url",
                word_count=600, missing_alt=0, json_ld_schemas=[{"@type": "Organization"}],
                missing_headers=[], response_time_ms=200.0, dom_complete_ms=1000.0,
            ),
            status_code=200,
        )
        assert findings == []

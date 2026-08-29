"""
tests/test_audits.py
====================
Unit tests for the audit signals extractor (Phase 3).
"""

import pytest

from app.utils.audits import (
    compile_full_audit,
    extract_runtime_audit,
    extract_security_audit,
    extract_seo_audit,
)


def test_extract_security_audit_all_present():
    headers = {
        "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
        "Content-Security-Policy": "default-src 'self'",
        "X-Frame-Options": "DENY",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "Permissions-Policy": "camera=(), microphone=()",
        "Set-Cookie": "session=abc; Secure; HttpOnly; SameSite=Strict",
        "Content-Encoding": "br",
    }
    sec = extract_security_audit(headers)
    assert sec["security_score"] == 100
    assert len(sec["missing_headers"]) == 0
    assert len(sec["insecure_cookies"]) == 0
    assert sec["compression"] == "br"
    assert sec["is_compressed"] is True


def test_extract_security_audit_missing_and_insecure():
    headers = {
        "X-Content-Type-Options": "nosniff",
        "Set-Cookie": "session=abc; Path=/",
    }
    sec = extract_security_audit(headers)
    assert sec["security_score"] < 50
    assert "HSTS" in sec["missing_headers"]
    assert "CSP" in sec["missing_headers"]
    assert len(sec["insecure_cookies"]) == 1
    assert "missing Secure" in sec["insecure_cookies"][0]
    assert sec["is_compressed"] is False


def test_extract_seo_audit_json_ld_and_headings():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Law Firm - Global Dispute Experts</title>
        <meta name="description" content="Top tier litigation and dispute resolution attorneys.">
        <link rel="canonical" href="https://example.com/about">
        <link rel="alternate" hreflang="en-GB" href="https://example.com/uk/about">
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "LegalService",
            "name": "Global Law LLC",
            "url": "https://example.com"
        }
        </script>
    </head>
    <body>
        <h1>International Dispute Resolution</h1>
        <h2>Arbitration</h2>
        <h2>Litigation</h2>
        <img src="/logo.png" alt="Company Logo" />
        <img src="/banner.jpg" />
        <p>Expert legal counsel for global corporate clients.</p>
    </body>
    </html>
    """
    headers = {"X-Robots-Tag": "index, follow"}
    markdown = "# International Dispute Resolution\n\nExpert legal counsel."

    seo = extract_seo_audit(
        html_text=html,
        headers=headers,
        canonical_url="https://example.com/about",
        markdown_text=markdown,
        request_url="https://example.com/about",
    )

    assert seo["title"] == "Law Firm - Global Dispute Experts"
    assert seo["meta_description"] == "Top tier litigation and dispute resolution attorneys."
    assert seo["canonical_status"] == "matches_url"
    assert seo["is_indexable"] is True
    assert seo["robots_conflict"] is False

    # Headings
    assert seo["headings"]["h1_count"] == 1
    assert seo["headings"]["h1_texts"] == ["International Dispute Resolution"]
    assert seo["headings"]["h2_count"] == 2
    assert seo["headings"]["heading_order_valid"] is True

    # JSON-LD Schema
    assert "LegalService" in seo["schema_types"]
    assert len(seo["json_ld_schemas"]) == 1
    assert seo["json_ld_schemas"][0]["name"] == "Global Law LLC"

    # Images
    assert seo["images"]["total"] == 2
    assert seo["images"]["missing_alt"] == 1
    assert seo["images"]["alt_coverage_pct"] == 50

    # Content
    assert seo["content"]["word_count"] > 0
    assert seo["content"]["is_thin_content"] is True  # Short sample


def test_extract_seo_audit_canonical_mismatch():
    html = """
    <html>
    <head>
        <link rel="canonical" href="https://otherdomain.com/page">
    </head>
    <body><h1>Home</h1></body>
    </html>
    """
    seo = extract_seo_audit(
        html_text=html,
        headers={},
        canonical_url="https://example.com/page",
        request_url="https://example.com/page",
    )
    assert seo["canonical_status"] == "cross_domain"


def test_extract_runtime_audit():
    res = extract_runtime_audit(
        response_time_ms=155.456,
        fcp_ms=350.2,
        dom_interactive_ms=620.8,
        js_errors=["Uncaught ReferenceError: foo is not defined"],
    )
    assert res["response_time_ms"] == 155.46
    assert res["fcp_ms"] == 350.2
    assert res["js_error_count"] == 1
    assert len(res["js_errors_sample"]) == 1


def test_compile_full_audit():
    html = "<html><head><title>Test</title></head><body><h1>Hello</h1></body></html>"
    headers = {"Content-Encoding": "gzip"}
    full = compile_full_audit(
        html_text=html,
        headers=headers,
        canonical_url="https://example.com",
        markdown_text="Hello",
        request_url="https://example.com",
        response_time_ms=100.0,
    )
    assert "security" in full
    assert "seo" in full
    assert "runtime" in full
    assert full["seo"]["title"] == "Test"
    assert full["security"]["compression"] == "gzip"

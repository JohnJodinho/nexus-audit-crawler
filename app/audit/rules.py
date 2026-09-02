"""
app/audit/rules.py
==================
Deterministic audit rules engine — Phase 2.

Converts the compiled audit metadata dict (from ``compile_full_audit()``) and
the HTTP ``status_code`` for a page into a flat list of ``AuditFinding``
dictionaries ready for bulk insertion into ``audit_findings``.

Design principles
-----------------
- **No LLMs**.  Every rule is a deterministic boolean predicate evaluated
  against already-collected signals.  Confidence scores in ``remediation``
  reflect rule specificity, not probabilistic inference.
- **id = "{page_id}:{rule_id}"** — the composite is globally unique across all
  pages in a crawl.  ``rule_id`` is the reusable taxonomy code.
- **canvas_zone** is a closed enum: ``"nav"`` | ``"head"`` | ``"content"``
  | ``"footer"`` | ``"server"``.
- **status** is seeded as ``"open"`` only.  Runtime state mutation lives in
  AuditMorph ``findingState`` and is not written back here until Phase 5.

Usage
-----
::

    from app.audit.rules import evaluate_findings

    findings = evaluate_findings(
        page_id=456,
        crawl_id="my-crawl-uuid",
        url="https://example.com/about",
        audit_metadata=metadata_dict,   # output of compile_full_audit()
        status_code=200,
    )
    # findings: List[dict] -- each is one AuditFinding row ready for upsert
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def evaluate_findings(
    page_id: int,
    crawl_id: str,
    url: str,
    audit_metadata: Dict[str, Any],
    status_code: int = 200,
) -> List[Dict[str, Any]]:
    """
    Evaluate all deterministic rules against the compiled audit metadata for
    a single page and return a list of AuditFinding dicts.

    Parameters
    ----------
    page_id:
        The integer primary key of the ``pages`` row (already persisted).
    crawl_id:
        The crawl UUID string.
    url:
        The canonical URL of the page.
    audit_metadata:
        The dict returned by ``compile_full_audit()``, containing keys
        ``"seo"``, ``"security"``, and ``"runtime"``.
    status_code:
        The HTTP response status code for this page.

    Returns
    -------
    List[dict]
        One dict per triggered rule. Empty list if no rules fire.
    """
    seo: Dict[str, Any] = audit_metadata.get("seo") or {}
    security: Dict[str, Any] = audit_metadata.get("security") or {}
    runtime: Dict[str, Any] = audit_metadata.get("runtime") or {}

    headings: Dict[str, Any] = seo.get("headings") or {}
    images: Dict[str, Any] = seo.get("images") or {}
    content: Dict[str, Any] = seo.get("content") or {}
    missing_sec_headers: List[str] = security.get("missing_headers") or []

    findings: List[Dict[str, Any]] = []

    def _finding(
        rule_id: str,
        category: str,
        severity: str,
        canvas_zone: str,
        explanation: str,
        evidence: Dict[str, Any],
        remediation: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "id": f"{page_id}:{rule_id}",    # globally-unique instance key
            "rule_id": rule_id,               # reusable taxonomy code
            "crawl_id": crawl_id,
            "page_id": page_id,
            "url": url,
            "category": category,
            "severity": severity,
            "canvas_zone": canvas_zone,
            "explanation": explanation,
            "evidence": evidence,
            "remediation": remediation,
            "status": "open",                 # seed state; never mutated here
            "finding_metadata": {},
        }

    # ------------------------------------------------------------------
    # SEO rules
    # ------------------------------------------------------------------
    h1_count: int = headings.get("h1_count", 0)
    h1_texts: List[str] = headings.get("h1_texts") or []

    if h1_count == 0:
        findings.append(_finding(
            rule_id="seo.missing_h1",
            category="seo",
            severity="critical",
            canvas_zone="head",
            explanation="No <h1> element was found on this page. Search engines rely on the H1 as the primary page heading.",
            evidence={"selector": "body", "observed": {"h1_count": 0}, "expected": {"h1_count": 1}},
            remediation={"proposed": "<h1>Insert descriptive page heading here</h1>", "confidence": 0.90},
        ))

    if h1_count > 1:
        findings.append(_finding(
            rule_id="seo.multiple_h1",
            category="seo",
            severity="warning",
            canvas_zone="head",
            explanation=f"Multiple <h1> elements found ({h1_count}). Only one H1 per page is recommended.",
            evidence={"selector": "h1", "observed": {"h1_count": h1_count, "h1_texts": h1_texts[:5]}, "expected": {"h1_count": 1}},
        ))

    title: str = seo.get("title") or ""
    if not title:
        findings.append(_finding(
            rule_id="seo.missing_title",
            category="seo",
            severity="critical",
            canvas_zone="head",
            explanation="No <title> tag found. The page title is critical for SEO and browser tab display.",
            evidence={"selector": "head > title", "observed": {"title": None}, "expected": {"title": "Non-empty string"}},
            remediation={"proposed": "<title>Descriptive Page Title</title>", "confidence": 0.95},
        ))

    meta_desc: str = seo.get("meta_description") or ""
    if not meta_desc:
        findings.append(_finding(
            rule_id="seo.missing_meta_description",
            category="seo",
            severity="warning",
            canvas_zone="head",
            explanation="No meta description tag found. Meta descriptions improve click-through rates in search results.",
            evidence={"selector": 'meta[name="description"]', "observed": {"meta_description": None}, "expected": {"meta_description": "120-160 chars"}},
            remediation={"proposed": '<meta name="description" content="Concise page summary here.">', "confidence": 0.85},
        ))

    canonical_status: str = seo.get("canonical_status", "missing")
    if canonical_status == "missing":
        findings.append(_finding(
            rule_id="seo.missing_canonical",
            category="seo",
            severity="warning",
            canvas_zone="head",
            explanation="No canonical link tag found. Without it, duplicate content signals may confuse crawlers.",
            evidence={"selector": 'link[rel="canonical"]', "observed": {"canonical_url": None}, "expected": {"canonical_url": url}},
            remediation={"proposed": f'<link rel="canonical" href="{url}" />', "confidence": 0.92},
        ))

    word_count: int = content.get("word_count", 0)
    if word_count < 300 and status_code == 200:
        findings.append(_finding(
            rule_id="seo.thin_content",
            category="seo",
            severity="warning",
            canvas_zone="content",
            explanation=f"Page has only {word_count} words. Content under 300 words is often considered thin by search engines.",
            evidence={"selector": "body", "observed": {"word_count": word_count}, "expected": {"word_count": ">= 300"}},
        ))

    missing_alt: int = images.get("missing_alt", 0)
    if missing_alt > 0:
        findings.append(_finding(
            rule_id="seo.images_missing_alt",
            category="seo",
            severity="info",
            canvas_zone="content",
            explanation=f"{missing_alt} image(s) are missing alt text, harming accessibility and image SEO.",
            evidence={"selector": "img:not([alt])", "observed": {"images_without_alt": missing_alt}, "expected": {"images_without_alt": 0}},
        ))

    json_ld_schemas: List[Any] = seo.get("json_ld_schemas") or []
    if not json_ld_schemas:
        findings.append(_finding(
            rule_id="seo.missing_schema_org",
            category="seo",
            severity="info",
            canvas_zone="content",
            explanation="No JSON-LD Schema.org markup found. Structured data enables rich results in search.",
            evidence={"selector": 'script[type="application/ld+json"]', "observed": {"json_ld_schemas": []}, "expected": {"json_ld_schemas": ">= 1 entity"}},
        ))

    # ------------------------------------------------------------------
    # Security rules  (keyed off the label list from extract_security_audit)
    # ------------------------------------------------------------------
    if "HSTS" in missing_sec_headers:
        findings.append(_finding(
            rule_id="security.missing_hsts",
            category="security",
            severity="critical",
            canvas_zone="server",
            explanation="The Strict-Transport-Security (HSTS) header is missing. This allows downgrade attacks from HTTPS to HTTP.",
            evidence={"selector": "response.headers", "observed": {"strict-transport-security": None}, "expected": {"strict-transport-security": "max-age=31536000; includeSubDomains"}},
            remediation={"proposed": "Strict-Transport-Security: max-age=31536000; includeSubDomains", "confidence": 0.99},
        ))

    if "CSP" in missing_sec_headers:
        findings.append(_finding(
            rule_id="security.missing_csp",
            category="security",
            severity="critical",
            canvas_zone="server",
            explanation="The Content-Security-Policy (CSP) header is missing, leaving the site vulnerable to XSS attacks.",
            evidence={"selector": "response.headers", "observed": {"content-security-policy": None}, "expected": {"content-security-policy": "policy string"}},
            remediation={"proposed": "Content-Security-Policy: default-src 'self'", "confidence": 0.85},
        ))

    if "X-Frame-Options" in missing_sec_headers:
        findings.append(_finding(
            rule_id="security.missing_x_frame",
            category="security",
            severity="warning",
            canvas_zone="server",
            explanation="X-Frame-Options header is missing. Pages can be embedded in iframes, enabling clickjacking attacks.",
            evidence={"selector": "response.headers", "observed": {"x-frame-options": None}, "expected": {"x-frame-options": "DENY or SAMEORIGIN"}},
            remediation={"proposed": "X-Frame-Options: SAMEORIGIN", "confidence": 0.95},
        ))

    if "X-Content-Type-Options" in missing_sec_headers:
        findings.append(_finding(
            rule_id="security.missing_x_content_type",
            category="security",
            severity="warning",
            canvas_zone="server",
            explanation="X-Content-Type-Options header is missing. Browsers may MIME-sniff responses, enabling content injection.",
            evidence={"selector": "response.headers", "observed": {"x-content-type-options": None}, "expected": {"x-content-type-options": "nosniff"}},
            remediation={"proposed": "X-Content-Type-Options: nosniff", "confidence": 0.99},
        ))

    if "Referrer-Policy" in missing_sec_headers:
        findings.append(_finding(
            rule_id="security.missing_referrer_policy",
            category="security",
            severity="info",
            canvas_zone="server",
            explanation="Referrer-Policy header is absent. The full URL may be sent to third parties via the Referer header.",
            evidence={"selector": "response.headers", "observed": {"referrer-policy": None}, "expected": {"referrer-policy": "strict-origin-when-cross-origin"}},
            remediation={"proposed": "Referrer-Policy: strict-origin-when-cross-origin", "confidence": 0.95},
        ))

    # ------------------------------------------------------------------
    # Performance rules
    # ------------------------------------------------------------------
    response_time_ms: float = runtime.get("response_time_ms", 0.0) or 0.0
    if response_time_ms > 800:
        findings.append(_finding(
            rule_id="performance.slow_response",
            category="performance",
            severity="warning",
            canvas_zone="server",
            explanation=f"Server response time is {response_time_ms:.0f}ms, exceeding the 800ms threshold. Slow TTFB negatively impacts Core Web Vitals.",
            evidence={"selector": "network", "observed": {"response_time_ms": response_time_ms}, "expected": {"response_time_ms": "<= 800"}},
        ))

    dom_complete_ms: Optional[float] = runtime.get("dom_complete_ms")
    if dom_complete_ms is not None and dom_complete_ms > 3000:
        findings.append(_finding(
            rule_id="performance.slow_load",
            category="performance",
            severity="warning",
            canvas_zone="server",
            explanation=f"DOM complete time is {dom_complete_ms:.0f}ms, exceeding 3000ms. Users on slower connections may abandon the page.",
            evidence={"selector": "network", "observed": {"dom_complete_ms": dom_complete_ms}, "expected": {"dom_complete_ms": "<= 3000"}},
        ))

    # ------------------------------------------------------------------
    # Structure rules
    # ------------------------------------------------------------------
    if status_code in (404, 410):
        findings.append(_finding(
            rule_id="structure.broken_link",
            category="seo",
            severity="warning",
            canvas_zone="content",
            explanation=f"This page returned HTTP {status_code}, indicating a broken or removed resource.",
            evidence={"selector": "http.response", "observed": {"status_code": status_code}, "expected": {"status_code": 200}},
        ))

    return findings

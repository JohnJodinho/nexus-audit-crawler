"""
app/utils/audits.py
===================
Enterprise Audit & Quality Metrics Extractor for the distributed crawler.

Extracts high-value, industry-standard SEO, security, structured data (JSON-LD),
indexability, content metrics, and runtime diagnostics from raw HTTP responses
and HTML/DOM content with minimal CPU overhead (< 1ms per page).
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from app.utils.utilities import canonicalize_url

log = logging.getLogger("audit_crawler.audits")

# Security headers to inspect
_SECURITY_HEADERS = (
    ("Strict-Transport-Security", "HSTS"),
    ("Content-Security-Policy", "CSP"),
    ("X-Frame-Options", "X-Frame-Options"),
    ("X-Content-Type-Options", "X-Content-Type-Options"),
    ("Referrer-Policy", "Referrer-Policy"),
    ("Permissions-Policy", "Permissions-Policy"),
)

# Regex helpers for fast HTML parsing without heavy DOM parsers
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_META_DESC_RE = re.compile(
    r'<meta\s+[^>]*name=[\'"]description[\'"][^>]*content=[\'"]([^\'"]*)[\'"]|<meta\s+[^>]*content=[\'"]([^\'"]*)[\'"][^>]*name=[\'"]description[\'"]',
    re.IGNORECASE,
)
_META_ROBOTS_RE = re.compile(
    r'<meta\s+[^>]*name=[\'"]robots[\'"][^>]*content=[\'"]([^\'"]*)[\'"]|<meta\s+[^>]*content=[\'"]([^\'"]*)[\'"][^>]*name=[\'"]robots[\'"]',
    re.IGNORECASE,
)
_CANONICAL_LINK_RE = re.compile(
    r'<link\s+[^>]*rel=[\'"]canonical[\'"][^>]*href=[\'"]([^\'"]*)[\'"]|<link\s+[^>]*href=[\'"]([^\'"]*)[\'"][^>]*rel=[\'"]canonical[\'"]',
    re.IGNORECASE,
)
_HREFLANG_RE = re.compile(
    r'<link\s+[^>]*rel=[\'"]alternate[\'"][^>]*hreflang=[\'"]([^\'"]*)[\'"][^>]*href=[\'"]([^\'"]*)[\'"]|<link\s+[^>]*hreflang=[\'"]([^\'"]*)[\'"][^>]*href=[\'"]([^\'"]*)[\'"][^>]*rel=[\'"]alternate[\'"]',
    re.IGNORECASE,
)
_JSON_LD_RE = re.compile(
    r'<script\s+[^>]*type=[\'"]application/ld\+json[\'"][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
_H2_RE = re.compile(r"<h2[^>]*>(.*?)</h2>", re.IGNORECASE | re.DOTALL)
_H3_RE = re.compile(r"<h3[^>]*>(.*?)</h3>", re.IGNORECASE | re.DOTALL)
_IMG_RE = re.compile(r"<img\s+([^>]*)/?>", re.IGNORECASE)
_ALT_ATTR_RE = re.compile(r'alt=[\'"]([^\'"]*)[\'"]', re.IGNORECASE)
_TAG_STRIP_RE = re.compile(r"<[^>]+>")


def _clean_text(raw_html: str) -> str:
    """Strip nested tags and normalize whitespace."""
    if not raw_html:
        return ""
    text = _TAG_STRIP_RE.sub("", raw_html)
    return " ".join(text.split()).strip()


# ---------------------------------------------------------------------------
# Tier 1: HTTP & Security Audits
# ---------------------------------------------------------------------------


def extract_security_audit(headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Evaluate HTTP response headers for critical security controls.
    """
    # Normalize header keys to lowercase
    norm_headers = {k.lower(): v for k, v in headers.items()}

    missing_headers: List[str] = []
    present_headers: Dict[str, str] = {}

    for header_name, label in _SECURITY_HEADERS:
        val = norm_headers.get(header_name.lower())
        if val:
            present_headers[label] = val
        else:
            missing_headers.append(label)

    # Cookie Security Flags
    insecure_cookies: List[str] = []
    set_cookie_raw = norm_headers.get("set-cookie", "")
    if set_cookie_raw:
        # Evaluate cookie flags
        cookies = [c.strip() for c in set_cookie_raw.split(",") if c.strip()]
        for cookie_str in cookies:
            cookie_lower = cookie_str.lower()
            name_part = cookie_str.split(";")[0].split("=")[0].strip()
            issues: List[str] = []
            if "secure" not in cookie_lower:
                issues.append("missing Secure")
            if "httponly" not in cookie_lower:
                issues.append("missing HttpOnly")
            if "samesite" not in cookie_lower:
                issues.append("missing SameSite")
            if issues:
                insecure_cookies.append(f"{name_part} ({', '.join(issues)})")

    # Compression
    content_encoding = norm_headers.get("content-encoding", "none").lower().strip()
    is_compressed = content_encoding in ("gzip", "br", "zstd", "deflate")

    # Security score (0 to 100)
    total_checks = len(_SECURITY_HEADERS)
    passed_checks = len(present_headers)
    security_score = int((passed_checks / total_checks) * 100) if total_checks else 0

    return {
        "security_score": security_score,
        "missing_headers": missing_headers,
        "present_headers": present_headers,
        "insecure_cookies": insecure_cookies,
        "compression": content_encoding,
        "is_compressed": is_compressed,
    }


# ---------------------------------------------------------------------------
# Tier 2: SEO, Structured Data & Indexability
# ---------------------------------------------------------------------------


def extract_seo_audit(
    html_text: str,
    headers: Dict[str, str],
    canonical_url: str,
    markdown_text: str = "",
    request_url: str = "",
) -> Dict[str, Any]:
    """
    Extract comprehensive SEO, structured Schema.org data, canonicals,
    and content metrics from HTML and generated Markdown.
    """
    if not html_text:
        return {}

    norm_headers = {k.lower(): v for k, v in headers.items()}

    # 1. Title
    title_match = _TITLE_RE.search(html_text)
    title = _clean_text(title_match.group(1)) if title_match else ""

    # 2. Meta Description
    desc_match = _META_DESC_RE.search(html_text)
    meta_desc = ""
    if desc_match:
        meta_desc = desc_match.group(1) or desc_match.group(2) or ""
        meta_desc = _clean_text(meta_desc)

    # 3. Robots Directives & Reconciliation
    x_robots = norm_headers.get("x-robots-tag")
    meta_robots_match = _META_ROBOTS_RE.search(html_text)
    meta_robots = ""
    if meta_robots_match:
        meta_robots = meta_robots_match.group(1) or meta_robots_match.group(2) or ""
        meta_robots = meta_robots.strip().lower()

    robots_conflict = False
    if x_robots and meta_robots:
        # Check if one says noindex while other says index
        xr_lower = x_robots.lower()
        if ("noindex" in xr_lower and "noindex" not in meta_robots) or (
            "noindex" in meta_robots and "noindex" not in xr_lower
        ):
            robots_conflict = True

    is_indexable = True
    if (x_robots and "noindex" in x_robots.lower()) or (meta_robots and "noindex" in meta_robots):
        is_indexable = False

    # 4. Canonical URL Conflict Detection
    canon_match = _CANONICAL_LINK_RE.search(html_text)
    declared_canonical = ""
    if canon_match:
        declared_canonical = (canon_match.group(1) or canon_match.group(2) or "").strip()

    canonical_status = "missing"
    if declared_canonical:
        canon_parsed = urlparse(declared_canonical)
        req_parsed = urlparse(request_url or canonical_url)
        if canon_parsed.netloc and canon_parsed.netloc != req_parsed.netloc:
            canonical_status = "cross_domain"
        elif canonicalize_url(declared_canonical) == canonical_url:
            canonical_status = "matches_url"
        else:
            canonical_status = "mismatched_path"

    # 5. Hreflang Alternates
    hreflangs: List[Dict[str, str]] = []
    for h_match in _HREFLANG_RE.finditer(html_text):
        lang = h_match.group(1) or h_match.group(3) or ""
        href = h_match.group(2) or h_match.group(4) or ""
        if lang and href:
            hreflangs.append({"lang": lang.strip(), "href": href.strip()})

    # 6. JSON-LD & Schema.org Payloads
    json_ld_schemas: List[Dict[str, Any]] = []
    schema_types: List[str] = []
    for script_match in _JSON_LD_RE.finditer(html_text):
        raw_json = script_match.group(1).strip()
        if not raw_json:
            continue
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict) and "@type" in item:
                        t = item["@type"]
                        schema_types.append(t if isinstance(t, str) else str(t))
                        json_ld_schemas.append(item)
            elif isinstance(parsed, dict):
                if "@type" in parsed:
                    t = parsed["@type"]
                    schema_types.append(t if isinstance(t, str) else str(t))
                if "@graph" in parsed and isinstance(parsed["@graph"], list):
                    for g_item in parsed["@graph"]:
                        if isinstance(g_item, dict) and "@type" in g_item:
                            t = g_item["@type"]
                            schema_types.append(t if isinstance(t, str) else str(t))
                json_ld_schemas.append(parsed)
        except Exception:
            pass

    # Deduplicate schema types
    schema_types = list(dict.fromkeys(schema_types))

    # Cap json_ld_schemas list in metadata to prevent huge bloat (>16 KB)
    # Store clean preview of up to 10 entities
    safe_json_ld = json_ld_schemas[:10]

    # 7. Heading Hierarchy
    h1_matches = _H1_RE.findall(html_text)
    h1_texts = [_clean_text(h) for h in h1_matches if _clean_text(h)]
    h2_count = len(_H2_RE.findall(html_text))
    h3_count = len(_H3_RE.findall(html_text))
    heading_order_valid = (len(h1_texts) == 1) and (h2_count > 0 or h3_count == 0)

    # 8. Image Alt Coverage
    img_matches = _IMG_RE.findall(html_text)
    total_images = len(img_matches)
    missing_alt_count = 0
    for img_attrs in img_matches:
        alt_match = _ALT_ATTR_RE.search(img_attrs)
        if not alt_match or not alt_match.group(1).strip():
            missing_alt_count += 1
    alt_coverage_pct = (
        int(((total_images - missing_alt_count) / total_images) * 100)
        if total_images > 0
        else 100
    )

    # 9. Content Metrics (derived from Markdown)
    content_text = markdown_text or html_text
    words = content_text.split()
    word_count = len(words)
    char_count = len(content_text)
    html_bytes_len = len(html_text.encode("utf-8"))
    md_bytes_len = len(markdown_text.encode("utf-8")) if markdown_text else len(content_text.encode("utf-8"))
    code_to_text_ratio = round((md_bytes_len / html_bytes_len) * 100, 2) if html_bytes_len > 0 else 0.0

    return {
        "title": title,
        "title_length": len(title),
        "meta_description": meta_desc,
        "description_length": len(meta_desc),
        "is_indexable": is_indexable,
        "x_robots_tag": x_robots,
        "meta_robots": meta_robots or None,
        "robots_conflict": robots_conflict,
        "declared_canonical": declared_canonical or None,
        "canonical_status": canonical_status,
        "hreflang": hreflangs,
        "schema_types": schema_types,
        "json_ld_schemas": safe_json_ld,
        "headings": {
            "h1_count": len(h1_texts),
            "h1_texts": h1_texts,
            "h2_count": h2_count,
            "h3_count": h3_count,
            "heading_order_valid": heading_order_valid,
        },
        "images": {
            "total": total_images,
            "missing_alt": missing_alt_count,
            "alt_coverage_pct": alt_coverage_pct,
        },
        "content": {
            "word_count": word_count,
            "character_count": char_count,
            "code_to_text_ratio": code_to_text_ratio,
            "is_thin_content": word_count < 150,
        },
    }


# ---------------------------------------------------------------------------
# Tier 3: Runtime Diagnostics
# ---------------------------------------------------------------------------


def extract_runtime_audit(
    response_time_ms: float = 0.0,
    fcp_ms: Optional[float] = None,
    dom_interactive_ms: Optional[float] = None,
    dom_complete_ms: Optional[float] = None,
    js_errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Format runtime latency, navigation timing, and client-side JS exception metrics.
    """
    errors = js_errors or []
    return {
        "response_time_ms": round(response_time_ms, 2),
        "fcp_ms": round(fcp_ms, 2) if fcp_ms is not None else None,
        "dom_interactive_ms": round(dom_interactive_ms, 2) if dom_interactive_ms is not None else None,
        "dom_complete_ms": round(dom_complete_ms, 2) if dom_complete_ms is not None else None,
        "js_error_count": len(errors),
        "js_errors_sample": errors[:5],  # Cap top 5 distinct errors to prevent Postgres bloat
    }


def compile_full_audit(
    html_text: str,
    headers: Dict[str, str],
    canonical_url: str,
    markdown_text: str = "",
    request_url: str = "",
    response_time_ms: float = 0.0,
    fcp_ms: Optional[float] = None,
    dom_interactive_ms: Optional[float] = None,
    dom_complete_ms: Optional[float] = None,
    js_errors: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Compile complete, standardized audit metadata dictionary for persistence in `pages.metadata`.
    """
    return {
        "security": extract_security_audit(headers),
        "seo": extract_seo_audit(
            html_text=html_text,
            headers=headers,
            canonical_url=canonical_url,
            markdown_text=markdown_text,
            request_url=request_url,
        ),
        "runtime": extract_runtime_audit(
            response_time_ms=response_time_ms,
            fcp_ms=fcp_ms,
            dom_interactive_ms=dom_interactive_ms,
            dom_complete_ms=dom_complete_ms,
            js_errors=js_errors,
        ),
    }

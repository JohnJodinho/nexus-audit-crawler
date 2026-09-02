"""
app/api/schemas.py
==================
Pydantic schemas for request validation and structured API responses.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class CrawlConfig(BaseModel):
    max_pages: int = Field(default=15, ge=1, le=1000)
    max_depth: int = Field(default=2, ge=0, le=10)
    worker_count: int = Field(
        default=2,
        ge=1,
        le=4,
        description="Worker concurrency count (must be between 1 and 4 to stay within free concurrency limits).",
    )
    categories: Optional[List[str]] = None



class CrawlCreateRequest(BaseModel):
    url: str = Field(..., description="Target seed URL to crawl and audit.")
    crawl_id: Optional[str] = Field(default=None, description="Optional custom crawl ID.")
    config: Optional[CrawlConfig] = Field(default_factory=CrawlConfig)


class CrawlCreateResponse(BaseModel):
    crawl_id: str
    status: str
    message: str


class CrawlResponse(BaseModel):
    id: str
    target_url: str
    target_domain: str
    status: str
    started_at: Optional[datetime.datetime] = None
    finished_at: Optional[datetime.datetime] = None
    worker_count: int = 1
    pages_discovered: int = 0
    pages_processed: int = 0
    pages_failed: int = 0
    config: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class CrawlStatusResponse(BaseModel):
    crawl_id: str
    status: str
    pages_discovered: int = 0
    pages_processed: int = 0
    pages_failed: int = 0
    findings_count: int = 0
    progress: float = Field(..., ge=0.0, le=1.0, description="Estimated completion progress (0.0 to 1.0).")
    next_recommended_action: Literal["wait", "retrieve", "none"]


class AuditFindingResponse(BaseModel):
    id: str
    rule_id: str
    crawl_id: str
    page_id: int
    url: str
    category: str
    severity: str
    canvas_zone: str
    explanation: str
    evidence: Dict[str, Any]
    remediation: Optional[Dict[str, Any]] = None
    status: str
    detected_at: Optional[datetime.datetime] = None


class FindingUpdateRequest(BaseModel):
    status: Literal["open", "approved", "rejected", "pending_review"]


class PaginatedFindingsResponse(BaseModel):
    total: int
    page: int
    per_page: int
    total_pages: int
    findings: List[AuditFindingResponse]


class PageSummaryItem(BaseModel):
    id: int
    url: str
    canonical_url: str
    status_code: int
    markdown_file_id: Optional[str] = None
    markdown_byte_size: int = 0
    markdown_token_count: int = 0
    finding_counts: Dict[str, int]
    findings_sample: List[Dict[str, Any]] = Field(default_factory=list)


class PaginatedPagesResponse(BaseModel):
    total: int
    page: int
    per_page: int
    total_pages: int
    pages: List[PageSummaryItem]


class PageDetailResponse(BaseModel):
    page: Dict[str, Any]
    findings: List[AuditFindingResponse]


class CrawlSummaryResponse(BaseModel):
    crawl_id: str
    target_url: str
    status: str
    pages_total: int
    pages_with_issues: int
    finding_counts_by_category: Dict[str, int]
    finding_counts_by_severity: Dict[str, int]
    top_findings: List[AuditFindingResponse]

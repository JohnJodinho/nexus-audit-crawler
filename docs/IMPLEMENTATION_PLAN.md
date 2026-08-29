# Nexus Audit Crawler → Intelligent Website Audit System
## Repo-Aware Implementation Plan

> **Purpose:** This document is the implementation blueprint for evolving the current `JohnJodinho/nexus-audit-crawler` codebase into a multi-crawl, durable, auditable website-audit platform.
>
> **Grounding:** The plan below was reconciled against the current `main` branch, not treated as a greenfield design. The existing crawler is Redis-stream based, uses Scrapling/Playwright with a waterfall extraction pipeline, and already has worker-side deduplication, depth/budget gates, domain throttling, retries, a DLQ, contact extraction, and PEL janitor logic. The uploaded Claude plan is retained conceptually, but several items are corrected where they do not match the repository today.

---

# 0. Executive Direction

The target architecture is:

```text
                         ┌──────────────────────────┐
                         │       Audit API/UI       │
                         │  create / status / query │
                         └────────────┬─────────────┘
                                      │
                                      ▼
                         ┌──────────────────────────┐
                         │   Crawl Orchestrator     │
                         │ create crawl + dispatch  │
                         └────────────┬─────────────┘
                                      │
                           crawl-scoped Redis
                                      │
                ┌─────────────────────┼─────────────────────┐
                ▼                     ▼                     ▼
        ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
        │ Worker 1     │ ...  │ Worker N     │ ...  │ Worker N+1   │
        │ main.py      │      │ main.py      │      │ main.py      │
        └──────┬───────┘      └──────┬───────┘      └──────┬───────┘
               │                     │                     │
               └──────────────┬──────┴─────────────────────┘
                              ▼
                       audit_results stream
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
             persistence sink      telemetry/DLQ
                    │
                    ▼
        ┌──────────────────────────────┐
        │ Supabase Postgres + pgvector │
        │ pages / links / sections /   │
        │ entities / findings / graph  │
        └──────────────┬───────────────┘
                       │
                       ▼
                 query / RAG layer
```

### Design principles

1. **Keep the existing crawler core.** Do not rewrite `spider.py` or the extraction waterfall merely to introduce persistence and multi-crawl support.
2. **Make crawl identity explicit.** Every shared Redis key and every persisted record must belong to a `crawl_id`.
3. **Separate transport from system of record.** Redis Streams remain the coordination/message layer; Postgres becomes durable truth.
4. **Make completion deterministic.** A crawl is not `finished` merely because a worker stopped; it is finished only when the task graph is drained, active work is zero, persistence is caught up, and consolidation succeeds.
5. **Prefer structured audit data over LLM-generated prose.** Capture facts during crawling; use LLMs only at query/report time.
6. **Treat reliability as a first-class feature.** Fix PEL recovery, atomic budget allocation, idempotent persistence, and failure recovery before adding GraphRAG.
7. **Keep the system incrementally deployable.** Each phase should be testable against the existing crawler.

---

# 1. Repository Audit — Current Reality

## 1.1 Current tree

```text
nexus-audit-crawler/
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
├── app/
│   ├── config.py
│   ├── extraction.py
│   ├── logger.py
│   ├── main.py
│   ├── orchestrator.py
│   ├── redis_client.py
│   ├── spider.py
│   ├── telemetry.py
│   └── utils/
│       ├── flush_state.py
│       └── utilities.py
├── scripts/
│   ├── export_data.py
│   ├── find_dups.py
│   ├── inspect.py
│   └── test_config.py
└── tests/
    └── smoke_test.py
```

The README describes the worker as stateless and the pipeline as Redis-backed. The actual implementation confirms that `main.py` consumes tasks with `XREADGROUP`, applies dedup/depth/budget/throttle gates, invokes `AuditSpider`, and publishes results/discoveries back to Redis.

The current environment/configuration is small: Redis URL, worker/page/depth limits, PEL/retry settings, hostname, domain throttling, and optional datacenter/residential proxy lists. There is currently no Postgres, storage bucket, embedding, GitHub Actions, crawl API, or LLM configuration in the application.

## 1.2 Existing pipeline

```text
Redis task stream
      │
      ▼
XREADGROUP
      │
      ├── visited fingerprint?
      ├── processing lock?
      ├── depth limit?
      ├── global budget?
      ├── domain slot?
      ├── per-worker page cap?
      ▼
AuditSpider
      │
      ├── FetcherSession
      │       └── optional datacenter proxy
      │
      ├── blocked/challenge detection
      │       └── lazy AsyncStealthySession
      │               └── optional residential proxy
      │
      ├── link gates
      │       ├── page-local duplicate
      │       ├── mailto/tel extraction
      │       ├── HTTP/S scheme
      │       ├── same-domain/subdomain fence
      │       ├── deny pattern
      │       └── queued fingerprint
      │
      └── Triple-Threat extraction
              ├── XHR JSON
              ├── hydration JSON
              └── MarkItDown
      │
      ├── audit_results
      ├── audit_tasks
      └── dropped_telemetry
```

The extraction implementation is a deterministic XHR → hydration → MarkItDown waterfall; it does not currently produce normalized sections, embeddings, Lighthouse metrics, screenshots, or accessibility findings.

## 1.3 Important mismatches from the earlier plan

### A. `set:queued_fingerprints` exists but was missing from the earlier key inventory

The spider atomically registers discovered URLs in `set:queued_fingerprints`, and the orchestrator pre-registers the seed there. The flush utility also deletes it. This must be namespaced in Phase 1.

### B. The current telemetry implementation is effectively Redis-first, not file-first

`spider.py` writes drop events directly to `stream:dropped_telemetry`. `TelemetrySink` exists as a file JSONL abstraction, but the crawler's active link-gate path does not use it. The implementation plan should therefore treat Redis telemetry as the canonical event bus and make local JSONL an optional/debug sink rather than assuming it is the live path.

### C. The test suite is stale

`tests/smoke_test.py` imports `ALLOW_PATTERN`, but the current `spider.py` has a deny-list model and no `ALLOW_PATTERN`. The test therefore does not represent the current implementation and must be repaired before the feature work is trusted.

### D. The current global budget check is not atomic

`main.py` first reads `global_budget:tickets_dispensed`, checks the limit, and then increments it. Multiple workers can pass the check simultaneously and overshoot the configured budget. Phase 1 must replace this with a Redis Lua script or equivalent atomic increment-and-check operation.

### E. PEL recovery needs correction before horizontal scaling

The janitor currently `XCLAIM`s stale messages to the consumer named `janitor`. A claimed message remains pending under that consumer; it is not automatically returned to the `>` delivery path used by workers. Therefore the existing implementation should be changed to explicitly reclaim-and-requeue, or to have the janitor consume/process pending work itself. Do not build the GitHub Actions matrix on top of this unresolved recovery behavior.

### F. The current fingerprint normalization is intentionally minimal

`get_fingerprint()` only strips trailing `/` before SHA-256 hashing. It does not canonicalize host casing, fragments, default ports, tracking query parameters, or URL encoding. That is acceptable for the current crawler, but Phase 1 should define a deliberate canonicalization policy before multi-crawl dedup becomes durable.

### G. The current `find_dups.py` contains an unused dependency

`trafilatura.deduplication` is imported but not used. It should either be removed or the script should be rewritten around the canonical URL/fingerprint logic used by the crawler.

### H. The current requirements are intentionally minimal

The runtime currently declares only Scrapling, Redis, Pydantic, pydantic-settings, MarkItDown, and AnyIO. Postgres, pgvector, spaCy, graph libraries, embeddings, accessibility tooling, and API frameworks must therefore be introduced deliberately rather than assumed to exist.

---

# 2. Phase 0 — Stabilize the Existing Crawler

**Priority: P0 — do this before feature expansion.**

### 2.1 Repair tests

Replace the stale `ALLOW_PATTERN` assertions with tests for:

- `DENY_PATTERN`
- same-domain acceptance
- valid subdomain acceptance
- off-domain rejection
- `mailto:` extraction
- `tel:` extraction
- invalid scheme rejection
- queued-fingerprint deduplication
- URL fingerprint normalization
- XHR extraction
- hydration extraction
- MarkItDown fallback
- DLQ payload construction
- budget behavior
- domain-slot acquire/release
- flush behavior

Move from a single executable smoke script toward `pytest` tests with mocks/fakes for Redis and Scrapling.

### 2.2 Fix atomic global budget allocation

Implement:

```text
reserve_page_ticket(crawl_id)
    ├── if limit disabled → allow
    ├── atomically increment
    ├── if new value <= limit → allow
    └── otherwise → decrement/rollback and reject
```

The operation must be atomic across all workers.

### 2.3 Fix stale-task recovery

Preferred strategy:

1. inspect stale PEL entries;
2. atomically claim them;
3. re-publish the task payload to the available task stream;
4. `XACK` the old pending entry;
5. increment a delivery/recovery counter;
6. DLQ when the recovery budget is exhausted.

This makes the semantics obvious to `XREADGROUP(..., ">")`.

### 2.4 Make lock cleanup crash-safe

Current processing locks use a fixed 600-second TTL. Keep the safety TTL, but ensure the lock is scoped to `crawl_id` and the fingerprint. Add tests for successful completion, spider exception, process termination simulation, lock expiration, and retry after lock expiry.

### 2.5 Normalize URLs deliberately

Introduce a single `canonicalize_url()` function used by seed publication, fingerprints, link discovery, persistence, and duplicate detection.

Initial rules should include:

- lowercase scheme and hostname;
- remove fragments;
- normalize trailing slash consistently;
- remove default ports;
- normalize obvious percent-encoding cases;
- preserve meaningful query parameters;
- optionally strip known analytics parameters behind configuration.

Do **not** blindly sort or discard arbitrary query strings because they may identify different resources.

### 2.6 Fix Playwright session manager leak

`_run_spider()` in `main.py` places `session_manager.close()` in a plain `try` block *after* the `spider.stream()` loop.  If `spider.stream()` raises any unhandled exception (Playwright navigation timeout, proxy reset, target crash), execution jumps directly to the outer `except` in `worker_loop` and `session_manager.close()` is never called.  Leaked Playwright contexts and Chromium child processes accumulate until the process is OOM-killed.

Wrap the spider invocation in a `try...finally` block that unconditionally calls `session_manager.close()`.

### 2.7 Fix per-worker-cap double-billing

When Gate 5 (`MAX_PAGES_PER_RUN`) is reached, the task is re-queued via `XADD` with its payload unchanged: `retry_count` and `throttle_count` both remain 0.  The next worker evaluates `is_first_attempt = True` and purchases a second global budget ticket for a page that was already counted in Gate 3.

Before calling `XADD`, increment `throttle_count` by 1 in the re-queued payload so that the budget gate's `is_first_attempt` guard correctly identifies the re-queued task as having already been billed.

### 2.8 Resolve MarkItDown singleton thread-safety

`_MARKITDOWN = MarkItDown(enable_plugins=False)` is a module-level singleton in `extraction.py`.  `spider.py` dispatches `run_triple_threat` via `loop.run_in_executor(None, ...)`, which routes it to the default `ThreadPoolExecutor`.  Multiple threads can therefore call `_MARKITDOWN.convert_stream()` concurrently on the same instance.

Verify thread-safety from the `markitdown` library's documentation and source.  If mutable state exists inside the converter, instantiate `MarkItDown` per-call inside `extract_via_markitdown()` rather than sharing a global instance.

### 2.9 Redesign extraction from winner-takes-all to cumulative

The current `run_triple_threat()` in `extraction.py` short-circuits on the first successful strategy: if XHR JSON wins, Hydration and MarkItDown are skipped entirely.  This design assumes the three strategies are **substitutes**.  They are not — they are **complements** that serve different downstream consumers:

- **XHR / Hydration JSON** → structured fact extraction, pricing intelligence, knowledge graph nodes, structured API data.
- **MarkItDown** → full-text representation needed for embeddings, page section splitting, TextRank summaries, SEO audit signals (H1 count, heading hierarchy, meta description, content length, image alt coverage).

A Next.js pricing page that succeeds on Hydration currently produces JSON but **no Markdown** — meaning it can never be sectioned or embedded in Phase 5.  This is a permanent data loss.

Replace the short-circuit waterfall with a cumulative runner:

```python
def run_triple_threat(response, logger):
    # Always attempt all three — no early exits
    xhr_state      = extract_from_xhr(captured_xhr, logger)      # fast, pure Python
    hydration_state = extract_from_hydration(html_str, logger)   # fast, regex
    raw_markdown    = extract_via_markitdown(raw_bytes, logger)   # CPU-bound, in executor

    return {
        "url":              url,
        "xhr_state":        xhr_state,        # None if no relevant XHR captured
        "hydration_state":  hydration_state,  # None if no hydration block found
        "raw_markdown":     raw_markdown or "",  # Always attempted
        "extraction_method": _determine_methods(xhr_state, hydration_state, raw_markdown),
    }
```

`_determine_methods()` returns a list (e.g. `["hydration", "markitdown"]`) rather than a single winner string.  Update `on_scraped_item()` to store all three fields separately and drop only if `raw_markdown` is empty **and** both JSON fields are `None` (true empty response).

Update the result payload schema, the Postgres `pages` schema (`json_state jsonb` already added; `hydration_state jsonb` should be added), and all downstream references in `on_scraped_item()` and `persist_consumer.py`.

### 2.10 Replace the XHR relevance allow-list with a tracking deny-list

`_XHR_RELEVANCE_KEYS` in `extraction.py` is a narrow frozenset of assumed business keys (`"pricing"`, `"products"`, `"team"`, etc.) originally written for B2B SaaS company sites.  It is wrong for a general-purpose audit crawler:

- **False negatives**: A real estate site's `"listings"`, a healthcare site's `"doctors"`, a retail site's `"inventory"` — all useful, all silently rejected.
- **Wrong abstraction**: The filter was designed to answer "is this XHR good enough to be our only result?" (winner-takes-all logic).  Under the new cumulative design that question is gone entirely.

Replace with a **URL-pattern deny-list** that rejects known analytics and tracking requests, and accept all other structured JSON:

```python
_XHR_TRACKING_DENY_PATTERNS: tuple[str, ...] = (
    "google-analytics", "googletagmanager", "doubleclick",
    "hotjar", "segment.io", "mixpanel", "amplitude",
    "sentry", "datadog", "newrelic",
    "facebook.com/tr", "bat.bing", "clarity.ms",
    "analytics.", ".tracking.", "beacon", "telemetry", "pixel",
)
```

Acceptance criteria for an XHR response:
1. Request URL does not match any deny pattern.
2. Response is a JSON object (dict) with at least 2 keys (filters `{"ok": true}` heartbeats).

Collect **all accepted responses**, not just the first.  Store the list in `xhr_state` in the result payload.  Phase 5 (NER, knowledge graph) decides at analysis time which payloads are semantically useful — that decision does not belong at capture time.

---

# 3. Phase 1 — Multi-Crawl Namespacing

**Priority: P0**

## 3.1 Introduce `crawl_id`

Add a runtime crawl identity, but do not rely solely on environment state. The crawl identity should also be carried in task payloads and result payloads.

Recommended task fields:

```text
crawl_id
url
canonical_url
depth
domain
retry_count
throttle_count
published_at
```

## 3.2 Make Redis keys functions, not constants

Replace global string constants with functions:

```python
tasks_key(crawl_id)
results_key(crawl_id)
telemetry_key(crawl_id)
dlq_key(crawl_id)

visited_key(crawl_id)
queued_key(crawl_id)
budget_key(crawl_id)
processing_lock_key(crawl_id, fingerprint)
domain_throttle_key(crawl_id, domain)
```

Example:

```text
crawl:{crawl_id}:stream:audit_tasks
crawl:{crawl_id}:stream:audit_results
crawl:{crawl_id}:stream:dropped_telemetry
crawl:{crawl_id}:stream:dlq
crawl:{crawl_id}:set:visited_fingerprints
crawl:{crawl_id}:set:queued_fingerprints
crawl:{crawl_id}:budget:tickets_dispensed
crawl:{crawl_id}:throttle:domain:{domain}
crawl:{crawl_id}:lock:processing:{fingerprint}
```

## 3.3 Consumer groups

Use a crawl-scoped group:

```text
audit_workers:{crawl_id}
```

This avoids accidental cross-crawl group reuse and makes PEL inspection unambiguous.

## 3.4 Flush semantics

Implement:

```bash
python -m app.orchestrator --flush --crawl-id <id>
```

and make it delete only that crawl's keys.

An unrestricted global flush should require an explicit administrative flag, e.g.:

```bash
python -m app.orchestrator --flush-all
```

Do not let a normal developer typo erase every active crawl.

The crawl-scoped flush implementation must also sweep:

- `crawl:{id}:lock:processing:*` — release processing locks left by dead workers;
- `crawl:{id}:throttle:domain:*` — reset domain concurrency counters so new workers are not throttled by stale state.

The existing `flush_crawler_state` sweeps these patterns globally; the crawl-scoped refactor must preserve both sweeps using the `crawl:{id}:` prefix.

---

# 4. Phase 2 — Durable Crawl State & Dual-Tier Storage

**Priority: P0**

Introduce a high-performance **dual-tier storage architecture**:
1. **System of Record & Relational Metadata**: Supabase Postgres (crawls, page metadata, contacts, link graph, telemetry, audit logs, DLQ).
2. **Blob & Unstructured Artifact Storage**: Appwrite Storage Buckets (`APP_WRITE_BUCKET_ID`) for large Markdown documents, raw HTML dumps, and screenshot images.

### Why Dual-Tier Separation?
- **Postgres Lean Performance**: Storing large text blobs and binary screenshots in Postgres bloats the WAL (Write-Ahead Logging), increases table I/O cache pressure, and degrades B-Tree indexing.
- **Cost & Capacity**: Blob storage in Appwrite provides scalable capacity for heavy artifacts without straining the database connection pool.
- **Pointers in Postgres**: Postgres stores the `markdown_file_id`, `markdown_byte_size`, and short summaries, while full documents stream directly from Appwrite.

## 4.1 Core Schema

```sql
crawls (
    id uuid primary key,
    target_url text not null,
    target_domain text not null,
    status text not null,
    started_at timestamptz,
    finished_at timestamptz,
    worker_count integer,
    pages_discovered integer default 0,
    pages_processed integer default 0,
    pages_failed integer default 0,
    config jsonb,
    error text
);

pages (
    id bigserial primary key,
    crawl_id uuid not null references crawls(id) on delete cascade,
    url text not null,
    canonical_url text not null,
    path text,
    status_code integer,
    extraction_methods text[],
    markdown_file_id text,          -- Appwrite Storage Bucket file ID
    markdown_byte_size integer,     -- Size in bytes of the uploaded markdown
    markdown_token_count integer,   -- Estimated token count for LLM budgeting
    summary text,                   -- Optional extractive summary for quick search
    json_state jsonb,
    hydration_state jsonb,
    xhr_payloads jsonb,
    screenshot_file_id text,        -- Appwrite Storage Bucket file ID for screenshots
    fetched_at timestamptz,
    metadata jsonb,
    unique (crawl_id, canonical_url)
);

page_contacts (
    id bigserial primary key,
    page_id bigint not null references pages(id),
    kind text not null,
    value text not null
)

page_links (
    id bigserial primary key,
    crawl_id uuid not null references crawls(id),
    from_page_id bigint,
    to_url text not null,
    canonical_to_url text not null,
    link_text text,
    is_internal boolean,
    unique (crawl_id, from_page_id, canonical_to_url)
)

audit_events (
    id bigserial primary key,
    crawl_id uuid not null references crawls(id),
    event_type text not null,
    url text,
    payload jsonb,
    created_at timestamptz default now()
)
```

Only after this is stable should the system add sections/entities/embeddings.

## 4.2 Idempotent Dual-Tier Persistence

The persistence consumer (`app/persistence_worker.py`) consumes from `crawl:{id}:stream:audit_results`:

```text
Redis Result Stream
       ↓
Validate Schema (version=1)
       ↓
Upload Markdown Blob → Appwrite Storage Bucket ({crawl_id}/{fingerprint}.md)
       ↓
Begin Postgres Transaction
       ↓
UPSERT page record with markdown_file_id pointer (ON CONFLICT (crawl_id, canonical_url) DO UPDATE)
       ↓
UPSERT page_contacts
       ↓
UPSERT page_links
       ↓
UPDATE crawls statistics (pages_processed = pages_processed + 1)
       ↓
Commit Postgres Transaction
       ↓
XACK Redis message
```

### Crash & Failure Guarantees
- If Appwrite upload fails: transaction is not opened, Redis message is NOT acknowledged; task is re-delivered via PEL.
- If Postgres fails: Appwrite file is overwritten idempotently on next retry, transaction rolls back, Redis message is NOT acknowledged.
- `XACK` happens **only** after both Appwrite storage and Postgres transaction succeed.

## 4.3 Result schema versioning

Add `schema_version=1` to every result payload.

Future changes should use explicit versions rather than making the persistence consumer guess which fields a worker emitted.

---

# 5. Phase 3 — Enterprise Audit Signals & Quality Metrics (Per Page)

**Priority: P1**

Capture high-value, industry-standard audit signals across HTTP, security, indexability, structured semantic data, and runtime performance without paying the latency/memory overhead of a full Lighthouse run.

All extracted audit metrics are structured and persisted directly into PostgreSQL `pages.metadata` (`JSONB`).

---

## 5.1 Tier 1: HTTP & Security Audits (Raw Fetch Tier — Zero Latency)
Extracted during the fast `FetcherSession` stage directly from `response.headers`:

* **HTTP Security Headers:** Inspect headers for `Strict-Transport-Security` (HSTS), `Content-Security-Policy` (CSP), `X-Frame-Options`, `X-Content-Type-Options`, and `Referrer-Policy`. Flag missing or insecure directives.
* **`X-Robots-Tag` Resolution & Reconciliation:** Capture header-level indexing directives and reconcile them with HTML `<meta name="robots">` tags to flag conflicting directives (e.g. header says `noindex` but HTML says `index`).
* **Cookie Security Attributes:** Parse `Set-Cookie` headers for missing `Secure`, `HttpOnly`, and `SameSite` flags.
* **Transport Compression:** Verify `Content-Encoding` (`br`, `gzip`, `zstd`) to flag uncompressed text/asset transfers.

---

## 5.2 Tier 2: Structured Data & Indexability (Extraction Tier)
Extracted directly during HTML/DOM parsing, feeding directly into downstream Knowledge Graph pipelines in Phase 5:

* **JSON-LD & Schema.org Payloads:** Extract all `<script type="application/ld+json">` blocks. Store parsed JSON objects in `pages.metadata` (e.g., `Organization`, `Product`, `Person`, `LocalBusiness`, `FAQPage`, `BreadcrumbList`) — providing 100% accurate structured entity nodes without expensive LLM extraction.
* **Canonical URL Conflict Detection:** Compare the page's requested URL against its self-declared `<link rel="canonical">` and `canonicalize_url()` output. Flag `matches`, `cross_domain`, `mismatched_path`, or `missing`.
* **Hreflang & Multi-Region Alternate Tags:** Extract `<link rel="alternate" hreflang="...">` tags to check for missing self-referential tags, invalid language/region ISO codes, and missing `x-default`.
* **Content Metrics & Thin Content Detection:** Compute raw word count, character count, and code-to-text ratio directly from the generated `raw_markdown` to detect doorway or thin-content pages.
* **Heading & Image Alt Hierarchy:** Single H1 presence, heading depth progression (H1 $\rightarrow$ H2 $\rightarrow$ H3), and image `alt` attribute coverage ratio.

---

## 5.3 Tier 3: Runtime Diagnostics & Adaptive Page Settle (The "Settle Shield")
When a page escalates to Playwright stealth rendering, capture lightweight native browser diagnostics with an intelligent, non-blocking settle mechanism:

* **Why Unconditional `networkidle` Hangs:**
  Modern websites frequently run long-polling WebSockets (chat widgets like Intercom, Crisp) and endless analytics beacons (Google Analytics, Hotjar, Sentry, Facebook Pixel) sending heartbeat pings every few seconds. Unconditional `networkidle` waits for "0 active network connections for 500ms", which **never resolves** on such pages, causing the browser to hang for 30–60 seconds.

* **The "Settle Shield" Architecture:**
  1. **Tracking & Telemetry Abort:** Intercept and abort known analytics, advertising, and chat beacon requests (`google-analytics`, `hotjar`, `sentry.io`, `doubleclick`, `facebook.net`). This eliminates 90% of infinite polling connections.
  2. **Bounded Network Idle Race:** Navigate on `wait_until="domcontentloaded"` (or `"load"`), then perform an optional non-blocking settle race:
     ```python
     try:
         await page.wait_for_load_state("networkidle", timeout=settings.NETWORK_IDLE_TIMEOUT_MS)
     except Exception:
         pass  # Proceed immediately if background long-polling persists past timeout
     ```
     If the page settles in 200ms, it completes in 200ms. If an infinite stream exists, it hard-caps the wait at 3,000ms and proceeds without stalling!
  3. **Configurable Settings in `.env`:**
     - `NETWORK_IDLE_TIMEOUT_MS: int = 3000` (Max wait cap for network settle; never blocks indefinitely).
     - `PAGE_SETTLE_DELAY_MS: int = 0` (Optional post-load delay in ms, useful for heavy client-side React/Vue SPAs).
* **Console & Runtime JS Exceptions:** Log unhandled client-side JavaScript crashes and failed resource fetches.
* **Pre-JS vs. Post-JS DOM Mutation:** Compare raw HTML title/canonical/meta tags against the hydrated DOM state to flag client-side scripts overwriting SEO tags post-render.
* **Lightweight Navigation Timing:** Inject `window.performance.getEntriesByType("navigation")[0]` via Playwright to extract First Contentful Paint (FCP), DOM Interactive, and DOM Complete in milliseconds without running Lighthouse.

---

## 5.4 Tier 4: Advanced Screenshot Engine (PIL Vertical Stitching & Blob Storage)
When `SCREENSHOT_ENABLED=true` in Playwright stealth sessions:

* **The Virtual DOM Unmounting Problem:** Standard `page.screenshot(full_page=True)` resizes the virtual viewport, causing virtualized lists (e.g. `react-virtualized`, `react-window`) to unmount offscreen components and producing blank sections. It also duplicates sticky headers across tall pages.
* **Incremental Scrolling & Lazy-Load Triggering:**
  1. Programmatically scroll the page in viewport increments (`window.scrollBy(0, viewport_height)`).
  2. Sleep briefly (100–200ms) to trigger `IntersectionObserver` lazy images and ensure virtualized DOM nodes mount.
  3. Temporarily hide duplicate `position: fixed` / sticky header elements on subsequent slices.
* **In-Memory PIL (Pillow) Vertical Stitching:**
  - Capture individual viewport screenshot slices (`page.screenshot()`).
  - Stitch slices vertically in memory using Pillow (`PIL.Image.new('RGB', (w, total_h))`).
  - Compress as optimized PNG/WebP and upload bytes directly to **Appwrite Storage Bucket** (`nexus-audit-7637-ncx90`).
* **PostgreSQL Pointer:** Store only the returned `screenshot_file_id` in Postgres `pages.screenshot_file_id`.

---

## 5.5 Heavy State Decoupling (The 16 KB Rule to Prevent Postgres TOAST Bloat)
To keep PostgreSQL `pages` table lean, fast, and cache-friendly (< 2 KB per row) and prevent heavy TOAST table thrashing:

* **Hydration State (`__NEXT_DATA__`, `__NUXT__`)**: If the serialized JSON exceeds **16 KB**, upload the raw JSON to Appwrite Storage as `{crawl_id}/{fingerprint}_hydration.json`. In Postgres, store `hydration_file_id` pointer + high-level boolean/summary.
* **Intercepted XHR Dumps**: If raw XHR responses exceed **16 KB**, upload to Appwrite as `{crawl_id}/{fingerprint}_xhr.json`, keeping only the endpoint status summary and `xhr_file_id` pointer in Postgres.
* **JSON-LD Schema Dumps**: Extract schema types (`["Organization", "FAQPage"]`) and primary entity metadata into Postgres `pages.metadata` for instant SQL indexing; offload oversized raw payloads (> 16 KB) to Appwrite.

---

## 5.6 `pages.metadata` JSONB Schema Specification

```json
{
  "security": {
    "missing_headers": ["Content-Security-Policy", "Strict-Transport-Security"],
    "insecure_cookies": ["session_id: missing Secure flag"],
    "compression": "br",
    "protocol": "HTTP/2"
  },
  "seo": {
    "title": "Aetna Health Insurance Plans",
    "title_length": 30,
    "meta_description": "Explore health insurance options...",
    "declared_canonical": "https://www.aetna.com/",
    "canonical_status": "matches_url",
    "x_robots_tag": null,
    "meta_robots": "index, follow",
    "headings": {
      "h1_count": 1,
      "h1_texts": ["Health insurance made easy"],
      "heading_order_valid": true
    },
    "images": {
      "total": 14,
      "missing_alt": 2
    },
    "hreflang": [{"lang": "en-US", "href": "https://www.aetna.com/"}],
    "word_count": 482,
    "character_count": 3210,
    "json_ld_schemas": [
      {"@type": "Organization", "name": "Aetna", "url": "https://www.aetna.com"}
    ]
  },
  "runtime": {
    "response_time_ms": 142.5,
    "fcp_ms": 420.5,
    "dom_interactive_ms": 780.2,
    "js_errors": []
  }
}
```

---

# 6. Phase 4 — Crawl Lifecycle State Machine & Consolidation Engine

**Priority: P0/P1**

Manages the full lifecycle state machine of a crawl and executes domain-level aggregation and consolidation once traversal finishes.

```text
created
  ↓
running (workers & persistence active)
  ↓
draining (task stream empty, PEL resolving)
  ↓
consolidating (graph & quality rollup execution)
  ↓
finished
```

Failure states: `failed`, `cancelled`, `timed_out`.

---

## 6.1 Completion Watchdog Conditions (Janitor in `orchestrator.py`)
A crawl is ready for consolidation only when:
1. Redis task stream has no pending work (`XPENDING` / stream length == 0);
2. Worker PELs are empty across all consumers;
3. Active processing locks (`crawl:{id}:lock:processing:*`) are zero;
4. Persistence consumer lag is zero (`results`, `telemetry`, `dlq` streams acknowledged);
5. Crawl budget is exhausted OR link traversal is naturally complete.

---

## 6.2 Crawl Consolidation & Site-Wide Quality Rollup (`app/consolidation.py`)
When the watchdog triggers the `consolidating` state, it executes an atomic aggregation over the crawl's persisted records:

1. **Volume & Coverage Metrics:** Total pages discovered, successfully processed, failed in DLQ, and dropped by telemetry filters.
2. **Contact Graph Rollup:** Unique deduplicated emails and phone numbers discovered across the entire crawl.
3. **Site-Wide SEO & Health Score:**
   - Percentage of pages with valid H1 and meta description.
   - Overall image `alt` tag coverage percentage across all pages.
   - Missing security headers compliance score (HSTS, CSP, etc.).
4. **Internal Anchor (`#hash`) Verification:**
   - Evaluates `page_links` containing URL fragments (e.g. `/about#team`) against target page DOM elements or Markdown headings, flagging broken anchor targets.
5. **State Transition:** Updates `crawls.status = 'finished'`, sets `crawls.finished_at = now()`, and writes the rollup summary into `crawls.config['consolidation']`.

---

# 7. Phase 5 — Knowledge Graph + Retrieval Foundation

**Priority: P1**

Only start this after durable page persistence works.

## 7.1 Page sections

Create:

```sql
page_sections (
    id bigserial primary key,
    page_id bigint references pages(id),
    heading text,
    content text,
    order_idx integer,
    embedding vector(<DIM>)
)
```

Split Markdown into meaningful sections rather than embedding an entire page as one vector.

> **Decision checkpoint:** Before generating the Postgres migration for `page_sections`, select the embedding model, verify its output dimension, and hard-code the `vector(N)` value.  The migration must not be committed with `<DIM>` as a placeholder.  The embedding provider, model name, and dimension must be locked in `EMBEDDING_PROVIDER`, `EMBEDDING_MODEL`, and `EMBEDDING_DIMENSION` before the schema is applied.
>
> **Phase 0 dependency:** The Phase 0 extraction redesign (item 2.9) guarantees that `raw_markdown` is always present for every successfully scraped page, regardless of which JSON strategy succeeds.  Phase 5 can therefore assume that every `pages` row has embeddable content available.  Do not build the sectionizer assuming the old fallback-only Markdown behaviour.

## 7.2 Embeddings

Choose the embedding model before finalizing the `vector(N)` dimension.

Make the provider/model configurable:

```text
EMBEDDING_PROVIDER=
EMBEDDING_MODEL=
EMBEDDING_DIMENSION=
```

The database migration must be tied to the selected dimension.

## 7.3 Entities

Use local NER first with spaCy.

Store normalized entities and `section_entities` mentions. Deduplicate entities by normalized name + type within a crawl.

## 7.4 Graph edges

Use existing crawl data for the first edge type:

```text
PAGE --LINKS_TO--> PAGE
```

Add semantic similarity edges only after embeddings are proven useful.

Do not introduce a dedicated graph database initially. Postgres tables are sufficient for the first implementation.

## 7.5 Clustering

If needed, cluster section/page embeddings with a tested graph/clustering library.

Do not assume URL-path clustering will always correspond to semantic topic clusters; keep both concepts available.

---

# 8. Phase 6 — Extractive Summaries

**Priority: P1**

The original Claude plan's token-free summary idea is good.

For each cluster:

```text
sections
   ↓
similarity graph
   ↓
TextRank / LexRank
   ↓
representative sentences
   ↓
cluster summary
```

Store summaries for section, cluster, and site scopes.

Do not call a generative LLM during graph formation.

---

# 9. Phase 7 — Lazy Query-Time Intelligence

**Priority: P1/P2**

Generative LLM usage belongs here.

### Local question

Vector search over top-k sections, then direct answer or one small LLM call.

### Global question

Use site/cluster summaries first; optionally synthesize with an LLM. Never dump every page into one prompt.

### Audit question

Prefer SQL findings + severity scoring + stored evidence, with the LLM acting as a presentation layer. The LLM should not invent findings absent from structured audit data.

---

# 10. Phase 8 — GitHub Actions Orchestration

**Priority: P1**

GitHub Actions is a good fit for ephemeral crawl workers, but the workflow should not become the source of truth.

## 10.1 Dispatch

The orchestrator/API should dispatch `crawl_id`, `target_url`, `worker_count`, and crawl configuration.

## 10.2 Worker matrix

Each matrix worker runs:

```bash
python -m app.main
```

with `CRAWL_ID=<crawl_id>` and consumes the same crawl-scoped Redis stream.

## 10.3 Do not use a fixed concurrency assumption

The original plan's specific free-tier concurrency number should not be treated as a design invariant. Read the current GitHub Actions account/repository limits before production rollout.

Expose `max_parallel` as deployment configuration.

## 10.4 Checkpoint/resume

Large sites can exceed a single runner's lifetime. The crawler must support stopping workers, recovering PEL tasks, starting replacement workers, and continuing the same `crawl_id`.

---

# 11. Phase 9 — Query API

**Priority: P2**

Possible API surface:

```text
POST   /crawls
GET    /crawls/{crawl_id}
POST   /crawls/{crawl_id}/cancel

GET    /crawls/{crawl_id}/pages
GET    /crawls/{crawl_id}/findings
GET    /crawls/{crawl_id}/graph
GET    /crawls/{crawl_id}/summary

POST   /crawls/{crawl_id}/query
```

The API should read durable Postgres state rather than querying Redis directly for historical crawl data.

---

# 12. Phase 10 — Client-Facing Audit Reports

**Priority: P2**

Generate structured reports containing executive summary, overall score, critical issues, accessibility, SEO/metadata, content quality, broken links, duplicate content, site structure, performance indicators, recommended fixes, affected URLs, and evidence.

Every recommendation should be traceable to one or more stored findings.

---

# 13. Redis Key Contract

The final key contract should look like:

```text
crawl:{id}:stream:audit_tasks
crawl:{id}:stream:audit_results
crawl:{id}:stream:dropped_telemetry
crawl:{id}:stream:dlq

crawl:{id}:group:audit_workers

crawl:{id}:set:visited_fingerprints
crawl:{id}:set:queued_fingerprints

crawl:{id}:budget:tickets_dispensed

crawl:{id}:throttle:domain:{domain}

crawl:{id}:lock:processing:{fingerprint}

crawl:{id}:stats:*
```

Avoid scattering literal Redis strings throughout the application.

---

# 14. Result/Event Contracts

## 14.1 Page result

Minimum contract (updated for cumulative extraction design from Phase 0):

```json
{
  "schema_version": 1,
  "crawl_id": "...",
  "url": "...",
  "canonical_url": "...",
  "status_code": 200,
  "depth": 1,
  "extraction_methods": ["hydration", "markitdown"],
  "xhr_state": [{}, {}],
  "hydration_state": {},
  "raw_markdown": "...",
  "contacts": {"emails": [], "phones": []},
  "audit": {},
  "screenshot_key": null,
  "fetched_at": "..."
}
```

Key differences from the original contract:

- `extraction_method` (single string) is replaced by `extraction_methods` (list): multiple strategies can succeed simultaneously.
- `extracted_json_state` (merged single dict) is replaced by `xhr_state` (list of all accepted XHR payloads) and `hydration_state` (single hydration block or null).  This preserves all captured structured data rather than discarding all but the first match.
- `raw_markdown` is now always present (empty string at minimum); it is never absent from a successful result.

## 14.2 Telemetry event

```json
{
  "schema_version": 1,
  "crawl_id": "...",
  "timestamp_utc": "...",
  "source_url": "...",
  "target_url": "...",
  "drop_reason": "DENY_LIST"
}
```

## 14.3 DLQ event

```json
{
  "schema_version": 1,
  "crawl_id": "...",
  "url": "...",
  "retry_count": 3,
  "dlq_reason": "...",
  "dlq_at_utc": "..."
}
```

---

# 15. Persistence Consumer Design

Create `app/persist_consumer.py`.

Responsibilities:

1. consume result events;
2. validate the event schema;
3. upload Markdown text and screenshot objects to Appwrite Storage Bucket;
4. persist page metadata and storage file IDs in a Postgres transaction;
5. acknowledge Redis (XACK) only after durable Postgres commit;
6. retry transient database or storage failures;
7. expose consumer lag/health;
8. support graceful shutdown.

Use a dedicated consumer group:

```text
audit_persist:{crawl_id}
```

This is separate from the worker group.

**Important:** Do not run a "drain everything after workers finish" consumer as the normal architecture. It creates a durability gap during the crawl. The persistence consumer should run concurrently with workers whenever practical.

---

# 16. Graph Builder Design

Create `app/graph_builder.py`.

Execution:

```text
crawl status = consolidating
        ↓
load persisted pages/links
        ↓
sectionize
        ↓
embed
        ↓
NER
        ↓
build graph
        ↓
cluster
        ↓
extractive summaries
        ↓
mark graph_complete
```

The builder must be restartable. Each stage should have an idempotent checkpoint rather than requiring the whole graph to rebuild after one failure.

---

# 17. Configuration Expansion

Add configuration only when the corresponding phase is implemented.

Suggested future settings:

```text
CRAWL_ID=
DATABASE_URL=

SCREENSHOT_ENABLED=false
SCREENSHOT_FULL_PAGE=true
SCREENSHOT_BUCKET=

AXE_ENABLED=true
PERFORMANCE_METRICS_ENABLED=true

EMBEDDING_PROVIDER=
EMBEDDING_MODEL=
EMBEDDING_DIMENSION=

PERSIST_BATCH_SIZE=50
PERSIST_MAX_RETRIES=5

CRAWL_TIMEOUT_SECONDS=
MAX_DISCOVERED_URLS=
```

Never put secrets into GitHub repository files.

---

# 18. Dependency Plan

Current runtime dependencies are intentionally small.

### Phase 0

```text
pytest
pytest-asyncio
```

### Phase 2

Add one standardized PostgreSQL driver/ORM/query layer.

### Phase 3

Only if used, add the selected axe-core integration.

### Phase 5

Only when graph work starts, add the selected embedding client, spaCy, a graph/clustering library, text-ranking library, and pgvector support.

Pin compatible versions once the deployment environment is defined.

---

# 19. Observability

The current logger already separates worker/system and spider activity sinks.

Extend observability with structured crawl metrics:

```text
crawl_id
worker_id
url
domain
duration_ms
status_code
extraction_method
retry_count
depth
bytes
```

Key metrics:

```text
pages/sec
success rate
blocked rate
retry rate
DLQ rate
average fetch latency
p95 fetch latency
Redis queue depth
PEL size
persistence lag
pages persisted
graph build duration
```

A production audit platform needs these before performance tuning.

---

# 20. Security and Data Handling

Because the crawler captures contacts and potentially sensitive page content:

- keep credentials in environment/secret stores;
- never log proxy passwords;
- redact authorization headers/cookies from logs;
- define retention for raw HTML and screenshots;
- restrict database access by role;
- validate target URLs before dispatch;
- prevent SSRF through the API/orchestrator;
- enforce the crawl's domain boundary at both scheduling and fetch layers;
- never allow user-supplied URLs to override internal service endpoints.

The crawler's existing contact extraction should be treated as client data, not merely debug output.

---

# 21. Updated Build Order

## P0 — Reliability first

1. Repair stale tests (`ALLOW_PATTERN` import crash; align with deny-list model).
2. Add pytest infrastructure (`pytest`, `pytest-asyncio`, Redis/Scrapling fakes).
3. Fix atomic global budget (replace non-atomic GET→INCR with a Lua script or atomic increment-and-check).
4. Fix PEL reclaim/requeue semantics (XCLAIM → XADD re-publish + XACK; stale tasks return to `>` delivery path).
5. Fix Playwright session manager leak (`try...finally` in `_run_spider` so browser is closed on exception).
6. Fix per-worker-cap double-billing (increment `throttle_count` on `MAX_PAGES_PER_RUN` requeue to prevent double budget ticket).
7. Verify or fix MarkItDown singleton thread-safety under `ThreadPoolExecutor`.
8. Centralize URL canonicalization (`canonicalize_url()` used by seed, fingerprints, link discovery, persistence).
9. Centralize Redis key generation (key functions parameterized by `crawl_id`, not global string constants).
10. Add `crawl_id` to task, result, telemetry, and DLQ payloads.
11. Add `schema_version=1` to result, telemetry, and DLQ payloads.
12. Make flush crawl-scoped (`--flush --crawl-id <id>`; add `--flush-all` for global administrative reset).
13. Redesign `run_triple_threat()` as cumulative: always run all three strategies, return `xhr_state` (list), `hydration_state`, and `raw_markdown` separately; remove short-circuit exits.
14. Replace `_XHR_RELEVANCE_KEYS` allow-list with `_XHR_TRACKING_DENY_PATTERNS` URL deny-list; collect all accepted XHR responses (not just the first).

## P1 — Durable crawler

9. Create Postgres schema.
10. Implement idempotent persistence consumer.
11. Add crawl state machine.
12. Add completion/draining logic.
13. Add result schema versioning.
14. Add screenshots and audit signals.

## P1 — Intelligence foundation

15. Sectionize pages.
16. Add embeddings.
17. Build page/link graph.
18. Add entities.
19. Add clustering.
20. Add extractive summaries.

## P2 — Product layer

21. GitHub Actions dispatch/matrix.
22. Query API.
23. Lazy LLM answering.
24. Audit scoring/report generation.
25. Dashboard/client UI.

> GitHub Actions can be introduced earlier for development once PEL recovery and crawl namespacing are reliable, but production matrix execution should not precede those fixes.

---

# 22. Acceptance Criteria

## Phase 0

- [ ] `pytest` passes with no skipped tests.
- [ ] Global page budget cannot exceed the configured limit under concurrent workers (verified with a concurrency test).
- [ ] A stale PEL task is re-published to the task stream via `XADD` and becomes available to healthy workers again.
- [ ] A Playwright session is unconditionally closed even when `spider.stream()` raises an exception.
- [ ] A per-run-cap requeue does not consume an additional global budget ticket.
- [ ] MarkItDown thread-safety is either verified or resolved.
- [ ] Locks are scoped and released safely under crash simulation.
- [ ] Canonical URLs produce deterministic fingerprints regardless of scheme casing, trailing slash, default ports, or URL fragment.
- [ ] Every result, telemetry, and DLQ event carries `crawl_id` and `schema_version`.
- [ ] Every successfully scraped page result contains a non-empty `raw_markdown` field regardless of which JSON extraction strategy succeeded.
- [ ] XHR capture collects all non-tracking JSON responses; no site-specific allow-list is applied.

## Phase 1

- [ ] Two simultaneous crawls cannot see each other's tasks.
- [ ] Two crawls can use the same domain independently.
- [ ] Flush for crawl A does not affect crawl B.
- [ ] Every result/telemetry/DLQ event carries `crawl_id`.

## Phase 2

- [ ] Every successful page is persisted.
- [ ] Duplicate Redis delivery does not duplicate pages.
- [ ] Postgres failure leaves the Redis message retryable.
- [ ] Crawl status survives worker restarts.

## Phase 3

- [ ] Screenshot storage is optional.
- [ ] Accessibility findings are persisted in normalized form.
- [ ] SEO/structural signals are queryable.
- [ ] Performance collection does not make the normal crawl unusably slow.

## Phase 4/5

- [ ] Graph build can be restarted without duplicating entities/edges.
- [ ] Embedding dimension is explicit.
- [ ] Site/cluster summaries are reproducible without generative LLM calls.

## Phase 6+

- [ ] Query answers can be traced to stored evidence.
- [ ] LLM output cannot silently create unsupported audit findings.
- [ ] A completed crawl can be queried without Redis being available.

---

# 23. What Should NOT Be Built Yet

Avoid premature complexity:

- dedicated graph database;
- separate vector database;
- always-on scraping servers;
- full Lighthouse on every page;
- LLM call per page;
- agentic browsing;
- autonomous remediation;
- giant site-wide prompts;
- complex microservice decomposition.

The existing crawler already provides the most expensive primitive: concurrent, state-coordinated page acquisition with fallback extraction. The next value comes from making that acquisition durable, measurable, queryable, and auditable.

---

# 24. Final Target Architecture

```text
                         ┌─────────────────────────────┐
                         │          Client UI           │
                         └──────────────┬──────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │       Query / Audit API      │
                         └──────────────┬──────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │       Crawl Orchestrator     │
                         │ state + dispatch + lifecycle │
                         └──────────────┬──────────────┘
                                        │
                               GitHub Actions
                                        │
               ┌────────────────────────┼────────────────────────┐
               │                        │                        │
               ▼                        ▼                        ▼
        ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
        │ Worker      │          │ Worker      │          │ Worker      │
        │ main.py     │          │ main.py     │          │ main.py     │
        └──────┬──────┘          └──────┬──────┘          └──────┬──────┘
               └────────────────────────┼────────────────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │   Redis crawl namespace     │
                         │ tasks/results/telemetry/DLQ │
                         └──────────────┬──────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │     Persistence Consumer     │
                         └──────────────┬──────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │     Supabase Postgres        │
                         │ pages / links / findings     │
                         └──────────────┬──────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │      Graph / Embeddings      │
                         │ sections / entities / edges │
                         └──────────────┬──────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │ Extractive summaries + RAG   │
                         └──────────────┬──────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │ Lazy LLM + Audit Reporting    │
                         └─────────────────────────────┘
```

## Immediate next step

Before implementing Claude's higher-level GraphRAG ideas, make the current crawler trustworthy under concurrency:

**tests → atomic budget → PEL recovery → canonical URLs → crawl namespacing → durable persistence → crawl lifecycle → audit signals → graph → query/RAG.**

That ordering minimizes rework and ensures every later layer is built on reliable crawl data rather than on today's Redis-only, single-crawl assumptions.

---

# 25. Forensic Agent Review — Additional Findings

A second forensic review was performed against the same implementation after the repo-aware plan above was written. Its findings are incorporated here as **implementation-level P0/P1 corrections**, not as a replacement for the broader roadmap.

The review confirms the existing architecture and identifies four additional reliability defects that must be fixed before the crawler is treated as production-safe. fileciteturn20file0L114-L153

## 25.1 P0 — Guaranteed cleanup of Playwright/session resources

`_run_spider()` currently closes the spider session only after successful completion of the async stream. If `spider.stream()` raises, control jumps to the worker exception handler before the cleanup code executes. This can leak browser contexts/processes and eventually exhaust worker memory. fileciteturn20file0L137-L150

### Required implementation

Change the lifecycle to:

```python
session_manager = None
try:
    async for _item in spider.stream():
        ...
finally:
    session_manager = getattr(spider, "_session_manager", None)
    if session_manager is not None:
        await session_manager.close()
```

Also test successful completion, HTTP failure, Playwright navigation timeout, browser/page crash, proxy failure, and extraction exception.

**Acceptance criterion:** no browser/session resource remains open after `_run_spider()` returns or raises.

## 25.2 P1 — Fix page-cap requeue semantics and budget accounting

When `MAX_PAGES_PER_RUN` is reached, the task is requeued without incrementing `throttle_count`. The task can subsequently look like a first attempt and consume another page-budget ticket. fileciteturn20file0L169-L171

### Required implementation

Do not treat a worker-local page-cap deferral as a fresh first attempt.

Preferred task semantics:

```text
task:
    retry_count
    throttle_count
    budget_reserved
```

If the page has already reserved a global ticket, the requeued task must carry that state rather than acquire another ticket.

The key invariant is:

> **One logical page attempt consumes at most one global page ticket.**

Add a concurrency test with multiple workers repeatedly hitting the local page cap and verify the global ticket count.

## 25.3 P1 — Make domain-slot acquisition atomic and crash-safe

The current domain semaphore performs `INCR`, checks the result, then potentially `DECR`. A crash window can leave the counter elevated, and the TTL is established only after the threshold check. fileciteturn20file0L173-L182

### Required implementation

Replace the application-level sequence with one Redis Lua operation:

```text
if current < limit:
    INCR
    EXPIRE
    return acquired
else:
    return rejected
```

The script must guarantee no counter overshoot, TTL on every successful acquisition, no mutation on rejection, and no negative counter on release.

## 25.4 P2 — Remove or explicitly repurpose the dead TelemetrySink

`TelemetrySink` is not used by the active crawler path; `spider.py` writes drop events directly to Redis. fileciteturn20file0L151-L153

Keep `DropReason` as the canonical vocabulary, then either remove `TelemetrySink` or explicitly document it as an optional local/debug sink and add a real call site.

Preferred production architecture:

```text
spider → Redis telemetry stream → telemetry consumer / durable sink
```

## 25.5 P2 — Protect the MarkItDown conversion boundary

The review flags the module-level `_MARKITDOWN` singleton because `run_triple_threat()` executes in a thread pool. Multiple worker threads can therefore call the same converter concurrently. fileciteturn20file0L189-L194

Treat this as a risk to verify with a dependency-level concurrency test rather than assuming thread-unsafety. Prefer a thread-local instance or per-conversion instance, depending on measured initialization cost.

## 25.6 P2 — Canonical URL normalization

The review confirms that `get_fingerprint()` currently hashes only a trailing-slash-normalized URL. fileciteturn20file0L195-L200

Separate:

```python
canonicalize_url(url) -> str
get_fingerprint(canonical_url) -> str
```

rather than putting URL semantics inside the hash function.

## 25.7 P2 — Remove the unused `trafilatura` import

`find_dups.py` imports `trafilatura.deduplication` without using it, while `trafilatura` is not declared in `requirements.txt`. fileciteturn20file0L202-L205

Remove the import and align duplicate detection with the canonical URL/fingerprint implementation.

# 26. Revised P0/P1 Execution Order

## P0 — Fix before adding new architecture

1. Repair `tests/smoke_test.py`.
2. Establish pytest-based regression tests.
3. Fix PEL stale-task recovery.
4. Fix atomic global budget reservation.
5. Fix `_run_spider()` cleanup with `finally`.
6. Centralize URL canonicalization.
7. Namespace Redis keys with `crawl_id`.
8. Make flush operations crawl-scoped.

## P1 — Concurrency correctness

9. Fix page-cap requeue/ticket semantics.
10. Replace domain semaphore with atomic Lua acquire/release.
11. Add concurrency tests for budget, domain slots, locks, and queue recovery.
12. Add result schema versioning.
13. Introduce durable Postgres persistence.
14. Add crawl lifecycle/completion state.

## P2 — Extraction/runtime hardening

15. Resolve MarkItDown thread-safety through a concurrency test and thread-local/per-call instantiation.
16. Remove or explicitly repurpose `TelemetrySink`.
17. Remove unused `trafilatura` import.

## Then proceed to product expansion

18. Screenshots/accessibility/SEO signals.
19. Sectionization.
20. Embeddings/entities/graph.
21. Extractive summaries.
22. GitHub Actions matrix.
23. Query API.
24. Lazy LLM/RAG.
25. Audit reporting/UI.

# 27. Forensic Acceptance Tests

Before Phase 2 durable persistence begins, the following tests should pass.

### Queue recovery

```text
worker A receives task
worker A dies
        ↓
janitor detects stale PEL
        ↓
task reclaimed
        ↓
task requeued
        ↓
worker B receives task
```

### Budget correctness

```text
N workers
M concurrent pages
GLOBAL_MAX_PAGES = K

assert total logical page tickets <= K
```

### Domain throttle correctness

```text
N workers
same domain
MAX_CONCURRENT_PER_DOMAIN = K

assert active domain slots <= K
assert counter eventually returns to 0
assert TTL exists while slot is held
```

### Browser cleanup

```text
spider.stream() succeeds → session closed
spider.stream() raises   → session closed
```

### Page-cap behavior

```text
worker page cap reached
        ↓
task deferred
        ↓
task resumes elsewhere
        ↓
no second global ticket charged
```

### Canonicalization

At minimum, the equivalence rules for these URLs must be explicitly defined and tested:

```text
https://EXAMPLE.com
https://example.com/
https://example.com#section
https://example.com:443/
```

# 28. Overall Forensic Assessment

The second review does **not** invalidate the previous architecture plan. It strengthens the case for the same central strategy:

> **Do not add GraphRAG, Postgres, GitHub Actions matrices, or an LLM query layer until the existing Redis crawler is concurrency-correct and failure-safe.**

The highest-value immediate work is now clearly bounded to the crawler reliability layer:

```text
PEL recovery
    +
atomic budget
    +
browser cleanup
    +
domain semaphore
    +
page-ticket accounting
    +
canonical URLs
    +
crawl namespacing
    +
regression tests
```

Once these invariants are enforced, the existing crawler becomes a much safer foundation for the durable persistence and intelligent audit layers described in the earlier phases.

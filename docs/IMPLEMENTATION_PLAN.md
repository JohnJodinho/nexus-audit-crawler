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

---

# 4. Phase 2 — Durable Crawl State

**Priority: P0**

Introduce Supabase Postgres as the system of record.

## 4.1 Core schema

Start smaller than the earlier plan:

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
)

pages (
    id bigserial primary key,
    crawl_id uuid not null references crawls(id),
    url text not null,
    canonical_url text not null,
    path text,
    status_code integer,
    extraction_method text,
    raw_markdown text,
    fetched_at timestamptz,
    metadata jsonb,
    unique (crawl_id, canonical_url)
)

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

## 4.2 Idempotent persistence

The consumer must tolerate duplicate delivery.

Use `crawl_id + canonical_url` as the page idempotency key.

Persistence should be:

```text
Redis result
   ↓
validate schema
   ↓
UPSERT page
   ↓
UPSERT contacts
   ↓
UPSERT discovered links
   ↓
XACK only after transaction succeeds
```

If Postgres fails, the Redis message must remain retryable.

## 4.3 Result schema versioning

Add `schema_version=1` to every result payload.

Future changes should use explicit versions rather than making the persistence consumer guess which fields a worker emitted.

---

# 5. Phase 3 — Capture Real Audit Signals During Fetch

**Priority: P1**

Do not turn the crawler into a full Lighthouse replacement. Capture high-value signals that naturally fit the existing render.

## 5.1 Screenshot

Add an optional full-page screenshot for successful Playwright/stealth fetches.

Store the object in Supabase Storage and persist only the object key/URL in Postgres.

Make screenshots configurable:

```text
SCREENSHOT_ENABLED=false
SCREENSHOT_FULL_PAGE=true
```

because screenshots can materially increase bandwidth and storage.

## 5.2 Accessibility

Use `axe-core` only when a Playwright page is available.

Persist normalized findings:

```json
{
  "rule_id": "...",
  "impact": "serious",
  "description": "...",
  "help_url": "...",
  "nodes": 3
}
```

Do not store a giant unstructured axe payload as the only representation.

## 5.3 HTML/SEO structural signals

Derive from the fetched DOM/Markdown:

- title present/missing;
- meta description present/missing;
- H1 count;
- heading hierarchy;
- canonical URL;
- robots directives;
- image alt coverage;
- internal/external link counts;
- broken internal links when status information is available;
- content length;
- indexability indicators.

## 5.4 Performance

Treat Lighthouse as optional.

Start with inexpensive browser/network measurements: response duration, time to first byte if available, DOM/content readiness, transferred bytes, request count, and basic Core Web Vitals where the browser/runtime exposes them reliably.

A separate Lighthouse pass should be introduced only if its accuracy/value justifies the extra page cost.

---

# 6. Phase 4 — Crawl Completion and Consolidation

**Priority: P0/P1**

This is a missing architectural layer in the original plan.

A crawl needs an explicit state machine:

```text
created
  ↓
seeding
  ↓
running
  ↓
draining
  ↓
persisting
  ↓
consolidating
  ↓
finished
```

Failure states:

```text
failed
cancelled
timed_out
```

## 6.1 Completion conditions

Do not rely on GitHub Actions job completion alone.

A crawl is ready for consolidation only when:

1. no new tasks are being generated;
2. Redis task stream has no available work;
3. worker PELs are empty;
4. active processing locks are zero;
5. persistence consumer lag is zero;
6. no unrecovered task remains;
7. crawl budget is either exhausted or traversal is naturally complete.

## 6.2 Counters

Track:

```text
discovered
queued
processing
processed
failed
dlq
persisted
```

These can live in Redis during the run and be finalized in Postgres.

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

Minimum contract:

```json
{
  "schema_version": 1,
  "crawl_id": "...",
  "url": "...",
  "canonical_url": "...",
  "status_code": 200,
  "depth": 1,
  "extraction_method": "xhr|hydration|markitdown|none",
  "extracted_json_state": {},
  "extracted_markdown": "...",
  "contacts": {"emails": [], "phones": []},
  "audit": {},
  "screenshot_key": null,
  "fetched_at": "..."
}
```

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
3. persist in a Postgres transaction;
4. upload binary objects when required;
5. acknowledge Redis only after durable commit;
6. retry transient database failures;
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

1. Repair stale tests.
2. Add pytest infrastructure.
3. Fix atomic global budget.
4. Fix PEL reclaim/requeue semantics.
5. Centralize URL canonicalization.
6. Centralize Redis key generation.
7. Add crawl ID to task/result/telemetry contracts.
8. Make flush crawl-scoped.

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

- [ ] `pytest` passes.
- [ ] Budget cannot exceed the configured limit under concurrent workers.
- [ ] A stale PEL task becomes available to workers again.
- [ ] Locks are scoped and released safely.
- [ ] Canonical URLs produce deterministic fingerprints.

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

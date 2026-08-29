# Nexus Audit Crawler

An enterprise-grade, highly concurrent, distributed web auditor and dual-engine crawler engineered to perform deep technical SEO audits, security header analysis, structured data entity extraction, contact harvesting, and LLM-ready Markdown generation while evading modern anti-bot protections.

Powered by **Scrapling**, **Playwright / Patchright**, **Redis Streams**, **PostgreSQL (Supabase)**, and **Appwrite Blob Storage**, the system combines high-velocity raw HTTP fetching with residential proxy-backed stealth browser escalation, dual-tier durable storage, and automated site-wide crawl consolidation.

---

## Key Capabilities & Features

- **Dual-Engine Waterfall Crawler (`app/spider.py`)**:
  - Primary raw HTTP tier (`FetcherSession`) with proxy rotation and zero-latency parsing.
  - Automatic escalation to stealth browser (`AsyncStealthySession`) upon detection of Cloudflare, DataDome, PerimeterX, JavaScript rendering challenges, or HTTP 403/429 codes.
- **Triple-Threat Extraction Pipeline (`app/extraction.py`)**:
  - **XHR / API Interception**: Captures underlying API JSON payloads while automatically filtering out analytics telemetry (GA4, Sentry, Hotjar, Segment, Pixel).
  - **Hydration State Extractor**: Parses embedded framework states (`__NEXT_DATA__`, `__NUXT__`, window states).
  - **MarkItDown Conversion**: Transforms DOM structure into clean, token-efficient Markdown for downstream LLM retrieval.
- **Enterprise Audit Signals Engine (`app/utils/audits.py`)**:
  - **Tier 1 (Security & HTTP)**: Audits HSTS, CSP, X-Frame-Options, X-Content-Type-Options, Referrer-Policy, Permissions-Policy, transport compression (`br`, `gzip`, `zstd`), and insecure `Set-Cookie` flags (`Secure`, `HttpOnly`, `SameSite`). Computes a 0–100 security score.
  - **Tier 2 (SEO & Indexability)**: Reconciles `<meta name="robots">` vs `X-Robots-Tag`, classifies canonical integrity (`matches_url`, `cross_domain`, `mismatched_path`, `missing`), extracts Schema.org JSON-LD entities, validates H1 $\rightarrow$ H2 $\rightarrow$ H3 heading progression, calculates image `alt` coverage %, and analyzes content thickness.
  - **Tier 3 (Runtime Diagnostics)**: Response time, FCP, DOM interactive, and capped JS error telemetry.
- **Dual-Tier Decoupled Storage (`app/storage/appwrite_client.py`, `app/persistence_worker.py`)**:
  - **PostgreSQL**: Stores relational crawl sessions, page metadata, extracted contacts, link graphs, and dead-letter tasks.
  - **Appwrite Storage**: Persists raw Markdown files, full-page screenshots, and oversized state dumps ($> 16\text{ KB}$) to eliminate TOAST table bloat.
- **Full-Page Stitched Screenshot Engine (`app/utils/screenshots.py`)**:
  - Incremental viewport scrolling with `IntersectionObserver` delay, sticky-header suppression (`position: fixed`), and in-memory Pillow (`PIL.Image`) stitching.
- **Contact Harvesting (`app/utils/contacts.py`)**:
  - Extracts and normalizes RFC-compliant business emails and E.164 phone numbers with strict false-positive rejection (retina asset filenames, Unix timestamps, UUIDs, disallowed test domains).
- **Lifecycle State Machine & Site-Wide Consolidation (`app/consolidation.py`)**:
  - Aggregates site-wide health scorecards, schema distributions, unique contacts, and automatically marks crawls `finished`.

---

## Architecture Topology

```mermaid
graph TD
    subgraph "Redis Stream Orchestration"
        Tasks[(stream:audit_tasks)]
        Results[(stream:audit_results)]
        Telemetry[(stream:dropped_telemetry)]
        DLQ[(stream:dlq)]
        SetVisited((set:visited_fingerprints))
        GlobalBudget((global_budget:tickets))
        DomainSlots((throttle:domain:*))
    end

    subgraph "Distributed Crawler Workers (app.main)"
        Worker1[Crawler Worker 1]
        Worker2[Crawler Worker 2]
        WorkerN[Crawler Worker N]
    end

    subgraph "Dual-Engine Spider (app.spider)"
        FastFetch[Fast HTTP Session]
        StealthBrowser[Stealth Patchright Browser]
        Audits[Audit Signals Engine]
        Stitcher[Pillow Screenshot Stitcher]
    end

    subgraph "Durable Persistence Tier (app.persistence_worker)"
        PersistWorker[Persistence Consumer]
        Appwrite[(Appwrite Blob Storage)]
        Postgres[(Supabase PostgreSQL)]
    end

    subgraph "Orchestration & Consolidation"
        Publisher[Seed Publisher]
        Janitor[PEL Janitor & Watchdog]
        Consolidator[Consolidation Engine]
    end

    Publisher -- "Enqueue Seed" --> Tasks
    Janitor -- "Reclaim Stale Tasks / Dead Letters" --> Tasks
    Janitor --> Consolidator

    Tasks -- "XREADGROUP" --> Worker1
    Worker1 --> SetVisited
    Worker1 --> GlobalBudget
    Worker1 --> DomainSlots
    Worker1 --> FastFetch

    FastFetch -. "Escalate (403/429/JS)" .-> StealthBrowser
    FastFetch & StealthBrowser --> Audits
    StealthBrowser --> Stitcher

    Audits -- "Publish Audit Payload" --> Results
    Audits -- "Publish Dropped Link" --> Telemetry
    Audits -- "Enqueue Child Links" --> Tasks

    Results & Telemetry & DLQ -- "XREADGROUP" --> PersistWorker
    PersistWorker -- "Upload Markdown & Screenshots" --> Appwrite
    PersistWorker -- "Atomic Upsert Pages & Contacts" --> Postgres
    Consolidator -- "Generate Scorecard & Finalize" --> Postgres
```

---

## Project Structure

```text
nexus-audit-crawler/
├── .env.example              # Environment configuration template
├── .gitignore                # Git exclusion patterns
├── pyproject.toml            # Build system & pytest configuration
├── requirements.txt          # Python dependencies
├── README.md                 # System overview and operational guide
├── app/                      # Core application package
│   ├── __init__.py
│   ├── config.py             # Pydantic Settings & environment variables
│   ├── consolidation.py      # Site-wide crawl consolidation & scorecard engine
│   ├── extraction.py         # Triple-Threat extraction (XHR, Hydration, MarkItDown)
│   ├── logger.py             # Structured logging router
│   ├── main.py               # Distributed crawler worker loop & gatekeeper
│   ├── orchestrator.py       # Seed publisher, CLI commands & PEL watchdog
│   ├── persistence_worker.py # Dual-tier Redis-to-Postgres/Appwrite persistence worker
│   ├── redis_client.py       # Redis connection pool, streams & concurrency semaphores
│   ├── spider.py             # Dual-engine crawler & Scrapling spider implementation
│   ├── telemetry.py          # Telemetry sink configuration
│   ├── db/                   # Database engine & session management
│   │   ├── __init__.py
│   │   └── engine.py         # SQLAlchemy async engine & sessionmaker
│   ├── models/               # Relational declarative ORM models
│   │   ├── __init__.py
│   │   └── schema.py         # Crawl, Page, PageContact, PageLink, DeadLetterTask
│   ├── storage/              # Blob storage integrations
│   │   ├── __init__.py
│   │   └── appwrite_client.py# Appwrite SDK client for Markdown & screenshots
│   └── utils/                # Utility helpers & signal extractors
│       ├── __init__.py
│       ├── audits.py         # Security headers, SEO signals, Schema.org parser
│       ├── contacts.py       # Email & phone regex extractors & validation filters
│       ├── flush_state.py    # Redis cache & stream flushing utilities
│       ├── screenshots.py    # Full-page scrolling & in-memory PIL stitcher
│       └── utilities.py      # Canonicalization, hashing & DLQ helpers
├── docs/                     # Architectural documentation
│   └── IMPLEMENTATION_PLAN.md# Technical specification & phased roadmap
├── scripts/                  # Operational & administrative scripts
│   ├── __init__.py
│   ├── export_data.py        # Dump Redis streams to JSON
│   ├── find_dups.py          # Duplicate analysis utility
│   ├── inspect_crawl_db.py   # PostgreSQL inspection script
│   ├── inspect_output.py     # Output stream inspector
│   └── verify_phase34.py     # Live end-to-end verification script
└── tests/                    # Comprehensive unit & integration test suite
    ├── __init__.py
    ├── conftest.py           # Pytest fixtures & mock environment
    ├── smoke_test.py         # Pipeline smoke tests
    ├── test_audits.py        # Security, SEO, JSON-LD, and runtime audit tests
    ├── test_consolidation.py # Consolidation engine & scorecard rollup tests
    ├── test_contacts.py      # Email & phone filter validation tests
    ├── test_extraction.py    # XHR, hydration & MarkItDown extraction tests
    ├── test_orchestrator.py  # Seed publishing, PEL recovery & DLQ tests
    ├── test_persistence.py   # PostgreSQL & Appwrite persistence tests
    ├── test_redis_client.py  # Ticket reservation & semaphore tests
    ├── test_screenshots.py   # PIL stitched screenshot tests
    ├── test_storage.py       # Appwrite storage client unit tests
    └── test_utils.py         # Canonicalization & fingerprinting tests
```

---

## Environment Setup & Configuration

### Prerequisites
- **Python 3.10+** (Python 3.12 recommended)
- **Redis 7.0+** (Local container or cloud instance)
- **PostgreSQL 14+** (Supabase or self-hosted with `uuid-ossp` and `jsonb` support)
- **Appwrite Cloud or Self-Hosted** (Optional for blob storage)

### 1. Clone & Setup Virtual Environment
```bash
git clone https://github.com/JohnJodinho/nexus-audit-crawler.git
cd nexus-audit-crawler

python -m venv venv
# Windows:
.\venv\Scripts\activate
# Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
playwright install chromium
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your connection strings:
```bash
cp .env.example .env
```

Key configuration options in `.env`:
```ini
# Redis Stream & Cluster Configuration
REDIS_URL=redis://localhost:6379/0
CRAWL_ID=setia-01

# PostgreSQL Database (Supabase / Neon / RDS)
DATABASE_URL=postgresql+asyncpg://user:password@aws-1-eu-west-1.pooler.supabase.com:5432/postgres

# Appwrite Blob Storage (Dual-Tier Offload)
APP_WRITE_PROJECT_ID=your_project_id
APP_WRITE_API_KEY=your_secret_api_key
APP_WRITE_API_ENDPOINT=https://cloud.appwrite.io/v1
APP_WRITE_BUCKET_ID=nexus-audit-bucket

# Crawler Tuning & Concurrency
WORKER_COUNT=3
GLOBAL_MAX_PAGES=50
MAX_DEPTH=2
MAX_CONCURRENT_PER_DOMAIN=2
DEFAULT_DOWNLOAD_DELAY=1.5

# Screenshots & Settle Shields
SCREENSHOT_ENABLED=true
SCREENSHOT_FULL_PAGE=true
NETWORK_IDLE_TIMEOUT_MS=3000
PAGE_SETTLE_DELAY_MS=500
```

---

## Operational Guide

### 1. Initialize Database Schema
The database tables (`crawls`, `pages`, `page_contacts`, `page_links`, `dropped_telemetry`, `dead_letter_tasks`) are automatically initialized on startup, or you can verify via:
```bash
python -m scripts.inspect_crawl_db
```

### 2. Publish Seed URL
Enqueue a seed URL to initiate a crawl for a target domain:
```bash
python -m app.orchestrator --seed https://www.setialaw.com --domain setialaw.com --crawl-id setia-01
```

### 3. Start Distributed Crawler Workers
Launch one or more worker instances to consume tasks from Redis Streams:
```bash
python -m app.main
```

### 4. Start Persistence Consumer
In a separate terminal or service process, start the persistence worker to drain audit results and write to Postgres + Appwrite:
```bash
python -m app.persistence_worker
```

### 5. Run the Janitor & Completion Watchdog
Start the janitor watchdog to reclaim abandoned tasks and trigger automatic consolidation on completion:
```bash
python -m app.orchestrator --janitor
```

### 6. Run Standalone Crawl Consolidation
Manually compute scorecards and finalize a completed crawl at any time:
```bash
python -m app.orchestrator --consolidate --crawl-id setia-01
```

### 7. Reset / Flush Crawler State
To reset Redis queues and semaphores for a specific crawl:
```bash
python -m app.orchestrator --flush --crawl-id setia-01
```

---

## Automated Test Suite

The project includes an extensive test suite covering unit, mock, and integration scenarios:

```bash
pytest -v
```

```text
============================ 106 passed in 21.37s =============================
```

---

## License

MIT License. Designed and engineered for high-performance enterprise web auditing.

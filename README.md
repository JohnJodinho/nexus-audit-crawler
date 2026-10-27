# Nexus — Distributed Audit Crawler & Query API

[![CI Pipeline](https://github.com/JohnJodinho/nexus-audit-crawler/actions/workflows/ci.yml/badge.svg)](https://github.com/JohnJodinho/nexus-audit-crawler/actions/workflows/ci.yml)
[![FastAPI](https://img.shields.io/badge/API-FastAPI-009688?style=flat&logo=fastapi)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/Database-Supabase%20Postgres-336791?style=flat&logo=postgresql)](https://supabase.com)
[![Redis](https://img.shields.io/badge/Queue-Upstash%20Redis-DC382D?style=flat&logo=redis)](https://upstash.com)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB?style=flat&logo=python)](https://www.python.org)

> **High-throughput, deterministic web crawler and audit analysis engine with dual-tier storage (Supabase PostgreSQL + Appwrite Storage) and FastAPI Query API.**

---

## Architecture Overview

```text
                                  USER / AGENT / CLIENTS
                                            │
                                            │ HTTPS (REST / MCP)
                                            ▼
                    ┌──────────────────────────────────────────────┐
                    │          FastAPI Query REST API              │
                    │   (/api/crawls, /status, /findings, /pages)   │
                    └───────┬──────────────┬──────────────┬────────┘
                            │              │              │
             ┌──────────────┘              │              └──────────────┐
             ▼                             ▼                             ▼
   ┌───────────────────┐         ┌───────────────────┐         ┌───────────────────┐
   │ Supabase Postgres │         │  Upstash Redis    │         │ Appwrite Storage  │
   │ pages / findings  │         │  Task Streams     │         │ Markdown Blobs    │
   └─────────▲─────────┘         └─────────▲─────────┘         └─────────▲─────────┘
             │                             │                             │
             └──────────────────────┐      │      ┌──────────────────────┘
                                    │      │      │
                          ┌─────────┴──────┴──────┴─────────┐
                          │   Distributed Crawler Workers   │
                          │   + Persistence Stream Worker   │
                          └─────────────────────────────────┘
```

---

## Key Features

1. **Deterministic Audit Pipeline (`app/audit/`):**
   - 16 strict boolean audit rules evaluating SEO, Security Headers (CSP, HSTS, X-Frame), Accessibility, and Performance.
   - Closed canvas zones (`head`, `content`, `nav`, `footer`, `server`) and collision-free finding identifiers (`{page_id}:{rule_id}`).
   - Zero LLM calls during crawl time — pure high-speed deterministic evaluation.

2. **Distributed Redis Task Stream & Lua Concurrency (`app/redis_client.py`):**
   - Atomic token bucket per-domain rate limiting via Lua scripts.
   - Pending Entries List (PEL) automatic recovery janitor loop for dead workers.
   - Dead Letter Queue (DLQ) isolation for unreachable or failing targets.

3. **Dual-Tier Durable Persistence (`app/persistence_worker.py`):**
   - Heavy Markdown bodies and screenshots saved as blobs in Appwrite Cloud Storage.
   - Structured metadata, contacts (emails/phones), links, and findings indexed in Supabase PostgreSQL via Async SQLAlchemy.

4. **FastAPI Query API (`app/api/`):**
   - Full REST endpoints for crawl lifecycle management, paginated finding lookups, and rollup summaries.

5. **Model Context Protocol (MCP) Adapter (`app/mcp/`):**
   - Exposes 6 standard MCP agent tools (`start_audit`, `get_audit_status`, `get_audit_summary`, `get_audit_findings`, `get_finding`, `get_page_audit`) for AI agents.

---

## REST API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/crawls` | Enqueue a new seed URL and start audit crawl job |
| `GET` | `/api/crawls/{id}` | Get top-level crawl configuration and metadata |
| `GET` | `/api/crawls/{id}/status` | Poll runtime progress (`0.0`–`1.0`) and next recommended action (`wait` \| `retrieve`) |
| `GET` | `/api/crawls/{id}/findings` | Retrieve paginated findings with category, severity, and zone filters |
| `GET` | `/api/crawls/{id}/findings/{id}` | Inspect full finding details with evidence DOM selector and proposed remediation |
| `PATCH` | `/api/crawls/{id}/findings/{id}` | Update finding review status (`open`, `approved`, `rejected`, `pending_review`) |
| `GET` | `/api/crawls/{id}/pages` | List crawled pages with nested finding counters |
| `GET` | `/api/crawls/{id}/pages/{id}` | Get full page detail including Appwrite storage pointers, contacts, and findings |
| `GET` | `/api/crawls/{id}/summary` | Aggregated rollup counts by category and severity with top 5 critical findings |
| `GET` | `/health` | Service uptime and health probe |

---

## Directory Structure

```text
nexus-audit-crawler/
├── .github/workflows/
│   ├── ci.yml                     # Automated pytest & Docker build validation
│   ├── deploy.yml                 # Render deployment hook trigger
│   └── crawler_dispatch.yml       # Ephemeral GitHub Actions crawler runner
├── app/
│   ├── api/                       # FastAPI Query REST API
│   │   ├── routes/                # Endpoint route handlers
│   │   ├── app.py                 # Application factory
│   │   ├── deps.py                # Database & Redis dependency injections
│   │   └── schemas.py             # Pydantic request & response schemas
│   ├── audit/                     # Deterministic rule engine & taxonomy
│   ├── db/                        # Async SQLAlchemy engine
│   ├── mcp/                       # FastMCP server & HTTP client
│   ├── models/                    # PostgreSQL ORM models (Crawl, Page, AuditFinding)
│   ├── storage/                   # Appwrite storage client
│   ├── utils/                     # URL canonicalization, extraction, contacts
│   ├── main.py                    # Crawler worker manager
│   ├── orchestrator.py            # Seed publisher & PEL janitor
│   ├── persistence_worker.py      # Stream persistence consumer
│   ├── redis_client.py            # Redis key namespaces & Lua rate limiters
│   └── spider.py                  # Scrapling multi-session spider
├── scripts/                       # Database initialization and management scripts
├── tests/                         # Comprehensive pytest test suite (147 tests)
├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── render.yaml                    # Render Blueprint Infrastructure-as-Code
└── requirements.txt
```

---

## Local Setup & Development

### 1. Prerequisites
- Python 3.12+
- Redis (or an [Upstash Redis](https://upstash.com/) instance)
- PostgreSQL (or a [Supabase](https://supabase.com/) project)

### 2. Installation
```bash
# Clone the repository
git clone https://github.com/JohnJodinho/nexus-audit-crawler.git
cd nexus-audit-crawler

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: .\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Environment Variables
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```

The API owns operator sign-in; no third-party identity provider is used.
Generate a password hash with `python -m scripts.generate_api_password_hash`
and a signing secret with
`python -c "import secrets; print(secrets.token_urlsafe(48))"`. Configure
`API_OPERATOR_USERNAME`, `API_OPERATOR_PASSWORD_HASH`, and `API_TOKEN_SECRET`
in the environment or secret manager. The public `POST /api/auth/login` endpoint
checks the credentials and returns a signed bearer token valid for 15 minutes.
The console must keep that token in memory only, not in local or session
storage; a reload or token expiry requires signing in again. Login attempts are
rate-limited through Redis. Protected API routes fail closed until the
credentials and signing secret are configured; `/health` remains public.

Set `API_CORS_ORIGINS` to the exact allowed browser origins (production defaults
to `https://console.jouledynamics.me`; add a local development origin explicitly
when needed). CORS is not an authentication boundary.

For SSE, use `DATABASE_LISTEN_URL` when the normal database URL uses a
transaction-mode pooler. The listener URL must use a direct or session-mode
PostgreSQL connection because SSE requires a persistent `LISTEN` connection.

### 4. Initialize Database Tables
```bash
python scripts/init_db.py
```

### 5. Running the Services

**Run the FastAPI REST Server:**
```bash
uvicorn app.api.app:app --reload --host 0.0.0.0 --port 8000
```
Interactive Swagger docs available at `http://localhost:8000/docs`.

**Run the Stream Persistence Worker:**
```bash
python -m app.persistence_worker --crawl-id default
```
The worker commits each result together with its event notification and a
message-id marker before acknowledging the Redis stream entry. This makes
redelivery after an acknowledgement failure idempotent; the marker table grows
with the number of successfully persisted stream messages.

**Run Crawler Workers:**
```bash
export CRAWL_ID="default"
export GLOBAL_MAX_PAGES="15"
python -m app.main
```

---

## Running Automated Tests

```bash
pytest tests/ -v
```

---

## Containerization & Deployment

### Run with Docker locally
```bash
docker build -t nexus-audit-crawler .
docker run -p 8000:8000 --env-file .env nexus-audit-crawler
```

### Deploy to Render
This repository includes a [`render.yaml`](render.yaml) Blueprint:
1. Go to [Render Dashboard](https://dashboard.render.com/) → **Blueprints** → **New Blueprint Instance**.
2. Select this repository.
3. Provide your environment variables (`REDIS_URL`, `DATABASE_URL`, `APP_WRITE_*`,
   `API_OPERATOR_USERNAME`, `API_OPERATOR_PASSWORD_HASH`, `API_TOKEN_SECRET`,
   and `API_CORS_ORIGINS`). Configure `DATABASE_LISTEN_URL` if `DATABASE_URL`
   is not a direct or session-mode PostgreSQL connection.
4. Render will automatically spin up the `nexus-query-api` Web Service and `nexus-persistence-worker` Background Worker.

---

## License

MIT License.

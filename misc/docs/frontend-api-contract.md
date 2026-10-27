# Nexus Console Frontend API Contract

This document is the API integration companion to the [frontend crawl management plan](./frontend-crawl-management-plan.md). It describes the existing FastAPI contract for a separate browser frontend; it does not define a new backend or change API behavior.

## API base URL and reference

- Production base URL: `https://nexus-query-api.onrender.com`
- Interactive API docs: [`/docs`](https://nexus-query-api.onrender.com/docs)
- OpenAPI JSON: [`/openapi.json`](https://nexus-query-api.onrender.com/openapi.json)
- Health check: [`/health`](https://nexus-query-api.onrender.com/health)

The OpenAPI URL is the machine-readable contract to consult when generating or updating frontend types. The local implementation is also defined by `app/api/schemas.py` and the route handlers under `app/api/routes/`. When this document and the live OpenAPI schema differ, verify the deployed behavior before relying on either.

At the time this document was prepared, a request to the production OpenAPI URL returned HTTP 503. The URL is provided as the deployed API reference, but confirm the service is available and the schema loads before generating client types.

## Authentication and browser access

### Login

`POST /api/auth/login` is the only unauthenticated authentication/API endpoint. The operational `/` and `/health` probes are also public and do not require a token.

Request:

```json
{
  "username": "operator@example.com",
  "password": "operator password"
}
```

Success (`200`):

```json
{
  "access_token": "<short-lived signed token>",
  "token_type": "bearer",
  "expires_in": 900
}
```

Send the token on every protected REST request as:

```http
Authorization: Bearer <access_token>
```

The frontend must keep the token in memory only. Do not put credentials or tokens in URLs, logs, local storage, session storage, or frontend build-time environment variables. There is no refresh-token or logout API: clear the in-memory token on logout, and request a new login after expiry or page reload. A `401` from a protected endpoint should return the user to login.

The login may return `401` for invalid credentials, `422` for invalid request data, or `503` if authentication or its Redis-backed rate limiter is unavailable. Protected endpoints require the bearer token; missing or invalid tokens return `401`.

The production browser origin must be permitted by the API's configured CORS origins. The API allows the `Authorization`, `Content-Type`, and `Last-Event-ID` headers. CORS is not authentication; the API validates the bearer token independently.

## REST endpoints

All paths below are relative to the production base URL. Use the bearer token on every `/api` route except `POST /api/auth/login`. The `/` and `/health` operational probes are public.

| Method | Path | Purpose and contract notes |
|---|---|---|
| `GET` | `/api/crawls` | List crawls. Query: `domain`, `status`, `limit` (1–100, default 20), `offset` (default 0). Returns `total`, `limit`, `offset`, and `crawls`. |
| `POST` | `/api/crawls` | Create a crawl. Body: `{ "url": "https://example.com", "crawl_id": "optional-id", "config": { "max_pages": 15, "max_depth": 2, "worker_count": 2, "categories": null } }`. `url` is required; `config` defaults to the API's `CrawlConfig`. Returns `201` with crawl ID, status, message, and duplicate information. |
| `GET` | `/api/crawls/{crawl_id}` | Get crawl metadata, status, timestamps, counters, configuration, optional consolidated report, and optional error. |
| `DELETE` | `/api/crawls/{crawl_id}` | Permanently delete the crawl and associated database records, Redis state, and storage assets. Require an explicit, crawl-specific confirmation in the UI. |
| `GET` | `/api/crawls/{crawl_id}/status` | Get current status, counts, estimated `progress` from 0 to 1, and `next_recommended_action` (`wait`, `retrieve`, or `none`). |
| `POST` | `/api/crawls/{crawl_id}/pause` | Pause a queued or running crawl. |
| `POST` | `/api/crawls/{crawl_id}/resume` | Resume a paused crawl. |
| `POST` | `/api/crawls/{crawl_id}/cancel` | Cancel a non-terminal crawl. Calling on an already terminal crawl returns its existing terminal state. |
| `POST` | `/api/crawls/{crawl_id}/retry-failed` | Requeue tasks from the dead-letter queue. The response includes the requeued count in `details`. |
| `POST` | `/api/crawls/{crawl_id}/consolidate` | Run final consolidation and return the report under `details`. |
| `GET` | `/api/crawls/{crawl_id}/summary` | Return page totals, pages with issues, counts by category and severity, and up to five top findings. |
| `GET` | `/api/crawls/{crawl_id}/pages` | Paginate pages with `page` (1-based), `per_page` (1–200, default 50), and optional `has_findings` boolean. |
| `GET` | `/api/crawls/{crawl_id}/pages/{page_id}` | Return one page's metadata and its findings. Page IDs are numeric. |
| `GET` | `/api/crawls/{crawl_id}/findings` | Paginate findings with `page` (1-based), `per_page` (1–200, default 50), and optional `category`, `severity`, `canvas_zone`, and `status` filters. |
| `GET` | `/api/crawls/{crawl_id}/findings/{finding_id}` | Get one finding, including evidence and optional remediation. Finding IDs are strings. |
| `PATCH` | `/api/crawls/{crawl_id}/findings/{finding_id}` | Set finding review status. Body: `{ "status": "open" }`; allowed values: `open`, `approved`, `rejected`, `pending_review`. |
| `GET` | `/api/crawls/{crawl_id}/telemetry` | Return total dropped links, reason counts, and up to 50 recent events. |
| `GET` | `/api/crawls/{crawl_id}/dlq` | Return dead-letter count and recent items. Query: `limit` (1–200, default 50). |
| `GET` | `/api/crawls/{crawl_id}/graph` | Return graph nodes and edges. Query: `limit` (1–1000, default 200). |
| `GET` | `/api/crawls/{crawl_id}/export` | Return an export wrapper. Query: `format=json` or `format=markdown` (default `json`). The response's `content` field is a string containing the serialized report. |

### Response shape highlights

- Crawl-list items include `id`, `target_url`, `target_domain`, `status`, timestamps, `pages_processed`, `pages_failed`, and `findings_count`.
- Crawl status can include `queued`, `running`, `paused`, `draining`, `consolidating`, `finished`, `completed`, `failed`, or `cancelled`. Do not assume the status set is limited to the currently common states.
- Page and finding list responses include `total`, `page`, `per_page`, `total_pages`, and their respective `pages` or `findings` arrays. Empty results report `total_pages: 1`.
- Finding categories currently include `seo`, `security`, `performance`, and `accessibility`; severities include `critical`, `warning`, and `info`; canvas zones include `nav`, `head`, `content`, `footer`, and `server`.
- Most errors use FastAPI's `{ "detail": ... }` response shape. Validation errors may have a structured `detail` array. Display a safe server-provided message when available, and provide a generic fallback for network errors.
- Handle `400` invalid state/cursor requests, `401` authentication failures, `404` missing crawl/page/finding, `422` validation failures, and `503` service/dependency unavailability. Do not treat a failed action as successful.
- The status `progress` is an estimate, not a guarantee of work completion. Use the API's status and recommended action alongside it.

## Live updates with authenticated SSE

The authenticated event stream is `GET /api/events`, optionally filtered to one crawl with `?crawl_id={crawl_id}`. It sends `Content-Type: text/event-stream`; response headers disable caching and buffering.

Each persisted event frame uses standard SSE fields (`id`, `event`, `data`). Its `data` JSON generally has this shape:

```json
{
  "crawl_id": "crawl-id-or-null",
  "payload": {}
}
```

Event names currently emitted include:

- `crawl.created`
- `crawl.status_changed`
- `crawl.completed`
- `crawl.deleted`
- `crawl.tasks_retried`
- `page.available`
- `finding.status_changed`
- `action.outcome`

Payload details depend on the event type. Treat these as invalidation/change notifications: refresh the relevant REST snapshot rather than using SSE as a replacement data store. For example, `page.available` signals that page/finding data changed; fetch the pages, findings, summary, or status needed for the screen.

On a new connection without a cursor, the server sends a `ready` event with the current cursor and begins streaming new events. To replay after reconnect, send the last processed event ID in the `Last-Event-ID` header. The API retains events for 30 days. If the cursor has expired, the server emits `resync_required` with reason `event_cursor_expired`; reload the screen's REST snapshots and reconnect from the new stream state. A cursor ahead of the current event stream returns `400`.

The stream may emit `: keep-alive` comments every 15 seconds. A `stream_error` event with `{"retryable":true}` indicates an event-query failure; reconnect with backoff and refresh REST snapshots as appropriate.

Native browser `EventSource` does not support setting an `Authorization` header. Use `fetch()` with a streaming `ReadableStream` (or an SSE client that explicitly supports custom headers) so the frontend can send both `Authorization: Bearer ...` and `Last-Event-ID`. Implement SSE frame parsing, cancellation on logout/token expiry, reconnect backoff, and cursor persistence in memory. Do not put the bearer token in the query string.

SSE notifications are backed by PostgreSQL events and notifications. They do not promise a fixed event frequency; use REST for initial screen state and authoritative current data, and SSE to trigger updates without timed polling.

## Frontend integration checklist

1. Load and review the deployed OpenAPI document before generating or changing API types; verify the live environment is healthy if the document is unavailable.
2. Keep the API base URL in frontend runtime/build configuration, but never place operator credentials, password hashes, Redis URLs, database URLs, or token-signing secrets in the frontend.
3. Use one typed API client that consistently adds the bearer header, handles JSON and HTTP errors, and clears auth state on `401`.
4. Keep response IDs as opaque strings except numeric `page_id`; URL-encode path/query values.
5. Use server-side pagination and filters rather than downloading a whole crawl for a list screen.
6. Confirm before delete and clearly communicate its permanent cascading effect.
7. Use authenticated SSE for live invalidation; do not add interval-based polling. Fetch REST snapshots on initial load, after relevant events, after `resync_required`, and after user-requested refresh.
8. Treat response bodies as untrusted display data: render text safely and do not inject returned HTML.

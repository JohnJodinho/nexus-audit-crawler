# Joule Dynamics Nexus Crawl Management Console — Project Plan

**Status**: Decisions resolved; Phase 1 API protection, audit, and SSE implementation is complete and locally verified. Production deployment and verification are still pending.
**Created**: 2026-10-27
**Mode**: Separate frontend repository integrating the deployed Nexus API

## 1. Product Goal & Scope

Create a secure, operator-facing web console for managing live Nexus Audit Crawler runs and reviewing their results. The console will be a dedicated application at `https://console.jouledynamics.me`, integrated visually and navigationally with `https://www.jouledynamics.me`.

The frontend is a dedicated repository and independent Vercel project, not a monorepo and not a new backend. It consumes the existing Nexus Query API at `https://nexus-query-api.onrender.com`; crawl data remains in that API and its existing backing services. The existing API must be secured before production mutation controls are enabled.

### In scope for V1

- List, filter, paginate, and inspect crawls.
- Create crawls using API defaults, with optional advanced limits and category configuration.
- Inspect crawl status, progress, summary, counts, and errors.
- Pause, resume, cancel, retry failed tasks, consolidate, and delete crawls.
- Inspect telemetry and the dead-letter queue (DLQ).
- Browse pages and findings, inspect finding evidence, and update finding review status.
- View a crawl graph.
- Export crawl reports as JSON or Markdown.
- Single-operator sign-in and API-side authorization.
- Responsive, accessible UI with useful loading, empty, error, and confirmation states.

### Explicitly out of scope

- A new backend, database, or duplicate crawl-data store.
- Changes to crawler internals or replacing the existing API.
- AI/LLM reasoning, ChatGPT/MCP integration, WebMCP, remediation execution, or a static fixture-only demo.
- GraphRAG, embeddings, Knowledge Graph, community detection, and pgvector; existing roadmap notes defer these indefinitely.
- Multi-tenant customer onboarding, teams, roles, billing, or public self-service.
- A user-facing action-history screen or API endpoint; server-side action audit records are retained for operations and security.

## 2. Users, Architecture & Deployment Shape

### Primary operator

One authorized Joule Dynamics operator uses the console to start and manage crawls, monitor execution, inspect audit results, and download reports. Treat all API requests as untrusted until the API verifies the operator's identity and access.

### Architecture

```text
Operator browser
  ├── https://www.jouledynamics.me (existing marketing/product site)
  └── https://console.jouledynamics.me (new Vercel frontend project)
          │ HTTPS + short-lived FastAPI bearer token
          ▼
  https://nexus-query-api.onrender.com
      existing Nexus Query API and existing data services
```

- Create a new Git repository for the console and connect it to a separate Vercel project.
- Build a React + TypeScript single-page application (SPA), with a typed API client generated from or checked against the live OpenAPI document.
- Call the existing API directly from the browser after operator sign-in; do not put a client secret or privileged API key in frontend code.
- Use the API as the only source of crawl and finding records. No frontend database is required.
- Keep any future Vercel preview deployments separate from production identity, API permissions, and CORS allowlists.
- Add a link from the existing Joule Dynamics site to the console. The user prefers the dedicated subdomain; do not make the console a path-mounted application under `/console`.

### Why a subdomain

`console.jouledynamics.me` can be deployed and rolled back independently of the current Vercel site and avoids coupling the console's SPA routing/build setup to the existing site. It remains under the same brand and registrable domain. It is not the same browser origin as `www.jouledynamics.me`, so API CORS must explicitly allow the console's origin. The application must also not assume that same-site cookies or sharing a parent domain provides API authentication.

## 3. Requirements & Assumptions

### Confirmed requirements

- The existing main site is hosted on Vercel.
- DNS for `console.jouledynamics.me` can be managed through Namecheap.
- The preferred integration is a separate console subdomain.
- V1 should manage all capabilities exposed by the deployed API listed in this plan.
- API defaults should populate the crawl form; additional crawl limits/categories are optional advanced settings.
- All browser-side timed polling is replaced by authenticated Server-Sent Events (SSE); the UI uses REST snapshots for initial loads and after relevant events.
- Existing API services remain the source of crawl data; no console datastore.
- Use a FastAPI-hosted username/password login for one operator. FastAPI verifies the password and issues a signed 15-minute bearer token; the browser keeps it in memory only.
- Require authorization for all API reads and mutations; allow only the configured operator in V1.
- No other API clients are currently active. Do not share the operator password or token with automation; define a separate service-authentication mechanism if a trusted integration is later required.
- Retain server-side action audit records in Postgres for one year.
- Vercel Preview deployments must not access production crawl data or receive production operator credentials.
- Frontend implementation defaults are React + TypeScript and Fluent UI v9.
- Use SSE for all live browser updates: lifecycle/progress, page and finding availability, telemetry/DLQ changes, and action outcomes. SSE sends durable change notifications; the UI fetches current REST snapshots after events.
- SSE replaces browser timer polling only. Existing server-side Redis stream consumers and janitor reads remain unchanged.
- Keep repository naming and final Joule Dynamics logo/brand assets open for the frontend planning/implementation step.

### Confirmed implementation prerequisites

- The project owner can modify this API repository and deploy/configure its Render service.
- The project owner can configure the operator username, scrypt password hash, and token signing secret through managed environment configuration, not source control.
- DNS for `console.jouledynamics.me` is managed through Namecheap; Vercel and DNS access are available.
- The deployed OpenAPI schema and endpoint behavior remain authoritative; verify them again during implementation because the API may evolve.

### API observations at planning time

The deployed OpenAPI document at `https://nexus-query-api.onrender.com/openapi.json` returned successfully during planning. It describes the operations mapped in Section 4. The OpenAPI document declared no security scheme. A preflight request for an unrelated example origin was answered with that origin and broad methods; CORS must be rechecked in the actual deployment. The local API app was observed to configure wildcard origins and credentials. Neither CORS nor an obscured frontend URL is authorization.

The current app exposes high-impact actions, including crawl deletion described as a cascading purge across database records, Redis stream keys, and storage assets. These controls must not be presented as protected merely because the UI has a login screen.

## 4. Routes, Screens & API Mapping

Use the `/api/crawls` prefix at the existing API base URL. Confirm response shapes and status codes against the OpenAPI contract and integration tests as part of implementation.

| Console route / screen | User capabilities | API operations |
|---|---|---|
| `/login` | Submit the operator username and password to the API; retain the returned token in memory and return to the intended console route. | `POST /api/auth/login`; protected API requests use the returned bearer token. |
| `/crawls` | Dashboard table; filter by domain/status; paginate; receive live invalidation events; manually refresh; navigate to detail; start a crawl. | `GET /api/crawls` with `domain`, `status`, `limit`, `offset`; `POST /api/crawls`; new authenticated `GET /api/events` SSE stream. |
| `/crawls/new` or create dialog | Enter target URL; submit using API defaults; optionally set advanced crawl config and category choices. Display duplicate/idempotency response clearly. | `POST /api/crawls`; request schema `CrawlCreateRequest` with required `url`, optional `crawl_id` and `config`. |
| `/crawls/:crawlId` | Show target, lifecycle status, progress, timestamps, page counts, findings counts, summary, errors, and eligible actions. Open an authenticated SSE connection; refresh REST snapshots only on connection/resync or relevant events, not on a timer. | `GET /api/crawls/{crawl_id}`, `GET /api/crawls/{crawl_id}/status`, `GET /api/crawls/{crawl_id}/summary`; `GET /api/events?crawl_id={crawl_id}` SSE stream. |
| Crawl detail actions | Pause/resume, cancel, retry failed work, consolidate, delete after explicit confirmation. Hide or disable actions that are invalid for the returned status; still handle API rejection. | `POST /api/crawls/{crawl_id}/pause`, `/resume`, `/cancel`, `/retry-failed`, `/consolidate`; `DELETE /api/crawls/{crawl_id}`. |
| `/crawls/:crawlId/operations` or detail tab | Inspect dropped-event telemetry and failed task records, including counts/reasons and bounded recent entries. | `GET /api/crawls/{crawl_id}/telemetry`; `GET /api/crawls/{crawl_id}/dlq` with `limit`. |
| `/crawls/:crawlId/pages` | Paginate pages; optionally filter to pages with findings; show status code and finding counts; navigate to page detail. | `GET /api/crawls/{crawl_id}/pages` with `has_findings`, `page`, `per_page`; `GET /api/crawls/{crawl_id}/pages/{page_id}`. |
| `/crawls/:crawlId/findings` | Paginate/filter by category, severity, canvas zone, and status; inspect a finding; update its review status. | `GET /api/crawls/{crawl_id}/findings` with filters and pagination; `GET /api/crawls/{crawl_id}/findings/{finding_id}`; `PATCH` the same path with `FindingUpdateRequest`. |
| `/crawls/:crawlId/graph` | Render bounded nodes and edges; provide a useful empty/error/large-graph fallback. | `GET /api/crawls/{crawl_id}/graph` with `limit`. |
| Export action from crawl detail | Download a report in the selected format, with filename and errors handled clearly. | `GET /api/crawls/{crawl_id}/export?format=json` or `format=markdown`. |

### API contract and UI behavior notes

- Crawl listing exposes `total`, `limit`, `offset`, and crawl summaries; cap page-size controls to the API's documented maximum (currently 100).
- Status includes progress, pages discovered/processed/failed, findings count, and `next_recommended_action`. Display progress as an estimate and never imply stronger completion guarantees than the API reports.
- The crawl creation config schema includes `max_pages`, `max_depth`, `worker_count`, and `categories`; the API documents worker count between 1 and 4. Show effective defaults, validation, and server-side errors. Do not invent defaults if absent from the returned contract.
- Finding review status values documented by the API are `open`, `approved`, `rejected`, and `pending_review`. Render those states distinctly and update only through the API.
- Use server-side filters and pagination, keeping query state in the URL where practical so list views can be bookmarked.
- Confirm delete by naming the target crawl and explaining that deletion is cascading and may be irreversible. Do not make delete the default or primary action.
- Respect API response headers/content type for export downloads; do not assume the JSON wrapper's `content` field is necessarily a direct file response until verified.
- The SSE endpoint is a new API contract, not part of the currently deployed OpenAPI. Define and document event names, payload schema, event IDs, authorization, reconnect behavior, retention, and `resync_required` semantics before frontend integration.

## 5. Authentication, Authorization & API Protection

### Plain-language explanation

FastAPI itself handles sign-in. The console sends the username and password to `POST /api/auth/login` over HTTPS. FastAPI checks the configured username and scrypt password hash, then signs a bearer token that expires after 15 minutes. Every crawl-data, mutation, and SSE route verifies that signature and expiry on the server. The frontend is not trusted to grant itself access.

The API uses one operator account. Store `API_OPERATOR_USERNAME`, `API_OPERATOR_PASSWORD_HASH`, and a randomly generated `API_TOKEN_SECRET` in Render's managed environment settings. Generate the password hash locally with `scripts/generate_api_password_hash.py`; never store the raw password or signing secret in the repository. Login attempts are rate-limited through Redis. The SPA keeps tokens in memory only; refresh, token expiry, or browser restart requires signing in again. There is no refresh token or persistent browser session in V1.

### Required API-side protections before production mutations

1. Add bearer-token authentication and authorization to **every** Nexus API operation that exposes crawl data or performs an action, including the SSE stream. Permit only the configured operator; do not trust an identity or role supplied in a request body or frontend flag.
2. Sign tokens with a high-entropy server-only secret using a fixed algorithm; verify signature, issuer, audience, subject, and expiry. Keep tokens short-lived and rotate the signing secret through managed configuration if it is exposed.
3. Ensure unauthenticated callers receive `401`, invalid credentials are reported generically, and unavailable server configuration fails closed with `503`. Test missing, malformed, expired, tampered, and wrong-subject tokens.
4. Protect destructive and state-changing routes at the API boundary: create, pause, resume, cancel, retry, consolidate, finding status updates, and delete. Protect reads as well unless an explicit public-data policy is approved.
5. Restrict production CORS to the exact origin `https://console.jouledynamics.me`, required methods, and required headers (including `Authorization` and `Content-Type` as needed). Do not combine wildcard origins with credentials. Allow unauthenticated browser preflight `OPTIONS` responses, but require valid authorization on the actual API request.
6. Configure a separate, explicit development/test allowlist if needed. Do not allow arbitrary Vercel preview deployment origins to access production credentials or data.
7. Keep the password hash and token signing secret in Render's managed secret configuration. Never put them in Vite `VITE_*` variables, frontend source, client bundles, commits, or logs.
8. Use HTTPS. Keep access tokens in JavaScript memory only, never local/session storage; do not log bearer tokens or sensitive finding payloads. Expect a sign-in after a page reload or 15-minute token expiry.
9. Rate-limit login attempts through Redis and return actionable but non-sensitive error messages. Use a strong unique operator password and rotate it by replacing its hash.
10. Do not expose the operator credentials as a shared API key or use them from automation. If trusted service clients are later needed, define a distinct server-to-server credential and authorization scheme.
11. Persist server-side action audit records in Postgres for one year, recording operator identity, operation, target crawl/finding, timestamp, and outcome while excluding credentials and sensitive token contents. No user-facing action-history endpoint is included in V1.
12. Add a transactional Postgres event log for SSE notifications, written in the same transaction as the crawl state change. Use Postgres LISTEN/NOTIFY to wake SSE connections; retain replayable events for 30 days and emit `resync_required` when a reconnect cursor has expired.
13. Provide one authenticated SSE endpoint for all crawl UI events, with an optional crawl filter for detail views. Include stable event IDs and replay via `Last-Event-ID`; publish minimal notifications for lifecycle/progress, page/finding availability, telemetry/DLQ changes, and action outcomes.
14. Use a fetch-based SSE client or equivalent that can attach the FastAPI-issued bearer token; native `EventSource` cannot set an `Authorization` header. Send keepalive heartbeat comments through the hosting proxy.

If the API's existing deployment cannot securely validate tokens or maintain an operator allowlist, do not ship mutation controls. Resolve the API deployment and secret-management approach first; do not work around the requirement by embedding a shared secret in the browser or introducing an unapproved proxy/backend.

## 6. Design System & UI

**Component Library**: Fluent UI v9

Use a professional, calm operations-console style aligned with Joule Dynamics branding. Favor clear page hierarchy, dense but readable data tables, status badges with text labels, progress indicators, accessible forms, and predictable detail tabs.

- Provide a persistent navigation path back to the Joule Dynamics site and a visible signed-in operator/session control.
- Keep lifecycle status and last-updated time visible; distinguish queued, active, paused, complete, failed, and unknown states based on actual API values.
- Make active controls state-aware but also handle races: another operator/process may change a crawl between display and action.
- Use dialogs for cancel/delete and other consequential operations, with the operation and crawl identity stated plainly.
- Ensure keyboard navigation, visible focus, semantic labels, sufficient contrast, and non-color-only status communication.
- Provide responsive layouts suitable for laptop/tablet; complex tables and graphs should degrade to scrollable or summarized views on small screens.
- For graph failures, empty results, or large payloads, show a clear explanation and provide the underlying crawl/page list as a fallback.

## 7. Data, SSE & Nonfunctional Requirements

### Live updates with SSE

- Do not use timed browser polling for crawl or result updates. Keep ordinary REST requests for initial page loads, filtering/pagination, user-triggered refreshes, and re-synchronization after SSE reconnect gaps.
- Establish one authenticated SSE stream for the dashboard and active crawl detail; use an optional crawl filter for detail. Events cover lifecycle/progress, page and finding availability, telemetry/DLQ changes, and action outcomes.
- The API stream is `GET /api/events`; it requires a bearer token and accepts optional `crawl_id` and `Last-Event-ID`. An initial `ready` event supplies a high-water cursor before the client fetches its REST snapshot, avoiding a snapshot-to-stream race.
- Current notification types are `crawl.created`, `crawl.deleted`, `crawl.status_changed`, `crawl.tasks_retried`, `crawl.completed`, `page.available`, `finding.status_changed`, `telemetry.updated`, `dlq.updated`, and `action.outcome`. Events carry minimal invalidation/action metadata, not full crawl data. `resync_required` instructs the client to reload the snapshot; `stream_error` indicates a retryable stream failure.
- Send durable, minimal change notifications rather than full data snapshots. On a relevant event, refetch only affected REST resource(s), avoid overlapping duplicate fetches, and show last event/refresh time.
- Give each event a stable ID and support `Last-Event-ID` replay. If an event cursor is no longer retained, emit `resync_required` and refetch the current snapshot before resuming.
- Store event records transactionally in Postgres alongside the state change, use Postgres LISTEN/NOTIFY to wake SSE connections, and retain replayable events for 30 days.
- Use keepalive heartbeats, client reconnect/backoff, clean stream cancellation on logout/navigation, and clear connection/error states. There is no timer-polling fallback.
- SSE is a browser delivery mechanism only. Preserve Redis consumer groups, blocking stream reads, and the janitor's internal recovery loop.

### Quality attributes

- **Security:** authorization enforced server-side for all protected operations; HTTPS only; no secrets or tokens in logs or source.
- **Reliability:** recoverable error states, no duplicate action submissions while a request is pending, clear feedback for timeouts and conflict/stale-state errors.
- **Performance:** paginate server-side, bound graph/DLQ payload sizes, lazy-load detail tabs and exports, avoid downloading unnecessary page/finding records.
- **Accessibility:** keyboard-operable controls and dialogs, semantic tables/forms, status text, visible focus, and contrast checks.
- **Maintainability:** typed API models aligned with OpenAPI; isolate authentication and API transport; document local setup and environment variables; never commit live credentials.
- **Privacy:** display data only to the authorized operator; minimize telemetry and client-side retention; do not persist crawl records in browser storage.

## 8. Implementation, Verification & Rollout Sequence

Implementation proceeds incrementally. Phase 1 authentication, audit, and SSE work is locally verified but not deployed; production API routes must not be used by the console until the operator credentials and database listener settings are configured and the deployed API is verified.

### Phase 0 — Resolve decisions and access

1. Generate a strong operator password hash and random token signing secret locally; configure those values and the operator username in Render's managed environment settings.
2. Confirm the API's Render environment configuration and protected deployment process; do not include credentials in source or plan files.
3. Keep the separate repository name and final brand assets open for the frontend planning/implementation step.
4. Save a versioned OpenAPI contract snapshot for reproducible client generation/testing; do not include environment secrets.

### Phase 1 — Secure the existing API

1. [x] Add fail-closed first-party operator login and short-lived JWT verification, and API-side protection for all crawl and event routes; leave `/health` public.
2. [x] Restrict CORS to configured exact origins and allow only the required methods/headers.
3. [x] Add Postgres mutation audit records with one-year cleanup and transactional event/outbox rows with 30-day replay cleanup.
4. [x] Add the authenticated SSE endpoint, `Last-Event-ID` replay, `ready`/`resync_required` events, and Postgres `LISTEN/NOTIFY` wake-ups. Move persistence `XACK` after commit and add durable message deduplication.
5. [x] Complete local regression/security tests: full suite passes (184 tests); `compileall` and `git diff --check` pass; the currently configured PostgreSQL URL passes a `SELECT 1` and `LISTEN/NOTIFY` smoke test.
6. [ ] Confirm the production `DATABASE_LISTEN_URL` is direct/session mode, verify deployed schema readiness and SSE replay against it, and exercise the deployed API.
7. [ ] Configure the operator username, password hash, signing secret, and Render settings; deploy the secured API and verify login, authenticated production requests, and console-origin preflight. Do not connect production mutation controls before this passes.

### Phase 2 — Build the separate frontend

1. Create a dedicated repository and Vercel project; configure independent build, preview, and production environments.
2. Implement the API username/password login, in-memory token handling, logout, protected routes, re-login after token expiry/reload, and safe unauthorized/error handling.
3. Generate or maintain typed client models from OpenAPI; implement list/create/detail, lifecycle actions, authenticated SSE notifications with REST resynchronization, telemetry/DLQ, pages/findings, summary, graph, and exports.
4. Apply Fluent UI v9 and Joule Dynamics visual identity; add accessible confirmation and error states.
5. Keep the production API URL in documented Vercel environment configuration. Do not configure or deploy operator passwords, password hashes, or token signing secrets to the frontend.

### Phase 3 — Test and security acceptance

- Unit/component tests for forms, filters, status mapping, confirmation dialogs, pagination, SSE event handling/replay/reconnect/resync, token expiry, and API error presentation.
- API contract tests against the saved OpenAPI schema and mocked response fixtures; integration tests against a controlled non-production API environment where available.
- End-to-end tests for login/logout, crawl list/detail, create, pause/resume/cancel, retry/consolidate, findings status update, telemetry/DLQ, graph, and both export formats.
- Verify unauthenticated and unauthorized requests cannot read protected data or execute any protected mutation; test direct HTTP calls, not only UI controls.
- Verify invalid/expired/wrong-audience token behavior, CORS from the production console origin, and rejection of unapproved origins.
- Confirm delete warning and server result; test duplicate submissions, network interruption, API 4xx/5xx, rate limits, SSE disconnect/reconnect and expired cursors, long-running requests, empty datasets, large result sets, and stream cleanup.
- Run keyboard and screen-reader-oriented checks, responsive checks, dependency/security review, and production build checks.

### Phase 4 — Vercel and Namecheap domain deployment

1. Import the dedicated frontend repository into Vercel and create a distinct project. Set the production branch and build command/output directory for the chosen React toolchain.
2. Configure required Vercel environment variables separately for Preview and Production. Preview must not receive production API access or production operator credentials.
3. In the Vercel project, add the custom domain `console.jouledynamics.me`. Use Vercel's domain setup/verification flow and copy the **exact DNS record values Vercel displays at that time**. Vercel's recommended target/verification records can vary by account and current platform configuration; do not assume or hard-code a generic CNAME target.
4. In Namecheap, open the domain's Advanced DNS settings. Add the record(s) Vercel requests, normally a CNAME for host `console` with the exact target shown by Vercel; add any requested TXT ownership-verification record with the exact host/value Vercel provides. If Vercel explicitly instructs use of a different record type, follow the current Vercel instruction instead.
5. Remove or resolve conflicting records for the `console` host before adding the requested record. Do not alter the existing `www` or apex records that serve the current Joule Dynamics website.
6. Wait for DNS propagation and Vercel verification; confirm Vercel provisions HTTPS/TLS. Test `https://console.jouledynamics.me` and SPA deep-link refreshes, sign-in callback routes, and logout.
7. Configure the API CORS production allowlist to exactly `https://console.jouledynamics.me` (no path or trailing slash), alongside the required headers/methods.
8. Verify authenticated and unauthorized API calls from the deployed console in a real browser. Confirm Vercel's assigned DNS value and actual deployed CORS behavior rather than relying solely on configuration files.
9. Add a link to the console from `www.jouledynamics.me` after the console and API security gates pass. Announce operator use only after smoke tests and rollback readiness are complete.

## 9. Acceptance Criteria & Release Gates

V1 is acceptable for production release only when all of the following hold:

1. `console.jouledynamics.me` is served over HTTPS by its own Vercel project/repository and does not disrupt the existing Vercel-hosted site.
2. A non-allowlisted or unauthenticated caller cannot access protected API data or perform any protected action, including through direct API requests.
3. The authorized operator can sign in, view and filter/paginate crawls, create a crawl with defaults or advanced options, and inspect accurate status/progress.
4. Pause, resume, cancel, retry failed tasks, consolidate, and delete invoke the correct API operations and report server outcomes. Delete is explicitly confirmed and clearly identified as a cascading purge.
5. Telemetry, DLQ, page and finding views, finding-status updates, summary, graph, and JSON/Markdown exports work against live API responses.
6. No timed browser polling is used for live crawl/result updates. Authenticated SSE covers crawl progress, result availability, telemetry/DLQ changes, and action outcomes; reconnection replays by event ID or emits `resync_required`, after which the UI fetches a current REST snapshot.
7. API CORS allows the exact console production origin and required headers while rejecting unapproved origins; authorization remains effective independently of CORS.
8. No API secret, password hash, token signing secret, or operator password is present in the frontend bundle, repository, or browser logs.
9. Empty/error/loading states, keyboard access, responsive layouts, and API validation errors are handled clearly.
10. Automated and manual security, integration, and end-to-end checks in Section 8 pass; production rollback steps are documented.

## 10. Decisions Resolved & Remaining

### Resolved

1. **Identity:** FastAPI-managed single-operator username/password login; scrypt password hash stored in managed configuration; signed 15-minute HS256 bearer tokens; memory-only frontend token storage and re-login after reload/expiry.
2. **Authorization:** all API reads and mutations require the configured operator bearer token; no anonymous API access.
3. **API ownership:** the project owner can modify this API repository and deploy/configure its Render service.
4. **Action audit retention:** persist server-side action records in Postgres for one year; do not expose an action-history UI/API in V1.
5. **Preview deployments:** use a test API environment; no production crawl data or operator credentials.
6. **Live browser updates:** replace all timed browser polling with one authenticated SSE event stream, optionally filtered by crawl. Use durable Postgres event records and LISTEN/NOTIFY; 30-day event replay, stable IDs, and `resync_required` on expired cursors. Events are notifications; fetch the latest REST representation after each relevant event.
7. **SSE scope:** browser updates only. Preserve internal Redis stream consumers and janitor reads.
8. **Frontend defaults:** React + TypeScript with Fluent UI v9.
9. **Trusted integrations:** none are currently active. Do not reuse the operator password for future automation; design separate service credentials if integrations are added.

### Remaining

1. **Repository name and visual assets:** decide with the frontend agent/planning step; the user will provide or approve logo and brand treatment there.
2. **Graph limits and export semantics:** validate practical graph limits, file naming/content type, and behavior on larger crawls against the API before finalizing UI interactions.
3. **Operator setup:** generate and configure the username, password hash, and token signing secret in Render's managed environment settings without placing credentials in source control.

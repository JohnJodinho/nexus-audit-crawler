# Nexus Frontend API Contract Instructions

This document contains frontend-only implementation instructions for the new
Nexus content, export, URL-rule, and crawl-inspection capabilities. Use it
alongside [`enhanced_plans.md`](./enhanced_plans.md).

The frontend must not infer the backend contract from this document alone.
Fetch the deployed OpenAPI document before implementation:

```text
GET https://nexus-query-api.onrender.com/openapi.json
```

For local development, use the configured local API origin and request:

```text
GET {API_BASE_URL}/openapi.json
```

The live `/openapi.json` document, including its paths, schemas, parameter
names, required fields, and response codes, is authoritative. If this guide
and OpenAPI differ, follow OpenAPI and report the discrepancy.

## 1. Scope and frontend responsibilities

The frontend is a client of the existing API. It must:

- use the API as the only source of crawl, page, inventory, export, preview,
  contact, finding, and event data;
- call the API through a typed client generated from or checked against
  `/openapi.json`;
- send the bearer token on every protected request;
- treat all response data as untrusted display data;
- preserve server decisions instead of reimplementing selection, matching,
  authorization, or export logic;
- display loading, empty, unavailable, failed, cancelled, and completed states
  distinctly;
- never access Appwrite directly;
- never store an API secret, operator password, or privileged key in frontend
  source;
- avoid assuming any particular framework, component library, layout, routing
  model, or existing screen design.

The frontend may provide local form affordances such as pattern chips and
client-side validation, but server validation remains authoritative.

## 2. Authentication and request transport

Use the existing login contract from OpenAPI:

```text
POST /api/auth/login
```

After successful login:

1. Keep the returned bearer token in memory only.
2. Send `Authorization: Bearer {token}` on every protected REST request.
3. Use the same authorization mechanism for authenticated event streaming.
4. On `401`, clear the in-memory token and return to the login state.
5. Do not put bearer tokens in query strings, browser history, URLs, logs, or
   analytics payloads.
6. Do not assume cookies or a shared parent domain provide authentication.

Use a single API client for:

- base URL resolution;
- authorization headers;
- JSON serialization;
- timeout and abort handling;
- structured error parsing;
- `401`, `403`, `404`, `409`, `413`, `422`, `429`, and `503` handling.

The API currently allows configured browser origins, including local frontend
ports `5173` and `5174`. The deployed CORS configuration remains authoritative.

## 3. Existing route compatibility

Do not replace or reinterpret existing routes.

The legacy report route remains:

```text
GET /api/crawls/{crawl_id}/export?format=json|markdown
```

It is a report export and is not the asynchronous ZIP archive flow.

The asynchronous archive flow is:

```text
POST /api/crawls/{crawl_id}/exports
GET  /api/crawls/{crawl_id}/exports/{export_id}
GET  /api/crawls/{crawl_id}/exports/{export_id}/download
POST /api/crawls/{crawl_id}/exports/{export_id}/cancel
```

Use the exact response schemas and status codes from OpenAPI. Do not assume
that a route returning `202` is complete; poll its status endpoint or use the
corresponding event stream where provided.

## 4. Crawl creation and URL patterns

Continue using:

```text
POST /api/crawls
```

When advanced configuration is present, send the server-supported fields,
including:

- `include_patterns`;
- `exclude_patterns`;
- `exclude_resource_types`;
- existing crawl limits and categories.

Pattern editor behavior:

1. Treat the documented glob grammar in `enhanced_plans.md` and the live API
   contract as the display language.
2. Validate a pattern when the operator commits it, such as by entering a
   comma or submitting the form.
3. Represent valid committed patterns as non-editable chips.
4. Keep invalid values as ordinary invalid input with a clear error.
5. Permit Backspace on an empty input to remove the last committed chip.
6. Do not claim that a pattern is valid merely because local validation passed.
7. Submit the complete pattern arrays to the backend unchanged except for
   documented client normalization.
8. Display structured `422` details, including field, pattern, index, and
   message when returned by the API.

The backend independently validates and persists the crawl configuration.
Client-side validation must never be treated as security enforcement.

## 5. URL-rule preview

Use the authenticated endpoint:

```text
POST /api/url-rules/preview
```

The request contains a target URL, include patterns, exclude patterns, and
sample URLs. The response contains validity and one decision per sample URL.

Frontend requirements:

- disable submission while the request is in flight;
- show the server's validation error without converting invalid rules into
  successful-looking results;
- display the returned decision and matched pattern exactly as supplied;
- do not enqueue a crawl from this operation;
- do not mutate a crawl from this operation;
- allow retry after correcting input;
- abort stale previews when the operator starts a newer preview.

## 6. Page content retrieval

Use:

```text
GET /api/crawls/{crawl_id}/pages/{page_id}/content
```

Supported representations are discovered from OpenAPI and runtime responses:

- `format=markdown`;
- `format=html`;
- `format=text`.

Optional query parameters include `download` and `include_metadata`.

Display rules:

- `markdown`: show as text or through a Markdown-safe renderer;
- `text`: show as plain text;
- `html`: treat as untrusted crawled content.

Never inject returned HTML directly into the authenticated application DOM.
Use an isolated, appropriately sandboxed preview mechanism if the product
design requires HTML viewing. Do not execute scripts from crawled HTML.

Distinguish:

- `404` with `CONTENT_UNAVAILABLE`: the representation was not captured;
- `503`: storage is temporarily unavailable;
- successful download: content is available.

When `include_metadata=true`, consume the exact response shape described by
OpenAPI. Do not assume metadata is present in a response header.

For downloads, use the response `Content-Disposition` and content type when
available. Do not construct unsafe local filenames from URLs.

## 7. Path discovery and crawl inspection

The following authenticated endpoints expose persisted crawl read models:

```text
GET /api/crawls/{crawl_id}/paths
GET /api/crawls/{crawl_id}/contacts
GET /api/crawls/{crawl_id}/search
GET /api/crawls/{crawl_id}/pages/{page_id}/links
GET /api/crawls/{crawl_id}/events
GET /api/crawls/{crawl_id}/failures
GET /api/crawls/{crawl_id}/statistics
GET /api/crawls/{crawl_id}/pages/{page_id}/metadata
GET /api/crawls/{crawl_id}/artifacts
```

Use the deployed OpenAPI document for exact query parameter names, paging
fields, and response schemas.

General rules:

- use server pagination and limits;
- do not load an entire crawl repeatedly when a paginated endpoint exists;
- preserve stable page IDs and crawl IDs in navigation state;
- show empty results separately from request failures;
- use returned counts and statuses rather than deriving authoritative counts
  in the browser;
- refresh a detail view after a mutation or durable event.

For paths, render the returned hierarchy or flat structure as provided. Do not
reconstruct a different URL tree from raw page URLs unless the API contract
explicitly requires it.

For contacts, preserve contact type and source-page associations when supplied.
For search, display snippets as untrusted text and link results using returned
page IDs. For links, distinguish incoming and outgoing results using the
server-provided direction and counts.

## 8. Asynchronous export workflow

Create an archive job with:

```text
POST /api/crawls/{crawl_id}/exports
```

Send only fields supported by the live `ExportRequest` schema, such as:

- `scope`;
- `format`;
- `include_patterns`;
- `exclude_patterns`;
- `exclude_resource_types`;
- `include_assets`;
- `include_metadata`;
- `include_manifest`.

The returned job is initially queued or processing. Track it with:

```text
GET /api/crawls/{crawl_id}/exports/{export_id}
```

Handle these states explicitly when returned:

- `queued`;
- `processing`;
- `completed`;
- `completed_with_errors`;
- `failed`;
- `cancelled`;
- `expired`.

Only offer download when the status permits it:

```text
GET /api/crawls/{crawl_id}/exports/{export_id}/download
```

The frontend must not fetch target websites, enqueue crawl work, or attempt to
assemble an archive locally. The backend applies scope, original crawl
restrictions, pattern matching, content availability, archive naming, and
manifest decisions.

For partial completion, show the backend's counts and item decisions. Do not
silently hide unavailable or failed pages.

Cancellation uses:

```text
POST /api/crawls/{crawl_id}/exports/{export_id}/cancel
```

Treat cancellation as asynchronous and refresh the job status after the
request. Handle a race where the job completes before cancellation is applied.

## 9. Export preview and authenticated SSE

Create a preview with:

```text
POST /api/crawls/{crawl_id}/exports/preview
```

Then retrieve its status with:

```text
GET /api/crawls/{crawl_id}/exports/previews/{preview_id}
```

Preview events are available at:

```text
GET /api/crawls/{crawl_id}/exports/previews/{preview_id}/events
```

Expected event categories include:

- `preview.started`;
- `url.evaluated`;
- `preview.completed`;
- `preview.failed`;
- `preview.cancelled`.

Events may include candidate totals, URL, scope decision, inclusion/exclusion
decision, matched pattern, content status, reason, and final counts. Render
these fields from the event payload without inventing categories.

Native browser `EventSource` cannot safely send the required bearer header.
Use an authenticated fetch-based streaming implementation or the project's
authenticated SSE client. Never append the token to the SSE URL.

SSE client requirements:

1. Send `Authorization` and `Last-Event-ID` when reconnecting, as supported by
   the API contract.
2. Parse `id`, `event`, and `data` fields.
3. Deduplicate events by server event ID.
4. Resume from the last received event ID after disconnect.
5. Refresh the preview status after terminal events.
6. Distinguish failed, cancelled, and completed previews.
7. Stop the stream after a terminal event or explicit cancellation.
8. Abort the stream when the user leaves the preview context.
9. Treat malformed event data as a visible recoverable error, not success.

## 10. Artifacts and security boundaries

Use the artifacts endpoint to discover what is available. Do not expose
Appwrite file IDs as public download URLs unless the API explicitly returns an
authorized route for that purpose.

The frontend must not:

- call Appwrite directly;
- expose storage credentials;
- execute downloaded HTML or JavaScript;
- use URL query parameters for bearer tokens;
- trust a client-side crawl ID without server authorization;
- treat hidden UI controls as authorization;
- assume an export is a complete offline website unless the backend manifest
  says so.

## 11. Error and state handling

Build a shared error adapter for the documented API error format. Preserve:

- HTTP status;
- backend error code;
- field;
- pattern;
- index;
- safe message;
- request correlation data if provided.

Recommended UI states for every async operation:

- idle;
- loading/submitting;
- success;
- empty;
- unavailable;
- failed;
- cancelled;
- expired;
- unauthorized.

Do not replace backend errors with a generic success-shaped fallback. Do not
retry mutations blindly. GET retries must respect cancellation, backoff, and
the API's rate limits.

## 12. OpenAPI-driven implementation checks

Before merging frontend work:

1. Fetch the deployed `/openapi.json`.
2. Compare generated client types with the checked-in implementation.
3. Confirm all request field names and enum values.
4. Confirm route status codes and response content types.
5. Confirm authentication requirements and CORS from the deployed API.
6. Test an authenticated request for every new route.
7. Test expired/invalid authentication.
8. Test unavailable Markdown, HTML, and text representations.
9. Test invalid URL patterns and server-returned field/index errors.
10. Test export success, partial completion, failure, cancellation, and
    download authorization.
11. Test preview SSE reconnect and `Last-Event-ID` replay.
12. Test that crawled HTML is isolated and never executed in the console DOM.
13. Test empty and paginated path, contact, search, link, failure, statistics,
    metadata, and artifact responses.
14. Confirm the legacy JSON/Markdown report route remains usable.

This document describes how a frontend should consume the API. It does not
authorize adding frontend routes, redesigning the product, or changing the
backend contract. Backend behavior and the live OpenAPI document remain the
source of truth.

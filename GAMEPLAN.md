# Gameplan: Brightpearl MCP Server

Natural-language access to East Coast Fabrics' Brightpearl ERP data through Claude, built as two components:

1. A **sync pipeline** that keeps a Google BigQuery dataset near-real-time-fresh with Brightpearl data (webhooks first, polling as reconciliation).
2. A **remote MCP server** (Python, streamable-HTTP on Cloud Run) whose tools query BigQuery, with live Brightpearl API passthrough for questions that need this-second accuracy.

This replaces reliance on SyncHub, whose hosted Brightpearl MCP was observed (2026-07-03) serving data 3+ days stale with no staleness visibility, missing purchase order line items entirely, and hosting the warehouse in Azure Sydney.

## Architecture

```
                    ┌──────────────────────────────┐
                    │        Brightpearl API        │
                    │  (200 req/min account limit)  │
                    └───────┬──────────────┬───────┘
              webhooks (thin │              │ REST fetches
               ID payloads)  │              │ (batched multi-ID GETs)
                    ┌────────▼──────────────▼───────┐
                    │     Cloud Run: sync service    │
                    │  webhook ingest + tiered poll  │
                    │  shared rate-limit budget      │
                    └───────────────┬───────────────┘
                                    │ Storage Write API (streaming)
                                    │ + MERGE-from-staging (batch)
                    ┌───────────────▼───────────────┐
                    │   BigQuery dataset `brightpearl` │
                    │  normalized tables + semantic   │
                    │  views + sync_state + audit log │
                    └───────────────┬───────────────┘
                                    │ read-only SQL
                    ┌───────────────▼───────────────┐      live passthrough
                    │   Cloud Run: MCP server        │──────► Brightpearl API
                    │  (FastMCP, streamable-HTTP)    │
                    └───────────────┬───────────────┘
                                    │
                              Claude clients
```

## Data freshness strategy

The core goal: data as close to real-time as possible within Brightpearl's **200 requests/min** account limit.

| Layer | Latency | Mechanism |
|---|---|---|
| Webhooks | Seconds | Brightpearl pushes resource IDs on create/modify/delete; we fetch + stream-upsert into BigQuery |
| Tiered polling | Minutes | `updatedOn`-filtered sweeps reconcile anything webhooks missed |
| Live passthrough | Real-time | MCP tools call the Brightpearl API directly for "right now" questions |

### Webhook mechanics (Integration service)

- Register via `POST /integration-service/webhook`, one subscription per event code (`order.created`, `order.modified`, `product.modified`, `contact.modified`, `goods-out-note.created`, etc.).
- Subscription fields: `subscribeTo`, `httpMethod: POST`, `uriTemplate` (our ingest URL), `contentType: application/json`, `bodyTemplate` using `${account-code}`, `${resource-type}`, `${resource-id}`, `${lifecycle-event}`, `${raised-on}`.
- **Payloads are thin** (IDs only) — the handler batches pending IDs and fetches full resources with multi-ID GETs (`/order/123,456,789`).
- `idSetAccepted: true` — bulk changes arrive as one ID-set message, not hundreds of posts.
- `qualityOfService: 1` — at-least-once delivery; handler must be idempotent (upserts keyed on resource ID) and ack 200 immediately, processing asynchronously.
- Registration is code (`sync webhooks register`), version-controlled, with a startup check that all expected subscriptions exist.

### Polling cadence tiers (reconciliation)

| Tier | Resources | Cadence |
|---|---|---|
| Hot | orders, order rows, goods movements, stock levels | ~5 min |
| Warm | contacts, prices, payments | ~30 min |
| Cold | products, brands, categories, tax codes, nominal codes | hourly–daily |

Each tier's request volume is budgeted against the 200/min limit; the rate limiter always reserves headroom for webhook-triggered fetches and live MCP calls, honoring the `brightpearl-requests-remaining` response header and backing off on 503.

### Freshness visibility

- `sync_state` table: per-resource watermarks, last webhook received, last sweep completed.
- MCP `get_data_freshness` tool: any user can ask "how current is this data?"
- Alerting (Phase 5) when any watermark exceeds its staleness budget — the silent 3-day SyncHub outage must be impossible here.

## BigQuery schema principles

- **Typed columns for every queryable field** (IDs, dates, amounts, statuses). BigQuery is columnar — typed columns enable column pruning, clustering, and cheap aggregation that flat JSON blobs defeat.
- **One native-JSON `raw_payload` column per table** for schema-drift resilience: when Brightpearl adds fields, nothing is lost before we promote them to typed columns.
- **Custom fields parsed to typed columns at ingest** (e.g. roll size, MOQ) — not left as raw strings.
- **Audit columns everywhere**: `when_created`, `when_modified` (from Brightpearl), `when_upserted` (our write time).
- **Documented foreign keys** so humans and LLMs both understand the joins.
- **Purchase orders and purchase_order_rows are first-class tables** (a known SyncHub gap).
- **Semantic view layer** for LLM consumption: denormalized views (`sales_flat`, `inventory_position`, `po_pipeline`, `customer_summary`) so most MCP queries are single-table scans.
- Per-table configurable historical backfill depth.
- Observed scale (from SyncHub, 2026-07): ~243k orders, ~561k order rows, ~57k products — tiny for BigQuery; full backfill is cheap.

## Phases

### Phase 0 — Prerequisites (manual, guided)
- [x] Create GCP project (`brightpearl-mcp-server`), attach billing, enable BigQuery + Storage Write + Secret Manager + Cloud Run + Scheduler + Tasks + Cloud Build + Artifact Registry APIs
- [x] BigQuery dataset `brightpearl` created (US region)
- [x] `gcloud auth application-default login` for local dev (service account comes with Phase 4 deployment)
- [x] Create a Brightpearl **private app** (Settings → API → Private apps) → record `app-ref` and `account-token`
- [x] Record Brightpearl account code and datacenter (base URL: `https://{datacenter}.brightpearlconnect.com/public-api/{account-code}/`)
- [x] Fill in `.env` from `.env.example`

### Phase 1 — Brightpearl API client (`src/brightpearl_client/`)
- [x] Config: account code, datacenter, auth headers (`brightpearl-app-ref`, `brightpearl-account-token`)
- [x] Rate limiter: token bucket against 200/min, reads `brightpearl-requests-remaining` / `brightpearl-next-throttle-period`, reserves headroom for webhooks + live calls (priority acquire)
- [x] Retry with backoff on 503 (throttle) and transient failures
- [x] Generic resource-search wrapper: pagination, column metadata, filter passthrough (incl. `updatedOn`)
- [x] Multi-ID GET support (`/order/123,456,789`) with ID-range chunking
- [x] Resource accessors: orders, products, contacts, goods-out notes (generic `ResourceAPI`; more added as Phase 2 needs them)
- [x] Unit tests with mocked HTTP (respx); live smoke script (`scripts/live_smoke.py`) verified against production account (242,799 orders visible)

Learned from live API: order-search has no `reference` column — it's `customerRef`; search pages return 500 rows.

### Phase 2 — BigQuery schema + batch sync (`src/sync/`)
- [ ] Dataset `brightpearl` (US region) + table DDL per schema principles above
- [ ] `sync_state` watermark table
- [ ] Full backfill mode (per-table depth config)
- [ ] Incremental mode: `updatedOn` sweeps with tiered cadence
- [ ] Idempotent upserts: MERGE-from-staging for batch, Storage Write API for streaming
- [ ] CLI entry points: `sync backfill`, `sync sweep --tier hot|warm|cold`, `sync status`

### Phase 2b — Webhook ingest (`src/sync/webhooks.py`)
- [ ] `sync webhooks register` / `verify` / `list` commands (subscriptions as code)
- [ ] Ingest HTTP endpoint: validate, ack 200 immediately, enqueue IDs
- [ ] Async worker: batch pending IDs → multi-ID fetch → stream upsert
- [ ] Dedupe/idempotency keyed on resource ID + modified time

### Phase 2c — Semantic view layer
- [ ] `sales_flat`, `inventory_position`, `po_pipeline`, `customer_summary` views
- [ ] View documentation strings the MCP server surfaces to the model

### Phase 3 — MCP server (`src/mcp_server/`)
- [ ] FastMCP app; stdio transport for local dev, streamable-HTTP for remote
- [ ] Domain tools: `query_sales`, `get_stock_levels`, `search_customers`, `get_financials`, `get_po_pipeline`
- [ ] Guarded `run_bigquery_sql` (read-only enforcement, byte-scan cap)
- [ ] Live passthrough tools: `get_order_live`, `get_stock_live`
- [ ] `get_data_freshness` tool (reads `sync_state`)
- [ ] Tool-call audit log table

### Phase 4 — Remote deployment
- [ ] Dockerfile (single image; entrypoints for MCP server, webhook ingest, sweep job)
- [ ] Cloud Run services: `mcp-server`, `sync-ingest`; Cloud Run Job + Cloud Scheduler for sweeps
- [ ] Secret Manager for Brightpearl credentials; least-privilege service accounts
- [ ] Bearer-token auth on the MCP endpoint
- [ ] Register webhooks against the deployed ingest URL

### Phase 5 — Hardening
- [ ] Staleness alerting (watermark budget exceeded → email/Slack)
- [ ] Structured logging + error reporting
- [ ] Integration tests; load-test the sweep within rate budget
- [ ] Team onboarding docs (connecting Claude to the MCP server)

## Status log

- **2026-07-03** — Project kicked off: gameplan + CLAUDE.md written, scaffold created, repo pushed to GitHub. Next: Phase 0 prerequisites.
- **2026-07-03 (later)** — Phase 0 nearly complete: GCP project `brightpearl-mcp-server` created with billing (freed a billing slot by unlinking dormant `alpine-task-194105`), APIs enabled, dataset `brightpearl` created, Brightpearl private-app credentials in `.env`. Remaining: ADC login. Next: Phase 1 (Brightpearl API client).
- **2026-07-03 (evening)** — Phase 0 complete (ADC verified). Phase 1 complete: async client with shared rate limiter, 7 unit tests passing, live smoke test pulled real orders. Next: Phase 2 (BigQuery schema + batch sync).

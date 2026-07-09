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

## Coverage vs SyncHub (target: superset)

Ryan provided SyncHub's full table list (43 tables, ~9.15M rows). Our coverage,
by sync mechanism (see `sync/resources.py` for config):

**Detail sync** (search → multi-ID GET → curated typed columns + raw_payload):
orders (SO/PO/SC/PC incl. line rows — SyncHub lacks PO rows), products, contacts.

**Search dumps** (typed schema generated from the search's own metaData):
journal_rows (1.43M — includes per-line debit/credit, covering SyncHub's
Journal + JournalCredit + JournalDebit), customer_payments, goods_movements,
goods_out_notes, companies, brands, collections, product_types, seasons,
contact_groups, nominal_codes, currencies, payment_methods, warehouses,
shipping_methods.

**Reference GETs** (truncate-reload): order_statuses, order_types,
order_stock_statuses, order_shipping_statuses, tax_codes, accounting_periods,
lead_sources, contact_tags, price_lists, channel_brands.

**Derived** (fan-out from products): product_prices (per price list),
product_availability (per warehouse, stock-tracked only), product_suppliers
(per supplier contact, with is_primary from the product payload).

**Endpoint audit (2026-07-03, against official API docs for all services):**
Later probe unlocked and synced: supplier_payments, categories
(brightpearl-category-search), warehouse_locations, product_options,
product_option_values, contact_group_members, channels.

**Known gaps (tracked, not yet implemented):**
- ~~Order/Product/Contact **custom-field values**~~ — DONE 2026-07-04:
  `includeOptional=customFields` on detail GETs lands them in `raw_payload`;
  verified 2026-07-05 that 0 of 391k orders/products/contacts are missing
  the key. Views extract the useful ones (see `sync/views.py`)
- ~~`supplier_payments` returned exactly 500 vs SyncHub's 7,001~~ — root
  cause found 2026-07-05: supplier-payment-search always misreports
  `resultsAvailable=500` (and silently ignores `paymentId` range filters),
  so pagination stopped after one page. Fixed with `page_past_available`
  config flag + full re-scan
- **Order notes** (order-service order-note GET) and **contact postal
  addresses** (GET per address id; order payloads embed delivery addresses so
  partially covered)
- **Goods-in notes** (docs list Goods-In Note SEARCH; both probed paths
  404'd — needs path research)
- **Stock transfers** (GET by id works, no search; derive ids from
  goods_movements or order goods notes)
- Journal **detail GETs** if per-line data beyond journal-search is needed
- Low value / on demand: landed-cost estimates, reservations, zones,
  drop-ship notes, contact balances (good live-MCP-tool candidate),
  all-transaction-statement (404'd), product groups (GET semantics unclear)

**Hardening backlog (from external code review, 2026-07-04):**
- [x] Durable webhook queue: Cloud Tasks (queue `webhook-events`, us-east4) —
  webhook enqueues in-request, /process runs under OIDC-signed callbacks with
  automatic retry; sync-ingest back on request-based billing (~$35/mo saved)
- Auth separation (partial): /process is OIDC-only in production (TASKS_QUEUE
  set; shared-token fallback is local-dev only, since the token leaks into
  request logs via /webhook query strings); /tick + /alert-check still use the
  shared token (constant-time compared) — move Scheduler to OIDC eventually. Token now preferred in the `X-Auth-Token` header (query
  param still accepted for /webhook, which Brightpearl can only template into
  the URL). **/alert-check is now POST-only** — its Scheduler job must POST
  with the header, not GET with `?token=`.
- Per-caller identity in mcp_audit (needs per-user bearer tokens)

**Security-review follow-ups (2026-07-05):**
- [x] run_bigquery_sql guard hardened: reject multi-statement scripts and
  BigQuery scripting/dynamic-SQL keywords (EXECUTE IMMEDIATE, DECLARE, BEGIN…)
  that slipped past the old keyword+dry-run checks (tests/test_mcp_guard.py)
- [x] MCP bearer token compared constant-time (was plain `!=`)
- [x] **IAM: MCP server runs as its own read-only SA** (`mcp-reader@…`):
  project-level `dataViewer` + `jobUser`, table-level `dataEditor` on
  `mcp_audit` only, per-secret `secretAccessor` on mcp-bearer-token +
  brightpearl-app-ref + brightpearl-account-token (NOT webhook-token).
  Deployed as mcp-server-00009 (2026-07-05); verified reads + audit writes
  work and no write role exists outside mcp_audit. The SQL guard is now
  defense-in-depth, not the sole control. sync-ingest stays on `bp-runtime`.
  Note: ensure_audit_table() is get-first because the SA can't create tables.

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
- [x] `sales_flat`, `inventory_position`, `po_pipeline`, `customer_summary`, `monthly_financials` views (`sync/views.py`; recreate with `sync.cli views`)
- [x] Views documented via MCP `get_schema` + server instructions

### Phase 3 — MCP server (`mcp_server/`)
- [x] FastMCP app; stdio transport for local dev (registered in `.mcp.json`); streamable-HTTP moves to Phase 4
- [x] Domain tools: `query_sales`, `get_stock_levels`, `search_customers`, `get_po_pipeline` (financials covered by `monthly_financials` view + SQL tool)
- [x] Guarded `run_bigquery_sql` (SELECT/WITH only, keyword blocklist, 2GB scan cap, 200-row result cap)
- [x] Live passthrough tools: `get_order_live`, `get_stock_live` (priority rate-limit lane)
- [x] `get_data_freshness` tool (reads `sync_state`)
- [x] Tool-call audit log table (`mcp_audit`)
- [ ] End-to-end test from a Claude client over stdio

### Phase 4 — Remote deployment
- [x] Dockerfile (single image; per-service entrypoint override)
- [x] Cloud Run us-east4: `mcp-server` (https://mcp-server-243337884757.us-east4.run.app, bearer auth) and `sync-ingest` (https://sync-ingest-243337884757.us-east4.run.app, token auth, max-instances=1 so the rate budget never splits)
- [x] Cloud Scheduler → `/tick/{tier}`: hot */5min, warm */30min, cold daily 07:00 UTC
- [x] Secret Manager: brightpearl-app-ref, brightpearl-account-token, webhook-token, mcp-bearer-token; `bp-runtime` SA (BigQuery dataEditor+jobUser, secretAccessor)
- [x] Webhooks registered (verified subscribable set): order.modified, product.created/modified, goods-out-note.created/modified, goods-in-note.created — thin events → 3s batch buffer → multi-ID fetch → BigQuery; goods-note events trigger incremental goods_movements sweeps
- [x] End-to-end verified: token auth enforced (401/403), test webhook → BigQuery upsert in ~8s, real Brightpearl events observed arriving, scheduler tick recorded a cloud-driven orders sweep

Notes: contact.* and order.created are not subscribable on this account (order.modified fires on creation; contacts ride the warm sweep). Connect remote Claude clients: `claude mcp add --transport http brightpearl https://mcp-server-243337884757.us-east4.run.app/mcp --header "Authorization: Bearer $MCP_BEARER_TOKEN"` (token in Secret Manager / .env).

### Phase 5 — Hardening
- [x] Staleness alerting: `/alert-check` (tier budgets hot 30m / warm 2h / cold 50h) on a 15-min schedule; STALENESS_ALERT log lines trigger a Cloud Monitoring policy → email ryan@eastcoastfabrics.com. First run immediately caught the reference-sync bookkeeping gap.
- [x] Deleted-record handling: `is_deleted`/`deleted_at` on orders/products/contacts; `product.destroyed` webhook marks immediately; daily cold-tier reconciliation sweep catches order/contact deletions (with a 20% mass-delete safety valve); views exclude deleted rows; upserts clear the flag
- [x] Structured logging: Cloud Logging severity levels on Cloud Run (Error Reporting picks up exception traces)
- [x] Integration tests: webhook auth/parsing/destroyed-path/alert-check via Starlette TestClient (16 tests total); rate-budget behavior covered by rate-limiter unit tests + production header-sync
- [x] Team onboarding doc: docs/ONBOARDING.md (connect, example questions, troubleshooting, boundaries)

## Status log

- **2026-07-03** — Project kicked off: gameplan + CLAUDE.md written, scaffold created, repo pushed to GitHub. Next: Phase 0 prerequisites.
- **2026-07-03 (later)** — Phase 0 nearly complete: GCP project `brightpearl-mcp-server` created with billing (freed a billing slot by unlinking dormant `alpine-task-194105`), APIs enabled, dataset `brightpearl` created, Brightpearl private-app credentials in `.env`. Remaining: ADC login. Next: Phase 1 (Brightpearl API client).
- **2026-07-03 (evening)** — Phase 0 complete (ADC verified). Phase 1 complete: async client with shared rate limiter, 7 unit tests passing, live smoke test pulled real orders. Next: Phase 2 (BigQuery schema + batch sync).
- **2026-07-04** — Phases 2–5 all complete: full warehouse (35+ tables, ~4.5M rows), semantic views, MCP server (local stdio + remote bearer-auth HTTP), Cloud Run deployment with webhooks + tiered scheduled sweeps, staleness alerting, deleted-record handling, integration tests, onboarding docs. Remaining work tracked under "Known gaps" (custom-field completion for products/contacts in progress, supplier_payments anomaly, minor coverage gaps).
- **2026-07-05** — Fixed the staleness cascade found in Cloud Run logs: id-mode search dumps (journal_rows, customer/supplier_payments) were silently full-scanning every warm tick because Brightpearl rejects range filters on INTEGER/IDSET columns; replaced with sort-DESC + early-stop at the watermark (2,900 requests/run → ~1). Added `_dml()` backoff on BigQuery "concurrent update" aborts and serialized the webhook-events Cloud Tasks queue (was 1000 concurrent), fixing the order_rows transaction-abort retry storms. Deployed as sync-ingest-00011; verified incremental runs return 0 rows with no fallback warnings.
- **2026-07-05 (later)** — Applied round-2 external review (derived loaders record sync_state, fallback-to-full-scan requires a 400, malformed supplier payloads abort before truncate); deployed as sync-ingest-00012. Backfill audit: custom fields complete everywhere; the real `supplier_payments` anomaly root-caused — Brightpearl misreports `resultsAvailable=500` for supplier-payment-search and ignores paymentId range filters, so scans stopped at one page (table had ids 1–500 + 6537–7036 only). Added `page_past_available` paging flag and re-scanned to recover all ~7k payments.
- **2026-07-09** — STALENESS_ALERT incident: `goods_out_notes`' full reload (`("full", None)`, ~270 requests/tick) occasionally overran the 30-min warm-tier cron; Cloud Scheduler fires `/tick/{tier}` regardless of whether the prior run finished, so overlapping ticks piled up concurrent full scans competing for the shared 200 req/min budget, starving `companies` and `derived_availability` (ordered after it in the same tick) for 2+ hours. Fixed two ways: (1) added a per-tier `asyncio.Lock` in `tick()` so an overlapping Scheduler call skips instead of piling on; (2) per Ryan, switched `goods_out_notes` to `("updated", "shippedOn")` — `shippedOn` is set once, when the note ships, and is effectively the `updatedOn` this resource never had, so it no longer needs a full reload every tick. Cleared the stuck instance by deleting the wedged revision (SIGTERM alone didn't stop it — the in-flight request kept running for ~5 min after deletion). Deployed as sync-ingest-00015; `get_data_freshness` confirmed `goods_out_notes`/`companies`/`product_availability` all caught up within the same tick.
- **2026-07-09 (later)** — Follow-up audit of every other `("full", None)` resource: probed live search metadata for columns. `companies` (warm tier) has no date field at all — full reload stays, but it's cheap (~20 requests/run) and now protected by the tick lock. Found 7 cold-tier resources (`brands`, `collections`, `product_types`, `seasons`, `categories`, `product_options`, `product_option_values`) that had an unused `updatedOn` all along; per Ryan, switched them to `("updated", "updatedOn")` and moved them from cold (daily) to warm (30-min) tier for near-real-time freshness now that incremental fetches make the tighter cadence cheap. `TIERS["warm"/"cold"]["dumps"]` in `sync/webhooks.py` is now derived from each resource's `tier` in `resources.py` (matching how `cold` already worked) instead of a separately hand-maintained list, so this kind of tier move can't silently desync the two again. `contact_groups`, `nominal_codes`, `currencies`, `payment_methods`, `warehouses`, `shipping_methods`, `warehouse_locations`, `contact_group_members` genuinely have no date field and stay full/cold — all tiny, daily cadence is fine.
- **2026-07-09 (evening)** — Second STALENESS_ALERT for `product_option_values`: its `key` was misconfigured as `"id"` (real field is `optionValueId`), harmless under `("full", None)` since `truncate_load` doesn't need a key, but broke the moment it moved to incremental — every warm tick since the 17:13 deploy threw `BadRequest: Unrecognized name: id` from the MERGE statement. Live-probed the other 6 resources moved earlier today; only `product_option_values` had the wrong key. Fixed. Also hardened `tick()`: wrapped each sweep/dump/derived call in `_safe()` so one resource's exception logs and continues instead of aborting the rest of the tier — this bug had been silently starving `derived_availability` (last in the loop) for hours with nothing but the eventual `product_option_values` staleness alert to show for it. Added a regression test (`test_tick_isolates_resource_failure`). Deployed as sync-ingest-00017.

# How the Code Works — a tour for a JavaScript developer

This walks through every module in the order data flows: Brightpearl API →
client → sync pipeline → BigQuery. Python idioms are translated to their
JavaScript equivalents as they appear. Skim the glossary at the bottom first
if Python syntax looks alien.

## The 30-second picture

```
Brightpearl API
   │  (rate-limited HTTPS, JSON)
   ▼
brightpearl_client/          "the SDK" — knows how to talk to Brightpearl
   │  plain dicts (= JS objects)
   ▼
sync/                        "the ETL" — decides what to fetch and shapes rows
   │  lists of row dicts
   ▼
BigQuery dataset `brightpearl`   one table per resource + sync_state bookkeeping
```

Everything is `async`/`await`, which works exactly like JavaScript — Python's
`asyncio` event loop is the same concept as Node's. `asyncio.run(main())` is
the equivalent of top-level `await main()`.

---

## brightpearl_client/ — the API SDK

### config.py — settings

`BrightpearlConfig` is a **pydantic-settings** class: think of it as `zod` +
`dotenv` combined. Declaring

```python
class BrightpearlConfig(BaseSettings):
    account: str          # required, crashes early if missing
    datacenter: str = "use1"   # optional with default
```

means "read `BRIGHTPEARL_ACCOUNT` and `BRIGHTPEARL_DATACENTER` from the
environment / .env file, validate their types, expose them as properties."
The `@property` methods (`base_url`, `auth_headers`) are like getters —
computed fields derived from the settings.

### rate_limiter.py — the shared 200 requests/min budget

This is the most important file for understanding why the system doesn't get
throttled. It's a classic **token bucket**:

- The bucket holds up to 200 tokens; every request spends one.
- Tokens refill continuously at 200/60 ≈ 3.33 per second.
- `acquire()` waits (async, non-blocking) until a token is available.

The twist is the **reserve**: 25% of the bucket is off-limits to background
work. `acquire(priority=False)` (bulk sync sweeps) can only draw the bucket
down to 50 tokens; `acquire(priority=True)` (webhook fetches, live MCP
lookups) can drain it to zero. That guarantees a big backfill can never
starve a real-time request.

Two server-feedback hooks make it self-correcting:
- `sync_remaining(n)` — every Brightpearl response includes a
  `brightpearl-requests-remaining` header. If the server says we have fewer
  tokens than we think (e.g. someone else is also using the API), we adopt
  the server's number.
- `note_throttled(seconds)` — on a 503, Brightpearl tells us how long to
  back off (`brightpearl-next-throttle-period` header). This freezes *all*
  acquisition until the window passes.

JS translation: `asyncio.Lock()` is needed because multiple coroutines
`await acquire()` concurrently — it's the mutex you'd reach for in Node with
`async-mutex`. `time.monotonic()` is `performance.now()/1000` (a clock that
can't jump backwards).

### client.py — HTTP + retry + the three access patterns

`BrightpearlClient` wraps **httpx.AsyncClient** (Python's `fetch`/`axios`).
The constructor pins the base URL and auth headers so call sites just use
paths. `async with BrightpearlClient() as bp:` is a **context manager** —
identical purpose to `try { ... } finally { client.close() }`; it guarantees
the connection pool closes.

`get(path, params, priority)` is the single choke point every request goes
through. Its retry loop, in order:
1. `await rate_limiter.acquire(...)` — wait for budget.
2. Send the request; network errors retry with exponential backoff
   (`2**attempt` seconds, capped at 30 — same as `Math.pow(2, attempt)`).
3. Fold the `requests-remaining` header back into the limiter.
4. Status handling: 503 → tell the limiter how long to pause, then retry;
   401/403 → raise `BrightpearlAuthError` (no retry — bad creds never fix
   themselves); 404 → `BrightpearlNotFound`; other 5xx → backoff and retry;
   other 4xx → raise immediately.
5. Success → return `body.response` (Brightpearl wraps everything in
   `{"response": ...}`).

"Raise" = `throw`. The custom exception classes in exceptions.py are just
`class BrightpearlAuthError extends BrightpearlError` so callers can
`catch` specific failures.

The three access patterns built on `get()`:

- **`search(service, resource, filters, ...)`** — Brightpearl's
  resource-search API returns column metadata + rows-as-arrays:
  ```json
  {"metaData": {"columns": [{"name": "orderId", ...}], "resultsAvailable": 243031},
   "results": [[100039, "SO", ...], [100040, "SO", ...]]}
  ```
  The client zips column names with each row array to give you normal
  objects: `[{orderId: 100039, ...}]`. Returned as a `SearchPage`
  **dataclass** (= a typed plain object, like an interface with a
  constructor) carrying pagination info and the raw column metadata.

- **`search_all(...)`** — an **async generator** (`async function*` in JS,
  and Python's `yield` = JS `yield`). It transparently walks all pages:
  consume it with `async for row in ...` (= `for await (const row of ...)`).

- **`get_resources(service, resource, ids)`** — Brightpearl lets you fetch up
  to ~100 full records in ONE request: `GET /order-service/order/1,2,3`.
  This is the whole reason webhook handling and backfills are affordable —
  100 orders per token instead of 1.

`ResourceAPI` instances (`bp.orders`, `bp.products`, ...) are just bound
shortcuts so call sites read nicely: `bp.orders.search(...)`.

---

## sync/ — the ETL

### schema.py — hand-written tables (the "detail" resources)

A dict of table definitions for the resources we curate by hand: `orders`,
`order_rows`, `products`, `contacts`, `sync_state`. Design rules:

- **Typed column for every field you'd filter/aggregate on.** BigQuery is
  columnar — a query only reads the columns it touches, so typed columns are
  fast and cheap.
- **`raw_payload` JSON column** holds the complete original API response.
  If Brightpearl adds a field tomorrow, we're already storing it — we just
  haven't promoted it to a typed column yet. Nothing is ever lost.
- **`when_upserted`** on every row = our write timestamp (audit trail).

### transforms.py — API JSON → table rows

Pure functions, no I/O — the easiest file to read and test. Each takes one
API payload (a nested dict) and returns flat row dict(s). Three things worth
knowing:

- Brightpearl sends money as strings (`"1745.50"`) — `_num()` converts to
  float, and empty string/None both become SQL NULL.
- `order_to_rows()` returns a tuple `(order_row, line_rows)` — Python tuples
  are just fixed-length arrays; `head, lines = order_to_rows(o)` is
  destructuring: `const [head, lines] = ...`.
- Order line items arrive as a dict keyed by row id
  (`{"81": {...}, "82": {...}}`) — we iterate `.items()` (= `Object.entries`)
  and flatten to one row per line with `order_id` on each.

### bq.py — the BigQuery writer

All writes go through a **staging + MERGE** pattern, which is how you do an
"upsert" in a data warehouse:

1. `_load_staging()` bulk-loads rows into `_stg_<table>` (WRITE_TRUNCATE =
   replace staging entirely). Batch loads are free in BigQuery.
2. `upsert()` runs a MERGE:
   ```sql
   MERGE target T USING (deduped staging) S ON T.key = S.key
   WHEN MATCHED THEN UPDATE ...        -- row exists: update it
   WHEN NOT MATCHED THEN INSERT ...    -- new row: insert it
   ```
   The "deduped staging" subquery keeps only the freshest copy of each key
   (`ROW_NUMBER() OVER (PARTITION BY key ORDER BY when_upserted DESC)`), so
   feeding the same record twice — overlapping sweeps, duplicate webhook
   deliveries — is harmless. **Idempotency is the property that makes the
   whole system self-healing.**
3. `replace_children()` handles order line items differently: DELETE all
   rows for the parent orders, INSERT fresh. A MERGE can't notice that a
   line was *removed* from an order; delete-and-replace can.
4. `truncate_load()` = replace the whole table. Used for small reference
   tables and snapshot-style data where history lives in the source.
5. `_load_json()` wraps every load with a retry: if a network blip makes the
   client resubmit a job and BigQuery answers "409 job already exists," we
   retry once under a fresh job id. (This exact failure killed a 77k-product
   backfill once; now it self-heals.)
6. `_dml()` wraps every mutating statement (MERGE, DELETE+INSERT, UPDATE)
   with exponential backoff on BigQuery's "concurrent update" abort —
   BigQuery only lets one statement mutate a table at a time, and a webhook
   task can collide with a scheduled sweep hitting the same table. Retrying
   is safe precisely because of the idempotency above.

### pipeline.py — detail-resource sync (orders/products/contacts)

`SyncPipeline.sync(resource, incremental, limit)` in plain steps:

1. **Watermark**: read `sync_state.watermark_updated_on` for this resource —
   the max `updatedOn` we've ever ingested. Subtract a 10-minute overlap
   (clock skew insurance; idempotent upserts make the overlap free).
2. **Collect IDs**: run the resource-search with
   `updatedOn=<watermark>/` ("everything updated since"). Collect just IDs.
3. **Fetch + load in batches of 500**: multi-ID GET the full payloads,
   transform, upsert parents, replace children.
4. **Record the run**: write the new watermark + stats back to `sync_state`.

`backfill` is the same flow with no filter (fetch everything);
`sweep` is the incremental flavor. Same code path, different filter — one
thing to debug, not two.

### How deletions get here

Sweeps only ever ask "what changed since X?", and a record deleted in
Brightpearl simply stops coming back — it never shows up as a change. So
deletions need their own path, and there are two of them.

**Soft delete, for `orders`/`products`/`contacts`.** We don't remove the row;
we flip `is_deleted = TRUE` (think tombstone, not `splice()`). Two things set
that flag:

- the `destroyed` webhook → `bq.mark_deleted()`. Only `product.destroyed` is
  subscribed — `contact.*` events aren't offered on this account, so contacts
  have no real-time path.
- `pipeline.reconcile_deletions()`, in the **cold tier, daily at 07:00 UTC**.
  It lists every ID the API still has, diffs against the live IDs in BigQuery,
  and tombstones the difference. It refuses to act if the API returns nothing,
  or if more than 20% of rows would vanish — a silently-failed search must
  never trigger a mass delete.

So a contact deleted in the Brightpearl UI disappears from BigQuery within
about a day, not instantly.

The flag is only half the job: **every view over a soft-deletable table must
filter it**, or the tombstoned rows keep getting served. `customer_summary`
shipped without that filter and served deleted contacts through
`search_customers`; `test_views_exclude_soft_deleted_rows` now guards all of
them. Note that `is_deleted` is `NULL` on most rows, hence
`NOT IFNULL(x.is_deleted, FALSE)` rather than `NOT x.is_deleted`.

Re-upserting resets `is_deleted` to `FALSE`, which is correct: the record only
gets re-upserted if the API returned it, meaning it exists again.

**Truncate-and-reload, for everything else.** Any `("full", None)` resource in
`resources.py` (`companies`, the reference tables) is wiped and re-downloaded
each tick, so deletions propagate for free — no flag, no sweep. That's why
`companies` has no `is_deleted` column.

### resources.py + dynamic.py — the config-driven "everything else"

Hand-writing 40 schemas would be miserable, so the rest of the tables are
generated. The trick: **Brightpearl's search API describes its own columns**
(`{"name": "journalId", "reportDataType": "IDSET"}`), so `SearchDumpSyncer`:

1. Runs page 1 of the search.
2. Maps Brightpearl types → BigQuery types (`TYPE_MAP`), converts camelCase
   → snake_case, and creates the table if needed. Schema comes *from the
   API*, not from us.
3. Streams every page through `_to_row()` (type coercion per column) into
   25,000-row buffers, MERGE-ing each buffer.
4. Records watermarks in `sync_state`.

Each resource is ~6 lines of config in `resources.py`. The `incremental`
field picks one of three update strategies:

- `("id", col)` — append-only data (journal rows): sort the search
  newest-first (`sort=col.DESC`) and stop paging the moment we see an id we
  already have. Cheapest possible. (We'd prefer a range filter — "ids greater
  than X" — but Brightpearl rejects range syntax on INTEGER/IDSET columns,
  and a rejected filter used to silently degrade into a 2,900-request full
  scan every 30 minutes.)
- `("updated", col)` — resources with a filterable update timestamp: a plain
  `updatedOn` (goods movements), or a domain field that serves the same
  purpose (goods-out notes: `shippedOn` is set once, when the note ships,
  and that's the transition our reporting actually cares about — pre-ship
  picked/packed/printed churn isn't tracked in near-real-time). Timestamp
  watermark, like the detail pipeline.
- `("full", None)` — no usable timestamp at all, or tiny tables: reload
  everything each run. Correctness beats cleverness at these sizes.

If a filter is rejected by the API, the syncer logs it and falls back to a
full scan — a wrong config degrades gracefully instead of crashing.

`ReferenceSyncer` is the simple sibling for plain GET endpoints (statuses,
tax codes...): fetch the list, store `id`/`name` best-effort + full
`raw_payload`, truncate-reload. These tables are tiny; brute force is right.

### derived.py — fan-out loaders

`product_prices`, `product_suppliers`, and `product_availability` have no
search endpoint; you ask for them *per product*. So these loaders read
product ids **from our own BigQuery products table**, chunk them 100 at a
time, and snapshot the results (truncate-reload). Each run records itself
in `sync_state` (`last_run_kind: "derived"`) so freshness monitoring covers
these tables too.

`_fetch_availability` shows a useful trick: one bad id in a chunk 400s the
whole request, so on a 400 it **recursively splits the chunk in half** until
bad ids are isolated and skipped — binary search as error handling.
`_fetch_suppliers` does the same, with one extra rule: only a 400 ("bad id")
is tolerated. Any other error — throttle, auth, 5xx, unexpected payload
shape — aborts the run *before* the truncate, so a flaky API call can never
replace a good table with a partial snapshot.

### cli.py — the entry point

`argparse` = `commander`/`yargs`. Subcommands:

```
python -m sync.cli init                      # create tables
python -m sync.cli backfill orders           # full load (detail resources)
python -m sync.cli sweep all                 # incremental (detail resources)
python -m sync.cli dump journal_rows         # search-dump tables
python -m sync.cli dump refs                 # all reference tables
python -m sync.cli derived all               # prices + availability
python -m sync.cli status                    # sync_state, one line per table
```

`python -m sync.cli` means "run this module" — like an npm script pointing
at a file. `load_dotenv()` at startup = `require('dotenv').config()`.

---

## tests/ — how to read them

pytest = vitest/jest. There's no `describe`/`it` — any function named
`test_*` is a test, plain `assert x == y` is the assertion API.
`tests/test_client.py` uses **respx** to mock HTTP at the transport level
(like `nock`/`msw`): declare a route, return a canned response, assert on
what was sent. No real network in tests, ever; the live API is only touched
by `scripts/live_smoke.py`, run manually.

## Python → JavaScript glossary

| Python | JavaScript |
|---|---|
| `dict` / `{}`  | plain object / `Map` |
| `list` / `[]` | array |
| tuple `(a, b)` + `x, y = f()` | array + destructuring `const [x, y] = f()` |
| `dataclass` | typed object literal (interface + constructor) |
| `async def` / `await` | `async function` / `await` (same semantics) |
| `async for x in gen()` | `for await (const x of gen())` |
| `yield` in `async def` | `async function*` generator |
| `asyncio.run(main())` | top-level `await main()` |
| `with` / `async with` | `try/finally` resource cleanup |
| `raise` / `except` | `throw` / `catch` |
| `f"{x}/"` | `` `${x}/` `` |
| `[f(x) for x in xs]` | `xs.map(f)` |
| `{k: f(v) for k, v in d.items()}` | `Object.fromEntries(Object.entries(d).map(...))` |
| `d.get("k")` | `d.k ?? undefined` (no KeyError) |
| `x or None` | `x || null` (0/"" become null — used deliberately for FKs) |
| `@property` | getter |
| pydantic | zod |
| httpx | fetch/axios |
| pytest + respx | vitest + msw/nock |
| `pyproject.toml` + uv | `package.json` + npm/pnpm |

## Where to make common changes

- **Add a new searchable resource** → 6 lines in `sync/resources.py`
  (copy an existing entry; probe the endpoint first with
  `scripts/probe_endpoints.py`).
- **Promote a field from raw_payload to a typed column** → add the
  SchemaField in `sync/schema.py`, map it in `sync/transforms.py`, add
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in `bq.ensure_tables()`.
- **Change sweep cadence** → `tier` values in `resources.py` /
  `pipeline.RESOURCES` (wired to schedules in Phase 4).
- **Change which order statuses count as real orders** → the `REPORTABLE_SO`
  / `REPORTABLE_PO` predicates in `sync/views.py` (pending/cancelled sales
  orders are filtered out of `sales_flat` and `customer_summary`, draft POs
  out of `po_pipeline`), then redeploy the views with
  `python -m sync.cli views`.
- **Debug a failed sync** → `python -m sync.cli status` shows per-table
  watermarks and last run; every module logs via `logging` (stderr).

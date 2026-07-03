# Brightpearl MCP Server

Natural-language access to East Coast Fabrics' Brightpearl ERP data: a sync pipeline (Brightpearl → BigQuery, webhooks-first for near-real-time freshness) plus a remote MCP server (FastMCP, streamable-HTTP on Cloud Run) that queries BigQuery and can hit the Brightpearl API live.

**Roadmap, phase status, and architecture details live in [GAMEPLAN.md](GAMEPLAN.md).** Update its Status log when a phase advances.

## Layout

- `src/brightpearl_client/` — Brightpearl API client: auth, rate limiting, resource search, typed service accessors
- `src/sync/` — BigQuery schema, backfill/sweep jobs, webhook ingest
- `src/mcp_server/` — FastMCP app and tools
- `tests/` — pytest; unit tests use recorded fixtures, never the live API

## Stack

Python 3.12+, managed with **uv** (`uv sync`, `uv run pytest`). Key deps: `fastmcp`/`mcp`, `google-cloud-bigquery`, `httpx`, `pydantic`, `python-dotenv`.

## Brightpearl API essentials

- Base URL: `https://{datacenter}.brightpearlconnect.com/public-api/{account-code}/`
- Auth headers (private app): `brightpearl-app-ref` + `brightpearl-account-token`
- **Rate limit: 200 requests/min per account** — everything (sweeps, webhook fetches, live MCP calls) shares one budget through the client's rate limiter; honor `brightpearl-requests-remaining`, back off on 503
- Webhooks deliver thin payloads (resource ID only); always batch follow-up fetches with multi-ID GETs (`/order/123,456,789`)
- Docs: https://api-docs.brightpearl.com/

## Conventions

- Credentials via env vars only (see `.env.example`); `.env` is gitignored; never commit secrets or service-account keys
- BigQuery: typed columns for queryable fields + one native-JSON `raw_payload` column per table; audit columns (`when_created`, `when_modified`, `when_upserted`) on every table
- Webhook/stream handlers must be idempotent — delivery is at-least-once
- All MCP SQL access is read-only with a byte-scan cap

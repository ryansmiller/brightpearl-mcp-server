# brightpearl-mcp-server

MCP server + sync pipeline giving Claude natural-language access to East Coast Fabrics' Brightpearl ERP data via Google BigQuery, with near-real-time freshness (Brightpearl webhooks) and live API passthrough.

See [GAMEPLAN.md](GAMEPLAN.md) for the architecture, data-freshness strategy, and phased roadmap.

## Quick start (dev)

```sh
uv sync
cp .env.example .env   # fill in Brightpearl + GCP credentials
uv run pytest
```

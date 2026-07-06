# Using the Brightpearl AI Assistant

East Coast Fabrics' Brightpearl data (orders, inventory, customers, financials)
is available to Claude through an MCP server. Data is synced near-real-time:
order changes land in seconds via webhooks; everything else within 5–30 minutes.

## Connect Claude

You sign in with your **@eastcoastfabrics.com Google account** — there are no
tokens or secrets to copy.

**Claude Code (terminal or desktop app):**

```sh
claude mcp add --transport http brightpearl \
  https://mcp-server-243337884757.us-east4.run.app/mcp
```

A browser window opens for Google sign-in (if it doesn't, run `/mcp` inside
Claude Code and choose "authenticate").

**Claude Desktop / claude.ai:** Settings → Connectors → Add custom connector →
paste the URL above (no headers needed) → Connect → sign in with your work
Google account.

## What you can ask

- "What were our top 20 SKUs by revenue last quarter?"
- "Show me stock levels for the Select Metal collection across warehouses"
- "Which customers haven't ordered in 6 months but have $10k+ lifetime value?"
- "What POs are still open from Morbern?"
- "Compare monthly sales this year vs last year by channel"
- "How fresh is the data right now?" (get_data_freshness)
- "What's the stock level of SKU X *right now*?" (live API lookup)

Claude can also write arbitrary read-only SQL against the warehouse
(`brightpearl` dataset in BigQuery) — it knows the schema.

## If something looks wrong

- **Stale data?** Ask Claude to run `get_data_freshness`. If a resource is
  stale, an alert email has probably already gone to Ryan (automated check
  every 15 minutes).
- **Alert email received?** Check Cloud Run logs:
  `gcloud run services logs read sync-ingest --region us-east4 --limit 50`
  Most transient failures self-heal on the next scheduled sweep.
- **Numbers that disagree with Brightpearl?** Deleted orders/products are kept
  with an `is_deleted` flag and excluded from the semantic views; check
  whether the record was deleted. For this-second truth, ask Claude to use
  the live lookup tools.

## Boundaries

- The server is **read-only** — nothing can write back to Brightpearl or
  modify the warehouse through Claude.
- Every tool call is logged to the `mcp_audit` table (tool, arguments, time,
  and the signed-in user's email).
- Access requires an @eastcoastfabrics.com Google account (enforced both by
  the OAuth consent screen and server-side).
- Live API lookups share Brightpearl's 200 requests/min budget with
  ShipStation and the website integration — they're throttled automatically.

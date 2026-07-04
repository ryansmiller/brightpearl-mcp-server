"""Brightpearl MCP server.

Tools answer questions from the BigQuery warehouse (fast, cheap, near-real-
time via the sync pipeline) and can hit the Brightpearl API live for
this-second lookups. Every tool call is written to the mcp_audit table.

Run locally (stdio):   uv run python -m mcp_server.server
Remote (Phase 4):      streamable-HTTP on Cloud Run
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from fastmcp import FastMCP
from google.cloud import bigquery

from brightpearl_client import BrightpearlClient

load_dotenv()
logger = logging.getLogger(__name__)

mcp = FastMCP(
    "brightpearl",
    instructions=(
        "East Coast Fabrics' Brightpearl ERP data. Prefer the semantic views "
        "(sales_flat, inventory_position, po_pipeline, customer_summary, "
        "monthly_financials) via run_bigquery_sql for anything the dedicated "
        "tools don't cover. Call get_schema first when writing SQL. Data is "
        "synced near-real-time; check get_data_freshness when currency matters. "
        "For this-second answers use the *_live tools."
    ),
)

PROJECT = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "brightpearl")
MAX_BYTES_BILLED = 2 * 1024**3  # 2 GB scan cap per query
MAX_ROWS = 200

_bq = bigquery.Client(project=PROJECT)
_bp: BrightpearlClient | None = None


def _brightpearl() -> BrightpearlClient:
    global _bp
    if _bp is None:
        _bp = BrightpearlClient()
    return _bp


def _audit(tool: str, args: dict[str, Any], ok: bool, detail: str = "") -> None:
    try:
        _bq.insert_rows_json(
            f"{PROJECT}.{DATASET}.mcp_audit",
            [{
                "called_at": datetime.now(timezone.utc).isoformat(),
                "tool": tool,
                "arguments": json.dumps(args)[:2000],
                "ok": ok,
                "detail": detail[:500],
            }],
        )
    except Exception:  # audit must never break a tool call
        logger.exception("audit insert failed")


def _rows_to_result(rows: list[dict[str, Any]]) -> str:
    out = [dict(r) for r in rows[:MAX_ROWS]]
    note = f"\n({len(rows)} rows total, showing first {MAX_ROWS})" if len(rows) > MAX_ROWS else ""
    return json.dumps(out, default=str, indent=1) + note


def _query(sql: str, params: list | None = None) -> list[dict[str, Any]]:
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_BILLED, query_parameters=params or []
    )
    return [dict(r) for r in _bq.query_and_wait(sql, job_config=job_config)]


_SQL_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|GRANT|CALL|EXPORT|LOAD)\b",
    re.IGNORECASE,
)


@mcp.tool
def run_bigquery_sql(sql: str) -> str:
    """Run a read-only SQL query against the brightpearl BigQuery dataset.

    Only SELECT/WITH statements are allowed; scans are capped at 2GB. Table
    names must be dataset-qualified like `brightpearl.sales_flat`. Call
    get_schema to see available tables, views, and columns first.
    """
    stripped = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL).strip()
    if not re.match(r"^(SELECT|WITH)\b", stripped, re.IGNORECASE):
        _audit("run_bigquery_sql", {"sql": sql}, False, "rejected: not a SELECT")
        return "Error: only SELECT/WITH queries are allowed."
    if _SQL_FORBIDDEN.search(stripped):
        _audit("run_bigquery_sql", {"sql": sql}, False, "rejected: forbidden keyword")
        return "Error: statement contains a write/DDL keyword; only reads are allowed."
    try:
        rows = _query(stripped)
        _audit("run_bigquery_sql", {"sql": sql}, True, f"{len(rows)} rows")
        return _rows_to_result(rows)
    except Exception as e:
        _audit("run_bigquery_sql", {"sql": sql}, False, str(e))
        return f"Query failed: {e}"


@mcp.tool
def get_schema() -> str:
    """List every table and view in the warehouse with its columns and types."""
    rows = _query(f"""
        SELECT table_name, ARRAY_AGG(column_name || ' ' || data_type ORDER BY ordinal_position) AS columns
        FROM `{PROJECT}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
        GROUP BY table_name ORDER BY table_name
    """)
    _audit("get_schema", {}, True)
    return _rows_to_result(rows)


@mcp.tool
def get_data_freshness() -> str:
    """Show how current each synced resource is: watermarks, last run, lag in minutes."""
    rows = _query(f"""
        SELECT resource, last_run_kind, last_run_rows,
               CAST(watermark_updated_on AS STRING) AS watermark,
               CAST(last_run_at AS STRING) AS last_run_at,
               TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_run_at, MINUTE) AS run_lag_minutes
        FROM `{PROJECT}.{DATASET}.sync_state` ORDER BY resource
    """)
    _audit("get_data_freshness", {}, True)
    return _rows_to_result(rows)


@mcp.tool
def query_sales(
    start_date: str,
    end_date: str,
    group_by: str = "month",
    top_n: int = 25,
) -> str:
    """Sales rollup between two dates (YYYY-MM-DD), from the sales_flat view.

    group_by: one of month | product | sku | customer | channel | state
    Returns revenue (net), quantity, order count, and margin per group.
    """
    dims = {
        "month": "FORMAT_DATE('%Y-%m', DATE(placed_on))",
        "product": "product_name",
        "sku": "product_sku",
        "customer": "customer_company_name",
        "channel": "channel_name",
        "state": "delivery_state",
    }
    if group_by not in dims:
        return f"Error: group_by must be one of {list(dims)}"
    rows = _query(
        f"""
        SELECT {dims[group_by]} AS {group_by},
               ROUND(SUM(row_net), 2) AS revenue,
               ROUND(SUM(quantity), 1) AS quantity,
               COUNT(DISTINCT order_id) AS orders,
               ROUND(SUM(row_margin), 2) AS margin
        FROM `{PROJECT}.{DATASET}.sales_flat`
        WHERE DATE(placed_on) BETWEEN @start AND @end
        GROUP BY 1 ORDER BY revenue DESC
        LIMIT @top_n
        """,
        [
            bigquery.ScalarQueryParameter("start", "DATE", start_date),
            bigquery.ScalarQueryParameter("end", "DATE", end_date),
            bigquery.ScalarQueryParameter("top_n", "INT64", top_n),
        ],
    )
    _audit("query_sales", {"start": start_date, "end": end_date, "group_by": group_by}, True)
    return _rows_to_result(rows)


@mcp.tool
def get_stock_levels(search: str, warehouse_id: int | None = None) -> str:
    """Stock position for products matching a SKU/name/pattern/collection search term.

    Data is from the last sync; for this-second numbers use get_stock_live.
    """
    where = "AND warehouse_id = @wh" if warehouse_id is not None else ""
    params = [bigquery.ScalarQueryParameter("q", "STRING", f"%{search}%")]
    if warehouse_id is not None:
        params.append(bigquery.ScalarQueryParameter("wh", "INT64", warehouse_id))
    rows = _query(
        f"""
        SELECT product_id, sku, name, collection, pattern, size, unit_of_measure,
               warehouse_name, on_hand, allocated, in_stock, on_order,
               CAST(stock_as_of AS STRING) AS stock_as_of
        FROM `{PROJECT}.{DATASET}.inventory_position`
        WHERE (LOWER(sku) LIKE LOWER(@q) OR LOWER(name) LIKE LOWER(@q)
               OR LOWER(collection) LIKE LOWER(@q) OR LOWER(pattern) LIKE LOWER(@q))
        {where}
        ORDER BY sku, warehouse_id LIMIT 100
        """,
        params,
    )
    _audit("get_stock_levels", {"search": search, "warehouse_id": warehouse_id}, True)
    return _rows_to_result(rows)


@mcp.tool
def search_customers(query: str) -> str:
    """Find customers by name, email, or company, with lifetime order stats."""
    rows = _query(
        f"""
        SELECT contact_id, first_name, last_name, email, organisation_name,
               order_count, ROUND(lifetime_value, 2) AS lifetime_value,
               CAST(last_order_on AS STRING) AS last_order_on, days_since_last_order
        FROM `{PROJECT}.{DATASET}.customer_summary`
        WHERE LOWER(CONCAT(IFNULL(first_name,''),' ',IFNULL(last_name,''),' ',
                    IFNULL(email,''),' ',IFNULL(organisation_name,''))) LIKE LOWER(@q)
        ORDER BY lifetime_value DESC NULLS LAST LIMIT 50
        """,
        [bigquery.ScalarQueryParameter("q", "STRING", f"%{query}%")],
    )
    _audit("search_customers", {"query": query}, True)
    return _rows_to_result(rows)


@mcp.tool
def get_po_pipeline(supplier: str | None = None) -> str:
    """Open purchase orders (inbound inventory) with lines, optionally filtered by supplier."""
    where = "AND LOWER(supplier_name) LIKE LOWER(@sup)" if supplier else ""
    params = [bigquery.ScalarQueryParameter("sup", "STRING", f"%{supplier}%")] if supplier else []
    rows = _query(
        f"""
        SELECT order_id, reference, supplier_name, order_status_name,
               CAST(placed_on AS STRING) AS placed_on, warehouse_name,
               product_sku, product_name, quantity, unit_cost, row_net
        FROM `{PROJECT}.{DATASET}.po_pipeline`
        WHERE order_status_name NOT IN ('Completed', 'Cancelled') {where}
        ORDER BY placed_on DESC LIMIT 200
        """,
        params,
    )
    _audit("get_po_pipeline", {"supplier": supplier}, True)
    return _rows_to_result(rows)


@mcp.tool
async def get_order_live(order_id: int) -> str:
    """Fetch one order directly from the Brightpearl API right now (bypasses the warehouse)."""
    try:
        orders = await _brightpearl().orders.get(
            [order_id], params={"includeOptional": "customFields"}, priority=True
        )
        _audit("get_order_live", {"order_id": order_id}, True)
        return json.dumps(orders[0], default=str, indent=1) if orders else "Order not found."
    except Exception as e:
        _audit("get_order_live", {"order_id": order_id}, False, str(e))
        return f"Live lookup failed: {e}"


@mcp.tool
async def get_stock_live(product_ids: list[int]) -> str:
    """Fetch this-second stock availability for up to 100 products from the Brightpearl API."""
    try:
        payload = await _brightpearl().get(
            "warehouse-service/product-availability/"
            + ",".join(str(i) for i in product_ids[:100]),
            priority=True,
        )
        _audit("get_stock_live", {"product_ids": product_ids}, True)
        return json.dumps(payload, default=str, indent=1)
    except Exception as e:
        _audit("get_stock_live", {"product_ids": product_ids}, False, str(e))
        return f"Live lookup failed: {e}"


AUDIT_SCHEMA = [
    bigquery.SchemaField("called_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("tool", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("arguments", "STRING"),
    bigquery.SchemaField("ok", "BOOL"),
    bigquery.SchemaField("detail", "STRING"),
]


def ensure_audit_table() -> None:
    table = bigquery.Table(f"{PROJECT}.{DATASET}.mcp_audit", schema=AUDIT_SCHEMA)
    _bq.create_table(table, exists_ok=True)


class BearerAuthMiddleware:
    """Minimal ASGI middleware: require Authorization: Bearer <MCP_BEARER_TOKEN>."""

    def __init__(self, app, token: str):
        self.app = app
        self.token = token

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("path") != "/health":
            headers = dict(scope.get("headers") or [])
            auth = headers.get(b"authorization", b"").decode()
            if auth != f"Bearer {self.token}":
                await send({
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [(b"content-type", b"text/plain")],
                })
                await send({"type": "http.response.body", "body": b"unauthorized"})
                return
        await self.app(scope, receive, send)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    ensure_audit_table()
    if os.environ.get("MCP_TRANSPORT") == "http":
        import uvicorn

        token = os.environ["MCP_BEARER_TOKEN"]
        app = BearerAuthMiddleware(mcp.http_app(), token)
        uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    else:
        mcp.run()  # stdio for local dev


if __name__ == "__main__":
    main()

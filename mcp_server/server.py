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
from fastmcp.server.dependencies import get_access_token
from fastmcp.server.middleware import Middleware
from google.cloud import bigquery
from mcp.shared.exceptions import McpError
from mcp.types import ErrorData
from starlette.responses import PlainTextResponse

from brightpearl_client import BrightpearlClient

load_dotenv()
logger = logging.getLogger(__name__)

PROJECT = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "brightpearl")
MAX_BYTES_BILLED = 2 * 1024**3  # 2 GB scan cap per query
MAX_ROWS = 200

ALLOWED_EMAIL_DOMAIN = "eastcoastfabrics.com"


def _build_auth():
    """Google OAuth for the remote HTTP deployment; users sign in with their
    Workspace account instead of pasting a shared bearer token. Local stdio
    dev returns None — same as before, auth only ever applied to HTTP."""
    if os.environ.get("MCP_TRANSPORT") != "http":
        return None
    from fastmcp.server.auth.providers.google import GoogleProvider
    from key_value.aio.stores.firestore import FirestoreStore, FirestoreV1KeySanitizationStrategy

    return GoogleProvider(
        client_id=os.environ["GOOGLE_OAUTH_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_OAUTH_CLIENT_SECRET"],
        base_url=os.environ["MCP_BASE_URL"],
        required_scopes=["openid", "https://www.googleapis.com/auth/userinfo.email"],
        # Cloud Run disk is ephemeral: FastMCP's default file-tree store would
        # forget client registrations and refresh tokens on every cold start
        # and send everyone back through the browser. Firestore persists them.
        # Claude Code registers with a URL client_id (a client ID metadata
        # document), and '/' isn't allowed in Firestore document ids. The
        # sanitizer rewrites only invalid ids, so existing keys are unchanged.
        client_storage=FirestoreStore(
            project=PROJECT,
            default_collection="mcp_oauth",
            key_sanitization_strategy=FirestoreV1KeySanitizationStrategy(),
        ),
    )


mcp = FastMCP(
    "brightpearl",
    instructions=(
        "East Coast Fabrics' Brightpearl ERP data. Prefer the semantic views "
        "(sales_unified, sales_flat, inventory_position, po_pipeline, customer_summary, "
        "monthly_financials, product_variations) via run_bigquery_sql for anything the dedicated "
        "tools don't cover. Sales, revenue, and customer questions MUST be "
        "answered from these views, never the raw orders table: they exclude "
        "quotes, drafts, pending, and cancelled orders that Brightpearl stores "
        "as sales orders but that are not real sales. IMPORTANT — history spans "
        "two eras: current Brightpearl AND a one-time legacy Nationwide Fabric "
        "Shopify store (2018 through 2026-02-20, no date overlap). The "
        "sales_unified view covers BOTH (tagged by a `source` column) and is the "
        "default for any sales/customer question; sales_flat is Brightpearl-only "
        "(use it when you specifically need margin/cost, which the legacy data "
        "lacks). The query_sales and search_customers tools already read "
        "sales_unified, so a legacy-only customer with no Brightpearl orders "
        "still shows real sales — do not conclude 'no sales history' from a "
        "Brightpearl-only source. sales_unified carries real Brightpearl "
        "product/customer ids, so join it to products/contacts by id. Call "
        "get_schema first when writing SQL. Data is synced "
        "near-real-time; check get_data_freshness when currency matters. "
        "For this-second answers use the *_live tools."
    ),
    auth=_build_auth(),
)


class RequireCompanyDomain(Middleware):
    """Server-side domain gate. The OAuth consent screen is set to Internal
    (Workspace-only) which already blocks outsiders; this is belt and braces
    so a misconfigured consent screen can never expose company data."""

    async def on_request(self, context, call_next):
        token = get_access_token()  # None on stdio, where no auth layer runs
        if token is not None:
            email = ((token.claims or {}).get("email") or "").lower()
            if not email.endswith(f"@{ALLOWED_EMAIL_DOMAIN}"):
                raise McpError(ErrorData(
                    code=-32003,
                    message=f"access is restricted to {ALLOWED_EMAIL_DOMAIN} accounts",
                ))
        return await call_next(context)


mcp.add_middleware(RequireCompanyDomain())


@mcp.custom_route("/health", methods=["GET"])
async def health(request):
    return PlainTextResponse("ok")


def _user_email() -> str | None:
    """Email of the signed-in user, for audit attribution. None on stdio."""
    try:
        token = get_access_token()
        return (token.claims or {}).get("email") if token else None
    except Exception:
        return None


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
                "user_email": _user_email(),
            }],
        )
    except Exception:  # audit must never break a tool call
        logger.exception("audit insert failed")


def _rows_to_result(rows: list[dict[str, Any]]) -> str:
    out = [dict(r) for r in rows[:MAX_ROWS]]
    note = f"\n(more rows exist; showing first {MAX_ROWS})" if len(rows) > MAX_ROWS else ""
    return json.dumps(out, default=str, indent=1) + note


def _query(sql: str, params: list | None = None) -> list[dict[str, Any]]:
    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_BILLED, query_parameters=params or []
    )
    rows = _bq.query_and_wait(sql, job_config=job_config, max_results=MAX_ROWS + 1)
    return [dict(r) for r in rows]


def _assert_only_brightpearl(sql: str) -> None:
    """Dry-run the query and reject references outside our dataset."""
    job = _bq.query(
        sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    )
    for t in job.referenced_tables:
        if t.project != PROJECT or t.dataset_id != DATASET:
            raise PermissionError(
                f"query references {t.project}.{t.dataset_id}.{t.table_id}; "
                f"only the {DATASET} dataset is allowed"
            )


# Write/DDL keywords plus BigQuery scripting/dynamic-SQL constructs. EXECUTE
# IMMEDIATE is the important one: it runs a string the static checks below
# can't see into, so a mutation assembled by concatenation
# (EXECUTE IMMEDIATE 'DEL' || 'ETE ...') would never trip a keyword match and
# its target table never shows up in the dry-run's referenced_tables.
_SQL_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|CALL|EXPORT"
    r"|LOAD|EXECUTE|DECLARE|BEGIN|SET|FOR|WHILE|LOOP|ASSERT)\b",
    re.IGNORECASE,
)


@mcp.tool
def run_bigquery_sql(sql: str) -> str:
    """Run a read-only SQL query against the brightpearl BigQuery dataset.

    Only SELECT/WITH statements are allowed; scans are capped at 2GB. Table
    names must be dataset-qualified like `brightpearl.sales_flat`. Call
    get_schema to see available tables, views, and columns first.

    For sales/revenue/customer questions always query the sales_flat or
    customer_summary views, not the raw orders table: the views exclude
    pending-status and cancelled orders (quotes, drafts, unprocessed Amazon
    orders) that must never count as sales.
    """
    stripped = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL).strip()
    # A single trailing ';' is fine; anything after it means a second statement
    # was smuggled in (SELECT 1; EXECUTE IMMEDIATE ...) — the prefix check only
    # sees the harmless first statement, so reject multi-statement scripts.
    if stripped.rstrip(";").count(";"):
        _audit("run_bigquery_sql", {"sql": sql}, False, "rejected: multiple statements")
        return "Error: only a single SELECT/WITH statement is allowed."
    if not re.match(r"^(SELECT|WITH)\b", stripped, re.IGNORECASE):
        _audit("run_bigquery_sql", {"sql": sql}, False, "rejected: not a SELECT")
        return "Error: only SELECT/WITH queries are allowed."
    if _SQL_FORBIDDEN.search(stripped):
        _audit("run_bigquery_sql", {"sql": sql}, False, "rejected: forbidden keyword")
        return "Error: statement contains a write/DDL/scripting keyword; only reads are allowed."
    try:
        _assert_only_brightpearl(stripped)
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
    source: str = "all",
) -> str:
    """Sales rollup between two dates (YYYY-MM-DD), from the sales_unified view.

    Spans BOTH eras: current Brightpearl sales and the one-time legacy
    Nationwide Fabric history (2018 → 2026-02-20). This is why a legacy-only
    customer that looks like it has no sales in Brightpearl still shows revenue
    here. Pending/draft/cancelled orders are already excluded.

    group_by: one of month | product | sku | customer | channel | state | source
    source:   all (default) | brightpearl | nationwide  — restrict the era
    Returns revenue (net), quantity, and order count per group. (Margin/cost is
    Brightpearl-only — for margin, run_bigquery_sql on the sales_flat view.)
    """
    dims = {
        "month": "FORMAT_DATE('%Y-%m', order_date)",
        "product": "product_name",
        "sku": "sku",
        "customer": "customer_company",
        "channel": "channel",
        "state": "ship_state",
        "source": "source",
    }
    if group_by not in dims:
        return f"Error: group_by must be one of {list(dims)}"
    if source not in ("all", "brightpearl", "nationwide"):
        return "Error: source must be one of ['all', 'brightpearl', 'nationwide']"
    top_n = max(1, min(top_n, MAX_ROWS))
    params = [
        bigquery.ScalarQueryParameter("start", "DATE", start_date),
        bigquery.ScalarQueryParameter("end", "DATE", end_date),
        bigquery.ScalarQueryParameter("top_n", "INT64", top_n),
    ]
    source_filter = ""
    if source != "all":
        source_filter = "AND source = @source"
        params.append(bigquery.ScalarQueryParameter("source", "STRING", source))
    rows = _query(
        f"""
        SELECT {dims[group_by]} AS {group_by},
               ROUND(SUM(line_net), 2) AS revenue,
               ROUND(SUM(quantity), 1) AS quantity,
               COUNT(DISTINCT order_ref) AS orders
        FROM `{PROJECT}.{DATASET}.sales_unified`
        WHERE order_date BETWEEN @start AND @end
        {source_filter}
        GROUP BY 1 ORDER BY revenue DESC
        LIMIT @top_n
        """,
        params,
    )
    _audit(
        "query_sales",
        {"start": start_date, "end": end_date, "group_by": group_by, "source": source},
        True,
    )
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
    """Find customers by name, email, or company, with lifetime order stats.

    Lifetime stats span BOTH eras via sales_unified (current Brightpearl + the
    legacy Nationwide Fabric history), so a customer whose only orders are
    pre-Brightpearl still shows a real lifetime value here. The `sources` column
    names which era(s) a customer has ('brightpearl', 'nationwide', or both);
    lifetime_value is net revenue.
    """
    rows = _query(
        f"""
        WITH matched AS (
          SELECT contact_id, first_name, last_name, email, organisation_name
          FROM `{PROJECT}.{DATASET}.contacts`
          WHERE NOT IFNULL(is_deleted, FALSE)
            AND LOWER(CONCAT(IFNULL(first_name,''),' ',IFNULL(last_name,''),' ',
                      IFNULL(email,''),' ',IFNULL(organisation_name,''))) LIKE LOWER(@q)
        ),
        stats AS (
          SELECT customer_id,
                 COUNT(DISTINCT order_ref) AS order_count,
                 ROUND(SUM(line_net), 2) AS lifetime_value,
                 MAX(order_date) AS last_order_on,
                 STRING_AGG(DISTINCT source ORDER BY source) AS sources
          FROM `{PROJECT}.{DATASET}.sales_unified`
          WHERE customer_id IN (SELECT contact_id FROM matched)
          GROUP BY customer_id
        )
        SELECT m.contact_id, m.first_name, m.last_name, m.email, m.organisation_name,
               IFNULL(s.order_count, 0) AS order_count,
               IFNULL(s.lifetime_value, 0) AS lifetime_value,
               CAST(s.last_order_on AS STRING) AS last_order_on,
               DATE_DIFF(CURRENT_DATE(), s.last_order_on, DAY) AS days_since_last_order,
               IFNULL(s.sources, '') AS sources
        FROM matched m LEFT JOIN stats s ON s.customer_id = m.contact_id
        ORDER BY lifetime_value DESC NULLS LAST LIMIT 50
        """,
        [bigquery.ScalarQueryParameter("q", "STRING", f"%{query}%")],
    )
    _audit("search_customers", {"query": query}, True)
    return _rows_to_result(rows)


@mcp.tool
def get_po_pipeline(supplier: str | None = None) -> str:
    """Open purchase orders (inbound inventory) with lines, optionally filtered by supplier.

    Draft purchase orders are already excluded (not yet committed inventory).
    """
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
        # product_ids is typed list[int]; only ever build API paths from typed
        # ids, never a caller-supplied string, so nothing can escape the path.
        payload = await _brightpearl().get(
            "warehouse-service/product-availability/"
            + ",".join(str(int(i)) for i in product_ids[:100]),
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
    bigquery.SchemaField("user_email", "STRING"),
]


def ensure_audit_table() -> None:
    """Create mcp_audit if missing, but check first: the server runs as a
    read-only SA (table-level write on mcp_audit only), so an unconditional
    create_table would 403 on the dataset even with exists_ok=True.
    Existing tables get any newly added AUDIT_SCHEMA columns appended
    (schema update, not DML, so table-level WRITER is enough)."""
    ref = f"{PROJECT}.{DATASET}.mcp_audit"
    try:
        table = _bq.get_table(ref)
    except Exception:
        _bq.create_table(bigquery.Table(ref, schema=AUDIT_SCHEMA))
        return
    have = {f.name for f in table.schema}
    missing = [f for f in AUDIT_SCHEMA if f.name not in have]
    if missing:
        table.schema = list(table.schema) + missing
        _bq.update_table(table, ["schema"])


def main() -> None:
    if os.environ.get("K_SERVICE"):
        import google.cloud.logging

        google.cloud.logging.Client().setup_logging(log_level=logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)
    ensure_audit_table()
    if os.environ.get("MCP_TRANSPORT") == "http":
        import uvicorn

        # Auth is Google OAuth via the GoogleProvider attached at construction
        uvicorn.run(mcp.http_app(), host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    else:
        mcp.run()  # stdio for local dev


if __name__ == "__main__":
    main()

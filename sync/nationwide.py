"""One-time load of the legacy Nationwide Fabric sales history.

Nationwide was a separate Shopify store whose orders were NOT in Brightpearl
until the 2026-02-23 cutover (order #10438 in the source numbering). This module
loads the pre-cutover history — matched to real Brightpearl product and contact
IDs — into a standalone `nationwide_sales` table so it can be unioned with
`sales_flat` for cross-era analysis (see the `sales_unified` view).

Unlike the sync tables this is static: there is no watermark, no webhook, no
sweep. Reloading is idempotent — it truncates and rewrites the whole table, and
each line's identity is a synthesized `line_uid` (source order + per-order
sequence), so the source file needs no line id of its own.

    uv run python -m sync.cli load-nationwide "<path/to/file.csv>"
"""

import csv
import logging
from datetime import datetime, timezone
from typing import Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)

TABLE = "nationwide_sales"

# Columns the source file MUST provide. Extra columns are preserved verbatim in
# raw_payload; missing ones here are a hard error (a silently absent column
# would load a table full of NULLs).
REQUIRED_COLUMNS = {
    "source_order_id",
    "order_date",
    "order_status",
    "brightpearl_customer_id",
    "customer_email",
    "customer_company",
    "ship_state",
    "channel",
    "brightpearl_product_id",
    "sku",
    "product_name",
    "unit_of_measure",
    "quantity",
    "line_net",
}

# Spreadsheet formula errors that must never reach BigQuery as data. A single
# leaked #REF! in a NUMERIC column would fail the whole load with an opaque
# parse error, so we catch them up front with the offending row number.
_ERROR_TOKENS = {
    "#REF!", "#N/A", "#VALUE!", "#DIV/0!", "#NAME?", "#NULL!", "#NUM!", "#ERROR!",
}

SCHEMA = [
    bigquery.SchemaField("source_order_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_line_id", "STRING"),
    # source_order_id + '-' + per-order sequence; the stable per-line identity
    bigquery.SchemaField("line_uid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("order_status", "STRING"),
    bigquery.SchemaField("brightpearl_customer_id", "INT64"),
    bigquery.SchemaField("customer_email", "STRING"),
    bigquery.SchemaField("customer_company", "STRING"),
    bigquery.SchemaField("ship_state", "STRING"),
    bigquery.SchemaField("channel", "STRING"),
    # NULL on shipping / non-product lines (they carry no SKU)
    bigquery.SchemaField("brightpearl_product_id", "INT64"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("unit_of_measure", "STRING"),
    bigquery.SchemaField("quantity", "NUMERIC"),
    bigquery.SchemaField("line_net", "NUMERIC"),
    bigquery.SchemaField("raw_payload", "JSON"),
    bigquery.SchemaField("when_upserted", "TIMESTAMP", mode="REQUIRED"),
]


def _clean(v: Any) -> str:
    return (v or "").strip()


def _int_or_none(v: str, *, line: int, field: str) -> int | None:
    v = _clean(v)
    if v == "":
        return None
    try:
        return int(v)
    except ValueError:
        raise ValueError(f"row {line}: {field} is not an integer: {v!r}") from None


def _numeric(v: str, *, line: int, field: str) -> str:
    """Validate a money/quantity cell, returning the original string.

    BigQuery parses NUMERIC from strings exactly, so we pass the source text
    through rather than round-tripping through float — but we still float() it
    here purely to reject junk (formula errors, stray text) with a clear row.
    """
    v = _clean(v).replace(",", "")
    if v.upper() in _ERROR_TOKENS:
        raise ValueError(f"row {line}: {field} contains a spreadsheet error value: {v!r}")
    if v == "":
        raise ValueError(f"row {line}: {field} is required but empty")
    try:
        float(v)
    except ValueError:
        raise ValueError(f"row {line}: {field} is not a number: {v!r}") from None
    return v


def _read_rows(path: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    now = datetime.now(timezone.utc).isoformat()
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        header = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - header
        if missing:
            raise ValueError(f"source file is missing required columns: {sorted(missing)}")

        rows: list[dict[str, Any]] = []
        seq: dict[str, int] = {}
        total = 0.0
        for i, src in enumerate(reader, start=2):  # line 1 is the header
            order = _clean(src["source_order_id"])
            if order == "":
                raise ValueError(f"row {i}: source_order_id is required but empty")
            date = _clean(src["order_date"])
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"row {i}: order_date must be YYYY-MM-DD, got {date!r}") from None

            n = seq[order] = seq.get(order, 0) + 1
            line_net = _numeric(src["line_net"], line=i, field="line_net")
            total += float(line_net)
            rows.append({
                "source_order_id": order,
                "source_line_id": _clean(src.get("source_line_id")) or f"{order}-{n}",
                "line_uid": f"{order}-{n}",
                "order_date": date,
                "order_status": _clean(src["order_status"]) or None,
                "brightpearl_customer_id": _int_or_none(
                    src["brightpearl_customer_id"], line=i, field="brightpearl_customer_id"),
                "customer_email": _clean(src["customer_email"]) or None,
                "customer_company": _clean(src["customer_company"]) or None,
                "ship_state": _clean(src["ship_state"]) or None,
                "channel": _clean(src["channel"]) or None,
                "brightpearl_product_id": _int_or_none(
                    src["brightpearl_product_id"], line=i, field="brightpearl_product_id"),
                "sku": _clean(src["sku"]) or None,
                "product_name": _clean(src["product_name"]) or None,
                "unit_of_measure": _clean(src["unit_of_measure"]) or None,
                "quantity": _numeric(src["quantity"], line=i, field="quantity"),
                "line_net": line_net,
                "raw_payload": {k: v for k, v in src.items() if v not in (None, "")},
                "when_upserted": now,
            })

    stats = {
        "rows": len(rows),
        "orders": len(seq),
        "total_line_net": round(total, 2),
        "min_date": min(r["order_date"] for r in rows) if rows else None,
        "max_date": max(r["order_date"] for r in rows) if rows else None,
        "product_lines": sum(1 for r in rows if r["brightpearl_product_id"] is not None),
        "shipping_lines": sum(1 for r in rows if r["brightpearl_product_id"] is None),
    }
    return rows, stats


def load_nationwide(bq, path: str) -> dict[str, Any]:
    """Truncate-and-load the Nationwide history into `nationwide_sales`.

    Partitioned by month on order_date and clustered on brightpearl_product_id:
    sales analysis filters by date range and joins to products by id, so those
    are the columns worth pruning scans on.
    """
    rows, stats = _read_rows(path)
    ref = bq._table_ref(TABLE)
    # Recreate so partitioning/clustering are applied even if an older,
    # unpartitioned version of the table already exists.
    bq.client.delete_table(ref, not_found_ok=True)
    table = bigquery.Table(ref, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH, field="order_date"
    )
    table.clustering_fields = ["brightpearl_product_id"]
    bq.client.create_table(table)
    bq.client.load_table_from_json(
        rows,
        ref,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA, write_disposition="WRITE_TRUNCATE"
        ),
    ).result()
    logger.info("loaded %d rows into %s", len(rows), TABLE)
    return stats

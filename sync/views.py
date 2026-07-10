"""Semantic view layer: denormalized BigQuery views optimized for LLM querying.

Most MCP questions should be answerable with a single-table scan of one of
these views instead of multi-join SQL against the base tables. Money columns
are cast to NUMERIC here so downstream SQL never worries about string money.

Custom fields (PCF_*) are extracted from raw_payload JSON — see GAMEPLAN's
storage-format decision.
"""

# Sales orders in "Pending - ..." statuses aren't really sales yet — quotes/
# estimates, drafts, unprocessed Amazon orders, sample-book batches — and
# Cancelled orders never happened. None of them may count as revenue, so every
# sales-facing view filters on this predicate (business rule, decided 2026-07).
# Belt and braces: the name match auto-catches newly created pending statuses,
# the pinned ids survive renames. Assumes the orders table is aliased `o`.
REPORTABLE_SO = (
    "NOT IFNULL("
    "STARTS_WITH(LOWER(o.order_status_name), 'pending') "
    "OR LOWER(o.order_status_name) = 'cancelled' "
    "OR o.order_status_id IN (1, 5, 38, 47, 57, 77, 79)"  # 5 = Cancelled
    ", FALSE)"
)

# Same rule for purchase orders: drafts aren't committed inbound inventory,
# so they stay out of po_pipeline (and any PO reporting) until placed.
REPORTABLE_PO = (
    "NOT IFNULL("
    "STARTS_WITH(LOWER(o.order_status_name), 'draft') "
    "OR o.order_status_id IN (6)"  # 6 = Draft Purchase Order
    ", FALSE)"
)

VIEWS: dict[str, str] = {
    # One row per sales-order line, with order context attached
    "sales_flat": """
        SELECT
          o.order_id,
          o.reference,
          o.placed_on,
          o.created_on,
          o.order_status_name,
          o.order_payment_status,
          o.shipping_status_code,
          o.customer_contact_id,
          o.customer_company_name,
          o.customer_email,
          o.delivery_state,
          o.delivery_country_iso,
          o.channel_id,
          ch.name AS channel_name,
          o.currency_code,
          o.total AS order_total,
          r.order_row_id,
          r.product_id,
          r.product_sku,
          r.product_name,
          r.quantity,
          r.product_price AS unit_price,
          r.item_cost AS unit_cost,
          r.row_net,
          r.row_tax,
          r.nominal_code,
          SAFE_MULTIPLY(r.quantity, r.item_cost) AS row_cost,
          SAFE_SUBTRACT(r.row_net, SAFE_MULTIPLY(r.quantity, r.item_cost)) AS row_margin
        FROM `{ds}.orders` o
        JOIN `{ds}.order_rows` r USING (order_id)
        LEFT JOIN `{ds}.channels` ch ON SAFE_CAST(ch.id AS INT64) = o.channel_id
        WHERE o.order_type_code = 'SO' AND NOT IFNULL(o.is_deleted, FALSE)
          AND {reportable_so}
    """,
    # One row per product per warehouse with stock position + key custom fields
    "inventory_position": """
        SELECT
          p.product_id,
          p.sku,
          p.name,
          p.status,
          b.brand_name,
          JSON_VALUE(p.raw_payload, '$.customFields.PCF_COLLECTI') AS collection,
          JSON_VALUE(p.raw_payload, '$.customFields.PCF_UOM') AS unit_of_measure,
          JSON_VALUE(p.raw_payload, '$.customFields.PCF_SIZE') AS size,
          JSON_VALUE(p.raw_payload, '$.customFields.PCF_PATTERN') AS pattern,
          a.warehouse_id,
          w.name AS warehouse_name,
          a.on_hand,
          a.allocated,
          a.in_stock,
          a.on_order,
          a.when_upserted AS stock_as_of
        FROM `{ds}.products` p
        LEFT JOIN `{ds}.product_availability` a USING (product_id)
        LEFT JOIN `{ds}.warehouses` w ON w.id = a.warehouse_id
        LEFT JOIN `{ds}.brands` b ON b.brand_id = p.brand_id
        WHERE p.status = 'LIVE' AND NOT IFNULL(p.is_deleted, FALSE)
    """,
    # One row per purchase-order line still open (inbound pipeline)
    "po_pipeline": """
        SELECT
          o.order_id,
          o.reference,
          o.placed_on,
          o.created_on,
          o.order_status_name,
          o.stock_status_code,
          JSON_VALUE(o.raw_payload, '$.parties.supplier.companyName') AS supplier_name,
          JSON_VALUE(o.raw_payload, '$.parties.supplier.contactId') AS supplier_contact_id,
          o.warehouse_id,
          w.name AS warehouse_name,
          o.currency_code,
          o.total AS order_total,
          r.order_row_id,
          r.product_id,
          r.product_sku,
          r.product_name,
          r.quantity,
          r.item_cost AS unit_cost,
          r.row_net
        FROM `{ds}.orders` o
        JOIN `{ds}.order_rows` r USING (order_id)
        LEFT JOIN `{ds}.warehouses` w ON w.id = o.warehouse_id
        WHERE o.order_type_code = 'PO' AND NOT IFNULL(o.is_deleted, FALSE)
          AND {reportable_po}
    """,
    # One row per customer with lifetime stats
    "customer_summary": """
        SELECT
          c.contact_id,
          c.first_name,
          c.last_name,
          c.email,
          c.organisation_name,
          c.company_id,
          c.created_on AS customer_since,
          COUNT(DISTINCT o.order_id) AS order_count,
          SUM(o.total) AS lifetime_value,
          AVG(o.total) AS avg_order_value,
          MIN(o.placed_on) AS first_order_on,
          MAX(o.placed_on) AS last_order_on,
          TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(o.placed_on), DAY) AS days_since_last_order
        FROM `{ds}.contacts` c
        LEFT JOIN `{ds}.orders` o
          ON o.customer_contact_id = c.contact_id AND o.order_type_code = 'SO'
          AND NOT IFNULL(o.is_deleted, FALSE) AND {reportable_so}
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    """,
    # Monthly P&L-style rollup from journal lines (debits/credits are strings)
    "monthly_financials": """
        SELECT
          DATE_TRUNC(DATE(j.journal_date), MONTH) AS month,
          j.nominal_code,
          n.name AS nominal_name,
          n.type AS nominal_type,
          SUM(SAFE_CAST(j.journal_debit AS NUMERIC)) AS total_debit,
          SUM(SAFE_CAST(j.journal_credit AS NUMERIC)) AS total_credit,
          SUM(SAFE_CAST(j.journal_credit AS NUMERIC)) - SUM(SAFE_CAST(j.journal_debit AS NUMERIC)) AS net_credit,
          COUNT(*) AS line_count
        FROM `{ds}.journal_rows` j
        LEFT JOIN `{ds}.nominal_codes` n ON n.code = j.nominal_code
        GROUP BY 1, 2, 3, 4
    """,
    # One row per product × option × value. The variant assignments already
    # live in each product's raw_payload.variations (synced on every product
    # update), and each entry carries its own optionValue name — so this view
    # is self-contained and needs no join to the product_option_values
    # catalog. Lets "which products are Graphite" / "all Color values in use"
    # be a single-table scan. status is kept as a column (not filtered) so
    # ARCHIVED products stay queryable; only deleted rows are excluded.
    "product_variations": """
        SELECT
          p.product_id,
          p.sku,
          p.name AS product_name,
          p.status,
          CAST(JSON_VALUE(v, '$.optionId') AS INT64) AS option_id,
          JSON_VALUE(v, '$.optionName') AS option_name,
          CAST(JSON_VALUE(v, '$.optionValueId') AS INT64) AS option_value_id,
          JSON_VALUE(v, '$.optionValue') AS option_value
        FROM `{ds}.products` p,
        UNNEST(JSON_QUERY_ARRAY(p.raw_payload, '$.variations')) AS v
        WHERE NOT IFNULL(p.is_deleted, FALSE)
    """,
}


def create_views(bq) -> list[str]:
    """CREATE OR REPLACE all semantic views. Returns view names created."""
    ds = f"{bq.project}.{bq.dataset}"
    created = []
    for name, sql in VIEWS.items():
        body = sql.format(ds=ds, reportable_so=REPORTABLE_SO, reportable_po=REPORTABLE_PO)
        bq.client.query_and_wait(f"CREATE OR REPLACE VIEW `{ds}.{name}` AS {body}")
        created.append(name)
    return created

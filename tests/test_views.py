"""Guardrails for the semantic view layer (no BigQuery involved)."""

from sync.views import REPORTABLE_PO, REPORTABLE_SO, VIEWS


def test_all_views_format_cleanly():
    # A typo'd placeholder in any view would raise KeyError/IndexError here
    # long before create_views hits BigQuery.
    for name, sql in VIEWS.items():
        body = sql.format(ds="p.d", reportable_so=REPORTABLE_SO, reportable_po=REPORTABLE_PO)
        assert "{" not in body, f"unresolved placeholder in view {name}"


def test_sales_views_exclude_non_reportable_orders():
    # Pending/cancelled sales orders must never reach revenue or customer
    # stats. If someone edits a view and drops the predicate, this fails.
    for name in ("sales_flat", "customer_summary"):
        assert "{reportable_so}" in VIEWS[name], f"{name} lost the pending/cancelled filter"


def test_views_exclude_soft_deleted_rows():
    # orders/products/contacts are soft-deleted (is_deleted flipped by the
    # destroyed webhook or the cold-tier reconcile sweep), so a view that reads
    # one without guarding the flag keeps serving records deleted in Brightpearl.
    # customer_summary shipped without the contacts guard; this catches a repeat.
    soft_delete_tables = {"orders": "o", "products": "p", "contacts": "c"}
    for name, sql in VIEWS.items():
        for table, alias in soft_delete_tables.items():
            if f"{{ds}}.{table}` {alias}" not in sql:
                continue
            assert f"NOT IFNULL({alias}.is_deleted, FALSE)" in sql, (
                f"view {name} reads {table} without filtering soft-deleted rows"
            )


def test_po_pipeline_excludes_draft_orders():
    assert "{reportable_po}" in VIEWS["po_pipeline"], "po_pipeline lost the draft filter"
    assert "'draft'" in REPORTABLE_PO
    assert "6" in REPORTABLE_PO  # pinned id: Draft Purchase Order


def test_reportable_predicate_covers_known_statuses():
    assert "'pending'" in REPORTABLE_SO
    assert "'cancelled'" in REPORTABLE_SO
    # Pinned ids: the pending statuses as of 2026-07 plus Cancelled (5).
    for status_id in (1, 5, 38, 47, 57, 77, 79):
        assert f"{status_id}" in REPORTABLE_SO

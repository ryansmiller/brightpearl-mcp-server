"""Unit tests for BigQueryWriter.upsert path selection.

The write path must pick a literal-keyed DELETE+INSERT (which BigQuery can
prune on a clustered target) for small integer-keyed batches, and fall back to
a staging-join MERGE for string keys or oversized batches. See the 2026-07-13
billing investigation: the staging-join MERGE full-scans the target.
"""

from google.cloud import bigquery

from sync import bq as bq_mod
from sync.bq import BigQueryWriter


def _writer(monkeypatch):
    """A BigQueryWriter that records the SQL it would run, no real client."""
    w = BigQueryWriter.__new__(BigQueryWriter)
    w.project, w.dataset = "p", "d"
    captured = {}
    monkeypatch.setattr(w, "_load_staging", lambda name, rows, schema: f"p.d._stg_{name}")
    monkeypatch.setattr(w, "_drop", lambda ref: None)
    monkeypatch.setattr(w, "_dml", lambda sql: captured.setdefault("sql", sql))
    return w, captured


_SCHEMA = [
    bigquery.SchemaField("thing_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("when_upserted", "TIMESTAMP", mode="REQUIRED"),
]


def test_small_int_batch_uses_literal_delete_insert(monkeypatch):
    w, captured = _writer(monkeypatch)
    w.upsert("thing", [{"thing_id": 7, "name": "a", "when_upserted": "t"}], _SCHEMA, "thing_id")
    sql = captured["sql"]
    assert "DELETE FROM `p.d.thing` WHERE thing_id IN (7)" in sql
    assert "INSERT INTO `p.d.thing`" in sql
    assert "MERGE" not in sql


def test_literal_path_inlines_all_distinct_keys(monkeypatch):
    w, captured = _writer(monkeypatch)
    rows = [
        {"thing_id": 7, "name": "a", "when_upserted": "t2"},
        {"thing_id": 7, "name": "a-older", "when_upserted": "t1"},  # dup key
        {"thing_id": 9, "name": "b", "when_upserted": "t1"},
    ]
    w.upsert("thing", rows, _SCHEMA, "thing_id")
    # both distinct keys inlined; staging dedup still applied via ROW_NUMBER
    assert "WHERE thing_id IN (7, 9)" in captured["sql"]
    assert "ROW_NUMBER() OVER (PARTITION BY thing_id ORDER BY when_upserted DESC)" in captured["sql"]


def test_string_key_falls_back_to_merge(monkeypatch):
    w, captured = _writer(monkeypatch)
    schema = [
        bigquery.SchemaField("resource", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_run_at", "TIMESTAMP"),
        bigquery.SchemaField("when_upserted", "TIMESTAMP", mode="REQUIRED"),
    ]
    w.upsert("thing", [{"resource": "orders", "last_run_at": "t", "when_upserted": "t"}],
             schema, "resource")
    assert "MERGE `p.d.thing`" in captured["sql"]
    assert "DELETE FROM" not in captured["sql"]


def test_oversized_batch_falls_back_to_merge(monkeypatch):
    w, captured = _writer(monkeypatch)
    rows = [{"thing_id": i, "name": "x", "when_upserted": "t"}
            for i in range(bq_mod._LITERAL_KEY_MAX + 1)]
    w.upsert("thing", rows, _SCHEMA, "thing_id")
    assert "MERGE `p.d.thing`" in captured["sql"]
    assert "DELETE FROM" not in captured["sql"]


def test_sync_state_orders_dedup_by_last_run_at(monkeypatch):
    # sync_state has a string key (resource) so it MERGEs, and its freshest-row
    # tiebreak must be last_run_at, not when_upserted (which it lacks a use for).
    w, captured = _writer(monkeypatch)
    schema = [
        bigquery.SchemaField("resource", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_run_at", "TIMESTAMP"),
    ]
    w.upsert("sync_state", [{"resource": "orders", "last_run_at": "t"}], schema, "resource")
    assert "ORDER BY last_run_at DESC" in captured["sql"]

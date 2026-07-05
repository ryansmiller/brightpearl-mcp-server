"""The run_bigquery_sql guard is the only thing between a (possibly prompt-
injected) query and BigQuery, so its rejection paths get direct coverage.

The server module builds a bigquery.Client and reads GCP_PROJECT_ID at import,
so both are stubbed before import.
"""

import sys
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "test-proj")
    monkeypatch.setenv("BQ_DATASET", "brightpearl")
    monkeypatch.setitem(sys.modules, "mcp_server.server", None)
    sys.modules.pop("mcp_server.server", None)
    import mcp_server.server as srv

    # Neutralize every path that would touch BigQuery. _query returning a
    # sentinel lets us assert whether a query was ever allowed to run.
    monkeypatch.setattr(srv, "_bq", MagicMock())
    monkeypatch.setattr(srv, "_audit", lambda *a, **k: None)
    ran = {}

    def fake_query(sql, params=None):
        ran["sql"] = sql
        return []

    monkeypatch.setattr(srv, "_query", fake_query)
    monkeypatch.setattr(srv, "_assert_only_brightpearl", lambda sql: None)
    return srv, ran


MALICIOUS = [
    "SELECT 1; DELETE FROM brightpearl.orders",             # multi-statement
    "SELECT 1;\nEXECUTE IMMEDIATE 'DELETE FROM x'",         # scripting after ;
    "EXECUTE IMMEDIATE 'DEL' || 'ETE FROM brightpearl.orders'",  # dynamic SQL
    "BEGIN DELETE FROM brightpearl.orders; END",           # script block
    "DECLARE x INT64; SELECT 1",                           # scripting keyword
    "DELETE FROM brightpearl.orders",                      # not a SELECT
    "DROP TABLE brightpearl.orders",                       # DDL
]


@pytest.mark.parametrize("sql", MALICIOUS)
def test_guard_rejects_and_never_runs(server, sql):
    srv, ran = server
    result = srv.run_bigquery_sql(sql)
    assert result.startswith("Error:"), result
    assert "sql" not in ran, f"guard let a query through: {sql}"


def test_guard_allows_plain_select(server):
    srv, ran = server
    srv.run_bigquery_sql("SELECT sku FROM brightpearl.inventory_position LIMIT 5")
    assert "sql" in ran


def test_guard_allows_trailing_semicolon(server):
    srv, ran = server
    srv.run_bigquery_sql("SELECT 1;")
    assert "sql" in ran

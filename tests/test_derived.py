"""DerivedSyncer.sync_suppliers: snapshot safety and sync_state recording."""

import pytest

from brightpearl_client import BrightpearlError
from sync.derived import DerivedSyncer


class FakeBP:
    """Maps idset path → payload; a BrightpearlError value is raised instead."""

    def __init__(self, responses: dict[str, object]):
        self.responses = responses
        self.calls: list[str] = []

    async def get(self, path: str):
        self.calls.append(path)
        ids = path.split("/product/")[1].split("/")[0]
        result = self.responses[ids]
        if isinstance(result, BrightpearlError):
            raise result
        return result


class FakeBQ:
    def __init__(self, product_rows: list[dict]):
        self.product_rows = product_rows
        self.truncates: list[tuple[str, list[dict]]] = []
        self.upserts: list[tuple[str, list[dict]]] = []

    def _table_ref(self, name):
        return f"p.d.{name}"

    def query(self, sql):
        return self.product_rows

    def truncate_load(self, name, rows, schema, cluster_fields=None):
        self.truncates.append((name, list(rows)))
        return len(rows)

    def upsert(self, name, rows, schema=None, key=None):
        self.upserts.append((name, list(rows)))
        return len(rows)


def _products(*pairs: tuple[int, int | None]) -> list[dict]:
    return [{"product_id": pid, "primary": primary} for pid, primary in pairs]


async def test_suppliers_loads_rows_and_records_sync_state():
    bq = FakeBQ(_products((1, 10), (2, None)))
    bp = FakeBP({"1,2": {"1": [10, 20], "2": [30]}})
    syncer = DerivedSyncer(bp, bq)

    n = await syncer.sync_suppliers()

    assert n == 3
    table, rows = bq.truncates[0]
    assert table == "product_suppliers"
    flags = {(r["product_id"], r["supplier_contact_id"]): r["is_primary"] for r in rows}
    assert flags == {(1, 10): True, (1, 20): False, (2, 30): False}
    state_table, state_rows = bq.upserts[0]
    assert state_table == "sync_state"
    assert state_rows[0]["resource"] == "product_suppliers"
    assert state_rows[0]["last_run_kind"] == "derived"
    assert state_rows[0]["last_run_rows"] == 3


async def test_suppliers_transient_error_aborts_before_truncate():
    bq = FakeBQ(_products((1, None), (2, None)))
    bp = FakeBP({"1,2": BrightpearlError("upstream down", 503)})
    syncer = DerivedSyncer(bp, bq)

    with pytest.raises(BrightpearlError):
        await syncer.sync_suppliers()
    assert bq.truncates == []  # old table survives


async def test_suppliers_splits_400_and_skips_bad_id():
    bq = FakeBQ(_products((1, None), (2, None)))
    bad = BrightpearlError("invalid id", 400)
    bp = FakeBP({"1,2": bad, "1": {"1": [10]}, "2": bad})
    syncer = DerivedSyncer(bp, bq)

    n = await syncer.sync_suppliers()

    assert n == 1
    assert bq.truncates[0][1][0]["product_id"] == 1


async def test_suppliers_valid_empty_snapshot_truncates():
    # every fetch succeeded and nothing has suppliers: clearing is correct
    bq = FakeBQ(_products((1, None)))
    bp = FakeBP({"1": {}})
    syncer = DerivedSyncer(bp, bq)

    n = await syncer.sync_suppliers()

    assert n == 0
    assert bq.truncates == [("product_suppliers", [])]


async def test_suppliers_malformed_payload_aborts_before_truncate():
    bq = FakeBQ(_products((1, None)))
    bp = FakeBP({"1": ["not", "a", "dict"]})
    syncer = DerivedSyncer(bp, bq)

    with pytest.raises(BrightpearlError, match="unexpected supplier payload"):
        await syncer.sync_suppliers()
    assert bq.truncates == []

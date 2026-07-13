"""SyncStateStore: cached reads, buffered writes, one batched flush.

The store exists to kill ~2,700 bookkeeping queries/day (each paying
BigQuery's per-query billing minimum): watermarks are read from memory and
sync_state writes buffer until one upsert per tick/CLI run.
"""

from datetime import datetime, timezone

from sync.state import SyncStateStore


class FakeBQ:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.queries: list[str] = []
        self.upserts: list[tuple[str, list[dict]]] = []

    def _table_ref(self, name):
        return f"p.d.{name}"

    def query(self, sql):
        self.queries.append(sql)
        return self.rows

    def upsert(self, name, rows, schema=None, key=None):
        self.upserts.append((name, list(rows)))
        return len(rows)


def test_get_loads_once_and_caches():
    bq = FakeBQ([{"resource": "orders", "watermark_id": None,
                  "watermark_updated_on": datetime(2026, 7, 1, tzinfo=timezone.utc)}])
    store = SyncStateStore(bq)

    assert store.get("orders")["watermark_updated_on"].year == 2026
    assert store.get("orders") is not None
    assert store.get("never_synced") == {}
    assert len(bq.queries) == 1  # one load serves every read


def test_record_visible_to_get_before_flush():
    store = SyncStateStore(FakeBQ())
    store.record("orders", watermark_id=42, last_run_kind="sweep")
    assert store.get("orders")["watermark_id"] == 42


def test_flush_batches_all_pending_into_one_upsert():
    bq = FakeBQ()
    store = SyncStateStore(bq)
    store.record("orders", last_run_rows=1)
    store.record("contacts", last_run_rows=2)

    n = store.flush()

    assert n == 2
    assert len(bq.upserts) == 1  # ONE write for the whole batch
    name, rows = bq.upserts[0]
    assert name == "sync_state"
    assert {r["resource"] for r in rows} == {"orders", "contacts"}
    # flushed and cleared: a second flush writes nothing
    assert store.flush() == 0
    assert len(bq.upserts) == 1


def test_last_write_wins_per_resource():
    bq = FakeBQ()
    store = SyncStateStore(bq)
    store.record("orders", last_run_rows=1)
    store.record("orders", last_run_rows=99)

    store.flush()

    (_, rows) = bq.upserts[0]
    assert rows == [{"resource": "orders", "last_run_rows": 99}]


def test_flush_serializes_datetimes():
    bq = FakeBQ()
    store = SyncStateStore(bq)
    ts = datetime(2026, 7, 13, 12, 0, tzinfo=timezone.utc)
    store.record("orders", watermark_updated_on=ts, last_run_at=ts)

    store.flush()

    (_, rows) = bq.upserts[0]
    assert rows[0]["watermark_updated_on"] == "2026-07-13T12:00:00+00:00"
    # but the in-memory cache keeps the datetime for arithmetic
    assert store.get("orders")["watermark_updated_on"] is ts

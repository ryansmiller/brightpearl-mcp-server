"""In-memory sync_state cache with batched writes.

sync_state is a ~40-row bookkeeping table, but reading and writing it
per-resource was costing real money: every sweep of every resource paid
BigQuery's ~10 MB per-query billing minimum twice (a watermark SELECT before,
a MERGE after) — ~2,700 queries/day of pure bookkeeping (2026-07-13 billing
investigation). The sync service runs with max-instances=1 (required anyway so
the Brightpearl rate budget never splits), so this process is the only writer:
an in-memory copy loaded once per process is authoritative, and writes can
batch into one upsert per tick instead of one per resource.

Consistency notes:
- record() updates the cache immediately, so a get() later in the same process
  always sees the newest watermark, flushed or not.
- Pending records are bookkeeping, not data — the actual rows are already in
  BigQuery when record() is called. A crash before flush means the next sweep
  re-reads a slightly older watermark (idempotent upserts absorb the overlap)
  and staleness alerting sees a last_run_at up to one flush interval old
  (ticks flush every ~5 min; alert budgets start at 30 min).
- A CLI run against the same dataset is a separate process with its own cache;
  it can't see this process's unflushed records and vice versa. Worst case is
  the same harmless re-sweep overlap.
"""

from datetime import datetime
from typing import Any


class SyncStateStore:
    def __init__(self, bq):
        self.bq = bq
        self._cache: dict[str, dict[str, Any]] | None = None
        self._pending: dict[str, dict[str, Any]] = {}

    def _load(self) -> dict[str, dict[str, Any]]:
        if self._cache is None:
            rows = self.bq.query(f"SELECT * FROM `{self.bq._table_ref('sync_state')}`")
            self._cache = {r["resource"]: dict(r) for r in rows}
        return self._cache

    def get(self, resource: str) -> dict[str, Any]:
        """Current state row for a resource ({} if never synced)."""
        return dict(self._load().get(resource, {}))

    def record(self, resource: str, **fields: Any) -> None:
        """Update a resource's state in memory; persisted on the next flush().

        Last write wins per resource — a resource recorded twice between
        flushes lands once, with the newest values (same end state the old
        per-record MERGEs produced, minus the per-record cost).
        """
        row = {"resource": resource, **fields}
        cache = self._load()
        cache[resource] = {**cache.get(resource, {}), **row}
        self._pending[resource] = row

    def flush(self) -> int:
        """Write all pending records as one sync_state upsert."""
        if not self._pending:
            return 0
        rows = [
            {k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()}
            for row in self._pending.values()
        ]
        self._pending.clear()
        self.bq.upsert("sync_state", rows)
        return len(rows)

"""Backfill and incremental sweeps: Brightpearl → BigQuery.

Watermarks live in the sync_state table. Incremental sweeps filter searches on
updatedOn from (watermark - overlap) so nothing is missed at boundaries;
upserts are idempotent so the overlap is harmless.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from brightpearl_client import BrightpearlClient

from .bq import BigQueryWriter
from .transforms import contact_to_row, order_to_rows, product_to_row

logger = logging.getLogger(__name__)

SWEEP_OVERLAP = timedelta(minutes=10)
FETCH_BATCH = 500  # searched IDs are fetched+loaded in batches of this size

RESOURCES: dict[str, dict[str, Any]] = {
    "orders": {
        "search": ("order-service", "order"),
        "id_column": "orderId",
        "tier": "hot",
    },
    "products": {
        "search": ("product-service", "product"),
        "id_column": "productId",
        "tier": "cold",
    },
    "contacts": {
        "search": ("contact-service", "contact"),
        "id_column": "contactId",
        "tier": "warm",
    },
}


def _fmt(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class SyncPipeline:
    def __init__(self, bp: BrightpearlClient, bq: BigQueryWriter):
        self.bp = bp
        self.bq = bq

    def get_watermark(self, resource: str) -> datetime | None:
        rows = self.bq.query(
            f"SELECT watermark_updated_on FROM `{self.bq._table_ref('sync_state')}` "
            f"WHERE resource = '{resource}'"
        )
        return rows[0]["watermark_updated_on"] if rows else None

    def _record_run(
        self, resource: str, kind: str, rows: int, watermark: datetime | None
    ) -> None:
        now = datetime.now(timezone.utc)
        self.bq.upsert(
            "sync_state",
            [
                {
                    "resource": resource,
                    "watermark_updated_on": watermark.isoformat() if watermark else None,
                    "last_run_at": now.isoformat(),
                    "last_run_kind": kind,
                    "last_run_rows": rows,
                }
            ],
        )

    async def _collect_ids(
        self, resource: str, updated_since: datetime | None, limit: int | None
    ) -> list[int]:
        spec = RESOURCES[resource]
        service, res = spec["search"]
        filters: dict[str, Any] = {}
        if updated_since:
            filters["updatedOn"] = f"{_fmt(updated_since)}/"
        ids: list[int] = []
        async for row in self.bp.search_all(service, res, filters=filters):
            ids.append(row[spec["id_column"]])
            if limit and len(ids) >= limit:
                break
        # De-dup while preserving order (an ID can reappear across pages)
        return list(dict.fromkeys(ids))

    async def _load_orders(self, ids: list[int]) -> tuple[int, datetime | None]:
        total = 0
        max_updated: datetime | None = None
        for i in range(0, len(ids), FETCH_BATCH):
            batch = ids[i : i + FETCH_BATCH]
            payloads = await self.bp.orders.get(batch)
            order_rows, line_rows, parent_ids = [], [], []
            for payload in payloads:
                head, lines = order_to_rows(payload)
                order_rows.append(head)
                line_rows.extend(lines)
                parent_ids.append(head["order_id"])
                if payload.get("updatedOn"):
                    ts = datetime.fromisoformat(payload["updatedOn"])
                    if max_updated is None or ts > max_updated:
                        max_updated = ts
            self.bq.upsert("orders", order_rows)
            self.bq.replace_children("order_rows", "order_id", parent_ids, line_rows)
            total += len(order_rows)
            logger.info("orders: %d/%d loaded", total, len(ids))
        return total, max_updated

    async def _load_simple(
        self, resource: str, ids: list[int]
    ) -> tuple[int, datetime | None]:
        transform = {"products": product_to_row, "contacts": contact_to_row}[resource]
        api = getattr(self.bp, resource)
        total = 0
        max_updated: datetime | None = None
        for i in range(0, len(ids), FETCH_BATCH):
            batch = ids[i : i + FETCH_BATCH]
            payloads = await api.get(batch)
            rows = []
            for payload in payloads:
                rows.append(transform(payload))
                if payload.get("updatedOn"):
                    ts = datetime.fromisoformat(payload["updatedOn"])
                    if max_updated is None or ts > max_updated:
                        max_updated = ts
            self.bq.upsert(resource, rows)
            total += len(rows)
            logger.info("%s: %d/%d loaded", resource, total, len(ids))
        return total, max_updated

    async def sync(
        self,
        resource: str,
        *,
        incremental: bool = True,
        limit: int | None = None,
    ) -> int:
        """Run one sync pass. incremental=False means full backfill."""
        updated_since = None
        if incremental:
            watermark = self.get_watermark(resource)
            if watermark:
                updated_since = watermark - SWEEP_OVERLAP
        kind = "sweep" if updated_since else "backfill"

        ids = await self._collect_ids(resource, updated_since, limit)
        logger.info("%s %s: %d changed resources", resource, kind, len(ids))
        if not ids:
            self._record_run(resource, kind, 0, self.get_watermark(resource))
            return 0

        if resource == "orders":
            total, max_updated = await self._load_orders(ids)
        else:
            total, max_updated = await self._load_simple(resource, ids)

        watermark = max_updated or self.get_watermark(resource)
        self._record_run(resource, kind, total, watermark)
        return total

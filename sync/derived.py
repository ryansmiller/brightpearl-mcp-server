"""Derived loaders that fan out from the products table:

- product_prices: GET product-price/{idset} → one row per product × price list
- product_availability: GET product-availability/{idset} → one row per
  product × warehouse (stock-tracked products only)

Both truncate-and-reload: the source data is a point-in-time snapshot.
"""

import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from brightpearl_client import BrightpearlClient, BrightpearlError

from .bq import BigQueryWriter

logger = logging.getLogger(__name__)

CHUNK = 100

PRICE_SCHEMA = [
    bigquery.SchemaField("product_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("price_list_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("price", "NUMERIC"),
    bigquery.SchemaField("quantity_prices", "JSON"),
    bigquery.SchemaField("when_upserted", "TIMESTAMP", mode="REQUIRED"),
]

SUPPLIER_SCHEMA = [
    bigquery.SchemaField("product_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("supplier_contact_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("is_primary", "BOOL"),
    bigquery.SchemaField("when_upserted", "TIMESTAMP", mode="REQUIRED"),
]

AVAILABILITY_SCHEMA = [
    bigquery.SchemaField("product_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("warehouse_id", "INT64"),
    bigquery.SchemaField("on_hand", "NUMERIC"),
    bigquery.SchemaField("allocated", "NUMERIC"),
    bigquery.SchemaField("in_stock", "NUMERIC"),
    bigquery.SchemaField("on_order", "NUMERIC"),
    bigquery.SchemaField("when_upserted", "TIMESTAMP", mode="REQUIRED"),
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class DerivedSyncer:
    def __init__(self, bp: BrightpearlClient, bq: BigQueryWriter):
        self.bp = bp
        self.bq = bq

    def _record(self, table: str, rows: int) -> None:
        """Mark the run in sync_state so freshness monitoring covers derived tables."""
        self.bq.upsert(
            "sync_state",
            [{
                "resource": table,
                "watermark_id": None,
                "watermark_updated_on": None,
                "last_run_at": _now(),
                "last_run_kind": "derived",
                "last_run_rows": rows,
            }],
        )

    def _product_ids(self, where: str = "TRUE") -> list[int]:
        rows = self.bq.query(
            f"SELECT product_id FROM `{self.bq._table_ref('products')}` "
            f"WHERE {where} ORDER BY product_id"
        )
        return [r["product_id"] for r in rows]

    async def sync_prices(self) -> int:
        ids = self._product_ids()
        rows = []
        now = _now()
        for i in range(0, len(ids), CHUNK):
            chunk = ids[i : i + CHUNK]
            id_set = ",".join(map(str, chunk))
            payload = await self.bp.get(f"product-service/product-price/{id_set}")
            for product in payload if isinstance(payload, list) else [payload]:
                for pl in product.get("priceLists", []):
                    qty = pl.get("quantityPrice") or {}
                    base = qty.get("1")
                    rows.append(
                        {
                            "product_id": product["productId"],
                            "price_list_id": pl["priceListId"],
                            "price": float(base) if base not in (None, "") else None,
                            "quantity_prices": qty or None,
                            "when_upserted": now,
                        }
                    )
            if i % 5000 == 0:
                logger.info("product_prices: fetched %d/%d products", i + len(chunk), len(ids))
        n = self.bq.truncate_load("product_prices", rows, PRICE_SCHEMA)
        self._record("product_prices", n)
        logger.info("product_prices: %d rows", n)
        return n

    async def sync_suppliers(self) -> int:
        """product-service/product/{idset}/supplier → one row per product × supplier.

        is_primary comes from the product payload's primarySupplierId, read
        from our own products table (no extra API calls).
        """
        primaries = {
            r["product_id"]: r["primary"]
            for r in self.bq.query(
                f"SELECT product_id, "
                f"SAFE_CAST(JSON_VALUE(raw_payload, '$.primarySupplierId') AS INT64) AS primary "
                f"FROM `{self.bq._table_ref('products')}` "
                f"WHERE NOT IFNULL(is_deleted, FALSE)"
            )
        }
        ids = sorted(primaries)
        rows = []
        now = _now()
        for i in range(0, len(ids), CHUNK):
            chunk = ids[i : i + CHUNK]
            # Only known-bad-id 400s are tolerated (split-and-skip); any other
            # error aborts BEFORE truncate_load so the existing table survives.
            payload = await self._fetch_suppliers(chunk)
            for pid, supplier_ids in payload.items():
                for sid in supplier_ids or []:
                    rows.append(
                        {
                            "product_id": int(pid),
                            "supplier_contact_id": int(sid),
                            "is_primary": primaries.get(int(pid)) == int(sid),
                            "when_upserted": now,
                        }
                    )
            if i % 10000 == 0:
                logger.info("product_suppliers: fetched %d/%d products", i + len(chunk), len(ids))
        # Reaching here means every fetch succeeded (anything unexpected
        # raised above), so an empty snapshot is the truth — truncate away.
        n = self.bq.truncate_load("product_suppliers", rows, SUPPLIER_SCHEMA)
        self._record("product_suppliers", n)
        logger.info("product_suppliers: %d rows", n)
        return n

    async def _fetch_suppliers(self, chunk: list[int]) -> dict:
        id_set = ",".join(map(str, chunk))
        try:
            payload = await self.bp.get(f"product-service/product/{id_set}/supplier")
        except BrightpearlError as e:
            if e.status_code == 400 and len(chunk) > 1:
                mid = len(chunk) // 2
                left = await self._fetch_suppliers(chunk[:mid])
                right = await self._fetch_suppliers(chunk[mid:])
                return {**left, **right}
            if e.status_code == 400:
                return {}  # isolated bad id (deleted product) — skip it
            raise
        if payload is not None and not isinstance(payload, dict):
            # A shape we don't understand must abort the run, not quietly
            # produce an empty snapshot that truncates good data
            raise BrightpearlError(
                f"unexpected supplier payload for ids {id_set}: {type(payload).__name__}"
            )
        return payload or {}

    async def _fetch_availability(self, chunk: list[int]) -> dict:
        """Fetch a chunk, splitting on 400s (invalid ids poison whole chunks)."""
        id_set = ",".join(map(str, chunk))
        try:
            payload = await self.bp.get(f"warehouse-service/product-availability/{id_set}")
            return payload if isinstance(payload, dict) else {}
        except BrightpearlError as e:
            if e.status_code == 400 and len(chunk) > 1:
                mid = len(chunk) // 2
                left = await self._fetch_availability(chunk[:mid])
                right = await self._fetch_availability(chunk[mid:])
                return {**left, **right}
            if e.status_code == 400:
                return {}
            raise

    async def refresh_availability(self, product_ids: list[int]) -> int:
        """Webhook-path partial refresh: replace availability rows for these products."""
        rows = []
        now = _now()
        for i in range(0, len(product_ids), CHUNK):
            payload = await self._fetch_availability(product_ids[i : i + CHUNK])
            for pid, avail in payload.items():
                for wid, w in (avail.get("warehouses") or {}).items():
                    rows.append(
                        {
                            "product_id": int(pid),
                            "warehouse_id": int(wid),
                            "on_hand": w.get("onHand"),
                            "allocated": w.get("allocated"),
                            "in_stock": w.get("inStock"),
                            "on_order": w.get("onOrder"),
                            "when_upserted": now,
                        }
                    )
        self.bq.ensure_table("product_availability", AVAILABILITY_SCHEMA)
        return self.bq.replace_children(
            "product_availability", "product_id", product_ids, rows,
            schema=AVAILABILITY_SCHEMA,
        )

    async def sync_availability(self) -> int:
        ids = self._product_ids("stock_tracked")
        rows = []
        now = _now()
        for i in range(0, len(ids), CHUNK):
            chunk = ids[i : i + CHUNK]
            payload = await self._fetch_availability(chunk)
            for pid, avail in payload.items():
                for wid, w in (avail.get("warehouses") or {}).items():
                    rows.append(
                        {
                            "product_id": int(pid),
                            "warehouse_id": int(wid),
                            "on_hand": w.get("onHand"),
                            "allocated": w.get("allocated"),
                            "in_stock": w.get("inStock"),
                            "on_order": w.get("onOrder"),
                            "when_upserted": now,
                        }
                    )
        n = self.bq.truncate_load("product_availability", rows, AVAILABILITY_SCHEMA)
        self._record("product_availability", n)
        logger.info("product_availability: %d rows", n)
        return n

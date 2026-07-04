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
        logger.info("product_prices: %d rows", n)
        return n

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
        logger.info("product_availability: %d rows", n)
        return n

"""Probe which Brightpearl endpoints exist on this account.

Used to ground sync/resources.py in reality. Makes ~40 tiny requests.
Usage: uv run python scripts/probe_endpoints.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from brightpearl_client import BrightpearlClient, BrightpearlError

SEARCHES = [
    ("accounting-service", "journal"),
    ("accounting-service", "customer-payment"),
    ("accounting-service", "nominal-code"),
    ("accounting-service", "currency"),
    ("accounting-service", "tax-code"),
    ("accounting-service", "payment-method"),
    ("accounting-service", "accounting-period"),
    ("warehouse-service", "goods-movement"),
    ("warehouse-service", "goods-note/goods-out"),
    ("warehouse-service", "goods-note/goods-in"),
    ("warehouse-service", "warehouse"),
    ("warehouse-service", "stock-transfer"),
    ("warehouse-service", "shipping-method"),
    ("order-service", "order-status"),
    ("order-service", "channel"),
    ("contact-service", "company"),
    ("contact-service", "contact-group"),
    ("contact-service", "lead-source"),
    ("contact-service", "tag"),
    ("contact-service", "project"),
    ("product-service", "brand"),
    ("product-service", "collection"),
    ("product-service", "season"),
    ("product-service", "product-type"),
    ("product-service", "product-group"),
    ("product-service", "price-list"),
    ("product-service", "category"),
    ("product-service", "channel-brand"),
    ("product-service", "product-price"),
]

GETS = [
    "order-service/order-status",
    "order-service/channel",
    "order-service/order-type",
    "order-service/order-stock-status",
    "order-service/order-shipping-status",
    "warehouse-service/warehouse",
    "warehouse-service/shipping-method",
    "accounting-service/tax-code",
    "accounting-service/payment-method",
    "accounting-service/currency",
    "accounting-service/accounting-period",
    "accounting-service/nominal-code",
    "product-service/brand",
    "product-service/product-type",
    "product-service/price-list",
    "product-service/collection",
    "product-service/season",
    "product-service/channel-brand",
    "product-service/product-category",
    "contact-service/lead-source",
    "contact-service/contact-tag",
    "contact-service/tag",
    "warehouse-service/product-availability/1000",
    "product-service/product-price/1000",
]


async def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    async with BrightpearlClient() as bp:
        print("== searches ==")
        for service, resource in SEARCHES:
            try:
                page = await bp.search(service, resource, filters={"pageSize": 1})
                cols = len(page.results[0]) if page.results else 0
                print(f"OK    {service}/{resource}-search  ({page.results_available} avail, {cols} cols)")
            except BrightpearlError as e:
                print(f"FAIL  {service}/{resource}-search  [{e.status_code}]")
        print("== plain GETs ==")
        for path in GETS:
            try:
                payload = await bp.get(path)
                n = len(payload) if isinstance(payload, list) else 1
                print(f"OK    {path}  ({n} items)")
            except BrightpearlError as e:
                print(f"FAIL  {path}  [{e.status_code}]")


asyncio.run(main())

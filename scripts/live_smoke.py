"""Live smoke test: pull one page of real orders from Brightpearl.

Usage: uv run python scripts/live_smoke.py
Reads credentials from .env. Makes exactly two API requests.
"""

import asyncio

from dotenv import load_dotenv

from brightpearl_client import BrightpearlClient


async def main() -> None:
    load_dotenv()
    async with BrightpearlClient() as bp:
        page = await bp.orders.search()
        print(f"orders available: {page.results_available}")
        print(f"first page rows:  {len(page.results)}")
        for row in page.results[:3]:
            slim = {k: row[k] for k in list(row)[:8]}
            print(f"  {slim}")

        recent_ids = [r["orderId"] for r in page.results[:3]]
        full = await bp.orders.get(recent_ids)
        for order in full:
            print(
                f"full order {order['id']}: type={order.get('orderTypeCode')} "
                f"status={order.get('orderStatus', {}).get('name')} "
                f"rows={len(order.get('orderRows', {}))}"
            )


if __name__ == "__main__":
    asyncio.run(main())

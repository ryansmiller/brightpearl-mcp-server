"""Exercise the MCP server tools in-process (no transport).

Usage: uv run python scripts/mcp_smoke.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp_server import server


async def main() -> None:
    server.ensure_audit_table()

    print("== get_data_freshness ==")
    print(server.get_data_freshness()[:600])

    print("\n== query_sales (2026 YTD by month) ==")
    print(server.query_sales("2026-01-01", "2026-07-04", "month")[:800])

    print("\n== get_stock_levels('Select Metal') ==")
    print(server.get_stock_levels("Select Metal")[:600])

    print("\n== search_customers('dimarco') ==")
    print(server.search_customers("dimarco")[:400])

    print("\n== run_bigquery_sql guard: write rejected ==")
    print(server.run_bigquery_sql("DELETE FROM brightpearl.orders WHERE true"))

    print("\n== run_bigquery_sql: top nominal codes by 2026 net ==")
    print(server.run_bigquery_sql(
        "SELECT nominal_code, nominal_name, ROUND(SUM(net_credit),0) AS net "
        "FROM brightpearl.monthly_financials WHERE month >= '2026-01-01' "
        "GROUP BY 1,2 ORDER BY ABS(SUM(net_credit)) DESC LIMIT 5"
    )[:700])

    print("\n== get_order_live (order 100039) ==")
    out = await server.get_order_live(100039)
    print(out[:300])


asyncio.run(main())

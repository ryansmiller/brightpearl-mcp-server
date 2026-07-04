"""Sync CLI.

Usage:
    uv run python -m sync.cli init                      # create BigQuery tables
    uv run python -m sync.cli backfill orders [--limit N]
    uv run python -m sync.cli sweep orders|products|contacts|all
    uv run python -m sync.cli status
"""

import argparse
import asyncio
import logging

from dotenv import load_dotenv

from brightpearl_client import BrightpearlClient

from .bq import BigQueryWriter
from .pipeline import RESOURCES, SyncPipeline


async def run(args: argparse.Namespace) -> None:
    bq = BigQueryWriter()
    if args.command == "init":
        bq.ensure_tables()
        print(f"tables ready in {bq.project}.{bq.dataset}")
        return
    if args.command == "status":
        rows = bq.query(
            f"SELECT resource, watermark_updated_on, last_run_at, last_run_kind, last_run_rows "
            f"FROM `{bq._table_ref('sync_state')}` ORDER BY resource"
        )
        for r in rows or [{"resource": "(no runs yet)"}]:
            print(r)
        return

    bq.ensure_tables()
    resources = list(RESOURCES) if args.resource == "all" else [args.resource]
    async with BrightpearlClient() as bp:
        pipeline = SyncPipeline(bp, bq)
        for resource in resources:
            n = await pipeline.sync(
                resource,
                incremental=(args.command == "sweep"),
                limit=args.limit,
            )
            print(f"{resource}: {n} upserted")


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(prog="sync")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    sub.add_parser("status")
    for cmd in ("backfill", "sweep"):
        p = sub.add_parser(cmd)
        p.add_argument("resource", choices=[*RESOURCES, "all"])
        p.add_argument("--limit", type=int, default=None)

    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()

"""Sync CLI.

Usage:
    uv run python -m sync.cli init                       # create BigQuery tables
    uv run python -m sync.cli backfill orders [--limit N]  # detail resources
    uv run python -m sync.cli sweep orders|products|contacts|all
    uv run python -m sync.cli dump all|refs|<table>      # search dumps + reference GETs
    uv run python -m sync.cli dump journal_rows --full   # ignore watermark
    uv run python -m sync.cli derived prices|availability|all
    uv run python -m sync.cli status
"""

import argparse
import asyncio
import logging

from dotenv import load_dotenv

from brightpearl_client import BrightpearlClient

from .bq import BigQueryWriter
from .derived import DerivedSyncer
from .dynamic import ReferenceSyncer, SearchDumpSyncer
from .pipeline import RESOURCES, SyncPipeline
from .resources import REFERENCE_GETS, SEARCH_DUMPS


async def run(args: argparse.Namespace) -> None:
    bq = BigQueryWriter()
    if args.command == "init":
        bq.ensure_tables()
        print(f"tables ready in {bq.project}.{bq.dataset}")
        return
    if args.command == "views":
        from .views import create_views

        for name in create_views(bq):
            print(f"view ready: {name}")
        return
    if args.command == "status":
        rows = bq.query(
            f"SELECT resource, watermark_updated_on, watermark_id, last_run_at, "
            f"last_run_kind, last_run_rows "
            f"FROM `{bq._table_ref('sync_state')}` ORDER BY resource"
        )
        for r in rows or [{"resource": "(no runs yet)"}]:
            print(r)
        return

    bq.ensure_tables()
    try:
        async with BrightpearlClient() as bp:
            if args.command in ("backfill", "sweep"):
                pipeline = SyncPipeline(bp, bq)
                resources = list(RESOURCES) if args.resource == "all" else [args.resource]
                for resource in resources:
                    n = await pipeline.sync(
                        resource, incremental=(args.command == "sweep"), limit=args.limit
                    )
                    print(f"{resource}: {n} upserted")
            elif args.command == "dump":
                searcher = SearchDumpSyncer(bp, bq)
                reference = ReferenceSyncer(bp, bq)
                if args.table == "all":
                    tables = [*REFERENCE_GETS, *SEARCH_DUMPS]
                elif args.table == "refs":
                    tables = list(REFERENCE_GETS)
                else:
                    tables = [args.table]
                for table in tables:
                    if table in REFERENCE_GETS:
                        n = await reference.sync(table)
                    else:
                        n = await searcher.sync(table, full=args.full)
                    print(f"{table}: {n} rows")
            elif args.command == "webhooks":
                from .webhooks import list_webhooks, register_webhooks

                if args.action == "register":
                    created = await register_webhooks(bp, args.url)
                    print(f"created subscriptions: {created or '(all already exist)'}")
                hooks = await list_webhooks(bp)
                for h in hooks:
                    print({k: h.get(k) for k in ("id", "subscribeTo", "uriTemplate")})
            elif args.command == "derived":
                derived = DerivedSyncer(bp, bq)
                if args.kind in ("prices", "all"):
                    print(f"product_prices: {await derived.sync_prices()} rows")
                if args.kind in ("availability", "all"):
                    print(f"product_availability: {await derived.sync_availability()} rows")
                if args.kind in ("suppliers", "all"):
                    print(f"product_suppliers: {await derived.sync_suppliers()} rows")
    finally:
        # sync_state records batch in memory (sync/state.py); persist before
        # exit even if a later resource in the run failed — the completed
        # resources' watermarks are real.
        bq.state.flush()


def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(prog="sync")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    sub.add_parser("status")
    sub.add_parser("views")
    for cmd in ("backfill", "sweep"):
        p = sub.add_parser(cmd)
        p.add_argument("resource", choices=[*RESOURCES, "all"])
        p.add_argument("--limit", type=int, default=None)
    p = sub.add_parser("dump")
    p.add_argument("table", choices=[*SEARCH_DUMPS, *REFERENCE_GETS, "all", "refs"])
    p.add_argument("--full", action="store_true")
    p = sub.add_parser("derived")
    p.add_argument("kind", choices=["prices", "availability", "suppliers", "all"])
    p = sub.add_parser("webhooks")
    p.add_argument("action", choices=["register", "list"])
    p.add_argument("--url", help="ingest URL incl. ?token= (required for register)")

    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()

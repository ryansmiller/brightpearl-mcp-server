"""Webhook ingest service + subscription management (GAMEPLAN Phase 2b).

Brightpearl POSTs thin events (resource type + ids) to /webhook; we ack 200
immediately, buffer ids briefly, then fetch full resources in batched
multi-ID GETs and upsert into BigQuery. Delivery is at-least-once and
upserts are idempotent, so duplicates are harmless.

The same app exposes /tick/{tier} for Cloud Scheduler to trigger sweeps —
one process shares one rate limiter across webhooks and sweeps (run with
max-instances=1 so the 200 req/min account budget is never split).

Serve:      python -m sync.webhooks
Subscribe:  python -m sync.cli webhooks register --url https://.../webhook?token=...
"""

import asyncio
import json
import logging
import os
from collections import defaultdict

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.routing import Route

from brightpearl_client import BrightpearlClient, BrightpearlNotFound

from .bq import BigQueryWriter
from .derived import DerivedSyncer
from .dynamic import ReferenceSyncer, SearchDumpSyncer
from .pipeline import SyncPipeline
from .resources import REFERENCE_GETS, SEARCH_DUMPS

logger = logging.getLogger(__name__)

FLUSH_SECONDS = 3.0

# Events verified subscribable on this account (2026-07): contact.* and
# order.created do not exist — order.modified fires on creation too, and
# contacts rely on the warm sweep.
WEBHOOK_EVENTS = [
    # order.modified verified firing in production logs; the order-status
    # sub-event registered too (per Ryan) in case some status changes only
    # emit the specific code. Duplicates are harmless (idempotent upserts).
    "order.modified",
    "order.modified.order-status",
    # Only fires for orders created via the newer sales-order POST endpoint —
    # UI-created orders do NOT trigger it (per Ryan/docs). Kept as free
    # insurance for future API integrations; order.modified covers UI orders.
    "sales-order.created",
    "product.created", "product.modified",
    # Stock-level change event (three-part code discovered from the account's
    # existing WooCommerce integration subscriptions)
    "product.modified.on-hand-modified",
    "goods-out-note.created", "goods-out-note.modified",
    "goods-in-note.created",
]
RESOURCE_MAP = {
    "order": "orders",
    "sales-order": "orders",
    "product": "products",
    "contact": "contacts",
}
# Goods-note events carry note ids, not order ids — they trigger an
# incremental goods_movements dump instead of a detail fetch.
MOVEMENT_TRIGGERS = {"goods-out-note", "goods-in-note"}

# Sweep tiers for /tick — sweeps for detail resources, dumps for the rest
TIERS: dict[str, dict] = {
    "hot": {"sweeps": ["orders"], "dumps": ["goods_movements"], "derived": []},
    "warm": {
        "sweeps": ["contacts"],
        "dumps": ["journal_rows", "customer_payments", "supplier_payments",
                  "goods_out_notes", "companies"],
        "derived": ["availability"],
    },
    "cold": {
        "sweeps": ["products"],
        "dumps": [t for t in SEARCH_DUMPS if SEARCH_DUMPS[t]["tier"] == "cold"]
                 + list(REFERENCE_GETS),
        "derived": ["prices"],
    },
}


class Ingestor:
    """Buffers webhook ids briefly, then fetches + upserts in batches."""

    def __init__(self):
        self.bp = BrightpearlClient()
        self.bq = BigQueryWriter()
        self.pipeline = SyncPipeline(self.bp, self.bq)
        self.pending: dict[str, set[int]] = defaultdict(set)
        self.movements_pending = False
        self.lock = asyncio.Lock()
        self.flusher: asyncio.Task | None = None

    async def enqueue(self, resource_type: str, ids: list[int]) -> None:
        if resource_type in MOVEMENT_TRIGGERS:
            async with self.lock:
                self.movements_pending = True
                if self.flusher is None or self.flusher.done():
                    self.flusher = asyncio.create_task(self._flush_later())
            return
        name = RESOURCE_MAP.get(resource_type)
        if not name:
            logger.info("ignoring webhook for unmapped resource %s", resource_type)
            return
        async with self.lock:
            self.pending[name].update(ids)
            if self.flusher is None or self.flusher.done():
                self.flusher = asyncio.create_task(self._flush_later())

    async def _flush_later(self) -> None:
        await asyncio.sleep(FLUSH_SECONDS)
        async with self.lock:
            batch, self.pending = dict(self.pending), defaultdict(set)
            movements, self.movements_pending = self.movements_pending, False
        if movements:
            try:
                n = await SearchDumpSyncer(self.bp, self.bq).sync("goods_movements")
                logger.info("webhook flush: goods_movements sweep, %d rows", n)
            except Exception:
                logger.exception("webhook-triggered goods_movements sweep failed")
        for name, ids in batch.items():
            try:
                if name == "orders":
                    n, _ = await self.pipeline._load_orders(sorted(ids))
                else:
                    n, _ = await self.pipeline._load_simple(name, sorted(ids))
                if name == "products":
                    # stock events arrive as product webhooks — keep the
                    # per-warehouse availability table current too
                    await DerivedSyncer(self.bp, self.bq).refresh_availability(sorted(ids))
                logger.info("webhook flush: %s x%d upserted", name, n)
            except BrightpearlNotFound:
                logger.warning("webhook flush: %s ids %s not found (deleted?)", name, ids)
            except Exception:
                logger.exception("webhook flush failed for %s", name)


ingestor: Ingestor | None = None


def _check_token(request: Request) -> bool:
    expected = os.environ.get("WEBHOOK_TOKEN", "")
    return bool(expected) and request.query_params.get("token") == expected


async def webhook(request: Request) -> JSONResponse:
    if not _check_token(request):
        return JSONResponse({"error": "bad token"}, status_code=403)
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return JSONResponse({"error": "bad json"}, status_code=400)
    # bodyTemplate fields; with idSetAccepted the id may be "1,2,3"
    resource = str(body.get("resource", ""))
    raw_id = str(body.get("id", ""))
    ids = [int(x) for x in raw_id.split(",") if x.strip().isdigit()]
    if resource and ids:
        await ingestor.enqueue(resource, ids)
    return JSONResponse({"accepted": len(ids)})


async def tick(request: Request) -> JSONResponse:
    if not _check_token(request):
        return JSONResponse({"error": "bad token"}, status_code=403)
    tier = request.path_params["tier"]
    if tier not in TIERS:
        return JSONResponse({"error": f"unknown tier {tier}"}, status_code=400)
    spec = TIERS[tier]
    results: dict[str, int] = {}
    searcher = SearchDumpSyncer(ingestor.bp, ingestor.bq)
    reference = ReferenceSyncer(ingestor.bp, ingestor.bq)
    derived = DerivedSyncer(ingestor.bp, ingestor.bq)
    for name in spec["sweeps"]:
        results[name] = await ingestor.pipeline.sync(name, incremental=True)
    for name in spec["dumps"]:
        if name in REFERENCE_GETS:
            results[name] = await reference.sync(name)
        else:
            results[name] = await searcher.sync(name)
    for kind in spec["derived"]:
        results[f"derived_{kind}"] = (
            await derived.sync_prices() if kind == "prices"
            else await derived.sync_availability()
        )
    logger.info("tick %s: %s", tier, results)
    return JSONResponse({"tier": tier, "results": results})


async def healthz(request: Request) -> PlainTextResponse:
    return PlainTextResponse("ok")


# Staleness budgets in minutes, by sweep tier. A resource is "stale" when its
# sync_state.last_run_at is older than its budget — this measures sync HEALTH
# (did the machinery run), not business activity, so quiet days don't alarm.
TIER_BUDGET_MINUTES = {"hot": 30, "warm": 120, "cold": 60 * 50}


def _resource_budgets() -> dict[str, int]:
    from .pipeline import RESOURCES

    budgets: dict[str, int] = {}
    for name, spec in RESOURCES.items():
        budgets[name] = TIER_BUDGET_MINUTES[spec["tier"]]
    for name, spec in SEARCH_DUMPS.items():
        budgets[name] = TIER_BUDGET_MINUTES[spec["tier"]]
    for name in REFERENCE_GETS:
        budgets[name] = TIER_BUDGET_MINUTES["cold"]
    return budgets


async def alert_check(request: Request) -> JSONResponse:
    """Compare each resource's last_run_at against its tier budget.

    Violations are logged at ERROR with the STALENESS_ALERT marker — a
    Cloud Monitoring log-based alert policy emails on that string. Returning
    them in the body lets get_data_freshness-style tooling reuse the check.
    """
    if not _check_token(request):
        return JSONResponse({"error": "bad token"}, status_code=403)
    budgets = _resource_budgets()
    rows = ingestor.bq.query(
        f"SELECT resource, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_run_at, MINUTE) AS lag "
        f"FROM `{ingestor.bq._table_ref('sync_state')}`"
    )
    seen = {r["resource"]: r["lag"] for r in rows}
    violations = []
    for resource, budget in budgets.items():
        lag = seen.get(resource)
        if lag is None:
            violations.append({"resource": resource, "lag_minutes": None, "budget": budget,
                               "problem": "never synced"})
        elif lag > budget:
            violations.append({"resource": resource, "lag_minutes": lag, "budget": budget,
                               "problem": "stale"})
    if violations:
        logger.error("STALENESS_ALERT: %d resources stale: %s", len(violations),
                     json.dumps(violations))
    return JSONResponse({"ok": not violations, "violations": violations})


def create_app() -> Starlette:
    global ingestor
    ingestor = Ingestor()
    return Starlette(routes=[
        Route("/webhook", webhook, methods=["POST"]),
        Route("/tick/{tier}", tick, methods=["POST"]),
        Route("/health", healthz, methods=["GET"]),
        Route("/alert-check", alert_check, methods=["POST", "GET"]),
    ])


# --- subscription management (called from sync.cli) ---

async def register_webhooks(bp: BrightpearlClient, base_url: str) -> list[str]:
    """Create one subscription per event in WEBHOOK_EVENTS. base_url includes ?token=."""
    body_template = json.dumps({
        "account": "${account-code}",
        "resource": "${resource-type}",
        "id": "${resource-id}",
        "event": "${lifecycle-event}",
        "raisedOn": "${raised-on}",
    })
    existing = await list_webhooks(bp)
    # other integrations (ShipStation, WooCommerce...) subscribe to the same
    # events — dedupe on event AND destination, not event alone
    subscribed = {
        w.get("subscribeTo") for w in existing
        if str(w.get("uriTemplate", "")).startswith(base_url.split("?")[0])
    }
    created = []
    for event in WEBHOOK_EVENTS:
        if event in subscribed:
            continue
        await bp.rate_limiter.acquire(priority=True)
        resp = await bp._http.post(  # client has no POST wrapper yet; one-time setup calls
            "integration-service/webhook",
            json={
                "subscribeTo": event,
                "httpMethod": "POST",
                "uriTemplate": base_url,
                "bodyTemplate": body_template,
                "contentType": "application/json",
                "idSetAccepted": True,
                "qualityOfService": 1,
            },
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"webhook create failed for {event}: {resp.status_code} {resp.text}")
        created.append(event)
    return created


async def list_webhooks(bp: BrightpearlClient) -> list[dict]:
    payload = await bp.get("integration-service/webhook")
    return payload if isinstance(payload, list) else [payload]


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(create_app(), host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


if __name__ == "__main__":
    main()

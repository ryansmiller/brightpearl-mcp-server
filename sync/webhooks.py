"""Webhook ingest service + subscription management (GAMEPLAN Phases 2b/4/5).

Durable processing via Cloud Tasks: the webhook handler validates, enqueues a
task (persisted by Cloud Tasks), and acks — all inside the request. Cloud
Tasks then calls /process with a signed OIDC identity token; the fetch +
BigQuery upsert happens inside THAT request, so request-based billing covers
everything and a crashed instance just means the task retries. Delivery is
at-least-once end to end and upserts are idempotent, so duplicates are
harmless.

Without TASKS_QUEUE set (local dev, tests), events process inline.

The same app exposes /tick/{tier} for Cloud Scheduler sweeps and /alert-check
for staleness monitoring. Run with max-instances=1 so the 200 req/min
Brightpearl budget is never split across instances.

Serve:      python -m sync.webhooks
Subscribe:  python -m sync.cli webhooks register --url https://.../webhook?token=...
"""

import asyncio
import hmac
import json
import logging
import os
import time
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

# Events verified subscribable on this account (2026-07): contact.* and
# order.created do not exist — order.modified fires on creation too, and
# contacts rely on the warm sweep.
WEBHOOK_EVENTS = [
    # order.modified verified firing in production logs; the order-status
    # sub-event registered too in case some status changes only emit the
    # specific code. Duplicates are harmless (idempotent upserts).
    "order.modified",
    "order.modified.order-status",
    # Only fires for orders created via the newer sales-order POST endpoint —
    # UI-created orders do NOT trigger it (confirmed against the docs and the
    # account owner). Kept as free insurance for future API integrations;
    # order.modified covers UI orders.
    "sales-order.created",
    "product.created", "product.modified", "product.destroyed",
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

def _dumps_for_tier(tier: str) -> list[str]:
    return [t for t in SEARCH_DUMPS if SEARCH_DUMPS[t]["tier"] == tier]


# Sweep tiers for /tick — sweeps for detail resources, dumps for the rest.
# Dumps are derived from each resource's "tier" in resources.py so moving a
# resource between tiers (as with goods_out_notes/brands/etc, 2026-07-09)
# doesn't also require updating a second, easy-to-forget list here.
TIERS: dict[str, dict] = {
    "hot": {"sweeps": ["orders"], "dumps": _dumps_for_tier("hot"), "derived": []},
    "warm": {
        "sweeps": ["contacts"],
        "dumps": _dumps_for_tier("warm"),
        "derived": ["availability"],
    },
    "cold": {
        "sweeps": ["products"],
        "dumps": _dumps_for_tier("cold") + list(REFERENCE_GETS),
        "derived": ["prices", "suppliers"],
    },
}


class Processor:
    """Fetches full resources for webhook events and upserts into BigQuery."""

    def __init__(self):
        self.bp = BrightpearlClient()
        self.bq = BigQueryWriter()
        self.pipeline = SyncPipeline(self.bp, self.bq)

    async def process(self, resource: str, ids: list[int]) -> int:
        if resource in MOVEMENT_TRIGGERS:
            n = await SearchDumpSyncer(self.bp, self.bq).sync("goods_movements")
            logger.info("goods-note event: goods_movements sweep, %d rows", n)
            return n
        name = RESOURCE_MAP.get(resource)
        if not name:
            logger.info("ignoring event for unmapped resource %s", resource)
            return 0
        unique = sorted(set(ids))
        try:
            if name == "orders":
                n, _ = await self.pipeline._load_orders(unique)
            else:
                n, _ = await self.pipeline._load_simple(name, unique)
                if name == "products":
                    # stock events arrive as product webhooks — keep the
                    # per-warehouse availability table current too
                    await DerivedSyncer(self.bp, self.bq).refresh_availability(unique)
            logger.info("processed %s x%d", name, n)
            return n
        except BrightpearlNotFound:
            logger.warning("%s ids %s not found (deleted?)", name, unique)
            return 0


processor: Processor | None = None
_tasks_client = None
# Cloud Scheduler fires /tick/{tier} on a blind cron regardless of whether the
# previous invocation finished. A tier whose dumps include a slow full-reload
# (goods_out_notes: ~270 requests, no updatedOn to filter on) can occasionally
# overrun its interval; without this guard the next tick piles a second
# concurrent full scan onto the same shared 200 req/min budget, which slows
# both down, causing the next tick to overlap too — an unbounded pile-up that
# starves the rate limiter and starves sync_state updates for every dump
# after the slow one. One lock per tier turns an overlapping tick into a
# no-op instead of a pile-up.
_tick_locks: dict[str, asyncio.Lock] = {}


# Coalescing: a burst of single-id webhooks (each an order/product edit) would
# otherwise become one Cloud Task → one fetch → one BigQuery upsert apiece, and
# every upsert pays BigQuery's per-query floor. Instead we buffer ids per
# resource in memory and schedule ONE delayed flush task per resource; ids that
# land during the window fold into the same flush. max-instances=1 makes the
# in-memory buffer authoritative. Durability: an instance restart loses at most
# the sub-window of un-flushed ids, which the tiered polling sweeps reconcile —
# the same safety net every webhook miss already relies on. Set the window to 0
# to disable and enqueue one task per delivery (the pre-coalescing behavior).
COALESCE_WINDOW = int(os.environ.get("WEBHOOK_COALESCE_WINDOW", "10"))
_pending_ids: dict[str, set[int]] = defaultdict(set)
_flush_scheduled: set[str] = set()
_pending_lock = asyncio.Lock()


def _create_task(queue: str, payload: dict, *, delay_seconds: int = 0) -> None:
    global _tasks_client
    from google.cloud import tasks_v2

    if _tasks_client is None:
        _tasks_client = tasks_v2.CloudTasksClient()
    service_url = os.environ["SERVICE_URL"]
    task: dict = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": f"{service_url}/process",
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(payload).encode(),
            "oidc_token": {
                "service_account_email": os.environ["SERVICE_ACCOUNT_EMAIL"],
                "audience": service_url,
            },
        }
    }
    if delay_seconds:
        from google.protobuf import timestamp_pb2

        ts = timestamp_pb2.Timestamp()
        ts.FromNanoseconds((time.time_ns()) + delay_seconds * 1_000_000_000)
        task["schedule_time"] = ts
    _tasks_client.create_task(parent=queue, task=task)


async def dispatch(resource: str, ids: list[int]) -> str:
    """Durable path: enqueue to Cloud Tasks. Inline fallback for local dev."""
    queue = os.environ.get("TASKS_QUEUE")
    if not queue:
        await processor.process(resource, ids)
        return "inline"
    if COALESCE_WINDOW <= 0:
        await asyncio.to_thread(_create_task, queue, {"resource": resource, "ids": ids})
        return "queued"
    async with _pending_lock:
        _pending_ids[resource].update(ids)
        need_task = resource not in _flush_scheduled
        if need_task:
            _flush_scheduled.add(resource)
    if need_task:
        await asyncio.to_thread(
            _create_task, queue, {"flush": resource}, delay_seconds=COALESCE_WINDOW
        )
    return "coalesced"


async def _drain(resource: str) -> int:
    """Flush a resource's buffered ids as one processing pass.

    Clearing the buffer and the scheduled-flag together (before processing)
    means any webhook arriving afterward schedules a fresh flush rather than
    dropping its id into an un-scheduled buffer.
    """
    async with _pending_lock:
        ids = sorted(_pending_ids.pop(resource, set()))
        _flush_scheduled.discard(resource)
    return await processor.process(resource, ids)


def _check_token(request: Request) -> bool:
    """Shared-secret check, constant-time.

    Brightpearl can only template the secret into the webhook URL, so /webhook
    has to accept it as a query param. Our own callers (Cloud Scheduler hitting
    /tick and /alert-check) send it in the X-Auth-Token header instead, keeping
    it out of request logs / proxy logs / browser history.
    """
    expected = os.environ.get("WEBHOOK_TOKEN", "")
    if not expected:
        return False
    supplied = request.headers.get("x-auth-token") or request.query_params.get("token", "")
    return hmac.compare_digest(supplied, expected)


def _check_oidc(request: Request) -> bool:
    """Verify a Google-signed identity token from Cloud Tasks."""
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        return False
    try:
        from google.auth.transport import requests as google_requests
        from google.oauth2 import id_token as google_id_token

        claims = google_id_token.verify_oauth2_token(
            auth[7:], google_requests.Request(), audience=os.environ.get("SERVICE_URL")
        )
        return claims.get("email") == os.environ.get("SERVICE_ACCOUNT_EMAIL")
    except Exception:
        return False


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
    event = str(body.get("event", ""))
    ids = [int(x) for x in raw_id.split(",") if x.strip().isdigit()]
    if not (resource and ids):
        return JSONResponse({"accepted": 0})
    if event == "destroyed":
        table = RESOURCE_MAP.get(resource)
        if table:
            from .schema import TABLES

            processor.bq.mark_deleted(table, TABLES[table]["key_field"], ids)
            logger.info("webhook: marked %d %s deleted", len(ids), table)
        return JSONResponse({"accepted": len(ids)})
    how = await dispatch(resource, ids)
    return JSONResponse({"accepted": len(ids), "dispatch": how})


async def process_task(request: Request) -> JSONResponse:
    """Cloud Tasks callback. Non-2xx → the task retries with backoff.

    In production (TASKS_QUEUE set) only Cloud Tasks' OIDC identity is
    accepted: the shared token appears in request logs via /webhook query
    strings, so it must not also open this endpoint. The token fallback
    exists only for local dev, where there's no Cloud Tasks to sign calls.
    """
    if os.environ.get("TASKS_QUEUE"):
        authorized = _check_oidc(request)
    else:
        authorized = _check_oidc(request) or _check_token(request)
    if not authorized:
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    try:
        body = await request.json()
    except json.JSONDecodeError:
        return JSONResponse({"error": "bad json"}, status_code=400)
    # A coalesced flush task carries only {"flush": resource} and drains the
    # in-memory buffer; a direct task (coalescing disabled) carries ids.
    flush = str(body.get("flush", ""))
    resource = flush or str(body.get("resource", ""))
    ids = [int(x) for x in body.get("ids", [])]
    try:
        n = await (_drain(resource) if flush else processor.process(resource, ids))
    except Exception:
        logger.exception("processing failed for %s %s; task will retry", resource, ids)
        return JSONResponse({"error": "processing failed"}, status_code=500)
    return JSONResponse({"processed": n})


async def _safe(name: str, coro) -> object:
    """Run one tick resource in isolation so a bad config or a flaky call on
    it (e.g. the 2026-07-09 product_option_values MERGE-key bug) can't also
    starve every resource scheduled after it in the same tick — including
    the derived steps, which run last."""
    try:
        return await coro
    except Exception as e:
        logger.exception("tick: %s failed, continuing with remaining resources", name)
        return {"error": str(e)}


async def tick(request: Request) -> JSONResponse:
    if not _check_token(request):
        return JSONResponse({"error": "bad token"}, status_code=403)
    tier = request.path_params["tier"]
    if tier not in TIERS:
        return JSONResponse({"error": f"unknown tier {tier}"}, status_code=400)
    lock = _tick_locks.setdefault(tier, asyncio.Lock())
    if lock.locked():
        logger.warning("tick %s: previous run still in progress, skipping", tier)
        return JSONResponse({"tier": tier, "skipped": "already running"})
    async with lock:
        spec = TIERS[tier]
        results: dict[str, object] = {}
        searcher = SearchDumpSyncer(processor.bp, processor.bq)
        reference = ReferenceSyncer(processor.bp, processor.bq)
        derived = DerivedSyncer(processor.bp, processor.bq)
        for name in spec["sweeps"]:
            results[name] = await _safe(name, processor.pipeline.sync(name, incremental=True))
        for name in spec["dumps"]:
            if name in REFERENCE_GETS:
                results[name] = await _safe(name, reference.sync(name))
            else:
                results[name] = await _safe(name, searcher.sync(name))
        derived_fns = {
            "prices": derived.sync_prices,
            "availability": derived.sync_availability,
            "suppliers": derived.sync_suppliers,
        }
        for kind in spec["derived"]:
            results[f"derived_{kind}"] = await _safe(f"derived_{kind}", derived_fns[kind]())
        if tier == "cold":
            for resource in ("orders", "products", "contacts"):
                results[f"reconcile_{resource}"] = await _safe(
                    f"reconcile_{resource}", processor.pipeline.reconcile_deletions(resource)
                )
        # One batched sync_state write per tick (also picks up records left by
        # webhook-driven sweeps since the last tick) instead of one per resource.
        try:
            processor.bq.state.flush()
        except Exception as e:
            logger.exception("tick %s: sync_state flush failed", tier)
            results["state_flush"] = {"error": str(e)}
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
    rows = processor.bq.query(
        f"SELECT resource, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_run_at, MINUTE) AS lag "
        f"FROM `{processor.bq._table_ref('sync_state')}`"
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
    global processor
    processor = Processor()
    _tick_locks.clear()
    _pending_ids.clear()
    _flush_scheduled.clear()
    return Starlette(routes=[
        Route("/webhook", webhook, methods=["POST"]),
        Route("/process", process_task, methods=["POST"]),
        Route("/tick/{tier}", tick, methods=["POST"]),
        Route("/health", healthz, methods=["GET"]),
        # POST-only: a GET would invite passing the token in the query string,
        # where it lands in access logs. Scheduler sends X-Auth-Token instead.
        Route("/alert-check", alert_check, methods=["POST"]),
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


def setup_logging() -> None:
    """Structured Cloud Logging on Cloud Run (K_SERVICE set); plain locally.

    Cloud Logging severity levels make STALENESS_ALERT and exception traces
    visible to log-based alerting and Error Reporting.
    """
    if os.environ.get("K_SERVICE"):
        import google.cloud.logging

        google.cloud.logging.Client().setup_logging(log_level=logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)


def main() -> None:
    setup_logging()
    uvicorn.run(create_app(), host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


if __name__ == "__main__":
    main()

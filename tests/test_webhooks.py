import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.testclient import TestClient

from sync import webhooks


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("WEBHOOK_TOKEN", "sekret")
    monkeypatch.delenv("TASKS_QUEUE", raising=False)  # inline processing path
    fake = MagicMock()
    fake.process = AsyncMock(return_value=3)
    fake.bq = MagicMock()
    monkeypatch.setattr(webhooks, "Processor", lambda: fake)
    application = webhooks.create_app()
    return TestClient(application), fake


def test_webhook_rejects_bad_token(app):
    client, _ = app
    resp = client.post("/webhook?token=wrong", json={"resource": "order", "id": "1"})
    assert resp.status_code == 403


def test_webhook_parses_idset_and_processes_inline(app):
    client, fake = app
    resp = client.post(
        "/webhook?token=sekret",
        json={"resource": "order", "id": "1,2,3", "event": "modified"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"accepted": 3, "dispatch": "inline"}
    fake.process.assert_awaited_once_with("order", [1, 2, 3])


def test_webhook_coalesces_burst_into_one_flush_task(app, monkeypatch):
    """A burst of webhooks for the same resource buffers into one delayed
    flush task instead of one task (and one BigQuery upsert) per delivery."""
    client, fake = app
    monkeypatch.setenv("TASKS_QUEUE", "projects/p/locations/l/queues/q")
    monkeypatch.setattr(webhooks, "COALESCE_WINDOW", 10)
    calls = []
    monkeypatch.setattr(
        webhooks, "_create_task",
        lambda q, payload, delay_seconds=0: calls.append((payload, delay_seconds)),
    )
    r1 = client.post("/webhook?token=sekret",
                     json={"resource": "order", "id": "7", "event": "modified"})
    r2 = client.post("/webhook?token=sekret",
                     json={"resource": "order", "id": "8,9", "event": "modified"})
    assert r1.json() == {"accepted": 1, "dispatch": "coalesced"}
    assert r2.json() == {"accepted": 2, "dispatch": "coalesced"}
    # exactly one flush task for the window; the second delivery folded in
    assert calls == [({"flush": "order"}, 10)]
    assert webhooks._pending_ids["order"] == {7, 8, 9}
    fake.process.assert_not_awaited()


def test_flush_task_drains_buffer_in_one_process_call(app, monkeypatch):
    client, fake = app
    monkeypatch.setenv("TASKS_QUEUE", "projects/p/locations/l/queues/q")
    monkeypatch.setattr(webhooks, "COALESCE_WINDOW", 10)
    monkeypatch.setattr(webhooks, "_create_task", lambda *a, **k: None)
    client.post("/webhook?token=sekret",
                json={"resource": "order", "id": "7", "event": "modified"})
    client.post("/webhook?token=sekret",
                json={"resource": "order", "id": "8", "event": "modified"})
    # Cloud Tasks fires the flush; drop the queue env so the local-dev token
    # path authorizes /process (production is OIDC-only, covered elsewhere).
    monkeypatch.delenv("TASKS_QUEUE", raising=False)
    resp = client.post("/process?token=sekret", json={"flush": "order"})
    assert resp.status_code == 200
    fake.process.assert_awaited_once_with("order", [7, 8])
    assert not webhooks._pending_ids.get("order")
    assert "order" not in webhooks._flush_scheduled


def test_webhook_direct_task_when_coalescing_disabled(app, monkeypatch):
    """WEBHOOK_COALESCE_WINDOW=0 restores one-task-per-delivery behavior."""
    client, fake = app
    monkeypatch.setenv("TASKS_QUEUE", "projects/p/locations/l/queues/q")
    monkeypatch.setattr(webhooks, "COALESCE_WINDOW", 0)
    calls = []
    monkeypatch.setattr(
        webhooks, "_create_task",
        lambda q, payload, delay_seconds=0: calls.append(payload),
    )
    resp = client.post("/webhook?token=sekret",
                       json={"resource": "order", "id": "7", "event": "modified"})
    assert resp.json() == {"accepted": 1, "dispatch": "queued"}
    assert calls == [{"resource": "order", "ids": [7]}]
    fake.process.assert_not_awaited()


def test_webhook_destroyed_marks_deleted_without_fetch(app):
    client, fake = app
    resp = client.post(
        "/webhook?token=sekret",
        json={"resource": "product", "id": "42", "event": "destroyed"},
    )
    assert resp.status_code == 200
    fake.bq.mark_deleted.assert_called_once_with("products", "product_id", [42])
    fake.process.assert_not_awaited()


def test_process_endpoint_requires_auth(app):
    client, _ = app
    resp = client.post("/process", json={"resource": "order", "ids": [1]})
    assert resp.status_code == 403


def test_process_endpoint_runs_processor(app):
    client, fake = app
    resp = client.post("/process?token=sekret", json={"resource": "order", "ids": [1, 2]})
    assert resp.status_code == 200
    assert resp.json() == {"processed": 3}
    fake.process.assert_awaited_once_with("order", [1, 2])


def test_process_rejects_shared_token_in_production(app, monkeypatch):
    """With TASKS_QUEUE set (production), /process is OIDC-only: the shared
    token leaks into request logs via /webhook query strings, so it must not
    open this endpoint too."""
    client, _ = app
    monkeypatch.setenv("TASKS_QUEUE", "projects/p/locations/l/queues/q")
    resp = client.post("/process?token=sekret", json={"resource": "order", "ids": [1]})
    assert resp.status_code == 403


def test_process_endpoint_500_triggers_task_retry(app):
    client, fake = app
    fake.process.side_effect = RuntimeError("BigQuery down")
    resp = client.post("/process?token=sekret", json={"resource": "order", "ids": [1]})
    assert resp.status_code == 500


def test_webhook_bad_json_is_400(app):
    client, _ = app
    resp = client.post(
        "/webhook?token=sekret",
        content=b"not json",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400


def test_health_is_open(app):
    client, _ = app
    assert client.get("/health").status_code == 200


def test_tick_skips_concurrent_run_of_same_tier(app, monkeypatch):
    """Cloud Scheduler fires /tick/{tier} on a blind cron regardless of
    whether the previous run finished. A second overlapping call must skip
    rather than pile a duplicate full scan onto the shared rate budget."""
    client, fake = app
    fake.pipeline.sync = AsyncMock(return_value=0)
    release = threading.Event()

    class SlowSearcher:
        def __init__(self, *a, **k):
            pass

        async def sync(self, name, **kwargs):
            while not release.is_set():
                await asyncio.sleep(0.01)
            return 0

    monkeypatch.setattr(webhooks, "SearchDumpSyncer", SlowSearcher)
    monkeypatch.setattr(
        webhooks, "ReferenceSyncer",
        lambda *a, **k: MagicMock(sync=AsyncMock(return_value=0)),
    )
    monkeypatch.setattr(
        webhooks, "DerivedSyncer",
        lambda *a, **k: MagicMock(
            sync_prices=AsyncMock(return_value=0),
            sync_availability=AsyncMock(return_value=0),
            sync_suppliers=AsyncMock(return_value=0),
        ),
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(client.post, "/tick/warm?token=sekret")
        time.sleep(0.2)  # let the first request enter the lock and start stalling
        second_resp = pool.submit(client.post, "/tick/warm?token=sekret").result(timeout=5)
        assert second_resp.json() == {"tier": "warm", "skipped": "already running"}
        release.set()
        first_resp = first.result(timeout=5)
        assert first_resp.status_code == 200
        assert first_resp.json()["tier"] == "warm"


def test_tick_isolates_resource_failure(app, monkeypatch):
    """A broken resource (bad MERGE key, rejected filter, etc.) must not
    starve the dumps/derived steps scheduled after it in the same tick —
    the 2026-07-09 product_option_values incident: a bad `key` config threw
    on every warm tick and silently blocked derived_availability, which runs
    last, for hours with no error surfaced in get_data_freshness."""
    client, fake = app
    fake.pipeline.sync = AsyncMock(return_value=0)

    class FlakySearcher:
        def __init__(self, *a, **k):
            pass

        async def sync(self, name, **kwargs):
            if name == "supplier_payments":
                raise RuntimeError("Unrecognized name: id")
            return 1

    monkeypatch.setattr(webhooks, "SearchDumpSyncer", FlakySearcher)
    monkeypatch.setattr(
        webhooks, "ReferenceSyncer",
        lambda *a, **k: MagicMock(sync=AsyncMock(return_value=0)),
    )
    availability = AsyncMock(return_value=0)
    monkeypatch.setattr(
        webhooks, "DerivedSyncer",
        lambda *a, **k: MagicMock(
            sync_prices=AsyncMock(return_value=0),
            sync_availability=availability,
            sync_suppliers=AsyncMock(return_value=0),
        ),
    )

    resp = client.post("/tick/warm?token=sekret")
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert results["supplier_payments"] == {"error": "Unrecognized name: id"}
    assert results["companies"] == 1  # a dump scheduled after the failure still ran
    availability.assert_awaited_once()  # the derived step still ran


def test_alert_check_flags_stale_and_missing(app, monkeypatch):
    client, fake = app
    fake.bq.query.return_value = [
        {"resource": "orders", "lag": 5},          # within hot budget
        {"resource": "journal_rows", "lag": 999},  # beyond warm budget
    ]
    fake.bq._table_ref = lambda n: f"p.d.{n}"
    resp = client.post("/alert-check", headers={"x-auth-token": "sekret"})
    body = resp.json()
    assert body["ok"] is False
    problems = {v["resource"]: v["problem"] for v in body["violations"]}
    assert problems["journal_rows"] == "stale"
    assert "orders" not in problems           # healthy
    assert problems["companies"] == "never synced"

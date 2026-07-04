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


def test_webhook_enqueues_task_when_queue_configured(app, monkeypatch):
    client, fake = app
    monkeypatch.setenv("TASKS_QUEUE", "projects/p/locations/l/queues/q")
    created = {}
    monkeypatch.setattr(webhooks, "_create_task", lambda q, payload: created.update(payload))
    resp = client.post(
        "/webhook?token=sekret",
        json={"resource": "order", "id": "7", "event": "modified"},
    )
    assert resp.json() == {"accepted": 1, "dispatch": "queued"}
    assert created == {"resource": "order", "ids": [7]}
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


def test_alert_check_flags_stale_and_missing(app, monkeypatch):
    client, fake = app
    fake.bq.query.return_value = [
        {"resource": "orders", "lag": 5},          # within hot budget
        {"resource": "journal_rows", "lag": 999},  # beyond warm budget
    ]
    fake.bq._table_ref = lambda n: f"p.d.{n}"
    resp = client.get("/alert-check?token=sekret")
    body = resp.json()
    assert body["ok"] is False
    problems = {v["resource"]: v["problem"] for v in body["violations"]}
    assert problems["journal_rows"] == "stale"
    assert "orders" not in problems           # healthy
    assert problems["companies"] == "never synced"

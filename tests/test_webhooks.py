from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.testclient import TestClient

from sync import webhooks


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setenv("WEBHOOK_TOKEN", "sekret")
    fake = MagicMock()
    fake.enqueue = AsyncMock()
    fake.bq = MagicMock()
    monkeypatch.setattr(webhooks, "Ingestor", lambda: fake)
    application = webhooks.create_app()
    return TestClient(application), fake


def test_webhook_rejects_bad_token(app):
    client, _ = app
    resp = client.post("/webhook?token=wrong", json={"resource": "order", "id": "1"})
    assert resp.status_code == 403


def test_webhook_parses_idset_and_enqueues(app):
    client, fake = app
    resp = client.post(
        "/webhook?token=sekret",
        json={"resource": "order", "id": "1,2,3", "event": "modified"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"accepted": 3}
    fake.enqueue.assert_awaited_once_with("order", [1, 2, 3])


def test_webhook_destroyed_marks_deleted_without_fetch(app):
    client, fake = app
    resp = client.post(
        "/webhook?token=sekret",
        json={"resource": "product", "id": "42", "event": "destroyed"},
    )
    assert resp.status_code == 200
    fake.bq.mark_deleted.assert_called_once_with("products", "product_id", [42])
    fake.enqueue.assert_not_awaited()


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

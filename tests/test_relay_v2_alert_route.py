import hashlib
import json

from app.api.routes import internal
from app.core import server as server_config


def _request(**overrides):
    payload = overrides.pop("alert_payload", {"message": "danger"})
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    values = {
        "contract_version": 1,
        "idempotency_key": "alert-1",
        "payload_digest": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        "processing_job_id": "job-1",
        "frame_set_uid": "frame-1",
        "transition_event_id": "transition-1",
        "hazard_key": "subject:rule",
        "from_state": "CLEAR",
        "to_state": "DANGER",
        "from_version": 0,
        "to_version": 1,
        "severity": "danger",
        "observation_event_utc_ms": 1_000,
        "delivery_deadline_utc_ms": 9_999_999_999_999,
        "alert_payload": payload,
    }
    values.update(overrides)
    return values


def test_v2_alert_route_is_closed_without_creating_database(
    client,
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "disabled.db"
    monkeypatch.setattr(server_config, "STREAM_RELAY_V2_ALERT_RECEIVER_ENABLED", False)
    monkeypatch.setattr(
        server_config,
        "STREAM_RELAY_V2_ALERT_RECEIVER_DATABASE_PATH",
        str(path),
    )

    response = client.post("/internal/relay-v2/alerts", json=_request())

    assert response.status_code == 404
    assert not path.exists()


def test_v2_alert_route_applies_then_deduplicates_durably(
    client,
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "enabled.db"
    monkeypatch.setattr(server_config, "STREAM_RELAY_V2_ALERT_RECEIVER_ENABLED", True)
    monkeypatch.setattr(
        server_config,
        "STREAM_RELAY_V2_ALERT_RECEIVER_DATABASE_PATH",
        str(path),
    )
    internal._relay_v2_receiver.cache_clear()

    applied = client.post("/internal/relay-v2/alerts", json=_request())
    duplicate = client.post("/internal/relay-v2/alerts", json=_request())

    assert applied.status_code == 200
    assert applied.json()["status"] == "APPLIED"
    assert applied.json()["user_visible_effect_applied"] is True
    assert duplicate.status_code == 200
    assert duplicate.json()["status"] == "DUPLICATE"
    assert duplicate.json()["user_visible_effect_applied"] is False
    assert path.exists()

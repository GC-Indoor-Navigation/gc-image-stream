import hashlib
import json
import sqlite3
from dataclasses import replace

from app.services.alerts.relay_v2_receiver import (
    RelayV2AlertEnvelope,
    RelayV2AlertReceiver,
)


class Clock:
    def __init__(self, value=1_000):
        self.value = value

    def now(self):
        return self.value


def _digest(payload):
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _envelope(**overrides):
    payload = overrides.pop("alert_payload", {"message": "danger"})
    values = {
        "contract_version": 1,
        "idempotency_key": "alert-1",
        "payload_digest": _digest(payload),
        "processing_job_id": "job-1",
        "frame_set_uid": "frame-1",
        "transition_event_id": "transition-1",
        "hazard_key": "subject:rule",
        "from_state": "CLEAR",
        "to_state": "DANGER",
        "from_version": 0,
        "to_version": 1,
        "severity": "danger",
        "observation_event_utc_ms": 900,
        "delivery_deadline_utc_ms": 1_100,
        "alert_payload": payload,
    }
    values.update(overrides)
    return RelayV2AlertEnvelope(**values)


def test_applies_once_and_duplicate_survives_receiver_restart(tmp_path):
    clock = Clock()
    path = tmp_path / "alerts.db"
    receiver = RelayV2AlertReceiver(path, current_time_ms=clock.now)
    envelope = _envelope()

    applied = receiver.receive(envelope)
    duplicate = RelayV2AlertReceiver(path, current_time_ms=clock.now).receive(envelope)

    assert applied.status == "APPLIED"
    assert applied.user_visible_effect_applied is True
    assert duplicate.status == "DUPLICATE"
    assert duplicate.user_visible_effect_applied is False
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT count(*) FROM relay_v2_alert_effects").fetchone()[0] == 1


def test_same_id_with_different_digest_is_integrity_conflict(tmp_path):
    clock = Clock()
    path = tmp_path / "alerts.db"
    receiver = RelayV2AlertReceiver(path, current_time_ms=clock.now)
    receiver.receive(_envelope())

    conflict = receiver.receive(
        _envelope(payload_digest="f" * 64, alert_payload={"message": "changed"})
    )

    assert conflict.status == "CONFLICT"
    assert conflict.user_visible_effect_applied is False
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT count(*) FROM relay_v2_alert_conflicts").fetchone()[0] == 1


def test_payload_digest_must_match_canonical_payload(tmp_path):
    receiver = RelayV2AlertReceiver(tmp_path / "alerts.db", current_time_ms=Clock().now)

    result = receiver.receive(_envelope(payload_digest="0" * 64))

    assert result.status == "CONFLICT"
    assert result.receiver_hazard_version == 0


def test_hazard_versions_are_compare_and_set_in_order(tmp_path):
    receiver = RelayV2AlertReceiver(tmp_path / "alerts.db", current_time_ms=Clock().now)
    first = _envelope()
    receiver.receive(first)
    second_payload = {"message": "warning"}
    second = _envelope(
        idempotency_key="alert-2",
        transition_event_id="transition-2",
        from_state="DANGER",
        to_state="WARNING",
        from_version=1,
        to_version=2,
        severity="warning",
        alert_payload=second_payload,
        payload_digest=_digest(second_payload),
    )

    assert receiver.receive(second).status == "APPLIED"
    gap = receiver.receive(
        replace(
            second,
            idempotency_key="alert-gap",
            transition_event_id="transition-gap",
            from_version=4,
            to_version=5,
        )
    )

    assert gap.status == "VERSION_GAP"
    assert gap.receiver_hazard_version == 2


def test_rejected_event_id_is_tombstoned_and_cannot_be_reused(tmp_path):
    receiver = RelayV2AlertReceiver(tmp_path / "alerts.db", current_time_ms=Clock().now)
    rejected = _envelope(to_version=4)

    assert receiver.receive(rejected).status == "REJECTED"
    # A lost rejection response must be replayed as rejection, never upgraded
    # to DUPLICATE (which means a prior effect was applied successfully).
    assert receiver.receive(rejected).status == "REJECTED"
    changed = replace(rejected, payload_digest="a" * 64)
    assert receiver.receive(changed).status == "CONFLICT"


def test_late_apply_is_recorded_without_claiming_deadline_met(tmp_path):
    clock = Clock(value=1_100)
    path = tmp_path / "alerts.db"
    receiver = RelayV2AlertReceiver(path, current_time_ms=clock.now)

    result = receiver.receive(_envelope(delivery_deadline_utc_ms=1_100))

    assert result.status == "APPLIED"
    assert result.delivery_deadline_met is False
    with sqlite3.connect(path) as connection:
        stored = connection.execute(
            "SELECT delivery_deadline_met FROM relay_v2_alert_receipts"
        ).fetchone()[0]
    assert stored == 0

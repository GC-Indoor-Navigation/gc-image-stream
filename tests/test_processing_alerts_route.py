import time


def current_time_ms() -> int:
    return int(time.time() * 1000)


def make_alert_payload(**overrides):
    payload = {
        "event_id": "alert-1",
        "frame_set_id": 12,
        "relay_run_id": 3,
        "timestamp_ms": current_time_ms(),
        "severity": "warning",
        "distance_m": 0.62,
        "joint": "pelvis",
        "obstacle_id": "unknown",
        "ttl_ms": 5_000,
        "source": {
            "processor": "mmpose_triangulation",
            "camera_devices": [
                "android_device_001",
                "android_device_002",
            ],
        },
    }
    payload.update(overrides)
    return payload


def test_internal_processing_alerts_accepts_and_lists_recent_alert(client):
    response = client.post(
        "/internal/processing-alerts",
        json=make_alert_payload(),
    )

    assert response.status_code == 202
    body = response.json()
    assert body["accepted"] is True
    assert body["duplicate"] is False
    assert body["expired"] is False
    assert body["event_id"] == "alert-1"
    assert body["routing"] == {
        "camera_devices": [
            "android_device_001",
            "android_device_002",
        ],
        "session_id": None,
        "delivery_status": "not_delivered",
    }

    recent = client.get("/internal/processing-alerts/recent")

    assert recent.status_code == 200
    recent_body = recent.json()
    assert recent_body["status"]["received_count"] == 1
    assert recent_body["status"]["active_count"] == 1
    assert len(recent_body["items"]) == 1
    item = recent_body["items"][0]
    assert item["event_id"] == "alert-1"
    assert item["source"]["processor"] == "mmpose_triangulation"
    assert item["routing"]["camera_devices"] == [
        "android_device_001",
        "android_device_002",
    ]
    assert item["expires_at_ms"] == item["timestamp_ms"] + item["ttl_ms"]


def test_internal_processing_alerts_deduplicates_event_id(client):
    payload = make_alert_payload()

    first = client.post("/internal/processing-alerts", json=payload)
    second = client.post("/internal/processing-alerts", json=payload)

    assert first.status_code == 202
    assert second.status_code == 202
    assert second.json()["accepted"] is False
    assert second.json()["duplicate"] is True

    recent = client.get("/internal/processing-alerts/recent")

    assert len(recent.json()["items"]) == 1
    assert recent.json()["status"]["received_count"] == 2
    assert recent.json()["status"]["duplicate_count"] == 1


def test_internal_processing_alerts_excludes_expired_alerts(client):
    response = client.post(
        "/internal/processing-alerts",
        json=make_alert_payload(
            event_id="expired-alert",
            timestamp_ms=1,
            ttl_ms=1,
        ),
    )

    assert response.status_code == 202
    assert response.json()["accepted"] is False
    assert response.json()["expired"] is True

    recent = client.get("/internal/processing-alerts/recent")

    assert recent.json()["items"] == []
    assert recent.json()["status"]["expired_count"] == 1
    assert recent.json()["status"]["active_count"] == 0


def test_internal_processing_alerts_validates_payload(client):
    payload = make_alert_payload(severity="critical")

    response = client.post("/internal/processing-alerts", json=payload)

    assert response.status_code == 422

import time

from app.services.alerts import phone_alert_delivery_hub


def current_time_ms() -> int:
    return int(time.time() * 1000)


def make_alert_payload(**overrides):
    payload = {
        "event_id": "phone-alert-1",
        "frame_set_id": 12,
        "relay_run_id": 3,
        "timestamp_ms": current_time_ms(),
        "severity": "warning",
        "distance_m": 0.62,
        "joint": "pelvis",
        "obstacle_id": "unknown",
        "ttl_ms": 60_000,
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


def test_phone_alert_events_requires_subscription_key(client):
    response = client.get("/phone/alerts/events?once=true")

    assert response.status_code == 400
    assert response.json()["detail"] == "device_id or session_id is required"


def test_phone_alert_events_streams_matching_recent_alert(client):
    post_response = client.post(
        "/internal/processing-alerts",
        json=make_alert_payload(event_id="matching-alert"),
    )

    response = client.get(
        "/phone/alerts/events?device_id=android_device_001&once=true",
    )

    assert post_response.status_code == 202
    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    assert "event: processing_alert" in response.text
    assert '"event_id":"matching-alert"' in response.text
    assert '"delivery_status":"not_delivered"' in response.text


def test_phone_alert_events_filters_unrelated_recent_alert(client):
    client.post(
        "/internal/processing-alerts",
        json=make_alert_payload(event_id="unrelated-alert"),
    )

    response = client.get(
        "/phone/alerts/events?device_id=android_device_999&once=true",
    )

    assert response.status_code == 200
    assert "unrelated-alert" not in response.text


def test_phone_alert_events_routes_distinct_phone_to_subscribed_camera(client):
    client.post(
        "/internal/processing-alerts",
        json=make_alert_payload(event_id="camera-subscription-alert"),
    )

    response = client.get(
        "/phone/alerts/events"
        "?device_id=android_device_004"
        "&camera_device_id=android_device_001"
        "&once=true",
    )

    assert response.status_code == 200
    assert "camera-subscription-alert" in response.text


def test_phone_alert_events_uses_env_subscription_mapping(client, monkeypatch):
    monkeypatch.setenv(
        "PHONE_ALERT_SUBSCRIPTIONS",
        "android_device_004=android_device_001,android_device_002",
    )
    client.post(
        "/internal/processing-alerts",
        json=make_alert_payload(event_id="env-subscription-alert"),
    )

    response = client.get(
        "/phone/alerts/events?device_id=android_device_004&once=true",
    )

    assert response.status_code == 200
    assert "env-subscription-alert" in response.text


def test_processing_alert_publish_enqueues_matching_phone_subscription(client):
    subscription = phone_alert_delivery_hub.subscribe(
        device_ids=["android_device_001"],
    )
    try:
        response = client.post(
            "/internal/processing-alerts",
            json=make_alert_payload(event_id="live-alert"),
        )
        payload = subscription.next_event(timeout_sec=0.1)
    finally:
        phone_alert_delivery_hub.unsubscribe(subscription.subscription_id)

    assert response.status_code == 202
    assert payload is not None
    assert payload["event_id"] == "live-alert"
    assert payload["source"]["camera_devices"] == [
        "android_device_001",
        "android_device_002",
    ]


def test_processing_alert_publish_skips_unrelated_phone_subscription(client):
    subscription = phone_alert_delivery_hub.subscribe(
        device_ids=["android_device_999"],
    )
    try:
        response = client.post(
            "/internal/processing-alerts",
            json=make_alert_payload(event_id="not-for-this-phone"),
        )
        payload = subscription.next_event(timeout_sec=0.1)
    finally:
        phone_alert_delivery_hub.unsubscribe(subscription.subscription_id)

    assert response.status_code == 202
    assert payload is None


def test_phone_alert_status_returns_subscriber_metrics(client):
    subscription = phone_alert_delivery_hub.subscribe(
        device_ids=["android_device_004"],
        camera_device_ids=["android_device_001"],
    )
    try:
        response = client.get("/phone/alerts/status")
    finally:
        phone_alert_delivery_hub.unsubscribe(subscription.subscription_id)

    assert response.status_code == 200
    body = response.json()
    assert body["subscriber_count"] == 1
    assert body["subscriptions"][0]["device_ids"] == ["android_device_004"]
    assert body["subscriptions"][0]["camera_device_ids"] == ["android_device_001"]

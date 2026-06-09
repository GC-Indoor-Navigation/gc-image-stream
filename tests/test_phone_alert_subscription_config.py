import pytest

from app.core.phone_alerts import (
    parse_phone_alert_subscriptions,
    resolve_phone_alert_camera_device_ids,
)


def test_parse_phone_alert_subscriptions_maps_phone_to_cameras():
    parsed = parse_phone_alert_subscriptions(
        "android_device_004=android_device_001,android_device_002;"
        "android_device_005:android_device_003",
    )

    assert parsed == {
        "android_device_004": ("android_device_001", "android_device_002"),
        "android_device_005": ("android_device_003",),
    }


def test_parse_phone_alert_subscriptions_rejects_invalid_entry():
    with pytest.raises(RuntimeError, match="PHONE_ALERT_SUBSCRIPTIONS"):
        parse_phone_alert_subscriptions("android_device_004")


def test_resolve_phone_alert_camera_device_ids_prefers_explicit_then_env():
    resolved = resolve_phone_alert_camera_device_ids(
        phone_device_ids=["android_device_004"],
        explicit_camera_device_ids=["android_device_003"],
        subscription_mapping={
            "android_device_004": ("android_device_001", "android_device_002"),
        },
    )

    assert resolved == [
        "android_device_003",
        "android_device_001",
        "android_device_002",
    ]


def test_resolve_phone_alert_camera_device_ids_falls_back_to_phone_id():
    assert resolve_phone_alert_camera_device_ids(
        phone_device_ids=["android_device_004"],
    ) == ["android_device_004"]

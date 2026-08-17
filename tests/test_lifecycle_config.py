from app.runtime import lifecycle
from app.runtime.lifecycle import resolve_sync_expected_cameras
from app.services.ingest.adapters.adapter_runtime import CameraInputConfig


def make_camera_config(device_id: str, source_kind: str = "grpc"):
    return CameraInputConfig(
        device_id=device_id,
        source_kind=source_kind,
        source_url="",
        collect_interval_sec=1.0,
        capture_timeout_sec=10.0,
    )


def test_resolve_sync_expected_cameras_prefers_explicit_config():
    expected_cameras = resolve_sync_expected_cameras(
        configured_expected_cameras=["manual_camera"],
        grpc_camera_configs=[
            make_camera_config("android_device_001"),
        ],
    )

    assert expected_cameras == ["manual_camera"]


def test_resolve_sync_expected_cameras_falls_back_to_grpc_camera_configs():
    expected_cameras = resolve_sync_expected_cameras(
        configured_expected_cameras=[],
        grpc_camera_configs=[
            make_camera_config("android_device_001"),
            make_camera_config("android_device_002"),
        ],
    )

    assert expected_cameras == ["android_device_001", "android_device_002"]


def test_resolve_sync_expected_cameras_returns_empty_without_grpc_configs():
    expected_cameras = resolve_sync_expected_cameras(
        configured_expected_cameras=[],
        grpc_camera_configs=[],
    )

    assert expected_cameras == []


def test_selected_relay_target_uses_independent_v2_target(monkeypatch):
    monkeypatch.setattr(lifecycle, "STREAM_RELAY_ENABLED", False)
    monkeypatch.setattr(lifecycle, "STREAM_FRAME_SET_RELAY_ENABLED", False)
    monkeypatch.setattr(lifecycle, "STREAM_RELAY_V2_SHADOW_ENABLED", True)
    monkeypatch.setattr(lifecycle, "STREAM_RELAY_V2_TARGET", "processing:50053")

    assert lifecycle.resolve_selected_relay_target() == "processing:50053"


def test_selected_relay_target_keeps_legacy_owner_during_shadow(monkeypatch):
    monkeypatch.setattr(lifecycle, "STREAM_RELAY_ENABLED", True)
    monkeypatch.setattr(lifecycle, "STREAM_FRAME_SET_RELAY_ENABLED", False)
    monkeypatch.setattr(lifecycle, "STREAM_RELAY_TARGET", "processing:50051")
    monkeypatch.setattr(lifecycle, "STREAM_RELAY_V2_SHADOW_ENABLED", True)
    monkeypatch.setattr(lifecycle, "STREAM_RELAY_V2_TARGET", "processing:50053")

    assert lifecycle.resolve_selected_relay_target() == "processing:50051"

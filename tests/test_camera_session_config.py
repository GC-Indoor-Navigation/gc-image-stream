import pytest

from app.core.cameras import build_camera_session_configs_from_env


def test_build_camera_session_configs_from_env(monkeypatch):
    monkeypatch.setenv("CAMERA_SESSIONS", "camera1,camera2")
    monkeypatch.delenv("CAMERA_INPUT_TYPE", raising=False)
    monkeypatch.delenv("CAMERA1_INPUT_TYPE", raising=False)
    monkeypatch.delenv("CAMERA2_INPUT_TYPE", raising=False)
    monkeypatch.setenv("CAMERA1_STREAM_URL", "http://camera1.local/video")
    monkeypatch.setenv("CAMERA1_COLLECT_INTERVAL_SEC", "0.1")
    monkeypatch.setenv("CAMERA1_CAPTURE_TIMEOUT_SEC", "8")
    monkeypatch.setenv("CAMERA2_STREAM_URL", "http://camera2.local/video")

    configs = build_camera_session_configs_from_env()

    assert [config.device_id for config in configs] == ["camera1", "camera2"]
    assert configs[0].source_url == "http://camera1.local/video"
    assert configs[0].source_kind == "mjpeg"
    assert configs[0].collect_interval_sec == 0.1
    assert configs[0].capture_timeout_sec == 8.0
    assert configs[1].source_url == "http://camera2.local/video"
    assert configs[1].collect_interval_sec == 1.0
    assert configs[1].capture_timeout_sec == 10.0


def test_build_camera_session_configs_requires_stream_url(monkeypatch):
    monkeypatch.setenv("CAMERA_SESSIONS", "camera1")
    monkeypatch.delenv("CAMERA_INPUT_TYPE", raising=False)
    monkeypatch.delenv("CAMERA1_INPUT_TYPE", raising=False)
    monkeypatch.delenv("CAMERA1_STREAM_URL", raising=False)

    with pytest.raises(RuntimeError, match="CAMERA1_STREAM_URL"):
        build_camera_session_configs_from_env()


def test_build_camera_session_configs_supports_snapshot_input(monkeypatch):
    monkeypatch.setenv("CAMERA_SESSIONS", "camera1")
    monkeypatch.setenv("CAMERA1_INPUT_TYPE", "snapshot")
    monkeypatch.setenv("CAMERA1_SNAPSHOT_URL", "http://camera1.local/shot.jpg")
    monkeypatch.setenv("CAMERA1_COLLECT_INTERVAL_SEC", "0.5")

    configs = build_camera_session_configs_from_env()

    assert configs[0].device_id == "camera1"
    assert configs[0].source_kind == "snapshot"
    assert configs[0].source_url == "http://camera1.local/shot.jpg"
    assert configs[0].collect_interval_sec == 0.5


def test_build_camera_session_configs_uses_global_input_type(monkeypatch):
    monkeypatch.setenv("CAMERA_SESSIONS", "camera1")
    monkeypatch.setenv("CAMERA_INPUT_TYPE", "snapshot")
    monkeypatch.delenv("CAMERA1_INPUT_TYPE", raising=False)
    monkeypatch.setenv("CAMERA1_SNAPSHOT_URL", "http://camera1.local/shot.jpg")

    configs = build_camera_session_configs_from_env()

    assert configs[0].source_kind == "snapshot"
    assert configs[0].source_url == "http://camera1.local/shot.jpg"


def test_build_camera_session_configs_supports_grpc_input(monkeypatch):
    monkeypatch.setenv("CAMERA_SESSIONS", "camera1")
    monkeypatch.setenv("CAMERA_INPUT_TYPE", "grpc")

    configs = build_camera_session_configs_from_env()

    assert configs[0].source_kind == "grpc"
    assert configs[0].source_url == ""

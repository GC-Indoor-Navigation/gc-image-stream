import json
from pathlib import Path

import pytest

from camera import core
from camera import snapshot_collector


def test_build_config_reads_required_values(monkeypatch):
    monkeypatch.setenv("CAMERA_NAME", "camera1")
    monkeypatch.setenv("CAMERA_SNAPSHOT_URL", "http://camera.local/shot.jpg")
    monkeypatch.setenv("COLLECT_INTERVAL_SEC", "0.1")
    monkeypatch.setenv("STORAGE_DIR", "captures")
    monkeypatch.setenv("REGISTER_API_URL", "http://server.local/frames/register")
    monkeypatch.delenv("EXPERIMENT_ID", raising=False)

    config = snapshot_collector.build_config()

    assert config.camera_name == "camera1"
    assert config.source_url == "http://camera.local/shot.jpg"
    assert config.collect_interval_sec == 0.1
    assert config.legacy_storage_dir == "captures"
    assert config.legacy_register_api_url == "http://server.local/frames/register"
    assert config.capture_timeout_sec == 5.0
    assert config.register_timeout_sec == 5.0
    assert config.legacy_grpc_relay_target is None
    assert config.legacy_grpc_relay_timeout_sec is None
    assert config.storage_dir == config.legacy_storage_dir
    assert config.register_api_url == config.legacy_register_api_url
    assert config.experiment_log_dir == "experiment_logs"
    assert config.experiment_id is None


def test_build_config_reads_optional_grpc_relay_env(monkeypatch):
    monkeypatch.setenv("CAMERA_NAME", "camera1")
    monkeypatch.setenv("CAMERA_SNAPSHOT_URL", "http://camera.local/shot.jpg")
    monkeypatch.setenv("GRPC_RELAY_TARGET", "127.0.0.1:50051")
    monkeypatch.setenv("GRPC_RELAY_TIMEOUT_SEC", "9.5")

    config = snapshot_collector.build_config()

    assert config.legacy_grpc_relay_target == "127.0.0.1:50051"
    assert config.legacy_grpc_relay_timeout_sec == 9.5
    assert config.grpc_relay_target == config.legacy_grpc_relay_target


def test_build_config_reads_optional_experiment_env(monkeypatch, tmp_path):
    monkeypatch.setenv("CAMERA_NAME", "camera1")
    monkeypatch.setenv("CAMERA_SNAPSHOT_URL", "http://camera.local/shot.jpg")
    monkeypatch.setenv("EXPERIMENT_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("EXPERIMENT_ID", "relay-e2e-camera1")

    config = snapshot_collector.build_config()

    assert config.experiment_log_dir == str(tmp_path)
    assert config.experiment_id == "relay-e2e-camera1"


def test_build_config_can_disable_experiment_logging(monkeypatch):
    monkeypatch.setenv("CAMERA_NAME", "camera1")
    monkeypatch.setenv("CAMERA_SNAPSHOT_URL", "http://camera.local/shot.jpg")
    monkeypatch.setenv("EXPERIMENT_ENABLED", "false")

    config = snapshot_collector.build_config()

    assert config.experiment_log_dir is None


def test_experiment_recorder_writes_events_and_summary(monkeypatch, tmp_path):
    monkeypatch.setenv("CAMERA_NAME", "camera1")
    monkeypatch.setenv("CAMERA_SNAPSHOT_URL", "http://camera.local/shot.jpg")
    monkeypatch.setenv("EXPERIMENT_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("EXPERIMENT_ID", "relay e2e camera1")

    config = snapshot_collector.build_config()
    recorder = core.start_experiment_recorder(config, "snapshot")

    assert recorder is not None
    recorder.record_capture(
        timestamp_ms=1_234,
        sequence=1,
        capture_label="download",
        capture_elapsed=0.01,
        save_elapsed=0.02,
        cycle_elapsed=0.03,
        queue_size=2,
        scheduled_at=10.0,
        captured_at=10.005,
        image_bytes_size=100,
    )
    recorder.record_registration(
        status="registered",
        timestamp_ms=1_234,
        elapsed=0.04,
        queue_size=1,
        status_code=200,
    )
    recorder.record_relay_enqueued(
        timestamp_ms=1_234,
        sequence=1,
        image_bytes_size=100,
        queue_size=1,
    )
    core.close_experiment_recorder(recorder)

    run_dir = tmp_path / "relay-e2e-camera1"
    events = (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))

    assert any(json.loads(line)["event"] == "captured" for line in events)
    assert summary["captured_count"] == 1
    assert summary["registered_count"] == 1
    assert summary["relay_enqueued_count"] == 1
    assert summary["image_bytes_total"] == 100


def test_enqueue_relay_is_noop_without_queue():
    core.enqueue_relay(
        relay_queue=None,
        camera_name="camera1",
        timestamp_ms=1234,
        sequence=1,
        image_bytes=b"frame",
        save_path="storage/camera1/frame.jpg",
    )


def test_build_config_rejects_missing_snapshot_env(monkeypatch):
    monkeypatch.delenv("CAMERA_NAME", raising=False)
    monkeypatch.delenv("CAMERA_SNAPSHOT_URL", raising=False)

    with pytest.raises(ValueError, match="CAMERA_NAME, CAMERA_SNAPSHOT_URL"):
        snapshot_collector.build_config()


def test_build_save_path_uses_camera_date_layout(tmp_path):
    path = core.build_save_path(
        camera_name="camera2",
        timestamp_ms=1712321562400,
        base_dir=str(tmp_path),
    )

    expected_dir = tmp_path / "camera2" / "2024" / "04" / "05"
    assert Path(path).parent == expected_dir
    assert Path(path).name == "camera2_1712321562400.jpg"
    assert expected_dir.is_dir()


def test_calculate_next_capture_at_skips_missed_intervals():
    next_capture_at = core.calculate_next_capture_at(
        scheduled_at=10.0,
        interval_sec=0.1,
        now=10.35,
    )

    assert next_capture_at == pytest.approx(10.4)

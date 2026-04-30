import json
import re
from threading import Event

from app.services.stream import experiment_recorder as experiment_recorder_module
from app.services.ingest.adapters.mjpeg_ingest import (
    CameraSessionConfig,
    run_mjpeg_camera_session,
)
from app.infrastructure.grpc.processing_relay_client import ProcessingRelayService
from app.services.stream.stream_experiment import (
    clear_stream_experiment_recorder,
    configure_stream_experiment_recorder,
)
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.state import StreamState


def test_ingest_frame_records_primary_path_experiment(tmp_path, session_factory):
    recorder = configure_stream_experiment_recorder(
        experiment_log_dir=str(tmp_path),
        experiment_id="primary relay",
        duration_sec=None,
        expected_device_count=None,
        storage_dir="storage",
        relay_target="127.0.0.1:50051",
        camera_ids=["camera1"],
    )
    assert recorder is not None

    db = session_factory()
    relay_service = ProcessingRelayService()
    relay_service.configure(target="127.0.0.1:50051", enabled=True)
    state = StreamState()
    try:
        ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame",
            state=state,
            relay_service=relay_service,
        )
    finally:
        db.close()
        clear_stream_experiment_recorder()

    summary = json.loads((tmp_path / "primary-relay" / "summary.json").read_text(encoding="utf-8"))
    events = (tmp_path / "primary-relay" / "events.jsonl").read_text(encoding="utf-8")

    assert summary["collector_type"] == "stream_server"
    assert summary["registered_count"] == 1
    assert summary["relay_enqueued_count"] == 1
    assert summary["camera_ids"] == ["camera1"]
    assert '"event":"registered"' in events
    assert '"event":"relay_enqueued"' in events


def test_camera_session_records_capture_events(tmp_path, session_factory, storage_dir):
    recorder = configure_stream_experiment_recorder(
        experiment_log_dir=str(tmp_path),
        experiment_id="camera session",
        duration_sec=None,
        expected_device_count=None,
        storage_dir=str(storage_dir),
        relay_target="",
        camera_ids=["camera1"],
    )
    assert recorder is not None

    config = CameraSessionConfig(
        device_id="camera1",
        source_url="http://camera.local/video",
        collect_interval_sec=0,
    )

    def fake_frame_iterator(_session, _config):
        return iter([b"frame-1"])

    run_mjpeg_camera_session(
        config,
        Event(),
        db_factory=session_factory,
        frame_iterator_factory=fake_frame_iterator,
        timestamp_factory=lambda _sequence: 1001,
        max_frames=1,
    )
    clear_stream_experiment_recorder()

    summary = json.loads((tmp_path / "camera-session" / "summary.json").read_text(encoding="utf-8"))
    events = (tmp_path / "camera-session" / "events.jsonl").read_text(encoding="utf-8")

    assert summary["captured_count"] == 1
    assert summary["registered_count"] == 1
    assert '"event":"captured"' in events
    assert '"device_id":"camera1"' in events


def test_stream_experiment_recorder_generates_timestamped_run_id(tmp_path):
    recorder = configure_stream_experiment_recorder(
        experiment_log_dir=str(tmp_path),
        experiment_id="",
        duration_sec=None,
        expected_device_count=None,
        storage_dir="storage",
        relay_target="127.0.0.1:50051",
        camera_ids=["camera1"],
    )
    assert recorder is not None

    run_id = recorder.run_id
    clear_stream_experiment_recorder()

    assert re.fullmatch(r"stream-server-\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", run_id)
    assert (tmp_path / run_id / "summary.json").is_file()


def test_stream_experiment_recorder_stops_after_duration_limit(tmp_path, monkeypatch):
    recorder = configure_stream_experiment_recorder(
        experiment_log_dir=str(tmp_path),
        experiment_id="duration-window",
        duration_sec=1.0,
        expected_device_count=None,
        storage_dir="storage",
        relay_target="127.0.0.1:50051",
        camera_ids=["camera1"],
    )
    assert recorder is not None

    started = recorder.started_at_monotonic
    wall = recorder.started_at_ms

    monkeypatch.setattr(
        experiment_recorder_module.time,
        "monotonic",
        lambda: started + 1.1,
    )
    monkeypatch.setattr(
        experiment_recorder_module.time,
        "time",
        lambda: (wall + 1100) / 1000,
    )

    recorder.record_registration(
        status="registered",
        device_id="camera1",
        timestamp_ms=1000,
        elapsed=0.01,
        queue_size=0,
    )

    summary = json.loads((tmp_path / "duration-window" / "summary.json").read_text(encoding="utf-8"))
    events = (tmp_path / "duration-window" / "events.jsonl").read_text(encoding="utf-8")

    assert summary["duration_limit_s"] == 1.0
    assert summary["registered_count"] == 0
    assert '"event":"experiment_finished"' in events


def test_stream_experiment_gate_opens_after_expected_device_count(tmp_path):
    recorder = configure_stream_experiment_recorder(
        experiment_log_dir=str(tmp_path),
        experiment_id="grpc-gate",
        duration_sec=60.0,
        expected_device_count=2,
        storage_dir="storage",
        relay_target="127.0.0.1:50051",
        camera_ids=["camera1", "camera2"],
    )
    assert recorder is not None

    assert recorder.observe_device("android_01") is False
    assert recorder.started_at_monotonic is None
    assert recorder.observe_device("android_02") is True
    assert recorder.started_at_monotonic is not None

    clear_stream_experiment_recorder()

    summary = json.loads((tmp_path / "grpc-gate" / "summary.json").read_text(encoding="utf-8"))
    assert summary["gate_enabled"] is True
    assert summary["gate_opened"] is True
    assert summary["expected_device_count"] == 2
    assert summary["observed_device_ids"] == ["android_01", "android_02"]
    assert summary["gate_wait_duration_s"] is not None

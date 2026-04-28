import json
import re

from app.services.ingest.adapters.mjpeg_stream import extract_mjpeg_frames
from app.services.ingest.capture_timing import calculate_next_capture_at
from app.services.stream.experiment_recorder import (
    ExperimentContext,
    close_experiment_recorder,
    start_generic_experiment_recorder,
)


def test_extract_mjpeg_frames_returns_complete_jpegs():
    buffer = bytearray(
        b"--frame\r\n"
        b"Content-Type: image/jpeg\r\n\r\n"
        b"\xff\xd8frame1\xff\xd9"
        b"\r\n--frame\r\n"
        b"Content-Type: image/jpeg\r\n\r\n"
        b"\xff\xd8frame2\xff\xd9"
    )

    frames = extract_mjpeg_frames(buffer)

    assert frames == [b"\xff\xd8frame1\xff\xd9", b"\xff\xd8frame2\xff\xd9"]
    assert buffer == bytearray()


def test_extract_mjpeg_frames_keeps_incomplete_tail():
    buffer = bytearray(
        b"noise"
        b"\xff\xd8complete\xff\xd9"
        b"\xff\xd8partial"
    )

    frames = extract_mjpeg_frames(buffer)

    assert frames == [b"\xff\xd8complete\xff\xd9"]
    assert buffer == bytearray(b"\xff\xd8partial")


def test_calculate_next_capture_at_skips_missed_intervals():
    next_capture_at = calculate_next_capture_at(
        scheduled_at=10.0,
        interval_sec=0.1,
        now=10.35,
    )

    assert next_capture_at == 10.4


def test_generic_experiment_recorder_writes_events_and_summary(tmp_path):
    recorder = start_generic_experiment_recorder(
        ExperimentContext(
            collector_type="stream_server",
            run_name="stream-server",
            experiment_log_dir=str(tmp_path),
            experiment_id="relay smoke run1",
            summary_fields={"camera_ids": ["camera1"]},
        )
    )

    assert recorder is not None
    recorder.record_capture(
        timestamp_ms=1_234,
        sequence=1,
        capture_label="stream",
        capture_elapsed=0.01,
        save_elapsed=0.02,
        cycle_elapsed=0.03,
        queue_size=2,
        scheduled_at=10.0,
        captured_at=10.005,
        image_bytes_size=100,
        device_id="camera1",
    )
    recorder.record_registration(
        status="registered",
        timestamp_ms=1_234,
        elapsed=0.04,
        queue_size=1,
        status_code=200,
        device_id="camera1",
    )
    recorder.record_relay_enqueued(
        timestamp_ms=1_234,
        sequence=1,
        image_bytes_size=100,
        queue_size=1,
        device_id="camera1",
    )
    close_experiment_recorder(recorder)

    run_dir = tmp_path / "relay-smoke-run1"
    events = (run_dir / "events.jsonl").read_text(encoding="utf-8").splitlines()
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))

    assert any(json.loads(line)["event"] == "captured" for line in events)
    assert summary["captured_count"] == 1
    assert summary["registered_count"] == 1
    assert summary["relay_enqueued_count"] == 1
    assert summary["image_bytes_total"] == 100


def test_generic_experiment_recorder_generates_timestamped_run_id(tmp_path):
    recorder = start_generic_experiment_recorder(
        ExperimentContext(
            collector_type="stream_server",
            run_name="stream-server",
            experiment_log_dir=str(tmp_path),
            experiment_id=None,
        )
    )

    assert recorder is not None
    run_id = recorder.run_id
    close_experiment_recorder(recorder)

    assert re.fullmatch(r"stream-server-\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}", run_id)
    assert (tmp_path / run_id / "summary.json").is_file()

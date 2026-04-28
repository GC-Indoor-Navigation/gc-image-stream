from threading import Event

from app.models import Frame
from app.services.ingest.adapters.mjpeg_ingest import (
    CameraSessionConfig,
    run_mjpeg_camera_session,
)
from app.services.ingest.adapters.snapshot_ingest import run_snapshot_camera_session
from app.services.stream.state import stream_state


def test_run_mjpeg_camera_session_ingests_frames(session_factory, storage_dir):
    stream_state.clear()
    config = CameraSessionConfig(
        device_id="camera1",
        source_url="http://camera.local/video",
        collect_interval_sec=0,
    )
    stop_event = Event()

    def fake_frame_iterator(_session, _config):
        return iter([b"frame-1", b"frame-2"])

    run_mjpeg_camera_session(
        config,
        stop_event,
        db_factory=session_factory,
        frame_iterator_factory=fake_frame_iterator,
        timestamp_factory=lambda sequence: 1_000 + sequence,
        max_frames=2,
    )

    db = session_factory()
    try:
        frames = db.query(Frame).order_by(Frame.timestamp.asc()).all()
    finally:
        db.close()

    camera = stream_state.get_camera("camera1")

    assert [frame.timestamp for frame in frames] == [1_001, 1_002]
    assert all(str(storage_dir) in frame.file_path for frame in frames)
    assert camera is not None
    assert camera.frame_count == 2
    assert camera.latest_frame is not None
    assert camera.latest_frame.sequence == 2
    assert camera.latest_frame.timestamp == 1_002
    stream_state.clear()


def test_run_snapshot_camera_session_ingests_frames(session_factory, storage_dir, monkeypatch):
    stream_state.clear()
    config = CameraSessionConfig(
        device_id="camera1",
        source_url="http://camera.local/shot.jpg",
        collect_interval_sec=0.01,
        source_kind="snapshot",
    )
    stop_event = Event()
    frames = iter([b"frame-1", b"frame-2"])

    def fake_download_snapshot(_session, _config):
        return next(frames)

    monkeypatch.setattr(
        "app.services.ingest.adapters.snapshot_ingest.download_snapshot",
        fake_download_snapshot,
    )

    run_snapshot_camera_session(
        config,
        stop_event,
        db_factory=session_factory,
        timestamp_factory=lambda sequence: 2_000 + sequence,
        max_frames=2,
    )

    db = session_factory()
    try:
        stored_frames = db.query(Frame).order_by(Frame.timestamp.asc()).all()
    finally:
        db.close()

    camera = stream_state.get_camera("camera1")

    assert [frame.timestamp for frame in stored_frames] == [2_001, 2_002]
    assert all(str(storage_dir) in frame.file_path for frame in stored_frames)
    assert camera is not None
    assert camera.frame_count == 2
    assert camera.latest_frame is not None
    assert camera.latest_frame.sequence == 2
    assert camera.latest_frame.timestamp == 2_002
    stream_state.clear()

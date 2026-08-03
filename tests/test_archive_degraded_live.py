from pathlib import Path

import app.services.ingest.archive as archive_module
import app.services.ingest.ingest_pipeline as ingest_pipeline_module
from app.infrastructure.grpc.processing_relay_client import (
    ProcessingFrameSetRelayService,
    ProcessingRelayService,
)
from app.models import Frame, FrameSetManifest
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.state import StreamState
from app.services.sync import StreamSyncService


def build_services():
    sync_service = StreamSyncService()
    sync_service.configure(
        enabled=True,
        expected_cameras=["device-1"],
        window_ms=50,
    )
    frame_set_relay = ProcessingFrameSetRelayService()
    frame_set_relay.configure(target="127.0.0.1:50051", enabled=True)
    return sync_service, frame_set_relay


def ingest_v2(db, sync_service, frame_set_relay, **kwargs):
    return ingest_frame(
        db,
        device_id="device-1",
        timestamp_ms=1000,
        image_bytes=b"live-payload",
        sequence=1,
        session_id="device-1-session-a",
        camera_stream_id="camera-1",
        state=StreamState(),
        relay_service=ProcessingRelayService(),
        sync_service=sync_service,
        frame_set_relay_service=frame_set_relay,
        relay_mode="frame_set",
        **kwargs,
    )


def test_file_archive_failure_keeps_v2_live_frame_set(session_factory, storage_dir):
    sync_service, frame_set_relay = build_services()

    def fail_write(_path, _payload):
        assert frame_set_relay.status()["queue_size"] == 1
        raise OSError("disk unavailable")

    db = session_factory()
    try:
        result = ingest_v2(
            db,
            sync_service,
            frame_set_relay,
            archive_writer=fail_write,
        )

        assert result["frame"] is not None
        assert result["frame"].file_path is None
        assert result["frame"].archive_state == "ARCHIVE_DEGRADED_LIVE_ONLY"
        assert result["archive_state"] == "ARCHIVE_DEGRADED_LIVE_ONLY"
        assert result["frame_set_relay_enqueued"] is True
        assert result["manifest_persisted"] is False
        assert db.query(FrameSetManifest).count() == 0
        assert sync_service.status()["archive_degraded_count"] == 1

        relayed = frame_set_relay.queue.get_nowait()
        assert relayed.frames[0].image_bytes == b"live-payload"
        assert relayed.frames[0].HasField("frame_id") is False
        assert relayed.frames[0].HasField("file_path") is False
    finally:
        db.close()


def test_metadata_archive_failure_keeps_payload_live(
    session_factory,
    storage_dir,
    monkeypatch,
):
    sync_service, frame_set_relay = build_services()

    def fail_create_frame(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(archive_module, "create_frame", fail_create_frame)
    db = session_factory()
    try:
        result = ingest_v2(db, sync_service, frame_set_relay)

        assert result["frame"] is None
        assert result["camera_state"].latest_frame.frame_id is None
        assert result["archive_state"] == "ARCHIVE_DEGRADED_LIVE_ONLY"
        assert result["frame_set_relay_enqueued"] is True
        assert db.query(Frame).count() == 0

        relayed = frame_set_relay.queue.get_nowait()
        assert relayed.frames[0].image_bytes == b"live-payload"
        assert relayed.frames[0].HasField("frame_id") is False
        assert relayed.frames[0].HasField("file_path") is False
        assert list(Path(storage_dir).rglob("*.jpg"))
    finally:
        db.close()


def test_manifest_archive_failure_is_explicit_and_does_not_block_live(
    session_factory,
    storage_dir,
    monkeypatch,
):
    sync_service, frame_set_relay = build_services()

    def fail_manifest(*_args, **_kwargs):
        raise RuntimeError("manifest store unavailable")

    monkeypatch.setattr(
        ingest_pipeline_module,
        "persist_frame_set_manifest",
        fail_manifest,
    )
    db = session_factory()
    try:
        result = ingest_v2(db, sync_service, frame_set_relay)

        assert result["frame"].archive_state == "ARCHIVE_DURABLE"
        assert result["archive_state"] == "ARCHIVE_DEGRADED_LIVE_ONLY"
        assert "manifest archive failed" in result["archive_error"]
        assert result["frame_set_relay_enqueued"] is True
        assert db.query(FrameSetManifest).count() == 0
        assert sync_service.status()["last_archive_state"] == (
            "ARCHIVE_DEGRADED_LIVE_ONLY"
        )
    finally:
        db.close()


def test_durable_retry_recovers_degraded_frame_record(session_factory, storage_dir):
    sync_service = StreamSyncService()
    sync_service.configure(enabled=False, expected_cameras=[])

    def fail_write(_path, _payload):
        raise OSError("temporary disk failure")

    db = session_factory()
    try:
        first = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1000,
            image_bytes=b"same-payload",
            sequence=1,
            session_id="device-1-session-a",
            camera_stream_id="camera-1",
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            sync_service=sync_service,
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            archive_writer=fail_write,
        )
        second = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1000,
            image_bytes=b"same-payload",
            sequence=1,
            session_id="device-1-session-a",
            camera_stream_id="camera-1",
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            sync_service=sync_service,
            frame_set_relay_service=ProcessingFrameSetRelayService(),
        )

        assert first["frame"].id == second["frame"].id
        assert second["frame"].archive_state == "ARCHIVE_DURABLE"
        assert second["frame"].archive_error is None
        assert Path(second["frame"].file_path).read_bytes() == b"same-payload"
        assert db.query(Frame).count() == 1
    finally:
        db.close()

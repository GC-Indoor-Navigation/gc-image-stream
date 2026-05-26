from pathlib import Path

from app.infrastructure.grpc.processing_relay_client import ProcessingRelayService
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.state import StreamState


def test_ingest_frame_saves_file_registers_metadata_and_updates_state(
    session_factory,
    storage_dir,
):
    db = session_factory()
    state = StreamState()
    relay_service = ProcessingRelayService()
    try:
        result = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1712321562400,
            sequence=7,
            content_type="image/jpeg",
            image_bytes=b"frame-bytes",
            state=state,
            relay_service=relay_service,
        )

        frame = result["frame"]
        camera_state = result["camera_state"]

        assert frame.id is not None
        assert frame.device_id == "camera1"
        assert frame.timestamp == 1712321562400
        assert Path(frame.file_path).is_file()
        assert Path(frame.file_path).read_bytes() == b"frame-bytes"
        assert str(storage_dir) in frame.file_path

        assert camera_state.latest_frame is not None
        assert camera_state.latest_frame.frame_id == frame.id
        assert camera_state.latest_frame.sequence == 7
        assert camera_state.latest_frame.image_bytes_size == len(b"frame-bytes")
        assert result["relay_enqueued"] is False
    finally:
        db.close()


def test_ingest_frame_uses_explicit_received_time_for_stream_state(
    session_factory,
    storage_dir,
):
    db = session_factory()
    state = StreamState()
    relay_service = ProcessingRelayService()
    try:
        result = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1712321562400,
            sequence=7,
            content_type="image/jpeg",
            image_bytes=b"frame-bytes",
            state=state,
            relay_service=relay_service,
            received_at_ms=1712321562999,
        )

        latest_frame = result["camera_state"].latest_frame
        assert latest_frame is not None
        assert latest_frame.received_at_ms == 1712321562999
        assert list(result["camera_state"].recent_received_at_ms) == [1712321562999]
    finally:
        db.close()


def test_ingest_frame_uses_duplicate_registration_fallback(
    session_factory,
    storage_dir,
):
    db = session_factory()
    state = StreamState()
    relay_service = ProcessingRelayService()
    try:
        first = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"first",
            state=state,
            relay_service=relay_service,
        )
        second = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=2,
            image_bytes=b"second",
            state=state,
            relay_service=relay_service,
        )

        assert second["frame"].id == first["frame"].id
        assert second["frame"].file_path == first["frame"].file_path
        assert Path(second["frame"].file_path).read_bytes() == b"second"
        assert str(storage_dir) in second["frame"].file_path
        assert second["camera_state"].latest_frame is not None
        assert second["camera_state"].latest_frame.sequence == 2
    finally:
        db.close()


def test_ingest_frame_enqueues_relay_when_enabled(session_factory):
    db = session_factory()
    state = StreamState()
    relay_service = ProcessingRelayService()
    relay_service.configure(target="127.0.0.1:50051", enabled=True)
    try:
        result = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame",
            state=state,
            relay_service=relay_service,
        )

        assert result["relay_enqueued"] is True
        assert relay_service.status()["queue_size"] == 1
    finally:
        db.close()


def test_ingest_frame_can_force_raw_relay_mode(session_factory):
    db = session_factory()
    state = StreamState()
    relay_service = ProcessingRelayService()
    relay_service.configure(target="127.0.0.1:50051", enabled=True)
    try:
        result = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame",
            state=state,
            relay_service=relay_service,
            relay_mode="raw",
        )

        assert result["relay_mode"] == "raw"
        assert result["relay_enqueued"] is True
        assert relay_service.status()["queue_size"] == 1
    finally:
        db.close()


def test_ingest_frame_enqueues_relay_without_sequence(session_factory):
    db = session_factory()
    state = StreamState()
    relay_service = ProcessingRelayService()
    relay_service.configure(target="127.0.0.1:50051", enabled=True)
    try:
        result = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            image_bytes=b"frame",
            state=state,
            relay_service=relay_service,
        )

        queued_frame = relay_service.queue.get_nowait()
        assert result["relay_enqueued"] is True
        assert queued_frame.sequence == 0
        assert queued_frame.device_id == "camera1"
    finally:
        db.close()

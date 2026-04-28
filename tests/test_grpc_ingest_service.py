from pathlib import Path

import pytest

from app.models import Frame
from app.services.stream.grpc_ingest import (
    GrpcIngestService,
    IngestAck,
    IngestFrame,
    IngestMetadata,
    build_frame_ingest_stub,
    deserialize_ingest_ack,
    deserialize_ingest_frame,
    serialize_ingest_ack,
    serialize_ingest_frame,
)
from app.services.stream.relay import StreamRelayService
from app.services.stream.state import StreamState


def test_ingest_frame_round_trip_preserves_metadata_and_bytes():
    frame = IngestFrame(
        metadata=IngestMetadata(
            session_id="run-1",
            camera_id="camera1",
            device_id="android_a14_001",
            frame_sequence=7,
            device_timestamp_ms=1_234,
            device_monotonic_ns=9_876_543,
            width=1280,
            height=720,
            format="jpeg",
            orientation_deg=90,
            fps_target=10,
            focus_mode="fixed",
            exposure_locked=True,
            white_balance_locked=True,
            app_version="1.0.0",
        ),
        image_bytes=b"\xff\xd8frame\xff\xd9",
        content_length=9,
        app_sent_at_ms=1_240,
    )

    restored = deserialize_ingest_frame(serialize_ingest_frame(frame))

    assert restored == frame


def test_ingest_ack_round_trip_preserves_fields():
    ack = IngestAck(
        success=True,
        received_count=3,
        message="ok",
        server_ack_timestamp_ms=1_999,
        warnings=["content_length mismatch"],
    )

    restored = deserialize_ingest_ack(serialize_ingest_ack(ack))

    assert restored == ack


def test_grpc_ingest_service_streams_frames_into_ingest_path(session_factory, storage_dir):
    grpc = pytest.importorskip("grpc")

    service = GrpcIngestService(
        db_factory=session_factory,
        state=StreamState(),
        relay_service=StreamRelayService(),
    )
    service.configure(bind="127.0.0.1:0", enabled=True)
    service.start()

    try:
        channel = grpc.insecure_channel(service.status()["bind"])
        stub = build_frame_ingest_stub(channel)
        frames = [
            IngestFrame(
                metadata=IngestMetadata(
                    session_id="run-1",
                    camera_id="camera1",
                    device_id="android_a14_001",
                    frame_sequence=1,
                    device_timestamp_ms=1_000,
                    device_monotonic_ns=10,
                    width=1280,
                    height=720,
                    format="jpeg",
                    orientation_deg=90,
                    fps_target=10,
                ),
                image_bytes=b"frame-1",
                content_length=7,
                app_sent_at_ms=1_001,
            ),
            IngestFrame(
                metadata=IngestMetadata(
                    session_id="run-1",
                    camera_id="camera1",
                    device_id="android_a14_001",
                    frame_sequence=2,
                    device_timestamp_ms=1_100,
                    device_monotonic_ns=11,
                    width=1280,
                    height=720,
                    format="jpeg",
                    orientation_deg=90,
                    fps_target=10,
                ),
                image_bytes=b"frame-2",
                content_length=7,
                app_sent_at_ms=1_101,
            ),
        ]

        ack = stub(iter(frames), timeout=5.0)

        assert ack.success is True
        assert ack.received_count == 2
        assert ack.warnings == []

        db = session_factory()
        try:
            stored_frames = db.query(Frame).order_by(Frame.timestamp.asc()).all()
        finally:
            db.close()

        assert [(frame.device_id, frame.timestamp) for frame in stored_frames] == [
            ("camera1", 1_000),
            ("camera1", 1_100),
        ]
        assert Path(stored_frames[0].file_path).read_bytes() == b"frame-1"
        assert Path(stored_frames[1].file_path).read_bytes() == b"frame-2"
    finally:
        service.stop()

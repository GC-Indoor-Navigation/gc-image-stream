import json
from pathlib import Path

import pytest

from app.infrastructure.grpc.generated import (
    frame_ingest_pb2,
    frame_ingest_pb2_grpc,
    stream_ingest_pb2_grpc,
)
from app.infrastructure.grpc.grpc_ingest_server import (
    CollectorFrameMetadata,
    CollectorFramePacket,
    CollectorStreamFramesResponse,
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
from app.infrastructure.grpc.processing_relay_client import ProcessingRelayService
from app.models import Frame
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
        relay_service=ProcessingRelayService(),
    )
    service.configure(bind="127.0.0.1:0", enabled=True)
    service.start()

    try:
        channel = grpc.insecure_channel(service.status()["bind"])
        stub = stream_ingest_pb2_grpc.FrameIngestServiceStub(channel).StreamFrames
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


def test_collector_proto_field_numbers_match_android_contract():
    metadata_fields = frame_ingest_pb2.FrameMetadata.DESCRIPTOR.fields_by_name
    packet_fields = frame_ingest_pb2.FramePacket.DESCRIPTOR.fields_by_name

    assert packet_fields["metadata"].number == 1
    assert packet_fields["jpeg"].number == 2

    assert metadata_fields["camera_id"].number == 1
    assert metadata_fields["device_id"].number == 2
    assert metadata_fields["frame_sequence"].number == 3
    assert metadata_fields["device_timestamp_ms"].number == 4
    assert metadata_fields["device_monotonic_ns"].number == 5
    assert metadata_fields["sensor_timestamp_ns"].number == 16
    assert metadata_fields["focus_lock_requested"].number == 17
    assert metadata_fields["manual_exposure_requested"].number == 29
    assert metadata_fields["iso_applied"].number == 32
    assert metadata_fields["exposure_time_ns_applied"].number == 34
    assert metadata_fields["focal_length_mm"].number == 35


def test_collector_frame_packet_round_trip_preserves_metadata_and_bytes():
    packet = CollectorFramePacket(
        metadata=CollectorFrameMetadata(
            camera_id="camera1",
            device_id="android_a14_001",
            frame_sequence=11,
            device_timestamp_ms=22_000,
            device_monotonic_ns=333,
            width=1280,
            height=720,
            format="jpeg",
            fps_target=10,
            focus_mode="fixed",
            orientation_deg=90,
            sensor_timestamp_ns=444,
            focus_lock_requested=True,
            focus_lock_support="supported",
            focus_lock_applied="applied",
            exposure_lock_requested=True,
            exposure_lock_support="supported",
            exposure_lock_applied="applied",
            white_balance_lock_requested=True,
            white_balance_lock_support="supported",
            white_balance_lock_applied="applied",
            manual_exposure_requested=True,
            manual_exposure_support="supported",
            manual_exposure_applied="applied",
            iso_requested=200,
            iso_applied=180,
            exposure_time_ns_requested=4_000_000,
            exposure_time_ns_applied=3_800_000,
            focal_length_mm=5.43,
            resolution_support="1280x720",
        ),
        jpeg=b"\xff\xd8android\xff\xd9",
    )

    restored = CollectorFramePacket()
    restored.ParseFromString(packet.SerializeToString())

    assert restored == packet


def test_grpc_ingest_service_streams_android_collector_packets_into_ingest_path(
    session_factory,
    storage_dir,
):
    grpc = pytest.importorskip("grpc")

    service = GrpcIngestService(
        db_factory=session_factory,
        state=StreamState(),
        relay_service=ProcessingRelayService(),
    )
    service.configure(bind="127.0.0.1:0", enabled=True)
    service.start()

    try:
        channel = grpc.insecure_channel(service.status()["bind"])
        stub = frame_ingest_pb2_grpc.FrameIngestServiceStub(channel).StreamFrames
        packets = [
            CollectorFramePacket(
                metadata=CollectorFrameMetadata(
                    camera_id="rear_main",
                    device_id="android_a14_001",
                    frame_sequence=9,
                    device_timestamp_ms=55_000,
                    device_monotonic_ns=999,
                    width=1280,
                    height=720,
                    format="jpeg",
                    fps_target=10,
                    focus_mode="fixed",
                    orientation_deg=90,
                    sensor_timestamp_ns=1_234_567,
                    focus_lock_requested=True,
                    focus_lock_support="supported",
                    focus_lock_applied="applied",
                    exposure_lock_requested=True,
                    exposure_lock_support="supported",
                    exposure_lock_applied="applied",
                    white_balance_lock_requested=True,
                    white_balance_lock_support="supported",
                    white_balance_lock_applied="applied",
                    manual_exposure_requested=True,
                    manual_exposure_support="supported",
                    manual_exposure_applied="applied",
                    iso_requested=200,
                    iso_applied=180,
                    exposure_time_ns_requested=4_000_000,
                    exposure_time_ns_applied=3_800_000,
                    focal_length_mm=5.43,
                    resolution_support="1280x720",
                ),
                jpeg=b"\xff\xd8android-frame\xff\xd9",
            )
        ]

        response = stub(iter(packets), timeout=5.0)

        assert isinstance(response, CollectorStreamFramesResponse)
        assert response.received_frames == 1
        assert response.message == "collector ingest stream completed"

        db = session_factory()
        try:
            stored_frames = db.query(Frame).order_by(Frame.timestamp.asc()).all()
        finally:
            db.close()

        assert len(stored_frames) == 1
        stored_frame = stored_frames[0]
        assert stored_frame.device_id == "android_a14_001"
        assert stored_frame.timestamp == 55_000
        assert Path(stored_frame.file_path).name.endswith(
            "android_a14_001_rear_main_9.jpg"
        )
        assert Path(stored_frame.file_path).read_bytes() == b"\xff\xd8android-frame\xff\xd9"

        sidecar_payload = json.loads(
            Path(f"{stored_frame.file_path}.metadata.json").read_text(encoding="utf-8")
        )
        assert sidecar_payload["service"] == "gc.collector.v1.FrameIngestService"
        assert sidecar_payload["metadata"]["camera_id"] == "rear_main"
        assert sidecar_payload["metadata"]["device_id"] == "android_a14_001"
        assert sidecar_payload["metadata"]["frame_sequence"] == "9"
        assert sidecar_payload["metadata"]["device_timestamp_ms"] == "55000"
        assert sidecar_payload["metadata"]["device_monotonic_ns"] == "999"
        assert sidecar_payload["metadata"]["sensor_timestamp_ns"] == "1234567"
        assert sidecar_payload["metadata"]["focus_lock_requested"] is True
        assert sidecar_payload["metadata"]["focus_lock_support"] == "supported"
        assert sidecar_payload["metadata"]["manual_exposure_requested"] is True
        assert sidecar_payload["metadata"]["iso_requested"] == 200
        assert sidecar_payload["metadata"]["iso_applied"] == 180
        assert sidecar_payload["metadata"]["exposure_time_ns_requested"] == "4000000"
        assert sidecar_payload["metadata"]["exposure_time_ns_applied"] == "3800000"
        assert sidecar_payload["metadata"]["focal_length_mm"] == pytest.approx(5.43)
        assert sidecar_payload["metadata"]["resolution_support"] == "1280x720"
    finally:
        service.stop()


def test_collector_packets_prefer_device_id_and_fall_back_to_camera_id(session_factory, tmp_path):
    captured_calls: list[dict] = []

    def fake_ingest_func(db, **kwargs):
        captured_calls.append(kwargs)
        frame_path = tmp_path / kwargs["filename"]
        frame_path.write_bytes(kwargs["image_bytes"])
        return {
            "frame": Frame(
                id=1,
                device_id=kwargs["device_id"],
                timestamp=kwargs["timestamp_ms"],
                file_path=str(frame_path),
            ),
            "camera_state": None,
            "relay_enqueued": True,
        }

    service = GrpcIngestService(
        db_factory=session_factory,
        ingest_func=fake_ingest_func,
        state=StreamState(),
        relay_service=ProcessingRelayService(),
    )

    response = service._stream_collector_frames(
        iter(
            [
                CollectorFramePacket(
                    metadata=CollectorFrameMetadata(
                        camera_id="rear_main",
                        device_id="android_a14_001",
                        frame_sequence=1,
                        device_timestamp_ms=1_000,
                        format="jpeg",
                    ),
                    jpeg=b"frame-1",
                ),
                CollectorFramePacket(
                    metadata=CollectorFrameMetadata(
                        camera_id="rear_ultra_wide",
                        frame_sequence=2,
                        device_timestamp_ms=2_000,
                        format="jpeg",
                    ),
                    jpeg=b"frame-2",
                ),
            ]
        ),
        context=None,
    )

    assert response.received_frames == 2
    assert [call["device_id"] for call in captured_calls] == [
        "android_a14_001",
        "rear_ultra_wide",
    ]
    assert [call["sequence"] for call in captured_calls] == [1, 2]
    assert [call["timestamp_ms"] for call in captured_calls] == [1_000, 2_000]
    assert [call["image_bytes"] for call in captured_calls] == [b"frame-1", b"frame-2"]
    assert [call["content_type"] for call in captured_calls] == [
        "image/jpeg",
        "image/jpeg",
    ]
    assert [call["filename"] for call in captured_calls] == [
        "android_a14_001_rear_main_1.jpg",
        "rear_ultra_wide_rear_ultra_wide_2.jpg",
    ]

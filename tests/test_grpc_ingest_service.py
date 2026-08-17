import json
from pathlib import Path

import pytest

from app.infrastructure.grpc.generated import frame_ingest_pb2
from app.infrastructure.grpc.grpc_ingest_server import (
    FrameMetadata,
    FramePacket,
    GrpcIngestService,
    StreamFramesResponse,
    build_frame_ingest_stub,
    deserialize_frame_packet,
    serialize_frame_packet,
)
from app.infrastructure.grpc.processing_relay_client import ProcessingRelayService
from app.models import Frame
from app.services.stream import (
    clear_stream_experiment_recorder,
    configure_stream_experiment_recorder,
)
from app.services.stream.state import StreamState
from app.services.session_identity import AuthorizedSessionScope


def test_frame_packet_round_trip_preserves_metadata_and_bytes():
    frame = FramePacket(
        metadata=FrameMetadata(
            camera_id="rear_main",
            device_id="android_a14_001",
            frame_sequence=7,
            device_timestamp_ms=1_234,
            device_monotonic_ns=9_876_543,
            width=1280,
            height=720,
            format="jpeg",
            fps_target=10,
            focus_mode="fixed",
            orientation_deg=90,
            sensor_timestamp_ns=123_456,
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
            session_id="android_01_2026-05-17T14-32-10",
        ),
        jpeg=b"\xff\xd8frame\xff\xd9",
        session_scope=frame_ingest_pb2.SessionScope(
            tenant_id="tenant-1",
            site_id="site-1",
            capture_session_id="capture-1",
            processing_job_id="job-1",
            profile_digest="profile-1",
        ),
    )

    restored = deserialize_frame_packet(serialize_frame_packet(frame))

    assert restored == frame


def test_collector_proto_field_numbers_match_android_contract():
    metadata_fields = frame_ingest_pb2.FrameMetadata.DESCRIPTOR.fields_by_name
    packet_fields = frame_ingest_pb2.FramePacket.DESCRIPTOR.fields_by_name

    assert packet_fields["metadata"].number == 1
    assert packet_fields["jpeg"].number == 2
    assert packet_fields["session_scope"].number == 3

    scope_fields = frame_ingest_pb2.SessionScope.DESCRIPTOR.fields_by_name
    assert scope_fields["tenant_id"].number == 1
    assert scope_fields["site_id"].number == 2
    assert scope_fields["capture_session_id"].number == 3
    assert scope_fields["processing_job_id"].number == 4
    assert scope_fields["profile_digest"].number == 5

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
    assert metadata_fields["session_id"].number == 36


def test_frame_packet_parse_from_string_preserves_metadata_and_bytes():
    packet = FramePacket(
        metadata=FrameMetadata(
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
            session_id="android_01_2026-05-17T14-32-10",
            resolution_support="1280x720",
        ),
        jpeg=b"\xff\xd8android\xff\xd9",
    )

    restored = FramePacket()
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
        stub = build_frame_ingest_stub(channel)
        packets = [
            FramePacket(
                metadata=FrameMetadata(
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
                    session_id="android_01_2026-05-17T14-32-10",
                    resolution_support="1280x720",
                ),
                jpeg=b"\xff\xd8android-frame\xff\xd9",
            )
        ]

        response = stub(iter(packets), timeout=5.0)

        assert isinstance(response, StreamFramesResponse)
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
        assert "android_01_2026-05-17T14-32-10" in stored_frame.file_path
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
        assert sidecar_payload["metadata"]["session_id"] == "android_01_2026-05-17T14-32-10"
        assert sidecar_payload["metadata"]["resolution_support"] == "1280x720"
        server_payload = sidecar_payload["server"]
        assert isinstance(server_payload["received_at_ms"], int)
        assert isinstance(server_payload["received_monotonic_ns"], int)
        assert isinstance(server_payload["ingested_at_ms"], int)
        assert isinstance(server_payload["ingested_monotonic_ns"], int)
        assert server_payload["ingested_at_ms"] >= server_payload["received_at_ms"]
        assert server_payload["ingest_elapsed_ms"] >= 0
        assert server_payload["server_receive_sequence"] == 1
        assert server_payload["gate_start_timestamp_ms"] is None

        camera_state = service.state.get_camera("android_a14_001")
        assert camera_state is not None
        assert camera_state.latest_frame is not None
        assert camera_state.latest_frame.received_at_ms == server_payload["received_at_ms"]
    finally:
        service.stop()


def test_grpc_ingest_service_records_capture_metrics_for_experiment_window(
    tmp_path,
    session_factory,
):
    grpc = pytest.importorskip("grpc")

    recorder = configure_stream_experiment_recorder(
        experiment_log_dir=str(tmp_path),
        experiment_id="grpc-capture-metrics",
        duration_sec=None,
        expected_device_count=None,
        storage_dir="storage",
        relay_target="127.0.0.1:50051",
        camera_ids=["camera1"],
    )
    assert recorder is not None

    service = GrpcIngestService(
        db_factory=session_factory,
        state=StreamState(),
        relay_service=ProcessingRelayService(),
    )
    service.configure(bind="127.0.0.1:0", enabled=True)
    service.start()

    try:
        channel = grpc.insecure_channel(service.status()["bind"])
        stub = build_frame_ingest_stub(channel)
        response = stub(
            iter(
                [
                    FramePacket(
                        metadata=FrameMetadata(
                            camera_id="camera1",
                            device_id="android_01",
                            frame_sequence=1,
                            device_timestamp_ms=10_000,
                            format="jpeg",
                        ),
                        jpeg=b"grpc-frame",
                    )
                ]
            ),
            timeout=5.0,
        )

        assert response.received_frames == 1
    finally:
        service.stop()
        clear_stream_experiment_recorder()

    summary = json.loads(
        (tmp_path / "grpc-capture-metrics" / "summary.json").read_text(encoding="utf-8")
    )

    assert summary["captured_count"] == 1
    assert summary["registered_count"] == 1
    assert summary["image_bytes_total"] == len(b"grpc-frame")
    assert summary["average_fps"] > 0


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

    response = service._stream_frames(
        iter(
            [
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="rear_main",
                        device_id="android_a14_001",
                        frame_sequence=1,
                        device_timestamp_ms=1_000,
                        format="jpeg",
                        session_id="android_01_2026-05-17T14-32-10",
                    ),
                    jpeg=b"frame-1",
                ),
                FramePacket(
                    metadata=FrameMetadata(
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
    assert [call["session_id"] for call in captured_calls] == [
        "android_01_2026-05-17T14-32-10",
        None,
    ]


def test_grpc_ingest_service_drops_pre_gate_frames_until_all_devices_are_seen(
    session_factory,
    tmp_path,
):
    captured_calls: list[dict] = []

    def fake_ingest_func(db, **kwargs):
        captured_calls.append(kwargs)
        frame_path = tmp_path / kwargs["filename"]
        frame_path.write_bytes(kwargs["image_bytes"])
        return {
            "frame": Frame(
                id=len(captured_calls),
                device_id=kwargs["device_id"],
                timestamp=kwargs["timestamp_ms"],
                file_path=str(frame_path),
            ),
            "camera_state": None,
            "relay_enqueued": False,
        }

    service = GrpcIngestService(
        db_factory=session_factory,
        ingest_func=fake_ingest_func,
        state=StreamState(),
        relay_service=ProcessingRelayService(),
    )
    service.configure(bind="127.0.0.1:0", enabled=True, expected_device_count=2)

    response = service._stream_frames(
        iter(
            [
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_01",
                        device_id="android_01",
                        frame_sequence=1,
                        device_timestamp_ms=1_000,
                        format="jpeg",
                    ),
                    jpeg=b"drop-1",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_02",
                        device_id="android_02",
                        frame_sequence=1,
                        device_timestamp_ms=1_100,
                        format="jpeg",
                    ),
                    jpeg=b"drop-2",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_01",
                        device_id="android_01",
                        frame_sequence=2,
                        device_timestamp_ms=1_200,
                        format="jpeg",
                    ),
                    jpeg=b"keep-1",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_02",
                        device_id="android_02",
                        frame_sequence=2,
                        device_timestamp_ms=1_300,
                        format="jpeg",
                    ),
                    jpeg=b"keep-2",
                ),
            ]
        ),
        context=None,
    )

    assert response.received_frames == 2
    assert [call["device_id"] for call in captured_calls] == [
        "android_01",
        "android_02",
    ]
    assert [call["sequence"] for call in captured_calls] == [2, 2]
    assert [call["image_bytes"] for call in captured_calls] == [b"keep-1", b"keep-2"]
    assert service.status()["gate_enabled"] is True
    assert service.status()["gate_open"] is False
    assert service.status()["collection_started"] is True
    assert service.status()["collection_stopped"] is True
    assert service.status()["observed_device_ids"] == ["android_01", "android_02"]
    assert service.status()["gate_start_timestamp_ms"] == 1_100
    assert service.status()["pre_gate_dropped_count"] == 2
    assert service.status()["stale_after_gate_dropped_count"] == 0
    assert service.status()["first_accepted_timestamp_ms"] == 1_200


def test_grpc_ingest_service_drops_backlog_frames_older_than_gate_start(
    session_factory,
    tmp_path,
):
    captured_calls: list[dict] = []

    def fake_ingest_func(db, **kwargs):
        captured_calls.append(kwargs)
        frame_path = tmp_path / kwargs["filename"]
        frame_path.write_bytes(kwargs["image_bytes"])
        return {
            "frame": Frame(
                id=len(captured_calls),
                device_id=kwargs["device_id"],
                timestamp=kwargs["timestamp_ms"],
                file_path=str(frame_path),
            ),
            "camera_state": None,
            "relay_enqueued": False,
        }

    service = GrpcIngestService(
        db_factory=session_factory,
        ingest_func=fake_ingest_func,
        state=StreamState(),
        relay_service=ProcessingRelayService(),
    )
    service.configure(
        bind="127.0.0.1:0",
        enabled=True,
        expected_device_ids=["android_01", "android_02"],
    )

    response = service._stream_frames(
        iter(
            [
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_01",
                        device_id="android_01",
                        frame_sequence=1,
                        device_timestamp_ms=1_000,
                        format="jpeg",
                    ),
                    jpeg=b"pre-gate-1",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_02",
                        device_id="android_02",
                        frame_sequence=1,
                        device_timestamp_ms=1_500,
                        format="jpeg",
                    ),
                    jpeg=b"opens-gate",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_01",
                        device_id="android_01",
                        frame_sequence=2,
                        device_timestamp_ms=1_200,
                        format="jpeg",
                    ),
                    jpeg=b"stale-backlog",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_02",
                        device_id="android_02",
                        frame_sequence=2,
                        device_timestamp_ms=1_510,
                        format="jpeg",
                    ),
                    jpeg=b"keep-2",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_01",
                        device_id="android_01",
                        frame_sequence=3,
                        device_timestamp_ms=1_520,
                        format="jpeg",
                    ),
                    jpeg=b"keep-1",
                ),
            ]
        ),
        context=None,
    )

    assert response.received_frames == 2
    assert [call["timestamp_ms"] for call in captured_calls] == [1_510, 1_520]
    assert [call["image_bytes"] for call in captured_calls] == [b"keep-2", b"keep-1"]
    status = service.status()
    assert status["gate_start_timestamp_ms"] == 1_500
    assert status["pre_gate_dropped_count"] == 2
    assert status["stale_after_gate_dropped_count"] == 1
    assert status["first_accepted_timestamp_ms"] == 1_510


def test_grpc_ingest_service_gates_by_expected_device_ids(
    session_factory,
    tmp_path,
):
    captured_calls: list[dict] = []

    def fake_ingest_func(db, **kwargs):
        captured_calls.append(kwargs)
        frame_path = tmp_path / kwargs["filename"]
        frame_path.write_bytes(kwargs["image_bytes"])
        return {
            "frame": Frame(
                id=len(captured_calls),
                device_id=kwargs["device_id"],
                timestamp=kwargs["timestamp_ms"],
                file_path=str(frame_path),
            ),
            "camera_state": None,
            "relay_enqueued": False,
        }

    service = GrpcIngestService(
        db_factory=session_factory,
        ingest_func=fake_ingest_func,
        state=StreamState(),
        relay_service=ProcessingRelayService(),
    )
    service.configure(
        bind="127.0.0.1:0",
        enabled=True,
        expected_device_ids=["android_01", "android_02"],
    )

    response = service._stream_frames(
        iter(
            [
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_99",
                        device_id="unexpected_android",
                        frame_sequence=1,
                        device_timestamp_ms=900,
                        format="jpeg",
                    ),
                    jpeg=b"drop-unexpected",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_01",
                        device_id="android_01",
                        frame_sequence=1,
                        device_timestamp_ms=1_000,
                        format="jpeg",
                    ),
                    jpeg=b"drop-before-all-expected",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_02",
                        device_id="android_02",
                        frame_sequence=1,
                        device_timestamp_ms=1_100,
                        format="jpeg",
                    ),
                    jpeg=b"drop-open-gate",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_01",
                        device_id="android_01",
                        frame_sequence=2,
                        device_timestamp_ms=1_200,
                        format="jpeg",
                    ),
                    jpeg=b"keep-1",
                ),
                FramePacket(
                    metadata=FrameMetadata(
                        camera_id="camera_02",
                        device_id="android_02",
                        frame_sequence=2,
                        device_timestamp_ms=1_300,
                        format="jpeg",
                    ),
                    jpeg=b"keep-2",
                ),
            ]
        ),
        context=None,
    )

    assert response.received_frames == 2
    assert [call["device_id"] for call in captured_calls] == [
        "android_01",
        "android_02",
    ]
    assert [call["sequence"] for call in captured_calls] == [2, 2]
    assert [call["image_bytes"] for call in captured_calls] == [b"keep-1", b"keep-2"]
    assert service.status()["gate_enabled"] is True
    assert service.status()["gate_open"] is False
    assert service.status()["collection_started"] is True
    assert service.status()["collection_stopped"] is True
    assert service.status()["expected_device_count"] == 2
    assert service.status()["expected_device_ids"] == ["android_01", "android_02"]
    assert service.status()["missing_device_ids"] == ["android_01", "android_02"]
    assert service.status()["unexpected_device_ids"] == ["unexpected_android"]
    assert service.status()["gate_start_timestamp_ms"] == 1_100
    assert service.status()["pre_gate_dropped_count"] == 2
    assert service.status()["stale_after_gate_dropped_count"] == 0


def test_grpc_ingest_service_stops_collection_when_expected_device_disconnects():
    service = GrpcIngestService()
    service.configure(
        bind="127.0.0.1:0",
        enabled=True,
        expected_device_ids=["android_01", "android_02"],
    )

    service._mark_device_active("android_01")
    assert service._allow_ingest("android_01") is False

    service._mark_device_active("android_02")
    assert service._allow_ingest("android_02") is False

    status = service.status()
    assert status["gate_open"] is True
    assert status["collection_started"] is True
    assert status["collection_stopped"] is False
    assert status["active_device_ids"] == ["android_01", "android_02"]
    assert status["missing_device_ids"] == []
    assert service._allow_ingest("android_01") is True

    service._mark_stream_closed({"android_02"})

    status = service.status()
    assert status["gate_open"] is False
    assert status["collection_stopped"] is True
    assert status["collection_stop_reason"] == "expected device disconnected: android_02"
    assert status["active_device_ids"] == ["android_01"]
    assert status["missing_device_ids"] == ["android_02"]
    assert service._allow_ingest("android_01") is False

    service._mark_device_active("android_02")
    assert service._allow_ingest("android_02") is False


def test_grpc_ingest_service_keeps_device_active_until_all_streams_close():
    service = GrpcIngestService()
    service.configure(
        bind="127.0.0.1:0",
        enabled=True,
        expected_device_ids=["android_01", "android_02"],
    )

    service._mark_device_active("android_01")
    service._mark_device_active("android_01")
    service._mark_device_active("android_02")
    assert service._allow_ingest("android_02") is False

    service._mark_stream_closed({"android_01"})

    status = service.status()
    assert status["collection_stopped"] is False
    assert status["active_device_ids"] == ["android_01", "android_02"]

    service._mark_stream_closed({"android_01"})

    status = service.status()
    assert status["collection_stopped"] is True
    assert status["active_device_ids"] == ["android_02"]
    assert status["missing_device_ids"] == ["android_01"]


def test_authenticated_ingest_binds_token_scope_before_storage(session_factory):
    camera_id = "11111111-1111-1111-1111-111111111111"
    scope = _authorized_scope(camera_id)
    captured_calls = []

    def fake_ingest(db, **kwargs):
        captured_calls.append(kwargs)
        return {
            "frame": Frame(
                id=1,
                device_id=kwargs["device_id"],
                timestamp=kwargs["timestamp_ms"],
                file_path=None,
            )
        }

    service = GrpcIngestService(db_factory=session_factory, ingest_func=fake_ingest)
    service.configure(
        bind="127.0.0.1:0",
        session_token_verifier=_FakeVerifier(scope),
    )
    response = service._stream_frames(
        iter([_scoped_packet(camera_id, scope)]),
        _FakeContext("Bearer signed-token"),
    )

    assert response.received_frames == 1
    assert len(captured_calls) == 1
    assert captured_calls[0]["authorization_scope"] == scope


def test_authenticated_ingest_rejects_scope_substitution_before_storage(
    session_factory,
):
    camera_id = "11111111-1111-1111-1111-111111111111"
    scope = _authorized_scope(camera_id)
    captured_calls = []
    service = GrpcIngestService(
        db_factory=session_factory,
        ingest_func=lambda db, **kwargs: captured_calls.append(kwargs),
    )
    service.configure(
        bind="127.0.0.1:0",
        session_token_verifier=_FakeVerifier(scope),
    )
    packet = _scoped_packet(camera_id, scope)
    packet.session_scope.tenant_id = "99999999-9999-9999-9999-999999999999"

    with pytest.raises(_Aborted) as raised:
        service._stream_frames(
            iter([packet]),
            _FakeContext("Bearer signed-token"),
        )

    assert raised.value.code.name == "PERMISSION_DENIED"
    assert captured_calls == []


def test_authenticated_ingest_rejects_missing_credentials_before_iteration(
    session_factory,
):
    camera_id = "11111111-1111-1111-1111-111111111111"
    scope = _authorized_scope(camera_id)
    service = GrpcIngestService(db_factory=session_factory)
    service.configure(
        bind="127.0.0.1:0",
        session_token_verifier=_FakeVerifier(scope),
    )

    with pytest.raises(_Aborted) as raised:
        service._stream_frames(iter([]), _FakeContext(None))

    assert raised.value.code.name == "UNAUTHENTICATED"


class _FakeVerifier:
    def __init__(self, scope):
        self.scope = scope

    def verify(self, token):
        assert token == "signed-token"
        return self.scope

    def assert_active(self, scope):
        assert scope == self.scope


class _Aborted(RuntimeError):
    def __init__(self, code, detail):
        super().__init__(detail)
        self.code = code


class _FakeContext:
    def __init__(self, authorization):
        self.authorization = authorization

    def invocation_metadata(self):
        if self.authorization is None:
            return []
        return [type("Metadata", (), {"key": "authorization", "value": self.authorization})()]

    def abort(self, code, detail):
        raise _Aborted(code, detail)


def _authorized_scope(camera_id):
    return AuthorizedSessionScope(
        tenant_id="22222222-2222-2222-2222-222222222222",
        site_id="33333333-3333-3333-3333-333333333333",
        capture_session_id="44444444-4444-4444-4444-444444444444",
        processing_job_id="55555555-5555-5555-5555-555555555555",
        camera_ids=frozenset({camera_id}),
        profile_digest="a" * 64,
        authorized_subject="user-123",
        token_jti="55555555-5555-5555-5555-555555555555",
        expires_at=2_000_000_000,
    )


def _scoped_packet(camera_id, scope):
    return FramePacket(
        metadata=FrameMetadata(
            camera_id=camera_id,
            device_id="android-1",
            frame_sequence=1,
            device_timestamp_ms=1_000,
            format="jpeg",
            session_id="source-session-1",
        ),
        jpeg=b"frame",
        session_scope=frame_ingest_pb2.SessionScope(
            tenant_id=scope.tenant_id,
            site_id=scope.site_id,
            capture_session_id=scope.capture_session_id,
            processing_job_id=scope.processing_job_id,
            profile_digest=scope.profile_digest,
        ),
    )

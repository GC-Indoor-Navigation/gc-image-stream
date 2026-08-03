import json

from app.infrastructure.grpc.processing_relay_client import (
    ProcessingFrameSetRelayService,
    ProcessingRelayService,
)
from app.models import (
    CaptureRun,
    FrameSetDeliveryProjection,
    FrameSetManifest,
    FrameSetMember,
)
from app.services.identity import (
    IDENTITY_MODE_V2,
    build_capture_config_digest,
    build_source_frame_uid,
    canonical_json,
    sha256_bytes,
)
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.state import StreamState
from app.services.sync import (
    StreamSyncService,
    SyncFrameBufferManager,
    SyncInputFrame,
    SyncMatcher,
    get_newest_eligible_manifest,
    persist_frame_set_manifest,
)


def _sync_service() -> StreamSyncService:
    service = StreamSyncService()
    service.configure(
        enabled=True,
        expected_cameras=["device-1"],
        window_ms=50,
    )
    return service


def test_capture_config_digest_ignores_per_frame_observations():
    first = {
        "width": 1920,
        "height": 1080,
        "frame_sequence": "1",
        "device_timestamp_ms": "1000",
        "sensor_timestamp_ns": "1000000",
        "iso_applied": 100,
        "exposure_time_ns_applied": "8000000",
    }
    second = {
        **first,
        "frame_sequence": "2",
        "device_timestamp_ms": "1033",
        "sensor_timestamp_ns": "1033000",
        "iso_applied": 160,
        "exposure_time_ns_applied": "6000000",
    }

    assert build_capture_config_digest(first) == build_capture_config_digest(second)
    assert build_capture_config_digest(first) != build_capture_config_digest(
        {**first, "width": 1280}
    )


def test_ingest_persists_capture_and_sync_provenance_with_live_projection(
    session_factory,
    storage_dir,
):
    db = session_factory()
    frame_set_relay = ProcessingFrameSetRelayService()
    frame_set_relay.configure(target="127.0.0.1:50052", enabled=True)
    metadata = {
        "device_id": "device-1",
        "camera_id": "camera-1",
        "frame_sequence": "7",
        "device_timestamp_ms": "1000",
        "width": 1920,
        "height": 1080,
        "fps_target": 30,
        "focus_locked": True,
        "session_id": "session-a",
    }
    try:
        result = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1000,
            image_bytes=b"frame-7",
            sequence=7,
            session_id="session-a",
            camera_stream_id="camera-1",
            content_type="image/jpeg",
            received_at_ms=1100,
            capture_metadata=metadata,
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=frame_set_relay,
            sync_service=_sync_service(),
            relay_mode="frame_set",
        )

        frame = result["frame"]
        manifest = db.query(FrameSetManifest).one()
        member = db.query(FrameSetMember).one()
        projection = db.query(FrameSetDeliveryProjection).one()
        manifest_payload = json.loads(manifest.manifest_json)

        assert frame.content_type == "image/jpeg"
        assert frame.received_at_ms == 1100
        assert frame.capture_metadata_json == canonical_json(metadata)
        assert frame.capture_config_digest == build_capture_config_digest(metadata)
        assert manifest.sync_window_ms == 50
        assert manifest.synchronization_span_ms == 0
        assert manifest.synchronized_at_ms > 0
        assert manifest.member_count == 1
        assert manifest_payload["synchronization"] == {
            "algorithm": "minimum-span-v1",
            "anchor_timestamp_ms": 1000,
            "freshness_origin_ms": 1000,
            "span_ms": 0,
            "window_ms": 50,
        }
        assert manifest_payload["members"][0]["capture_config_digest"] == (
            frame.capture_config_digest
        )
        assert manifest_payload["members"][0]["capture_metadata_json"] == (
            canonical_json(metadata)
        )
        assert member.source_frame_uid == frame.source_frame_uid
        assert member.image_size == len(b"frame-7")
        assert projection.archive_state == "ARCHIVE_DURABLE"
        assert projection.live_state == "ELIGIBLE"
        assert projection.legacy_relay_state == "FRAME_SET_ENQUEUED"
    finally:
        db.close()


def test_capture_config_change_rotates_run_without_changing_source_session(
    session_factory,
    storage_dir,
):
    db = session_factory()
    sync_service = _sync_service()
    common = {
        "device_id": "device-1",
        "camera_id": "camera-1",
        "fps_target": 30,
        "session_id": "session-a",
    }
    try:
        first = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1000,
            image_bytes=b"frame-1",
            sequence=1,
            session_id="session-a",
            camera_stream_id="camera-1",
            capture_metadata={**common, "width": 1920, "height": 1080},
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=sync_service,
            relay_mode="off",
        )
        second = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1100,
            image_bytes=b"frame-2",
            sequence=2,
            session_id="session-a",
            camera_stream_id="camera-1",
            capture_metadata={**common, "width": 1280, "height": 720},
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=sync_service,
            relay_mode="off",
        )

        first_set = first["synchronized_frame_set"]
        second_set = second["synchronized_frame_set"]
        assert first_set.capture_session_id == second_set.capture_session_id
        assert first_set.capture_run_id != second_set.capture_run_id
        assert first_set.frame_set_id == 1
        assert second_set.frame_set_id == 1
        assert db.get(CaptureRun, first_set.capture_run_id).state == "CLOSED"
        assert db.get(CaptureRun, second_set.capture_run_id).state == "OPEN"
        first_projection = db.get(
            FrameSetDeliveryProjection,
            first_set.frame_set_uid,
        )
        second_projection = db.get(
            FrameSetDeliveryProjection,
            second_set.frame_set_uid,
        )
        newest = get_newest_eligible_manifest(db)
        assert first_projection.live_state == "SUPERSEDED_BEFORE_OFFER"
        assert first_projection.last_reason == "NEWER_FRAME_SET_AVAILABLE"
        assert second_projection.live_state == "ELIGIBLE"
        assert newest.frame_set_uid == second_set.frame_set_uid
    finally:
        db.close()


def test_raw_fallback_is_recorded_separately_from_live_frame_set_delivery(
    session_factory,
    storage_dir,
):
    db = session_factory()
    raw_relay = ProcessingRelayService()
    raw_relay.configure(target="127.0.0.1:50051", enabled=True)
    try:
        result = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1000,
            image_bytes=b"legacy-relay-frame",
            sequence=1,
            session_id="session-a",
            camera_stream_id="camera-1",
            capture_metadata={"width": 1920, "height": 1080},
            state=StreamState(),
            relay_service=raw_relay,
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=_sync_service(),
            relay_mode="raw",
        )

        projection = db.query(FrameSetDeliveryProjection).one()
        assert result["relay_enqueued"] is True
        assert projection.live_state == "ELIGIBLE"
        assert projection.legacy_relay_state == "RAW_ENQUEUED"
    finally:
        db.close()


def test_late_archive_cannot_replace_a_newer_eligible_manifest(session_factory):
    buffer = SyncFrameBufferManager(buffer_size=10)
    matcher = SyncMatcher(
        buffer_manager=buffer,
        expected_cameras=["device-1"],
        window_ms=50,
    )
    frame_sets = []
    for sequence, timestamp_ms in ((1, 1000), (2, 1100)):
        payload = f"frame-{sequence}".encode()
        source_frame_uid = build_source_frame_uid(
            "session-a",
            "device-1/camera-1",
            sequence,
        )
        stored = buffer.add_frame(
            SyncInputFrame(
                frame_id=sequence,
                device_id="device-1",
                timestamp_ms=timestamp_ms,
                sequence=sequence,
                content_type="image/jpeg",
                image_bytes=payload,
                file_path=f"frame-{sequence}.jpg",
                source_session_id="session-a",
                camera_stream_id="device-1/camera-1",
                source_frame_uid=source_frame_uid,
                content_digest=sha256_bytes(payload),
                identity_mode=IDENTITY_MODE_V2,
                archive_state="ARCHIVE_DURABLE",
                capture_config_digest=build_capture_config_digest(
                    {"width": 1920, "height": 1080}
                ),
            )
        )
        assert stored is not None
        frame_set = matcher.try_match(stored)
        assert frame_set is not None
        frame_sets.append(frame_set)

    older, newer = frame_sets
    db = session_factory()
    try:
        assert persist_frame_set_manifest(db, newer, created_at_ms=2000) is True
        assert persist_frame_set_manifest(db, older, created_at_ms=2100) is True

        newest = get_newest_eligible_manifest(db)
        older_projection = db.get(
            FrameSetDeliveryProjection,
            older.frame_set_uid,
        )
        assert newest.frame_set_uid == newer.frame_set_uid
        assert older_projection.live_state == "SUPERSEDED_BEFORE_OFFER"
    finally:
        db.close()

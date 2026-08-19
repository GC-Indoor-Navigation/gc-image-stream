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
from app.services.session_identity import (
    AuthorizedCameraIngestScope,
    AuthorizedSessionScope,
)
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


def test_authorized_ingest_persists_main_scope_in_frame_and_manifest(
    session_factory,
    storage_dir,
):
    db = session_factory()
    camera_id = "11111111-1111-1111-1111-111111111111"
    scope = _main_scope(camera_id=camera_id)
    try:
        result = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1000,
            image_bytes=b"authorized-frame",
            sequence=1,
            session_id="source-session-1",
            camera_stream_id=camera_id,
            capture_metadata={"width": 1920, "height": 1080},
            authorization_scope=scope,
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=_sync_service(),
            relay_mode="off",
        )

        frame = result["frame"]
        frame_set = result["synchronized_frame_set"]
        manifest = db.query(FrameSetManifest).one()
        member = db.query(FrameSetMember).one()
        payload = json.loads(manifest.manifest_json)

        assert frame.tenant_id == scope.tenant_id
        assert frame.site_id == scope.site_id
        assert frame.capture_session_id == scope.capture_session_id
        assert frame.processing_job_id == scope.processing_job_id
        assert frame.profile_digest == scope.profile_digest
        assert frame.authorized_subject == scope.authorized_subject
        assert frame.session_token_jti == scope.token_jti
        assert frame.authorized_camera_id == camera_id
        assert frame_set.capture_session_id == scope.capture_session_id
        assert manifest.tenant_id == scope.tenant_id
        assert manifest.site_id == scope.site_id
        assert manifest.processing_job_id == scope.processing_job_id
        assert manifest.profile_digest == scope.profile_digest
        assert manifest.authorized_subject == scope.authorized_subject
        assert manifest.session_token_jti == scope.token_jti
        assert member.authorized_camera_id == camera_id
        assert payload["authorization"]["tenant_id"] == scope.tenant_id
        assert payload["authorization"]["processing_job_id"] == scope.processing_job_id
        assert payload["members"][0]["authorized_camera_id"] == camera_id
    finally:
        db.close()


def test_sync_rejects_cross_session_frame_substitution(session_factory, storage_dir):
    db = session_factory()
    sync_service = StreamSyncService()
    sync_service.configure(
        enabled=True,
        expected_cameras=["device-1", "device-2"],
        window_ms=50,
    )
    first_scope = _main_scope(
        camera_id="11111111-1111-1111-1111-111111111111"
    )
    second_scope = AuthorizedSessionScope(
        **{
            **first_scope.__dict__,
            "capture_session_id": "99999999-9999-9999-9999-999999999999",
            "camera_ids": frozenset(
                {"22222222-2222-2222-2222-222222222222"}
            ),
        }
    )
    try:
        first = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1000,
            image_bytes=b"frame-1",
            sequence=1,
            session_id="source-1",
            camera_stream_id=next(iter(first_scope.camera_ids)),
            authorization_scope=first_scope,
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=sync_service,
            relay_mode="off",
        )
        second = ingest_frame(
            db,
            device_id="device-2",
            timestamp_ms=1005,
            image_bytes=b"frame-2",
            sequence=1,
            session_id="source-2",
            camera_stream_id=next(iter(second_scope.camera_ids)),
            authorization_scope=second_scope,
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=sync_service,
            relay_mode="off",
        )

        assert first["synchronized_frame_set"] is None
        assert second["synchronized_frame_set"] is None
        assert db.query(FrameSetManifest).count() == 0
        assert "authorization scope conflict" in sync_service.status()["last_reason"]
    finally:
        db.close()


def test_camera_scoped_participants_form_one_auditable_frame_set(
    session_factory,
    storage_dir,
):
    db = session_factory()
    sync_service = StreamSyncService()
    sync_service.configure(
        enabled=True,
        expected_cameras=["device-1", "device-2"],
        window_ms=50,
    )
    first_scope = _camera_scope(
        camera_id="11111111-1111-1111-1111-111111111111",
        camera_claim_id="66666666-6666-6666-6666-666666666661",
        subject="participant-1",
        token_jti="77777777-7777-7777-7777-777777777771",
        device_id="device-1",
    )
    second_scope = _camera_scope(
        camera_id="22222222-2222-2222-2222-222222222222",
        camera_claim_id="66666666-6666-6666-6666-666666666662",
        subject="participant-2",
        token_jti="77777777-7777-7777-7777-777777777772",
        device_id="device-2",
    )
    try:
        first = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1000,
            image_bytes=b"camera-1-frame",
            sequence=1,
            session_id="source-1",
            camera_stream_id=first_scope.camera_id,
            authorization_scope=first_scope,
            capture_metadata={"width": 1920, "height": 1080},
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=sync_service,
            relay_mode="off",
        )
        second = ingest_frame(
            db,
            device_id="device-2",
            timestamp_ms=1005,
            image_bytes=b"camera-2-frame",
            sequence=1,
            session_id="source-2",
            camera_stream_id=second_scope.camera_id,
            authorization_scope=second_scope,
            capture_metadata={"width": 1920, "height": 1080},
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=sync_service,
            relay_mode="off",
        )

        assert first["synchronized_frame_set"] is None
        frame_set = second["synchronized_frame_set"]
        assert frame_set is not None
        assert frame_set.capture_session_id == first_scope.capture_session_id
        assert frame_set.processing_job_id == first_scope.processing_job_id
        assert frame_set.authorized_subject is None
        assert frame_set.session_token_jti is None

        manifest = db.query(FrameSetManifest).one()
        members = db.query(FrameSetMember).order_by(
            FrameSetMember.authorized_camera_id
        ).all()
        payload = json.loads(manifest.manifest_json)
        assert manifest.authorized_subject is None
        assert manifest.session_token_jti is None
        assert [member.camera_claim_id for member in members] == [
            first_scope.camera_claim_id,
            second_scope.camera_claim_id,
        ]
        assert [member.authorized_subject for member in members] == [
            first_scope.authorized_subject,
            second_scope.authorized_subject,
        ]
        assert [member.session_token_jti for member in members] == [
            first_scope.token_jti,
            second_scope.token_jti,
        ]
        assert payload["authorization"]["processing_job_id"] == (
            first_scope.processing_job_id
        )
        assert payload["authorization"]["authorized_subject"] is None
        assert {
            member["camera_claim_id"] for member in payload["members"]
        } == {first_scope.camera_claim_id, second_scope.camera_claim_id}
    finally:
        db.close()


def _main_scope(*, camera_id):
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


def _camera_scope(
    *,
    camera_id,
    camera_claim_id,
    subject,
    token_jti,
    device_id,
):
    return AuthorizedCameraIngestScope(
        tenant_id="22222222-2222-2222-2222-222222222222",
        site_id="33333333-3333-3333-3333-333333333333",
        capture_session_id="44444444-4444-4444-4444-444444444444",
        processing_job_id="55555555-5555-5555-5555-555555555555",
        profile_digest="a" * 64,
        camera_claim_id=camera_claim_id,
        camera_id=camera_id,
        device_id=device_id,
        authorized_subject=subject,
        token_jti=token_jti,
        issued_at=1_000,
        expires_at=2_000_000_000,
    )

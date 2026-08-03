from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from app.db import Base, ensure_database_schema
from app.models import (
    CaptureRun,
    CaptureSession,
    Frame,
    FrameSetManifest,
    FrameSetMember,
)
from app.services.frames.service import FrameIntegrityError, create_frame
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.state import StreamState
from app.services.identity import (
    IDENTITY_MODE_LEGACY,
    IDENTITY_MODE_V2,
    build_source_frame_uid,
    sha256_bytes,
)
from app.services.sync import (
    SyncFrameBufferManager,
    SyncInputFrame,
    SyncMatcher,
    StreamSyncService,
    persist_frame_set_manifest,
)
from app.infrastructure.grpc.processing_relay_client import (
    ProcessingFrameSetRelayService,
    ProcessingRelayService,
)


def make_v2_frame(
    *,
    frame_id: int,
    timestamp_ms: int,
    source_session_id: str,
    sequence: int = 1,
) -> SyncInputFrame:
    payload = f"frame-{frame_id}".encode()
    camera_stream_id = "device-1/camera-1"
    return SyncInputFrame(
        frame_id=frame_id,
        device_id="device-1",
        timestamp_ms=timestamp_ms,
        sequence=sequence,
        content_type="image/jpeg",
        image_bytes=payload,
        file_path=f"storage/{source_session_id}/{frame_id}.jpg",
        source_session_id=source_session_id,
        camera_stream_id=camera_stream_id,
        source_frame_uid=build_source_frame_uid(
            source_session_id,
            camera_stream_id,
            sequence,
        ),
        content_digest=sha256_bytes(payload),
        identity_mode=IDENTITY_MODE_V2,
        archive_state="ARCHIVE_DURABLE",
    )


def match_one(matcher: SyncMatcher, buffer: SyncFrameBufferManager, frame):
    stored = buffer.add_frame(frame)
    assert stored is not None
    matched = matcher.try_match(stored)
    assert matched is not None
    return matched


def test_source_frame_uid_is_session_aware_and_deterministic():
    first = build_source_frame_uid("session-a", "device-1/camera-1", 7)
    retry = build_source_frame_uid("session-a", "device-1/camera-1", 7)
    next_session = build_source_frame_uid("session-b", "device-1/camera-1", 7)

    assert first == retry
    assert first != next_session


def test_create_frame_uses_v2_identity_and_rejects_digest_conflict(session_factory):
    db = session_factory()
    try:
        digest = sha256_bytes(b"same")
        first = create_frame(
            db,
            "device-1",
            1000,
            "first.jpg",
            source_session_id="session-a",
            camera_stream_id="device-1/camera-1",
            frame_sequence=1,
            content_digest=digest,
        )
        retry = create_frame(
            db,
            "device-1",
            1001,
            "retry.jpg",
            source_session_id="session-a",
            camera_stream_id="device-1/camera-1",
            frame_sequence=1,
            content_digest=digest,
        )
        next_session = create_frame(
            db,
            "device-1",
            1000,
            "next.jpg",
            source_session_id="session-b",
            camera_stream_id="device-1/camera-1",
            frame_sequence=1,
            content_digest=digest,
        )

        assert first.id == retry.id
        assert first.id != next_session.id
        assert first.identity_mode == IDENTITY_MODE_V2
        assert next_session.source_frame_uid != first.source_frame_uid

        try:
            create_frame(
                db,
                "device-1",
                1002,
                "conflict.jpg",
                source_session_id="session-a",
                camera_stream_id="device-1/camera-1",
                frame_sequence=1,
                content_digest=sha256_bytes(b"different"),
            )
        except FrameIntegrityError:
            pass
        else:
            raise AssertionError("digest conflict must fail")
    finally:
        db.close()


def test_matcher_rotates_capture_run_when_source_session_changes():
    buffer = SyncFrameBufferManager(buffer_size=10)
    matcher = SyncMatcher(
        buffer_manager=buffer,
        expected_cameras=["device-1"],
        window_ms=50,
    )

    first = match_one(
        matcher,
        buffer,
        make_v2_frame(frame_id=1, timestamp_ms=1000, source_session_id="session-a"),
    )
    second = match_one(
        matcher,
        buffer,
        make_v2_frame(frame_id=2, timestamp_ms=1100, source_session_id="session-b"),
    )

    assert first.identity_mode == IDENTITY_MODE_V2
    assert first.frame_set_uid
    assert first.manifest_digest
    assert first.capture_session_id != second.capture_session_id
    assert first.capture_run_id != second.capture_run_id
    assert first.frame_set_id == 1
    assert second.frame_set_id == 1


def test_matcher_routes_missing_stable_identity_to_legacy():
    buffer = SyncFrameBufferManager(buffer_size=10)
    matcher = SyncMatcher(
        buffer_manager=buffer,
        expected_cameras=["device-1"],
        window_ms=50,
    )
    legacy = SyncInputFrame(
        frame_id=1,
        device_id="device-1",
        timestamp_ms=1000,
        sequence=1,
        content_type="image/jpeg",
        image_bytes=b"legacy",
        file_path="legacy.jpg",
    )

    matched = match_one(matcher, buffer, legacy)

    assert matched.identity_mode == IDENTITY_MODE_LEGACY
    assert matched.frame_set_uid is None
    assert matcher.status()["legacy_identity_count"] == 1


def test_manifest_store_persists_exact_members_and_closes_rotated_run(session_factory):
    buffer = SyncFrameBufferManager(buffer_size=10)
    matcher = SyncMatcher(
        buffer_manager=buffer,
        expected_cameras=["device-1"],
        window_ms=50,
    )
    first = match_one(
        matcher,
        buffer,
        make_v2_frame(frame_id=1, timestamp_ms=1000, source_session_id="session-a"),
    )
    second = match_one(
        matcher,
        buffer,
        make_v2_frame(frame_id=2, timestamp_ms=1100, source_session_id="session-b"),
    )

    db = session_factory()
    try:
        assert persist_frame_set_manifest(db, first, created_at_ms=2000) is True
        assert persist_frame_set_manifest(db, first, created_at_ms=2001) is False
        assert persist_frame_set_manifest(db, second, created_at_ms=2100) is True

        manifests = db.query(FrameSetManifest).order_by(FrameSetManifest.created_at_ms).all()
        members = db.query(FrameSetMember).all()
        first_run = db.get(CaptureRun, first.capture_run_id)
        second_run = db.get(CaptureRun, second.capture_run_id)
        first_session = db.get(CaptureSession, first.capture_session_id)
        second_session = db.get(CaptureSession, second.capture_session_id)

        assert [item.frame_set_uid for item in manifests] == [
            first.frame_set_uid,
            second.frame_set_uid,
        ]
        assert len(members) == 2
        assert first_run.state == "CLOSED"
        assert first_run.close_reason == "CAPTURE_RUN_ROTATED"
        assert second_run.state == "OPEN"
        assert first_session.state == "CLOSED"
        assert first_session.close_reason == "SOURCE_SESSION_SET_CHANGED"
        assert second_session.state == "OPEN"
    finally:
        db.close()


def test_ingest_persists_v2_manifest_before_relay(session_factory, storage_dir):
    sync_service = StreamSyncService()
    sync_service.configure(
        enabled=True,
        expected_cameras=["device-1", "device-2"],
        window_ms=50,
    )
    db = session_factory()
    try:
        first = ingest_frame(
            db,
            device_id="device-1",
            timestamp_ms=1000,
            image_bytes=b"camera-1",
            sequence=1,
            session_id="device-1-session-a",
            camera_stream_id="camera-1",
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=sync_service,
        )
        second = ingest_frame(
            db,
            device_id="device-2",
            timestamp_ms=1010,
            image_bytes=b"camera-2",
            sequence=1,
            session_id="device-2-session-a",
            camera_stream_id="camera-1",
            state=StreamState(),
            relay_service=ProcessingRelayService(),
            frame_set_relay_service=ProcessingFrameSetRelayService(),
            sync_service=sync_service,
        )

        assert first["manifest_persisted"] is False
        assert second["manifest_persisted"] is True
        manifest = db.query(FrameSetManifest).one()
        assert manifest.frame_set_uid == second["synchronized_frame_set"].frame_set_uid
        assert db.query(FrameSetMember).count() == 2
    finally:
        db.close()


def test_schema_startup_closes_open_capture_run(session_factory):
    buffer = SyncFrameBufferManager(buffer_size=10)
    matcher = SyncMatcher(
        buffer_manager=buffer,
        expected_cameras=["device-1"],
        window_ms=50,
    )
    frame_set = match_one(
        matcher,
        buffer,
        make_v2_frame(frame_id=1, timestamp_ms=1000, source_session_id="session-a"),
    )
    db = session_factory()
    engine = db.get_bind()
    try:
        persist_frame_set_manifest(db, frame_set, created_at_ms=2000)
    finally:
        db.close()

    Base.metadata.create_all(engine)
    ensure_database_schema(engine)

    db = session_factory()
    try:
        run = db.get(CaptureRun, frame_set.capture_run_id)
        assert run.state == "CLOSED"
        assert run.close_reason == "PROCESS_RESTART"
    finally:
        db.close()


def test_legacy_frame_schema_is_backfilled_without_timestamp_uniqueness(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE frames (
                id INTEGER NOT NULL PRIMARY KEY,
                device_id VARCHAR NOT NULL,
                timestamp BIGINT NOT NULL,
                file_path VARCHAR NOT NULL,
                CONSTRAINT uq_frame_device_timestamp UNIQUE (device_id, timestamp)
            )
            """
        )
        connection.execute(
            text(
                "INSERT INTO frames (id, device_id, timestamp, file_path) "
                "VALUES (1, 'device-1', 1000, 'legacy.jpg')"
            )
        )

    ensure_database_schema(engine)

    column_metadata = {
        column["name"]: column for column in inspect(engine).get_columns("frames")
    }
    columns = set(column_metadata)
    factory = sessionmaker(bind=engine)
    db = factory()
    try:
        legacy = db.get(Frame, 1)
        v2 = create_frame(
            db,
            "device-1",
            1000,
            "v2.jpg",
            source_session_id="session-a",
            camera_stream_id="device-1/camera-1",
            frame_sequence=1,
            content_digest=sha256_bytes(b"v2"),
        )

        assert legacy.identity_mode == IDENTITY_MODE_LEGACY
        assert legacy.source_frame_uid is None
        assert v2.id != legacy.id
        assert "source_frame_uid" in columns
        assert "archive_state" in columns
        assert "content_type" in columns
        assert "received_at_ms" in columns
        assert "capture_config_digest" in columns
        assert "capture_metadata_json" in columns
        assert column_metadata["file_path"]["nullable"] is True
    finally:
        db.close()
        engine.dispose()

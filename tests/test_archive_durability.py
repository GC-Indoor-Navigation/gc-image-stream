from pathlib import Path

from sqlalchemy import create_engine, inspect

import app.services.ingest.archive as archive_module
from app.db.migrations import migrate_manifest_archive_schema
from app.infrastructure.grpc.processing_relay_client import (
    ProcessingFrameSetRelayService,
    ProcessingRelayService,
)
from app.models import (
    ArchiveReconciliationIssue,
    Frame,
    FrameSetDeliveryProjection,
    FrameSetManifest,
)
from app.services.frames.service import create_frame
from app.services.identity import sha256_bytes
from app.services.ingest.archive import durable_write_bytes
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.ingest.reconciliation import reconcile_archive
from app.services.stream.state import StreamState
from app.services.sync import StreamSyncService


def test_durable_write_fsyncs_and_atomically_replaces(tmp_path, monkeypatch):
    target = tmp_path / "camera" / "frame.jpg"
    fsync_calls = []
    real_fsync = archive_module.os.fsync

    def record_fsync(descriptor):
        fsync_calls.append(descriptor)
        return real_fsync(descriptor)

    monkeypatch.setattr(archive_module.os, "fsync", record_fsync)

    durable_write_bytes(target, b"durable-payload")

    assert target.read_bytes() == b"durable-payload"
    assert fsync_calls
    assert list(target.parent.glob(".*.tmp")) == []


def test_failed_atomic_replace_keeps_old_target_and_removes_temp(
    tmp_path,
    monkeypatch,
):
    target = tmp_path / "frame.jpg"
    target.write_bytes(b"old")

    def fail_replace(_source, _target):
        raise OSError("replace failed")

    monkeypatch.setattr(archive_module.os, "replace", fail_replace)

    try:
        durable_write_bytes(target, b"new")
    except OSError:
        pass
    else:
        raise AssertionError("replace failure must propagate")

    assert target.read_bytes() == b"old"
    assert list(tmp_path.glob(".*.tmp")) == []


def test_reconciliation_marks_missing_and_corrupt_data_and_lists_orphans(
    session_factory,
    storage_dir,
):
    sync_service = StreamSyncService()
    sync_service.configure(
        enabled=True,
        expected_cameras=["device-1"],
        window_ms=50,
    )
    db = session_factory()
    try:
        result = ingest_frame(
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
            frame_set_relay_service=ProcessingFrameSetRelayService(),
        )
        corrupt = result["frame"]
        Path(corrupt.file_path).write_bytes(b"evil-payload")

        missing_path = Path(storage_dir) / "device-2" / "missing.jpg"
        missing = create_frame(
            db,
            "device-2",
            1001,
            str(missing_path),
            content_digest=sha256_bytes(b"missing"),
            file_size=len(b"missing"),
        )
        orphan = Path(storage_dir) / "orphan.jpg"
        orphan.parent.mkdir(parents=True, exist_ok=True)
        orphan.write_bytes(b"orphan")
        partial = Path(storage_dir) / ".frame.jpg.crash.tmp"
        partial.write_bytes(b"partial")

        report = reconcile_archive(db, storage_dir, detected_at_ms=3000)

        db.refresh(corrupt)
        db.refresh(missing)
        manifest = db.query(FrameSetManifest).one()
        projection = db.query(FrameSetDeliveryProjection).one()
        issue_types = {
            issue.issue_type
            for issue in db.query(ArchiveReconciliationIssue).all()
        }

        assert report.checked_frames == 2
        assert report.degraded_frames == 2
        assert report.orphan_files == 1
        assert report.partial_files == 1
        assert corrupt.archive_error == "DIGEST_MISMATCH"
        assert missing.archive_error == "MISSING_FILE"
        assert manifest.archive_state == "ARCHIVE_DURABLE"
        assert projection.archive_state == "ARCHIVE_DEGRADED_LIVE_ONLY"
        assert projection.last_reason == "DIGEST_MISMATCH"
        assert {"DIGEST_MISMATCH", "MISSING_FILE", "ORPHAN_FILE", "PARTIAL_TEMP_FILE"} <= issue_types
    finally:
        db.close()


def test_manifest_archive_columns_migrate_additively(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'manifest.db'}")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE frame_set_manifests (
                frame_set_uid VARCHAR NOT NULL PRIMARY KEY,
                created_at_ms BIGINT NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            "INSERT INTO frame_set_manifests (frame_set_uid, created_at_ms) "
            "VALUES ('frame-set-1', 1000)"
        )

    assert migrate_manifest_archive_schema(engine) is True
    assert migrate_manifest_archive_schema(engine) is False

    columns = {
        column["name"]
        for column in inspect(engine).get_columns("frame_set_manifests")
    }
    with engine.connect() as connection:
        state = connection.exec_driver_sql(
            "SELECT archive_state FROM frame_set_manifests"
        ).scalar_one()

    assert {
        "archive_state",
        "archive_error",
        "sync_window_ms",
        "synchronized_at_ms",
        "member_count",
    } <= columns
    assert state == "ARCHIVE_DURABLE"
    engine.dispose()

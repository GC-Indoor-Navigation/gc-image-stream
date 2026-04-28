from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Frame, SyncFrame, SyncGroup
from app.services.frames.maintenance import (
    compress_old_dispatched_frames,
    get_frames_ready_for_compression,
)


def build_test_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return session_factory()


def test_get_frames_ready_for_compression_filters_success_age_and_state():
    db = build_test_session()
    try:
        old_success = Frame(
            device_id="camera1",
            timestamp=1_000,
            file_path="storage/camera1/old.jpg",
            compressed_at=None,
        )
        recent_success = Frame(
            device_id="camera1",
            timestamp=9_500,
            file_path="storage/camera1/recent.jpg",
            compressed_at=None,
        )
        old_failed = Frame(
            device_id="camera1",
            timestamp=1_500,
            file_path="storage/camera1/failed.jpg",
            compressed_at=None,
        )
        old_compressed = Frame(
            device_id="camera1",
            timestamp=1_700,
            file_path="storage/camera1/compressed.jpg",
            compressed_at=5_000,
        )
        db.add_all([old_success, recent_success, old_failed, old_compressed])
        db.flush()

        success_group = SyncGroup(group_timestamp=1_000, dispatch_status="success")
        failed_group = SyncGroup(group_timestamp=1_500, dispatch_status="failed")
        compressed_group = SyncGroup(group_timestamp=1_700, dispatch_status="success")
        recent_group = SyncGroup(group_timestamp=9_500, dispatch_status="success")
        db.add_all([success_group, failed_group, compressed_group, recent_group])
        db.flush()

        db.add_all([
            SyncFrame(sync_group_id=success_group.id, frame_id=old_success.id),
            SyncFrame(sync_group_id=failed_group.id, frame_id=old_failed.id),
            SyncFrame(sync_group_id=compressed_group.id, frame_id=old_compressed.id),
            SyncFrame(sync_group_id=recent_group.id, frame_id=recent_success.id),
        ])
        db.commit()

        frames = get_frames_ready_for_compression(db, older_than_ms=5_000, limit=10)

        assert [frame.id for frame in frames] == [old_success.id]
    finally:
        db.close()


def test_compress_old_dispatched_frames_marks_frames_after_compression(monkeypatch):
    db = build_test_session()
    try:
        frame = Frame(
            device_id="camera1",
            timestamp=1_000,
            file_path="storage/camera1/old.jpg",
            compressed_at=None,
        )
        db.add(frame)
        db.flush()

        group = SyncGroup(group_timestamp=1_000, dispatch_status="success")
        db.add(group)
        db.flush()

        db.add(SyncFrame(sync_group_id=group.id, frame_id=frame.id))
        db.commit()

        called = []

        def fake_compress(file_path: str, quality: int = 60):
            called.append((file_path, quality))

        monkeypatch.setattr(
            "app.services.frames.maintenance.compress_frame_file",
            fake_compress,
        )

        compressed_count = compress_old_dispatched_frames(
            db,
            compress_after_ms=5_000,
            quality=55,
            limit=10,
            now_ms=10_000,
        )

        db.refresh(frame)

        assert compressed_count == 1
        assert called == [("storage/camera1/old.jpg", 55)]
        assert frame.compressed_at == 10_000
    finally:
        db.close()

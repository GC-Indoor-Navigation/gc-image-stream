import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.services.frames.service import FrameIntegrityError, create_frame, get_frames


def build_test_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return session_factory()


def test_create_frame_returns_existing_record_for_duplicate():
    db = build_test_session()
    try:
        first = create_frame(db, "camera1", 1000, "storage/camera1/1000.jpg")
        second = create_frame(db, "camera1", 1000, "storage/camera1/1000-other.jpg")

        assert first.id == second.id
        assert second.file_path == "storage/camera1/1000.jpg"
    finally:
        db.close()


def test_get_frames_returns_latest_first():
    db = build_test_session()
    try:
        create_frame(db, "camera1", 1000, "storage/camera1/1000.jpg")
        create_frame(db, "camera2", 1200, "storage/camera2/1200.jpg")
        create_frame(db, "camera3", 1100, "storage/camera3/1100.jpg")

        frames = get_frames(db, limit=2)

        assert [frame.timestamp for frame in frames] == [1200, 1100]
    finally:
        db.close()


def test_create_frame_rolls_back_to_existing_row_after_integrity_error(monkeypatch):
    db = build_test_session()
    try:
        existing = create_frame(db, "camera1", 1000, "storage/camera1/1000.jpg")
        original_commit = db.commit
        state = {"calls": 0}

        def commit_with_duplicate_conflict():
            if state["calls"] == 0:
                state["calls"] += 1
                raise IntegrityError("insert", {}, Exception("duplicate"))
            original_commit()

        monkeypatch.setattr(db, "commit", commit_with_duplicate_conflict)

        duplicate = create_frame(db, "camera1", 1000, "storage/camera1/other.jpg")

        assert duplicate.id == existing.id
        assert duplicate.file_path == existing.file_path
    finally:
        db.close()


def test_duplicate_source_identity_rejects_authorization_scope_change():
    db = build_test_session()
    common = {
        "source_session_id": "source-1",
        "camera_stream_id": "device-1/camera-1",
        "frame_sequence": 1,
        "content_digest": "digest-1",
        "tenant_id": "tenant-1",
        "site_id": "site-1",
        "capture_session_id": "capture-1",
        "processing_job_id": "job-1",
        "profile_digest": "profile-1",
        "authorized_subject": "user-1",
        "session_token_jti": "job-1",
        "authorized_camera_id": "camera-1",
    }
    try:
        create_frame(db, "device-1", 1000, "first.jpg", **common)

        with pytest.raises(FrameIntegrityError, match="authorization scope"):
            create_frame(
                db,
                "device-1",
                1000,
                "second.jpg",
                **{**common, "tenant_id": "tenant-2"},
            )
    finally:
        db.close()

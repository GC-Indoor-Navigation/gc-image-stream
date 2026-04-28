import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.services.frames.service import create_frame, get_frames


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

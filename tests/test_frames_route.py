import asyncio
from io import BytesIO

import pytest
from fastapi import HTTPException, UploadFile

from app.api.routes import frames


def test_upload_frame_removes_saved_file_when_persistence_fails(monkeypatch, tmp_path):
    saved_path = tmp_path / "camera1_1712321562400.jpg"

    monkeypatch.setattr(
        frames,
        "build_frame_path",
        lambda device_id, timestamp, filename: str(saved_path),
    )
    monkeypatch.setattr(
        frames,
        "parse_filename",
        lambda filename: ("camera1", 1712321562400),
    )

    def fail_create_frame(**kwargs):
        raise RuntimeError("db write failed")

    monkeypatch.setattr(frames, "create_frame", fail_create_frame)

    upload = UploadFile(filename="camera1_1712321562400.jpg", file=BytesIO(b"frame-bytes"))

    class FakeSession:
        def __init__(self):
            self.rollback_called = False

        def rollback(self):
            self.rollback_called = True

    db = FakeSession()

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(frames.upload_frame(file=upload, db=db))

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Failed to persist uploaded frame."
    assert db.rollback_called is True
    assert saved_path.exists() is False

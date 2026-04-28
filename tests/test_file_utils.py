from pathlib import Path

import pytest

from app.infrastructure.storage import file_utils


def test_parse_filename_returns_device_and_timestamp():
    device_id, timestamp = file_utils.parse_filename("camera7_1712321562400.jpg")

    assert device_id == "camera7"
    assert timestamp == 1712321562400


def test_parse_filename_rejects_invalid_pattern():
    with pytest.raises(ValueError):
        file_utils.parse_filename("cameraA_invalid.jpg")


def test_build_frame_path_uses_storage_layout(monkeypatch, tmp_path):
    monkeypatch.setattr(file_utils, "STORAGE_DIR", str(tmp_path))

    path = file_utils.build_frame_path(
        device_id="camera1",
        timestamp=1712321562400,
        filename="camera1_1712321562400.jpg",
    )

    expected_dir = tmp_path / "camera1" / "2024" / "04" / "05"
    assert Path(path).parent == expected_dir
    assert Path(path).name == "1712321562400_camera1_1712321562400.jpg"
    assert expected_dir.is_dir()

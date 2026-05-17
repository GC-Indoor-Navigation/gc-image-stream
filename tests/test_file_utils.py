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


def test_build_frame_path_includes_session_directory_when_present(monkeypatch, tmp_path):
    monkeypatch.setattr(file_utils, "STORAGE_DIR", str(tmp_path))

    path = file_utils.build_frame_path(
        device_id="android_01",
        timestamp=1712321562400,
        filename="android_01_camera_01_15.jpg",
        session_id="android_01_2026-05-17T14-32-10",
    )

    expected_dir = (
        tmp_path
        / "android_01"
        / "2026-05-17T14-32-10"
    )
    assert Path(path).parent == expected_dir
    assert Path(path).name == "1712321562400_android_01_camera_01_15.jpg"
    assert expected_dir.is_dir()


def test_build_frame_path_keeps_session_directory_when_prefix_does_not_match(monkeypatch, tmp_path):
    monkeypatch.setattr(file_utils, "STORAGE_DIR", str(tmp_path))

    path = file_utils.build_frame_path(
        device_id="android_01",
        timestamp=1712321562400,
        filename="android_01_camera_01_15.jpg",
        session_id="capture_run_001",
    )

    expected_dir = (
        tmp_path
        / "android_01"
        / "capture_run_001"
    )
    assert Path(path).parent == expected_dir
    assert Path(path).name == "1712321562400_android_01_camera_01_15.jpg"
    assert expected_dir.is_dir()


def test_build_calibration_frame_path_uses_calibration_directory(monkeypatch, tmp_path):
    monkeypatch.setattr(file_utils, "STORAGE_DIR", str(tmp_path))

    path = file_utils.build_calibration_frame_path(
        device_id="android_01",
        timestamp=1712321562400,
        filename="android_01_camera_01_15.jpg",
    )

    expected_dir = tmp_path / "android_01" / "calibration"
    assert Path(path).parent == expected_dir
    assert Path(path).name == "1712321562400_android_01_camera_01_15.jpg"
    assert expected_dir.is_dir()

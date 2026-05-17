from pathlib import Path

from app.infrastructure.grpc.processing_relay_client import processing_relay_service
from app.models import Frame


def test_internal_calibration_upload_stores_frame_under_calibration_directory(client, session_factory):
    response = client.post(
        "/capture/internal-calibration",
        data={
            "device_id": "android_device_001",
            "camera_id": "camera_01",
            "frame_sequence": "1",
            "device_timestamp_ms": "1778820238123",
        },
        files={
            "file": ("capture.jpg", b"calibration-frame", "image/jpeg"),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["device_id"] == "android_device_001"
    assert payload["timestamp"] == 1778820238123
    assert "/android_device_001/calibration/" in payload["file_path"].replace("\\", "/")
    assert Path(payload["file_path"]).name == (
        "1778820238123_android_device_001_camera_01_1.jpg"
    )
    assert Path(payload["file_path"]).read_bytes() == b"calibration-frame"

    sidecar = Path(f"{payload['file_path']}.metadata.json")
    assert sidecar.is_file()
    sidecar_text = sidecar.read_text(encoding="utf-8")
    assert '"capture_type": "internal_calibration"' in sidecar_text
    assert '"device_id": "android_device_001"' in sidecar_text
    assert '"camera_id": "camera_01"' in sidecar_text

    db = session_factory()
    try:
        stored = db.query(Frame).one()
    finally:
        db.close()

    assert stored.device_id == "android_device_001"
    assert stored.timestamp == 1778820238123
    assert stored.file_path == payload["file_path"]


def test_internal_calibration_upload_does_not_enqueue_processing_relay(
    client,
):
    processing_relay_service.clear()
    response = client.post(
        "/capture/internal-calibration",
        data={
            "device_id": "android_device_001",
            "camera_id": "camera_01",
            "frame_sequence": "2",
            "device_timestamp_ms": "1778820239000",
        },
        files={
            "file": ("capture.jpg", b"calibration-frame-2", "image/jpeg"),
        },
    )

    assert response.status_code == 200
    assert processing_relay_service.status()["queue_size"] == 0
    assert processing_relay_service.status()["enqueued_count"] == 0

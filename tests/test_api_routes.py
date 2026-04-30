from pathlib import Path


def test_register_frame_is_idempotent(client):
    first = client.post(
        "/frames/register",
        data={
            "device_id": "camera1",
            "timestamp": 1000,
            "file_path": "storage/camera1/1000.jpg",
        },
    )
    second = client.post(
        "/frames/register",
        data={
            "device_id": "camera1",
            "timestamp": 1000,
            "file_path": "storage/camera1/other.jpg",
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert second.json()["file_path"] == "storage/camera1/1000.jpg"


def test_upload_frame_saves_file_and_metadata(client, read_file_bytes):
    response = client.post(
        "/frames/upload",
        files={
            "file": (
                "camera1_1712321562400.jpg",
                b"frame-bytes",
                "image/jpeg",
            )
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["device_id"] == "camera1"
    assert body["timestamp"] == 1712321562400
    assert Path(body["file_path"]).is_file()
    assert read_file_bytes(body["file_path"]) == b"frame-bytes"

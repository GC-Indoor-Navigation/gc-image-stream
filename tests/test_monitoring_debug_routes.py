from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.processing_relay_client import processing_relay_service
from app.services.stream.state import stream_state


def test_monitoring_cameras_returns_stream_state(client, session_factory):
    db = session_factory()
    try:
        ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame-1",
        )
        ingest_frame(
            db,
            device_id="camera2",
            timestamp_ms=1010,
            sequence=1,
            image_bytes=b"frame-2",
        )
    finally:
        db.close()

    response = client.get("/monitoring/cameras")

    assert response.status_code == 200
    items = response.json()["items"]
    assert [item["device_id"] for item in items] == ["camera1", "camera2"]
    assert items[0]["frame_count"] == 1
    assert items[0]["connected"] is True
    assert items[0]["status"] == "healthy"
    assert items[0]["latest_timestamp"] == 1000
    assert items[0]["latest_sequence"] == 1
    assert items[0]["latest_image_bytes"] == len(b"frame-1")


def test_monitoring_camera_returns_404_for_unknown_camera(client):
    response = client.get("/monitoring/cameras/unknown")

    assert response.status_code == 404
    assert response.json()["detail"] == "Camera state not found"


def test_debug_latest_frame_returns_state_file(client, session_factory):
    db = session_factory()
    try:
        ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame-bytes",
        )
    finally:
        db.close()

    response = client.get("/debug/cameras/camera1/latest-frame")

    assert response.status_code == 200
    assert response.content == b"frame-bytes"


def test_debug_latest_frame_falls_back_to_db(client):
    stream_state.clear()
    register = client.post(
        "/frames/upload",
        files={
            "file": (
                "camera1_1712321562400.jpg",
                b"stored-frame",
                "image/jpeg",
            )
        },
    )
    assert register.status_code == 200

    response = client.get("/debug/cameras/camera1/latest-frame")

    assert response.status_code == 200
    assert response.content == b"stored-frame"


def test_debug_timestamp_delta_uses_latest_stream_state(client, session_factory):
    db = session_factory()
    try:
        ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame-1",
        )
        ingest_frame(
            db,
            device_id="camera2",
            timestamp_ms=1037,
            sequence=1,
            image_bytes=b"frame-2",
        )
    finally:
        db.close()

    response = client.get("/debug/timestamp-delta")

    assert response.status_code == 200
    body = response.json()
    assert body["base_device_id"] == "camera2"
    assert body["base_timestamp"] == 1037
    assert body["items"] == [
        {
            "device_id": "camera1",
            "latest_timestamp": 1000,
            "delta_ms": -37,
        },
        {
            "device_id": "camera2",
            "latest_timestamp": 1037,
            "delta_ms": 0,
        },
    ]


def test_monitoring_relay_returns_relay_status(client):
    processing_relay_service.clear()
    processing_relay_service.configure(target="127.0.0.1:50051", enabled=True)

    response = client.get("/monitoring/relay")

    assert response.status_code == 200
    assert response.json()["enabled"] is True
    assert response.json()["target"] == "127.0.0.1:50051"


def test_monitoring_events_returns_sse_snapshot(client, session_factory):
    db = session_factory()
    try:
        ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame-1",
        )
    finally:
        db.close()

    response = client.get("/monitoring/events?once=true")

    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    assert '"device_id":"camera1"' in response.text
    assert '"relay"' in response.text
    assert '"timestamp_delta"' in response.text


def test_monitoring_viewer_returns_html(client):
    response = client.get("/monitoring/viewer")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "GC Monitoring" in response.text
    assert 'new EventSource("/monitoring/events")' in response.text


def test_debug_viewer_returns_html(client):
    response = client.get("/debug/viewer")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "GC Debug Viewer" in response.text
    assert 'new EventSource("/monitoring/events")' in response.text
    assert "/debug/timestamp-delta" in response.text

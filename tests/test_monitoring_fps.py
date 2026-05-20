from app.services.monitoring.service import (
    estimate_capture_fps,
    estimate_ingest_fps,
    serialize_camera_state,
)
from app.services.stream.state import StreamState


def test_monitoring_fps_uses_capture_timestamps_for_estimated_fps():
    state = StreamState()

    for index in range(10):
        state.update_frame(
            frame_id=index + 1,
            device_id="camera1",
            timestamp=100_000 + (index * 100),
            sequence=index + 1,
            file_path=f"storage/camera1/{index + 1}.jpg",
            content_type="image/jpeg",
            image_bytes_size=100,
            received_at_ms=1_000 + index,
        )

    camera = state.get_camera("camera1")
    assert camera is not None

    assert round(estimate_capture_fps(camera), 2) == 10.0
    assert round(estimate_ingest_fps(camera, now_ms=1_009), 2) == 1000.0

    payload = serialize_camera_state(camera, now_ms=1_009)
    assert round(payload["estimated_fps"], 2) == 10.0
    assert round(payload["estimated_capture_fps"], 2) == 10.0
    assert round(payload["estimated_ingest_fps"], 2) == 1000.0

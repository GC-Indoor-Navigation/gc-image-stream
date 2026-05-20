from app.services.stream.state import StreamState


def test_stream_state_tracks_latest_frame_and_sequence_gap():
    state = StreamState()

    state.update_frame(
        frame_id=1,
        device_id="camera1",
        timestamp=1000,
        sequence=1,
        file_path="storage/camera1/1000.jpg",
        content_type="image/jpeg",
        image_bytes_size=100,
        received_at_ms=10_000,
    )
    camera = state.update_frame(
        frame_id=2,
        device_id="camera1",
        timestamp=1100,
        sequence=3,
        file_path="storage/camera1/1100.jpg",
        content_type="image/jpeg",
        image_bytes_size=120,
        received_at_ms=10_100,
    )

    assert camera.device_id == "camera1"
    assert camera.frame_count == 2
    assert camera.last_sequence == 3
    assert camera.sequence_gap_count == 1
    assert camera.latest_frame is not None
    assert camera.latest_frame.frame_id == 2
    assert camera.latest_frame.timestamp == 1100
    assert list(camera.recent_timestamps_ms) == [1000, 1100]
    assert list(camera.recent_received_at_ms) == [10_000, 10_100]


def test_stream_state_lists_cameras():
    state = StreamState()

    state.update_frame(
        frame_id=1,
        device_id="camera1",
        timestamp=1000,
        sequence=1,
        file_path="storage/camera1/1000.jpg",
        content_type="image/jpeg",
        image_bytes_size=100,
        received_at_ms=10_000,
    )
    state.update_frame(
        frame_id=2,
        device_id="camera2",
        timestamp=1005,
        sequence=1,
        file_path="storage/camera2/1005.jpg",
        content_type="image/jpeg",
        image_bytes_size=100,
        received_at_ms=10_005,
    )

    cameras = state.list_cameras()

    assert {camera.device_id for camera in cameras} == {"camera1", "camera2"}

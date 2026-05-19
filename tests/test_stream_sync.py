from app.infrastructure.grpc.processing_relay_client import (
    ProcessingFrameSetRelayService,
    ProcessingRelayService,
)
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.state import StreamState
from app.services.sync import (
    StreamSyncService,
    SyncFrameBufferManager,
    SyncInputFrame,
    SyncMatcher,
    stream_sync_service,
)


def make_frame(
    frame_id: int,
    device_id: str,
    timestamp_ms: int,
    sequence: int | None,
) -> SyncInputFrame:
    return SyncInputFrame(
        frame_id=frame_id,
        device_id=device_id,
        timestamp_ms=timestamp_ms,
        sequence=sequence,
        content_type="image/jpeg",
        image_bytes=b"frame",
        file_path=f"storage/{device_id}/{timestamp_ms}.jpg",
    )


def test_stream_sync_matcher_builds_frame_set_when_all_cameras_match():
    buffer_manager = SyncFrameBufferManager(buffer_size=120)
    matcher = SyncMatcher(
        buffer_manager=buffer_manager,
        expected_cameras=["camera1", "camera2", "camera3"],
        window_ms=30,
    )
    buffer_manager.add_frame(make_frame(1, "camera1", 1000, 1))
    buffer_manager.add_frame(make_frame(2, "camera2", 1010, 1))
    anchor = buffer_manager.add_frame(make_frame(3, "camera3", 990, 1))

    frame_set = matcher.try_match(anchor)

    assert frame_set is not None
    assert frame_set.anchor_timestamp_ms == 990
    assert frame_set.max_delta_ms == 20
    assert set(frame_set.frames) == {"camera1", "camera2", "camera3"}
    assert matcher.status()["matched_count"] == 1
    assert matcher.status()["last_frame_set_id"] == 1


def test_stream_sync_matcher_returns_none_until_all_cameras_are_present():
    buffer_manager = SyncFrameBufferManager(buffer_size=120)
    matcher = SyncMatcher(
        buffer_manager=buffer_manager,
        expected_cameras=["camera1", "camera2"],
        window_ms=30,
    )
    anchor = buffer_manager.add_frame(make_frame(1, "camera1", 1000, 1))

    assert matcher.try_match(anchor) is None
    status = matcher.status()
    assert status["missed_count"] == 1
    assert status["last_missing_cameras"] == ["camera2"]
    assert status["last_reason"] == "missing cameras inside sync window"


def test_stream_sync_matcher_does_not_reemit_same_frame_combination():
    buffer_manager = SyncFrameBufferManager(buffer_size=120)
    matcher = SyncMatcher(
        buffer_manager=buffer_manager,
        expected_cameras=["camera1", "camera2"],
        window_ms=30,
    )
    first = buffer_manager.add_frame(make_frame(1, "camera1", 1000, 1))
    second = buffer_manager.add_frame(make_frame(2, "camera2", 1005, 1))

    assert matcher.try_match(second) is not None
    assert matcher.try_match(first) is None
    status = matcher.status()
    assert status["matched_count"] == 1
    assert status["missed_count"] == 1
    assert status["last_missing_cameras"] == ["camera1", "camera2"]
    assert status["last_reason"] == "missing cameras inside sync window"


def test_stream_sync_matcher_does_not_reuse_matched_frames():
    buffer_manager = SyncFrameBufferManager(buffer_size=120)
    matcher = SyncMatcher(
        buffer_manager=buffer_manager,
        expected_cameras=["camera1", "camera2"],
        window_ms=50,
    )
    camera1_first = buffer_manager.add_frame(make_frame(1, "camera1", 1000, 1))
    camera2_first = buffer_manager.add_frame(make_frame(2, "camera2", 1010, 1))
    camera2_second = buffer_manager.add_frame(make_frame(3, "camera2", 1020, 2))

    first_match = matcher.try_match(camera2_first)
    second_match = matcher.try_match(camera2_second)

    assert first_match is not None
    assert set(
        frame.frame_id for frame in first_match.frames.values()
    ) == {1, 2}
    assert second_match is None
    status = matcher.status()
    assert status["matched_count"] == 1
    assert status["missed_count"] == 1
    assert status["last_missing_cameras"] == ["camera1"]
    assert status["last_reason"] == "missing cameras inside sync window"

    camera1_second = buffer_manager.add_frame(make_frame(4, "camera1", 1025, 2))
    third_match = matcher.try_match(camera1_second)

    assert third_match is not None
    assert set(
        frame.frame_id for frame in third_match.frames.values()
    ) == {3, 4}
    assert matcher.status()["matched_count"] == 2


def test_stream_sync_service_ignores_duplicate_registered_frame_id():
    service = StreamSyncService()
    service.configure(
        enabled=True,
        expected_cameras=["camera1"],
        window_ms=30,
    )

    first = service.handle_frame(make_frame(1, "camera1", 1000, 1))
    second = service.handle_frame(make_frame(1, "camera1", 1000, 2))

    assert first is not None
    assert second is None
    status = service.status()
    assert status["matched_count"] == 1
    assert status["buffer"]["duplicate_frame_count"] == 1


def test_ingest_frame_runs_stream_sync_without_changing_raw_relay(
    session_factory,
    storage_dir,
):
    db = session_factory()
    state = StreamState()
    sync_service = StreamSyncService()
    sync_service.configure(
        enabled=True,
        expected_cameras=["camera1", "camera2"],
        window_ms=30,
    )
    try:
        first = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame-1",
            state=state,
            sync_service=sync_service,
        )
        second = ingest_frame(
            db,
            device_id="camera2",
            timestamp_ms=1010,
            sequence=1,
            image_bytes=b"frame-2",
            state=state,
            sync_service=sync_service,
        )
    finally:
        db.close()

    assert first["relay_enqueued"] is False
    assert first["synchronized_frame_set"] is None
    assert second["relay_enqueued"] is False
    assert second["synchronized_frame_set"] is not None
    assert set(second["synchronized_frame_set"].frames) == {"camera1", "camera2"}
    assert sync_service.status()["matched_count"] == 1


def test_ingest_frame_enqueues_frame_set_relay_when_sync_matches(
    session_factory,
    storage_dir,
):
    db = session_factory()
    state = StreamState()
    sync_service = StreamSyncService()
    sync_service.configure(
        enabled=True,
        expected_cameras=["camera1", "camera2"],
        window_ms=30,
    )
    frame_set_relay_service = ProcessingFrameSetRelayService()
    frame_set_relay_service.configure(target="127.0.0.1:50051", enabled=True)
    try:
        first = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame-1",
            state=state,
            sync_service=sync_service,
            frame_set_relay_service=frame_set_relay_service,
        )
        second = ingest_frame(
            db,
            device_id="camera2",
            timestamp_ms=1010,
            sequence=1,
            image_bytes=b"frame-2",
            state=state,
            sync_service=sync_service,
            frame_set_relay_service=frame_set_relay_service,
        )
    finally:
        db.close()

    assert first["frame_set_relay_enqueued"] is False
    assert second["frame_set_relay_enqueued"] is True
    status = frame_set_relay_service.status()
    assert status["queue_size"] == 1
    assert status["last_frame_set_id"] == 1


def test_ingest_frame_uses_one_selected_relay_mode_when_both_services_enabled(
    session_factory,
    storage_dir,
):
    db = session_factory()
    state = StreamState()
    sync_service = StreamSyncService()
    sync_service.configure(
        enabled=True,
        expected_cameras=["camera1", "camera2"],
        window_ms=30,
    )
    relay_service = ProcessingRelayService()
    relay_service.configure(target="127.0.0.1:50051", enabled=True)
    frame_set_relay_service = ProcessingFrameSetRelayService()
    frame_set_relay_service.configure(target="127.0.0.1:50051", enabled=True)
    try:
        first = ingest_frame(
            db,
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            image_bytes=b"frame-1",
            state=state,
            relay_service=relay_service,
            sync_service=sync_service,
            frame_set_relay_service=frame_set_relay_service,
        )
        second = ingest_frame(
            db,
            device_id="camera2",
            timestamp_ms=1010,
            sequence=1,
            image_bytes=b"frame-2",
            state=state,
            relay_service=relay_service,
            sync_service=sync_service,
            frame_set_relay_service=frame_set_relay_service,
        )
    finally:
        db.close()

    assert first["relay_mode"] == "frame_set"
    assert second["relay_mode"] == "frame_set"
    assert first["relay_enqueued"] is False
    assert second["relay_enqueued"] is False
    assert first["frame_set_relay_enqueued"] is False
    assert second["frame_set_relay_enqueued"] is True
    assert relay_service.status()["queue_size"] == 0
    assert frame_set_relay_service.status()["queue_size"] == 1


def test_monitoring_sync_returns_stream_sync_status(client):
    response = client.get("/monitoring/sync")

    assert response.status_code == 200
    body = response.json()
    assert body["enabled"] is False
    assert body["matched_count"] == 0
    assert body["buffer"]["camera_count"] == 0


def test_monitoring_recent_sync_frame_sets_returns_matched_sets(
    client,
    session_factory,
):
    stream_sync_service.configure(
        enabled=True,
        expected_cameras=["camera1", "camera2"],
        window_ms=30,
    )
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

    response = client.get("/monitoring/sync/recent-frame-sets")

    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    frame_set = items[0]
    assert frame_set["frame_set_id"] == 1
    assert frame_set["anchor_timestamp_ms"] == 1010
    assert frame_set["max_delta_ms"] == 10
    assert set(frame_set["frames"]) == {"camera1", "camera2"}
    assert frame_set["frames"]["camera1"]["timestamp_ms"] == 1000
    assert frame_set["frames"]["camera1"]["image_size"] == len(b"frame-1")

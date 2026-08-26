from concurrent.futures import ThreadPoolExecutor

from app.services.sync import StreamSyncService
from test_sync_concurrency import make_frame


def service_for(*cameras, size=4):
    service = StreamSyncService()
    service.configure(True, list(cameras), window_ms=30, buffer_size=size)
    return service


def send(service, camera, sequence, timestamp=None, session="a"):
    return service.handle_frame(make_frame(
        device_id=camera,
        timestamp_ms=timestamp if timestamp is not None else sequence * 100,
        sequence=sequence,
        session_id=f"{camera}-{session}",
    ))


def test_late_sequence_and_timestamp_cannot_rewind_sync_progress():
    service = service_for("a", "b")
    send(service, "a", 3)
    assert send(service, "b", 3) is not None
    assert send(service, "a", 2, 310) is None
    assert send(service, "b", 2, 310) is None
    assert send(service, "a", 4, 290) is None
    state = service.status()
    assert state["matched_count"] == 1
    assert state["watermark_timestamp_ms"] == 300
    assert state["buffer"]["out_of_order_frame_count"] == 3
    assert all(camera["last_sequence"] == 3 for camera in state["buffer"]["cameras"])
    send(service, "a", 4)
    assert send(service, "b", 4).frame_set_id == 2


def test_duplicate_storm_emits_only_one_frame_set():
    service = service_for("a", "b")
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda camera: send(service, camera, 1), ["a", "b"] * 50))
    assert sum(result is not None for result in results) == 1
    assert service.status()["buffer"]["duplicate_frame_count"] == 98


def test_missing_camera_bounds_payload_index_and_recovers_at_live_tick():
    service = service_for("a", "b", "c")
    for sequence in range(1, 101):
        send(service, "a", sequence)
        send(service, "b", sequence)
    state = service.status()
    assert state["matched_count"] == 0
    assert all(camera["buffered_count"] <= 4 for camera in state["buffer"]["cameras"])
    assert state["buffer"]["retained_frame_count"] <= 8
    assert state["buffer"]["evicted_frame_count"] == 192
    assert len(service.buffer_manager._frames_by_key) <= 8
    recovered = send(service, "c", 100)
    assert recovered is not None
    assert {frame.sequence for frame in recovered.frames.values()} == {100}
    # An evicted old frame cannot become eligible again after dedup retention ends.
    assert send(service, "a", 1) is None
    assert service.status()["matched_count"] == 1


def test_long_run_does_not_retain_every_matched_payload_or_key():
    service = service_for("a", "b")
    for sequence in range(1, 201):
        send(service, "a", sequence)
        assert send(service, "b", sequence) is not None
    assert service.status()["buffer"]["retained_frame_count"] <= 8
    assert len(service.recent_frame_sets()) == 20
    assert len(getattr(service.matcher, "_used_frame_keys", ())) <= 8
    assert len(getattr(service.matcher, "_emitted_keys", ())) <= 4


def test_source_session_change_allows_sequence_restart():
    service = service_for("a")
    first = send(service, "a", 100)
    restarted = send(service, "a", 1, 10100, session="b")
    assert restarted is not None
    assert restarted.capture_run_id != first.capture_run_id
    assert restarted.frame_set_id == 1


def test_archive_completion_survives_payload_index_eviction():
    service = service_for("a", size=1)
    frame = make_frame(device_id="a", timestamp_ms=100, sequence=1, session_id="a-a")
    matched = service.handle_frame(frame)
    send(service, "a", 2, session="a")
    updated = service.finalize_frame_archive(
        frame, matched, frame_id=1, file_path="/archive/a.jpg",
        archive_state="ARCHIVE_DURABLE", archive_error=None,
    )
    assert updated.frames["a"].archive_state == "ARCHIVE_DURABLE"
    assert service.recent_frame_sets()[0].frames["a"].file_path == "/archive/a.jpg"


def test_better_new_candidate_retires_older_complete_candidate():
    service = service_for("a", "b")
    stored = []
    for camera, sequence, timestamp in (("a", 1, 100), ("b", 1, 105),
                                        ("a", 2, 200), ("b", 2, 200)):
        stored.append(service.buffer_manager.add_frame(make_frame(
            device_id=camera, timestamp_ms=timestamp, sequence=sequence,
            session_id=f"{camera}-a",
        )))
    latest = service.matcher.try_match(stored[-1])
    assert {member.sequence for member in latest.frames.values()} == {2}
    assert service.matcher.try_match(stored[0]) is None
    assert service.status()["dropped_superseded_count"] == 2
    assert service.status()["matched_count"] == 1

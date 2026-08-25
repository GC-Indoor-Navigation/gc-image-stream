from concurrent.futures import ThreadPoolExecutor

from app.services.identity import (
    IDENTITY_MODE_V2,
    build_source_frame_uid,
    sha256_bytes,
)
from app.services.sync import StreamSyncService, SyncInputFrame


def make_frame(
    *,
    device_id: str,
    timestamp_ms: int,
    sequence: int,
    session_id: str,
) -> SyncInputFrame:
    payload = f"{device_id}-{sequence}".encode()
    camera_stream_id = f"{device_id}/camera-1"
    return SyncInputFrame(
        frame_id=None,
        device_id=device_id,
        timestamp_ms=timestamp_ms,
        sequence=sequence,
        content_type="image/jpeg",
        image_bytes=payload,
        file_path=None,
        source_session_id=session_id,
        camera_stream_id=camera_stream_id,
        source_frame_uid=build_source_frame_uid(
            session_id,
            camera_stream_id,
            sequence,
        ),
        content_digest=sha256_bytes(payload),
        identity_mode=IDENTITY_MODE_V2,
    )


def configured_service(expected_cameras=("camera-1", "camera-2")):
    service = StreamSyncService()
    service.configure(
        enabled=True,
        expected_cameras=list(expected_cameras),
        window_ms=30,
        buffer_size=120,
    )
    return service


def test_concurrent_ingest_serializes_matcher_mutation():
    service = configured_service()
    frames = []
    for sequence in range(1, 51):
        timestamp = sequence * 100
        frames.extend(
            [
                make_frame(
                    device_id="camera-1",
                    timestamp_ms=timestamp,
                    sequence=sequence,
                    session_id="camera-1-session-a",
                ),
                make_frame(
                    device_id="camera-2",
                    timestamp_ms=timestamp + 5,
                    sequence=sequence,
                    session_id="camera-2-session-a",
                ),
            ]
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(service.handle_frame, frames))

    matched = [frame_set for frame_set in results if frame_set is not None]
    ids = sorted(frame_set.frame_set_id for frame_set in matched)
    status = service.status()

    assert ids == list(range(1, 51))
    assert len({frame_set.frame_set_uid for frame_set in matched}) == 50
    assert status["matched_count"] == 50
    assert status["duplicate_count"] == 0


def test_late_archive_completion_updates_already_matched_frame_set():
    service = configured_service(("camera-1", "camera-2", "camera-3"))
    frames = [
        make_frame(
            device_id=f"camera-{number}",
            timestamp_ms=1000 + number,
            sequence=1,
            session_id=f"camera-{number}-session-a",
        )
        for number in range(1, 4)
    ]

    assert service.handle_frame(frames[0]) is None
    assert service.handle_frame(frames[1]) is None
    matched = service.handle_frame(frames[2])
    assert matched is not None

    after_trigger = service.finalize_frame_archive(
        frames[2],
        matched,
        frame_id=3,
        file_path="/archive/camera-3.jpg",
        archive_state="ARCHIVE_DURABLE",
        archive_error=None,
    )
    after_first = service.finalize_frame_archive(
        frames[0],
        None,
        frame_id=1,
        file_path="/archive/camera-1.jpg",
        archive_state="ARCHIVE_DURABLE",
        archive_error=None,
    )
    after_second = service.finalize_frame_archive(
        frames[1],
        None,
        frame_id=2,
        file_path="/archive/camera-2.jpg",
        archive_state="ARCHIVE_DURABLE",
        archive_error=None,
    )

    assert after_trigger is not None
    assert after_first is not None
    assert after_second is not None
    assert after_trigger.frames["camera-1"].archive_state == "ARCHIVE_PENDING"
    assert after_first.frames["camera-1"].archive_state == "ARCHIVE_DURABLE"
    assert all(
        member.archive_state == "ARCHIVE_DURABLE" and member.file_path
        for member in after_second.frames.values()
    )
    assert service.recent_frame_sets()[-1] == after_second


def test_new_service_restart_allocates_new_run_before_id_one():
    first_service = configured_service(("camera-1",))
    second_service = configured_service(("camera-1",))
    frame = make_frame(
        device_id="camera-1",
        timestamp_ms=1000,
        sequence=1,
        session_id="camera-1-session-a",
    )

    first = first_service.handle_frame(frame)
    second = second_service.handle_frame(frame)

    assert first.frame_set_id == 1
    assert second.frame_set_id == 1
    assert first.capture_session_id == second.capture_session_id
    assert first.capture_run_id != second.capture_run_id


def test_reconfigure_allocates_new_run_before_frame_set_id_reset():
    service = configured_service(("camera-1",))
    first = service.handle_frame(
        make_frame(
            device_id="camera-1",
            timestamp_ms=1000,
            sequence=1,
            session_id="camera-1-session-a",
        )
    )

    service.configure(
        enabled=True,
        expected_cameras=["camera-1"],
        window_ms=20,
        buffer_size=60,
    )
    after_config_change = service.handle_frame(
        make_frame(
            device_id="camera-1",
            timestamp_ms=1100,
            sequence=2,
            session_id="camera-1-session-a",
        )
    )

    assert after_config_change.frame_set_id == 1
    assert after_config_change.capture_session_id == first.capture_session_id
    assert after_config_change.capture_run_id != first.capture_run_id


def test_session_change_allocates_run_before_sequence_reset():
    service = configured_service(("camera-1",))
    first = service.handle_frame(
        make_frame(
            device_id="camera-1",
            timestamp_ms=1000,
            sequence=1,
            session_id="camera-1-session-a",
        )
    )
    next_session = service.handle_frame(
        make_frame(
            device_id="camera-1",
            timestamp_ms=1100,
            sequence=1,
            session_id="camera-1-session-b",
        )
    )

    assert next_session.frame_set_id == 1
    assert next_session.capture_session_id != first.capture_session_id
    assert next_session.capture_run_id != first.capture_run_id

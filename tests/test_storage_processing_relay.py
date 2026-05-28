from pathlib import Path

from app.infrastructure.grpc.processing_relay_client import RelayAck
from scripts.storage_processing_relay.core import relay_frame_sets, relay_raw_frames
from scripts.storage_sync_replay.models import ReplayFrame, ReplayInput


def make_frame(
    tmp_path: Path,
    *,
    frame_id: int,
    device_id: str,
    timestamp_ms: int,
    sequence: int,
    payload: bytes,
) -> ReplayFrame:
    path = tmp_path / f"{timestamp_ms}_{device_id}_camera_01_{sequence}.jpg"
    path.write_bytes(payload)
    return ReplayFrame(
        frame_id=frame_id,
        device_id=device_id,
        timestamp_ms=timestamp_ms,
        original_timestamp_ms=timestamp_ms,
        server_received_at_ms=None,
        server_receive_sequence=None,
        sequence=sequence,
        file_path=path,
        content_type="image/jpeg",
    )


def make_replay_input(frames: list[ReplayFrame], cameras: list[str]) -> ReplayInput:
    return ReplayInput(
        frames=frames,
        expected_cameras=cameras,
        per_camera_counts={
            camera: len([frame for frame in frames if frame.device_id == camera])
            for camera in cameras
        },
        original_per_camera_counts={
            camera: len([frame for frame in frames if frame.device_id == camera])
            for camera in cameras
        },
        skipped_image_files=0,
        non_image_files=0,
        overlap=None,
        timestamp_ranges={},
    )


def test_storage_processing_relay_sends_raw_frames(tmp_path):
    frames = [
        make_frame(
            tmp_path,
            frame_id=1,
            device_id="android_device_001",
            timestamp_ms=1000,
            sequence=1,
            payload=b"one",
        ),
        make_frame(
            tmp_path,
            frame_id=2,
            device_id="android_device_002",
            timestamp_ms=1010,
            sequence=1,
            payload=b"two",
        ),
    ]
    replay_input = make_replay_input(
        frames,
        ["android_device_001", "android_device_002"],
    )
    received = []

    def stub_factory(target):
        assert target == "127.0.0.1:50051"

        def stub(frame_iterator, timeout=None):
            received.extend(frame_iterator)
            return RelayAck(success=True, received_count=len(received), message="ok")

        return stub

    summary = relay_raw_frames(
        replay_input=replay_input,
        target="127.0.0.1:50051",
        timeout_sec=5,
        progress_interval=0,
        stub_factory=stub_factory,
    )

    assert summary["sent_count"] == 2
    assert summary["sent_image_bytes"] == len(b"one") + len(b"two")
    assert summary["ack_success"] is True
    assert summary["ack_received_count"] == 2
    assert [frame.device_id for frame in received] == [
        "android_device_001",
        "android_device_002",
    ]
    assert [frame.image_bytes for frame in received] == [b"one", b"two"]


def test_storage_processing_relay_sends_synced_frame_sets(tmp_path):
    frames = [
        make_frame(
            tmp_path,
            frame_id=1,
            device_id="android_device_001",
            timestamp_ms=1000,
            sequence=1,
            payload=b"one",
        ),
        make_frame(
            tmp_path,
            frame_id=2,
            device_id="android_device_002",
            timestamp_ms=1015,
            sequence=1,
            payload=b"two",
        ),
    ]
    replay_input = make_replay_input(
        frames,
        ["android_device_001", "android_device_002"],
    )
    received = []

    def stub_factory(target):
        assert target == "127.0.0.1:50051"

        def stub(frame_set_iterator, timeout=None):
            received.extend(frame_set_iterator)
            return RelayAck(success=True, received_count=len(received), message="ok")

        return stub

    summary = relay_frame_sets(
        replay_input=replay_input,
        target="127.0.0.1:50051",
        timeout_sec=5,
        window_ms=20,
        buffer_size=120,
        recent_limit=20,
        progress_interval=0,
        stub_factory=stub_factory,
    )

    assert summary["sent_count"] == 1
    assert summary["ack_success"] is True
    assert summary["ack_received_count"] == 1
    assert len(received) == 1
    assert received[0].frame_set_id == 1
    assert received[0].max_delta_ms == 15
    assert {frame.device_id for frame in received[0].frames} == {
        "android_device_001",
        "android_device_002",
    }

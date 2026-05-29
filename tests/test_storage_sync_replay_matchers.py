from pathlib import Path

from scripts.storage_sync_replay.core import build_replay_summary
from scripts.storage_sync_replay.models import ReplayFrame, ReplayInput


def make_replay_frame(
    tmp_path: Path,
    *,
    frame_id: int,
    device_id: str,
    timestamp_ms: int,
) -> ReplayFrame:
    path = tmp_path / f"{timestamp_ms}_{device_id}_camera_01_{frame_id}.jpg"
    path.write_bytes(b"image")
    return ReplayFrame(
        frame_id=frame_id,
        device_id=device_id,
        timestamp_ms=timestamp_ms,
        original_timestamp_ms=timestamp_ms,
        server_received_at_ms=None,
        server_receive_sequence=None,
        sequence=frame_id,
        file_path=path,
        content_type="image/jpeg",
    )


def make_replay_input(frames: list[ReplayFrame]) -> ReplayInput:
    cameras = ["camera1", "camera2", "camera3"]
    return ReplayInput(
        frames=frames,
        expected_cameras=cameras,
        per_camera_counts={camera: 1 for camera in cameras},
        original_per_camera_counts={camera: 1 for camera in cameras},
        skipped_image_files=0,
        non_image_files=0,
        overlap=None,
        timestamp_ranges={},
    )


def test_storage_sync_replay_can_compare_span_and_anchor_matchers(tmp_path):
    replay_input = make_replay_input(
        [
            make_replay_frame(
                tmp_path,
                frame_id=1,
                device_id="camera1",
                timestamp_ms=1000,
            ),
            make_replay_frame(
                tmp_path,
                frame_id=3,
                device_id="camera3",
                timestamp_ms=1140,
            ),
            make_replay_frame(
                tmp_path,
                frame_id=2,
                device_id="camera2",
                timestamp_ms=1070,
            ),
        ]
    )

    span_result = build_replay_summary(
        replay_input=replay_input,
        window_ms=70,
        buffer_size=120,
        recent_limit=20,
        timestamp_align="none",
        trim_overlap=False,
        order_by="capture",
        label="span",
        progress_interval=0,
        matcher_mode="span",
    )
    anchor_result = build_replay_summary(
        replay_input=replay_input,
        window_ms=70,
        buffer_size=120,
        recent_limit=20,
        timestamp_align="none",
        trim_overlap=False,
        order_by="capture",
        label="anchor",
        progress_interval=0,
        matcher_mode="anchor",
    )

    assert span_result.summary["matched_frame_set_count"] == 0
    assert span_result.summary["matcher_mode"] == "span"
    assert anchor_result.summary["matched_frame_set_count"] == 1
    assert anchor_result.summary["matcher_mode"] == "anchor"
    assert anchor_result.matched_frame_sets[0]["span_ms"] == 140

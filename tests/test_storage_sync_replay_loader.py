import json
from pathlib import Path

import pytest

from scripts.storage_sync_replay.loader import collect_replay_input


def write_frame(
    folder: Path,
    *,
    device_id: str,
    camera_id: str,
    timestamp_ms: int,
    sequence: int,
    received_at_ms: int | None = None,
    receive_sequence: int | None = None,
):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{timestamp_ms}_{device_id}_{camera_id}_{sequence}.jpg"
    path.write_bytes(b"image")
    if received_at_ms is not None and receive_sequence is not None:
        Path(f"{path}.metadata.json").write_text(
            json.dumps(
                {
                    "service": "gc.collector.v1.FrameIngestService",
                    "metadata": {
                        "device_id": device_id,
                        "device_timestamp_ms": str(timestamp_ms),
                    },
                    "server": {
                        "received_at_ms": received_at_ms,
                        "server_receive_sequence": receive_sequence,
                    },
                }
            ),
            encoding="utf-8",
        )
    return path


def test_replay_loader_defaults_to_capture_order_without_sidecars(tmp_path):
    camera1 = tmp_path / "camera1"
    camera2 = tmp_path / "camera2"
    write_frame(
        camera1,
        device_id="android_01",
        camera_id="camera_01",
        timestamp_ms=1_200,
        sequence=2,
    )
    write_frame(
        camera2,
        device_id="android_02",
        camera_id="camera_02",
        timestamp_ms=1_000,
        sequence=1,
    )

    replay_input = collect_replay_input(
        [("android_01", camera1), ("android_02", camera2)],
        limit_per_camera=None,
        timestamp_align="none",
        trim_overlap=False,
    )

    assert [frame.device_id for frame in replay_input.frames] == [
        "android_02",
        "android_01",
    ]
    assert [frame.timestamp_ms for frame in replay_input.frames] == [1_000, 1_200]
    assert replay_input.frames[0].server_received_at_ms is None


def test_replay_loader_can_sort_by_server_received_order(tmp_path):
    camera1 = tmp_path / "camera1"
    camera2 = tmp_path / "camera2"
    write_frame(
        camera1,
        device_id="android_01",
        camera_id="camera_01",
        timestamp_ms=1_000,
        sequence=1,
        received_at_ms=5_000,
        receive_sequence=2,
    )
    write_frame(
        camera2,
        device_id="android_02",
        camera_id="camera_02",
        timestamp_ms=1_010,
        sequence=1,
        received_at_ms=4_000,
        receive_sequence=1,
    )

    replay_input = collect_replay_input(
        [("android_01", camera1), ("android_02", camera2)],
        limit_per_camera=None,
        timestamp_align="none",
        trim_overlap=False,
        order_by="received",
    )

    assert [frame.device_id for frame in replay_input.frames] == [
        "android_02",
        "android_01",
    ]
    assert [frame.timestamp_ms for frame in replay_input.frames] == [1_010, 1_000]
    assert [frame.server_received_at_ms for frame in replay_input.frames] == [
        4_000,
        5_000,
    ]
    assert [frame.server_receive_sequence for frame in replay_input.frames] == [1, 2]


def test_replay_loader_received_order_requires_server_timing_sidecar(tmp_path):
    camera1 = tmp_path / "camera1"
    write_frame(
        camera1,
        device_id="android_01",
        camera_id="camera_01",
        timestamp_ms=1_000,
        sequence=1,
    )

    with pytest.raises(ValueError, match="Received-order replay requires"):
        collect_replay_input(
            [("android_01", camera1)],
            limit_per_camera=None,
            timestamp_align="none",
            trim_overlap=False,
            order_by="received",
        )

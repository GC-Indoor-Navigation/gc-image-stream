from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ReplayFrame:
    frame_id: int
    device_id: str
    timestamp_ms: int
    original_timestamp_ms: int
    server_received_at_ms: int | None
    server_receive_sequence: int | None
    sequence: int
    file_path: Path
    content_type: str


@dataclass(frozen=True)
class ReplayInput:
    frames: list[ReplayFrame]
    expected_cameras: list[str]
    per_camera_counts: dict[str, int]
    original_per_camera_counts: dict[str, int]
    skipped_image_files: int
    non_image_files: int
    overlap: dict | None
    timestamp_ranges: dict[str, dict]


@dataclass(frozen=True)
class ReplayRunResult:
    summary: dict
    matched_frame_sets: list[dict]
    no_set_frames: list[dict]

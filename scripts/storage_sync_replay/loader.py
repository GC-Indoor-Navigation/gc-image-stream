from __future__ import annotations

import json
import re
from pathlib import Path

from scripts.storage_sync_replay import ROOT_DIR
from scripts.storage_sync_replay.models import ReplayFrame, ReplayInput


IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
FRAME_NAME_RE = re.compile(
    r"^(?P<timestamp_ms>\d+)_(?P<device_id>.+)_(?P<camera_id>camera_\d+)_(?P<sequence>\d+)\.(?P<ext>jpg|jpeg|png|webp)$",
    re.IGNORECASE,
)


def parse_camera_mapping(raw_mapping: str) -> tuple[str, Path]:
    if "=" not in raw_mapping:
        raise ValueError(f"Invalid camera mapping: {raw_mapping}")
    device_id, raw_path = raw_mapping.split("=", 1)
    device_id = device_id.strip()
    if not device_id:
        raise ValueError(f"Missing device id in mapping: {raw_mapping}")
    path = Path(raw_path.strip())
    if not path.is_absolute():
        path = ROOT_DIR / path
    if not path.is_dir():
        raise ValueError(f"Camera folder does not exist: {path}")
    return device_id, path


def content_type_for(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    if ext == ".webp":
        return "image/webp"
    return "application/octet-stream"


def parse_frame_file(
    path: Path,
    expected_device_id: str,
    frame_id: int,
    order_by: str,
) -> ReplayFrame | None:
    if path.suffix.lower() not in IMAGE_EXTENSIONS:
        return None
    match = FRAME_NAME_RE.match(path.name)
    if match is None:
        return None

    device_id = match.group("device_id")
    if device_id != expected_device_id:
        return None

    timestamp_ms = int(match.group("timestamp_ms"))
    server_received_at_ms = None
    server_receive_sequence = None
    if order_by == "received":
        server_received_at_ms, server_receive_sequence = read_server_timing(path)

    return ReplayFrame(
        frame_id=frame_id,
        device_id=device_id,
        timestamp_ms=timestamp_ms,
        original_timestamp_ms=timestamp_ms,
        server_received_at_ms=server_received_at_ms,
        server_receive_sequence=server_receive_sequence,
        sequence=int(match.group("sequence")),
        file_path=path,
        content_type=content_type_for(path),
    )


def read_server_timing(path: Path) -> tuple[int, int]:
    sidecar_path = Path(f"{path}.metadata.json")
    if not sidecar_path.is_file():
        raise ValueError(
            "Received-order replay requires sidecar server timing: "
            + str(sidecar_path)
        )
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    server = payload.get("server") or {}
    received_at_ms = server.get("received_at_ms")
    receive_sequence = server.get("server_receive_sequence")
    if received_at_ms is None or receive_sequence is None:
        raise ValueError(
            "Received-order replay requires server.received_at_ms and "
            f"server.server_receive_sequence: {sidecar_path}"
        )
    return int(received_at_ms), int(receive_sequence)


def collect_replay_input(
    camera_mappings: list[tuple[str, Path]],
    limit_per_camera: int | None,
    timestamp_align: str,
    trim_overlap: bool,
    order_by: str = "capture",
) -> ReplayInput:
    (
        frames,
        per_camera_counts,
        original_per_camera_counts,
        skipped_image_files,
        non_image_files,
        overlap,
    ) = collect_replay_frames(
        camera_mappings=camera_mappings,
        limit_per_camera=limit_per_camera,
        timestamp_align=timestamp_align,
        trim_overlap=trim_overlap,
        order_by=order_by,
    )
    return ReplayInput(
        frames=frames,
        expected_cameras=[device_id for device_id, _ in camera_mappings],
        per_camera_counts=per_camera_counts,
        original_per_camera_counts=original_per_camera_counts,
        skipped_image_files=skipped_image_files,
        non_image_files=non_image_files,
        overlap=overlap,
        timestamp_ranges=build_timestamp_ranges(frames),
    )


def collect_replay_frames(
    camera_mappings: list[tuple[str, Path]],
    limit_per_camera: int | None,
    timestamp_align: str,
    trim_overlap: bool,
    order_by: str,
):
    if order_by not in {"capture", "received"}:
        raise ValueError(f"Unsupported replay order: {order_by}")

    frames_by_camera: dict[str, list[ReplayFrame]] = {}
    skipped_image_files = 0
    non_image_files = 0
    next_frame_id = 1
    per_camera_counts: dict[str, int] = {}
    original_per_camera_counts: dict[str, int] = {}

    for device_id, folder in camera_mappings:
        camera_frames: list[ReplayFrame] = []
        for path in sorted(folder.iterdir()):
            if not path.is_file():
                continue
            if path.suffix.lower() not in IMAGE_EXTENSIONS:
                non_image_files += 1
                continue
            frame = parse_frame_file(path, device_id, next_frame_id, order_by)
            if frame is None:
                skipped_image_files += 1
                continue
            camera_frames.append(frame)
            next_frame_id += 1
            if limit_per_camera is not None and len(camera_frames) >= limit_per_camera:
                break
        frames_by_camera[device_id] = camera_frames
        original_per_camera_counts[device_id] = len(camera_frames)

    overlap = calculate_overlap(frames_by_camera) if trim_overlap else None
    frames: list[ReplayFrame] = []
    for device_id, camera_frames in frames_by_camera.items():
        if overlap is not None:
            camera_frames = [
                frame
                for frame in camera_frames
                if overlap["start_timestamp_ms"]
                <= frame.original_timestamp_ms
                <= overlap["end_timestamp_ms"]
            ]
        camera_frames = align_camera_frames(camera_frames, timestamp_align)
        frames.extend(camera_frames)
        per_camera_counts[device_id] = len(camera_frames)

    frames = sort_replay_frames(frames, order_by)
    return (
        frames,
        per_camera_counts,
        original_per_camera_counts,
        skipped_image_files,
        non_image_files,
        overlap,
    )


def sort_replay_frames(frames: list[ReplayFrame], order_by: str) -> list[ReplayFrame]:
    if order_by == "capture":
        return sorted(
            frames,
            key=lambda frame: (frame.timestamp_ms, frame.device_id, frame.sequence),
        )
    if order_by == "received":
        return sorted(
            frames,
            key=lambda frame: (
                frame.server_received_at_ms
                if frame.server_received_at_ms is not None
                else -1,
                frame.server_receive_sequence
                if frame.server_receive_sequence is not None
                else -1,
                frame.timestamp_ms,
                frame.device_id,
                frame.sequence,
            ),
        )
    raise ValueError(f"Unsupported replay order: {order_by}")


def calculate_overlap(frames_by_camera: dict[str, list[ReplayFrame]]) -> dict | None:
    ranges = []
    for device_id, frames in frames_by_camera.items():
        if not frames:
            return None
        timestamps = [frame.original_timestamp_ms for frame in frames]
        ranges.append(
            {
                "device_id": device_id,
                "first_timestamp_ms": min(timestamps),
                "last_timestamp_ms": max(timestamps),
            }
        )

    start_timestamp_ms = max(item["first_timestamp_ms"] for item in ranges)
    end_timestamp_ms = min(item["last_timestamp_ms"] for item in ranges)
    return {
        "enabled": True,
        "has_overlap": start_timestamp_ms <= end_timestamp_ms,
        "start_timestamp_ms": start_timestamp_ms,
        "end_timestamp_ms": end_timestamp_ms,
        "duration_ms": max(end_timestamp_ms - start_timestamp_ms, 0),
        "camera_ranges": ranges,
    }


def align_camera_frames(frames: list[ReplayFrame], timestamp_align: str) -> list[ReplayFrame]:
    if timestamp_align == "none" or not frames:
        return frames
    if timestamp_align != "first-frame":
        raise ValueError(f"Unsupported timestamp alignment: {timestamp_align}")

    first_timestamp_ms = frames[0].timestamp_ms
    return [
        ReplayFrame(
            frame_id=frame.frame_id,
            device_id=frame.device_id,
            timestamp_ms=frame.timestamp_ms - first_timestamp_ms,
            original_timestamp_ms=frame.original_timestamp_ms,
            server_received_at_ms=frame.server_received_at_ms,
            server_receive_sequence=frame.server_receive_sequence,
            sequence=frame.sequence,
            file_path=frame.file_path,
            content_type=frame.content_type,
        )
        for frame in frames
    ]


def build_timestamp_ranges(frames: list[ReplayFrame]) -> dict[str, dict]:
    ranges: dict[str, dict] = {}
    for frame in frames:
        item = ranges.setdefault(
            frame.device_id,
            {
                "first_timestamp_ms": frame.timestamp_ms,
                "last_timestamp_ms": frame.timestamp_ms,
                "first_original_timestamp_ms": frame.original_timestamp_ms,
                "last_original_timestamp_ms": frame.original_timestamp_ms,
            },
        )
        item["first_timestamp_ms"] = min(item["first_timestamp_ms"], frame.timestamp_ms)
        item["last_timestamp_ms"] = max(item["last_timestamp_ms"], frame.timestamp_ms)
        item["first_original_timestamp_ms"] = min(
            item["first_original_timestamp_ms"],
            frame.original_timestamp_ms,
        )
        item["last_original_timestamp_ms"] = max(
            item["last_original_timestamp_ms"],
            frame.original_timestamp_ms,
        )
    return ranges

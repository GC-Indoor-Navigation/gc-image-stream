from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from pathlib import Path
from statistics import mean


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from app.services.sync import StreamSyncService, SyncInputFrame  # noqa: E402


IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}
FRAME_NAME_RE = re.compile(
    r"^(?P<timestamp_ms>\d+)_(?P<device_id>.+)_(?P<camera_id>camera_\d+)_(?P<sequence>\d+)\.(?P<ext>jpg|jpeg|png|webp)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ReplayFrame:
    frame_id: int
    device_id: str
    timestamp_ms: int
    original_timestamp_ms: int
    sequence: int
    file_path: Path
    content_type: str


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


def parse_frame_file(path: Path, expected_device_id: str, frame_id: int) -> ReplayFrame | None:
    if path.suffix.lower() not in IMAGE_EXTENSIONS:
        return None
    match = FRAME_NAME_RE.match(path.name)
    if match is None:
        return None

    device_id = match.group("device_id")
    if device_id != expected_device_id:
        return None

    return ReplayFrame(
        frame_id=frame_id,
        device_id=device_id,
        timestamp_ms=int(match.group("timestamp_ms")),
        original_timestamp_ms=int(match.group("timestamp_ms")),
        sequence=int(match.group("sequence")),
        file_path=path,
        content_type=content_type_for(path),
    )


def collect_replay_frames(
    camera_mappings: list[tuple[str, Path]],
    limit_per_camera: int | None,
    timestamp_align: str,
    trim_overlap: bool,
):
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
            frame = parse_frame_file(path, device_id, next_frame_id)
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

    frames.sort(key=lambda frame: (frame.timestamp_ms, frame.device_id, frame.sequence))
    return (
        frames,
        per_camera_counts,
        original_per_camera_counts,
        skipped_image_files,
        non_image_files,
        overlap,
    )


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
            sequence=frame.sequence,
            file_path=frame.file_path,
            content_type=frame.content_type,
        )
        for frame in frames
    ]


def serialize_frame(frame, original_timestamps: dict[int, int]) -> dict:
    return {
        "frame_id": frame.frame_id,
        "device_id": frame.device_id,
        "timestamp_ms": frame.timestamp_ms,
        "original_timestamp_ms": original_timestamps.get(frame.frame_id, frame.timestamp_ms),
        "sequence": frame.sequence,
        "image_size": frame.image_size,
        "file_path": frame.file_path,
    }


def serialize_frame_set(frame_set, original_timestamps: dict[int, int]) -> dict:
    return {
        "frame_set_id": frame_set.frame_set_id,
        "anchor_timestamp_ms": frame_set.anchor_timestamp_ms,
        "max_delta_ms": frame_set.max_delta_ms,
        "frames": {
            device_id: serialize_frame(frame, original_timestamps)
            for device_id, frame in sorted(frame_set.frames.items())
        },
    }


def percentile(values: list[int], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = round((len(ordered) - 1) * p)
    return float(ordered[index])


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


def write_jsonl(path: Path, items: list[dict]):
    with path.open("w", encoding="utf-8") as file:
        for item in items:
            file.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            file.write("\n")


def build_output_dir(base_dir: Path, run_id: str | None) -> Path:
    resolved_run_id = run_id or datetime.now().strftime("storage-sync-replay-%Y%m%d-%H%M%S")
    output_dir = base_dir / resolved_run_id
    output_dir.mkdir(parents=True, exist_ok=False)
    return output_dir


def replay_frames(
    frames: list[ReplayFrame],
    expected_cameras: list[str],
    window_ms: int,
    buffer_size: int,
    recent_limit: int,
):
    service = StreamSyncService()
    service.configure(
        enabled=True,
        expected_cameras=expected_cameras,
        window_ms=window_ms,
        buffer_size=buffer_size,
        recent_limit=recent_limit,
    )

    matched_frame_sets: list[dict] = []
    missed_frames: list[dict] = []
    original_timestamps = {
        frame.frame_id: frame.original_timestamp_ms
        for frame in frames
    }
    for frame in frames:
        result = service.handle_frame(
            SyncInputFrame(
                frame_id=frame.frame_id,
                device_id=frame.device_id,
                timestamp_ms=frame.timestamp_ms,
                sequence=frame.sequence,
                content_type=frame.content_type,
                image_bytes=frame.file_path.read_bytes(),
                file_path=str(frame.file_path.relative_to(ROOT_DIR))
                if frame.file_path.is_relative_to(ROOT_DIR)
                else str(frame.file_path),
            )
        )
        if result is not None:
            matched_frame_sets.append(serialize_frame_set(result, original_timestamps))
        else:
            status = service.status()
            if status["last_anchor_timestamp_ms"] == frame.timestamp_ms:
                missed_frames.append(
                    {
                        "frame_id": frame.frame_id,
                        "device_id": frame.device_id,
                        "timestamp_ms": frame.timestamp_ms,
                        "original_timestamp_ms": frame.original_timestamp_ms,
                        "sequence": frame.sequence,
                        "reason": status["last_reason"],
                        "missing_cameras": status["last_missing_cameras"],
                        "file_path": str(frame.file_path),
                    }
                )

    return service.status(), matched_frame_sets, missed_frames


def build_summary(
    *,
    expected_cameras: list[str],
    window_ms: int,
    buffer_size: int,
    timestamp_align: str,
    trim_overlap: bool,
    overlap: dict | None,
    per_camera_counts: dict[str, int],
    original_per_camera_counts: dict[str, int],
    skipped_image_files: int,
    non_image_files: int,
    input_frame_count: int,
    timestamp_ranges: dict[str, dict],
    status: dict,
    matched_frame_sets: list[dict],
):
    max_deltas = [item["max_delta_ms"] for item in matched_frame_sets]
    matched_count = len(matched_frame_sets)
    largest_camera_ratio = (
        matched_count / max(per_camera_counts.values())
        if per_camera_counts
        else 0.0
    )
    overlap_sync_opportunity_count = (
        min(per_camera_counts.values())
        if per_camera_counts
        else 0
    )
    matched_ratio_in_overlap = (
        matched_count / overlap_sync_opportunity_count
        if overlap_sync_opportunity_count > 0
        else 0.0
    )
    return {
        "expected_cameras": expected_cameras,
        "window_ms": window_ms,
        "buffer_size": buffer_size,
        "timestamp_align": timestamp_align,
        "trim_overlap": trim_overlap,
        "overlap": overlap or {"enabled": False},
        "input_frame_count": input_frame_count,
        "per_camera_counts": per_camera_counts,
        "original_per_camera_counts": original_per_camera_counts,
        "trimmed_frame_counts": {
            device_id: original_per_camera_counts.get(device_id, 0) - count
            for device_id, count in per_camera_counts.items()
        },
        "skipped_image_files": skipped_image_files,
        "non_image_files": non_image_files,
        "timestamp_ranges": timestamp_ranges,
        "matched_frame_set_count": matched_count,
        "overlap_sync_opportunity_count": overlap_sync_opportunity_count,
        "matched_ratio_in_overlap": matched_ratio_in_overlap,
        "matched_ratio_vs_largest_camera": largest_camera_ratio,
        "missed_count": status["missed_count"],
        "duplicate_count": status["duplicate_count"],
        "ignored_count": status["ignored_count"],
        "last_reason": status["last_reason"],
        "last_missing_cameras": status["last_missing_cameras"],
        "max_delta_ms": {
            "min": min(max_deltas) if max_deltas else None,
            "avg": mean(max_deltas) if max_deltas else None,
            "p95": percentile(max_deltas, 0.95),
            "max": max(max_deltas) if max_deltas else None,
        },
    }


def build_replay_summary(
    *,
    frames: list[ReplayFrame],
    expected_cameras: list[str],
    window_ms: int,
    buffer_size: int,
    recent_limit: int,
    timestamp_align: str,
    trim_overlap: bool,
    overlap: dict | None,
    per_camera_counts: dict[str, int],
    original_per_camera_counts: dict[str, int],
    skipped_image_files: int,
    non_image_files: int,
    timestamp_ranges: dict[str, dict],
):
    status, matched_frame_sets, missed_frames = replay_frames(
        frames=frames,
        expected_cameras=expected_cameras,
        window_ms=window_ms,
        buffer_size=buffer_size,
        recent_limit=recent_limit,
    )
    summary = build_summary(
        expected_cameras=expected_cameras,
        window_ms=window_ms,
        buffer_size=buffer_size,
        timestamp_align=timestamp_align,
        trim_overlap=trim_overlap,
        overlap=overlap,
        per_camera_counts=per_camera_counts,
        original_per_camera_counts=original_per_camera_counts,
        skipped_image_files=skipped_image_files,
        non_image_files=non_image_files,
        input_frame_count=len(frames),
        timestamp_ranges=timestamp_ranges,
        status=status,
        matched_frame_sets=matched_frame_sets,
    )
    return summary, matched_frame_sets, missed_frames


def build_pairwise_summaries(
    *,
    frames: list[ReplayFrame],
    expected_cameras: list[str],
    window_ms: int,
    buffer_size: int,
    recent_limit: int,
    timestamp_align: str,
    trim_overlap: bool,
    overlap: dict | None,
    per_camera_counts: dict[str, int],
    original_per_camera_counts: dict[str, int],
    skipped_image_files: int,
    non_image_files: int,
):
    pairwise = []
    for camera_pair in combinations(expected_cameras, 2):
        pair = list(camera_pair)
        pair_frames = [
            frame
            for frame in frames
            if frame.device_id in pair
        ]
        pair_per_camera_counts = {
            device_id: per_camera_counts.get(device_id, 0)
            for device_id in pair
        }
        pair_original_per_camera_counts = {
            device_id: original_per_camera_counts.get(device_id, 0)
            for device_id in pair
        }
        pair_summary, _, _ = build_replay_summary(
            frames=pair_frames,
            expected_cameras=pair,
            window_ms=window_ms,
            buffer_size=buffer_size,
            recent_limit=recent_limit,
            timestamp_align=timestamp_align,
            trim_overlap=trim_overlap,
            overlap=overlap,
            per_camera_counts=pair_per_camera_counts,
            original_per_camera_counts=pair_original_per_camera_counts,
            skipped_image_files=skipped_image_files,
            non_image_files=non_image_files,
            timestamp_ranges=build_timestamp_ranges(pair_frames),
        )
        pairwise.append(
            {
                "cameras": pair,
                **pair_summary,
            }
        )
    return pairwise


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replay stored camera images through the Stream sync matcher."
    )
    parser.add_argument(
        "--camera",
        action="append",
        required=True,
        help="Camera mapping in the form device_id=folder_path.",
    )
    parser.add_argument("--window-ms", type=int, default=50)
    parser.add_argument("--buffer-size", type=int, default=120)
    parser.add_argument("--recent-limit", type=int, default=20)
    parser.add_argument("--limit-per-camera", type=int, default=None)
    parser.add_argument(
        "--timestamp-align",
        choices=["none", "first-frame"],
        default="none",
        help="Use first-frame to normalize each camera folder to a zero-based timeline.",
    )
    parser.add_argument(
        "--trim-overlap",
        action="store_true",
        help="Use only the common absolute timestamp range across all camera folders.",
    )
    parser.add_argument(
        "--pairwise",
        action="store_true",
        help="Also replay every 2-camera combination and write pairwise_summary.json.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT_DIR / "experiment_logs"),
        help="Base output directory for replay artifacts.",
    )
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    camera_mappings = [parse_camera_mapping(raw) for raw in args.camera]
    expected_cameras = [device_id for device_id, _ in camera_mappings]
    (
        frames,
        per_camera_counts,
        original_per_camera_counts,
        skipped_image_files,
        non_image_files,
        overlap,
    ) = collect_replay_frames(
        camera_mappings,
        limit_per_camera=args.limit_per_camera,
        timestamp_align=args.timestamp_align,
        trim_overlap=args.trim_overlap,
    )
    timestamp_ranges = build_timestamp_ranges(frames)
    output_dir = build_output_dir(Path(args.output_dir), args.run_id)

    summary, matched_frame_sets, missed_frames = build_replay_summary(
        frames=frames,
        expected_cameras=expected_cameras,
        window_ms=args.window_ms,
        buffer_size=args.buffer_size,
        recent_limit=args.recent_limit,
        timestamp_align=args.timestamp_align,
        trim_overlap=args.trim_overlap,
        overlap=overlap,
        per_camera_counts=per_camera_counts,
        original_per_camera_counts=original_per_camera_counts,
        skipped_image_files=skipped_image_files,
        non_image_files=non_image_files,
        timestamp_ranges=timestamp_ranges,
    )

    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_jsonl(output_dir / "matched_frame_sets.jsonl", matched_frame_sets)
    write_jsonl(output_dir / "missed_frames.jsonl", missed_frames)
    pairwise_summaries = []
    if args.pairwise:
        pairwise_summaries = build_pairwise_summaries(
            frames=frames,
            expected_cameras=expected_cameras,
            window_ms=args.window_ms,
            buffer_size=args.buffer_size,
            recent_limit=args.recent_limit,
            timestamp_align=args.timestamp_align,
            trim_overlap=args.trim_overlap,
            overlap=overlap,
            per_camera_counts=per_camera_counts,
            original_per_camera_counts=original_per_camera_counts,
            skipped_image_files=skipped_image_files,
            non_image_files=non_image_files,
        )
        (output_dir / "pairwise_summary.json").write_text(
            json.dumps(pairwise_summaries, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    print(f"output_dir: {output_dir}")
    print(f"expected_cameras: {','.join(expected_cameras)}")
    print(f"input_frames: {len(frames)}")
    print(f"per_camera_counts: {per_camera_counts}")
    print(f"original_per_camera_counts: {original_per_camera_counts}")
    print(f"timestamp_align: {args.timestamp_align}")
    print(f"trim_overlap: {args.trim_overlap}")
    print(f"overlap: {overlap or {'enabled': False}}")
    print(f"matched_frame_sets: {summary['matched_frame_set_count']}")
    print(f"overlap_sync_opportunity_count: {summary['overlap_sync_opportunity_count']}")
    print(f"matched_ratio_in_overlap: {summary['matched_ratio_in_overlap']:.4f}")
    print(f"missed_count: {summary['missed_count']}")
    print(f"duplicate_count: {summary['duplicate_count']}")
    print(f"ignored_count: {summary['ignored_count']}")
    print(f"matched_ratio_vs_largest_camera: {summary['matched_ratio_vs_largest_camera']:.4f}")
    print(f"max_delta_ms: {summary['max_delta_ms']}")
    if pairwise_summaries:
        print("pairwise:")
        for item in pairwise_summaries:
            print(
                "  "
                + ",".join(item["cameras"])
                + f": matched={item['matched_frame_set_count']} "
                + f"opportunity={item['overlap_sync_opportunity_count']} "
                + f"ratio={item['matched_ratio_in_overlap']:.4f} "
                + f"p95_delta={item['max_delta_ms']['p95']}"
            )


if __name__ == "__main__":
    main()

from __future__ import annotations

from itertools import combinations
from statistics import mean

from app.services.sync import StreamSyncService, SyncInputFrame
from scripts.storage_sync_replay import ROOT_DIR
from scripts.storage_sync_replay.console import ProgressBar
from scripts.storage_sync_replay.loader import build_timestamp_ranges
from scripts.storage_sync_replay.models import ReplayFrame, ReplayInput, ReplayRunResult


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
    span_ms = (
        frame_set.span_ms
        if frame_set.span_ms is not None
        else frame_set.max_delta_ms
    )
    return {
        "frame_set_id": frame_set.frame_set_id,
        "anchor_timestamp_ms": frame_set.anchor_timestamp_ms,
        "max_delta_ms": frame_set.max_delta_ms,
        "span_ms": span_ms,
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


def replay_frames(
    frames: list[ReplayFrame],
    expected_cameras: list[str],
    window_ms: int,
    buffer_size: int,
    recent_limit: int,
    label: str,
    progress_interval: int,
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
    no_set_frames: list[dict] = []
    original_timestamps = {
        frame.frame_id: frame.original_timestamp_ms
        for frame in frames
    }
    progress_bar = ProgressBar(label=label, total=len(frames), interval=progress_interval)
    progress_bar.start()

    for index, frame in enumerate(frames, start=1):
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
                no_set_frames.append(
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

        status = service.status()
        progress_bar.draw(
            index,
            matched=len(matched_frame_sets),
            ignored=status["ignored_count"],
        )

    status = service.status()
    progress_bar.finish(
        matched=len(matched_frame_sets),
        ignored=status["ignored_count"],
    )
    return status, matched_frame_sets, no_set_frames


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
    span_values = [item["span_ms"] for item in matched_frame_sets]
    matched_count = len(matched_frame_sets)
    largest_camera_count = max(per_camera_counts.values()) if per_camera_counts else 0
    largest_camera_ratio = (
        matched_count / largest_camera_count
        if largest_camera_count > 0
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
        "no_set_event_count": status["missed_count"],
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
        "span_ms": {
            "min": min(span_values) if span_values else None,
            "avg": mean(span_values) if span_values else None,
            "p95": percentile(span_values, 0.95),
            "max": max(span_values) if span_values else None,
        },
        "watermark_timestamp_ms": status.get("watermark_timestamp_ms"),
        "dropped_stale_count": status.get("dropped_stale_count", 0),
    }


def build_replay_summary(
    *,
    replay_input: ReplayInput,
    window_ms: int,
    buffer_size: int,
    recent_limit: int,
    timestamp_align: str,
    trim_overlap: bool,
    label: str,
    progress_interval: int,
) -> ReplayRunResult:
    status, matched_frame_sets, no_set_frames = replay_frames(
        frames=replay_input.frames,
        expected_cameras=replay_input.expected_cameras,
        window_ms=window_ms,
        buffer_size=buffer_size,
        recent_limit=recent_limit,
        label=label,
        progress_interval=progress_interval,
    )
    summary = build_summary(
        expected_cameras=replay_input.expected_cameras,
        window_ms=window_ms,
        buffer_size=buffer_size,
        timestamp_align=timestamp_align,
        trim_overlap=trim_overlap,
        overlap=replay_input.overlap,
        per_camera_counts=replay_input.per_camera_counts,
        original_per_camera_counts=replay_input.original_per_camera_counts,
        skipped_image_files=replay_input.skipped_image_files,
        non_image_files=replay_input.non_image_files,
        input_frame_count=len(replay_input.frames),
        timestamp_ranges=replay_input.timestamp_ranges,
        status=status,
        matched_frame_sets=matched_frame_sets,
    )
    return ReplayRunResult(
        summary=summary,
        matched_frame_sets=matched_frame_sets,
        no_set_frames=no_set_frames,
    )


def build_subset_replay_summary(
    *,
    replay_input: ReplayInput,
    expected_cameras: list[str],
    window_ms: int,
    buffer_size: int,
    recent_limit: int,
    timestamp_align: str,
    trim_overlap: bool,
    label: str,
    progress_interval: int,
) -> ReplayRunResult:
    subset_frames = [
        frame
        for frame in replay_input.frames
        if frame.device_id in expected_cameras
    ]
    per_camera_counts = {
        device_id: replay_input.per_camera_counts.get(device_id, 0)
        for device_id in expected_cameras
    }
    original_per_camera_counts = {
        device_id: replay_input.original_per_camera_counts.get(device_id, 0)
        for device_id in expected_cameras
    }
    status, matched_frame_sets, no_set_frames = replay_frames(
        frames=subset_frames,
        expected_cameras=expected_cameras,
        window_ms=window_ms,
        buffer_size=buffer_size,
        recent_limit=recent_limit,
        label=label,
        progress_interval=progress_interval,
    )
    summary = build_summary(
        expected_cameras=expected_cameras,
        window_ms=window_ms,
        buffer_size=buffer_size,
        timestamp_align=timestamp_align,
        trim_overlap=trim_overlap,
        overlap=replay_input.overlap,
        per_camera_counts=per_camera_counts,
        original_per_camera_counts=original_per_camera_counts,
        skipped_image_files=replay_input.skipped_image_files,
        non_image_files=replay_input.non_image_files,
        input_frame_count=len(subset_frames),
        timestamp_ranges=build_timestamp_ranges(subset_frames),
        status=status,
        matched_frame_sets=matched_frame_sets,
    )
    return ReplayRunResult(
        summary=summary,
        matched_frame_sets=matched_frame_sets,
        no_set_frames=no_set_frames,
    )


def build_pairwise_summaries(
    *,
    replay_input: ReplayInput,
    window_ms: int,
    buffer_size: int,
    recent_limit: int,
    timestamp_align: str,
    trim_overlap: bool,
    label_prefix: str,
    progress_interval: int,
):
    pairwise = []
    for camera_pair in combinations(replay_input.expected_cameras, 2):
        pair = list(camera_pair)
        pair_summary = build_subset_replay_summary(
            replay_input=replay_input,
            expected_cameras=pair,
            window_ms=window_ms,
            buffer_size=buffer_size,
            recent_limit=recent_limit,
            timestamp_align=timestamp_align,
            trim_overlap=trim_overlap,
            label=f"{label_prefix} {'+'.join(pair)}",
            progress_interval=progress_interval,
        ).summary
        pairwise.append(
            {
                "cameras": pair,
                **pair_summary,
            }
        )
    return pairwise


def compact_summary(summary: dict) -> dict:
    return {
        "window_ms": summary["window_ms"],
        "matched_frame_set_count": summary["matched_frame_set_count"],
        "overlap_sync_opportunity_count": summary["overlap_sync_opportunity_count"],
        "matched_ratio_in_overlap": summary["matched_ratio_in_overlap"],
        "no_set_event_count": summary["no_set_event_count"],
        "missed_count": summary["missed_count"],
        "duplicate_count": summary["duplicate_count"],
        "ignored_count": summary["ignored_count"],
        "max_delta_ms": summary["max_delta_ms"],
        "span_ms": summary["span_ms"],
        "watermark_timestamp_ms": summary["watermark_timestamp_ms"],
        "dropped_stale_count": summary["dropped_stale_count"],
        "last_reason": summary["last_reason"],
        "last_missing_cameras": summary["last_missing_cameras"],
    }


def build_window_sweep_summaries(
    *,
    windows_ms: list[int],
    replay_input: ReplayInput,
    buffer_size: int,
    recent_limit: int,
    timestamp_align: str,
    trim_overlap: bool,
    include_pairwise: bool,
    progress_interval: int,
    precomputed_summaries: dict[int, dict] | None = None,
    precomputed_pairwise: dict[int, list[dict]] | None = None,
):
    sweep = []
    precomputed_summaries = precomputed_summaries or {}
    precomputed_pairwise = precomputed_pairwise or {}
    for window_ms in windows_ms:
        if window_ms in precomputed_summaries:
            summary = precomputed_summaries[window_ms]
        else:
            summary = build_replay_summary(
                replay_input=replay_input,
                window_ms=window_ms,
                buffer_size=buffer_size,
                recent_limit=recent_limit,
                timestamp_align=timestamp_align,
                trim_overlap=trim_overlap,
                label=f"window-sweep {window_ms}ms",
                progress_interval=progress_interval,
            ).summary
        item = compact_summary(summary)
        if include_pairwise:
            pairwise_summaries = precomputed_pairwise.get(window_ms)
            if pairwise_summaries is None:
                pairwise_summaries = build_pairwise_summaries(
                    replay_input=replay_input,
                    window_ms=window_ms,
                    buffer_size=buffer_size,
                    recent_limit=recent_limit,
                    timestamp_align=timestamp_align,
                    trim_overlap=trim_overlap,
                    label_prefix=f"window-sweep {window_ms}ms pairwise",
                    progress_interval=progress_interval,
                )
            item["pairwise"] = [
                {
                    "cameras": pair_summary["cameras"],
                    **compact_summary(pair_summary),
                }
                for pair_summary in pairwise_summaries
            ]
        sweep.append(item)
    return sweep

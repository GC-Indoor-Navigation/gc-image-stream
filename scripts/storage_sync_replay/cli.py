from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from scripts.storage_sync_replay import ROOT_DIR
from scripts.storage_sync_replay.console import print_summary_block, setup_log
from scripts.storage_sync_replay.core import (
    build_pairwise_summaries,
    build_replay_summary,
    build_window_sweep_summaries,
)
from scripts.storage_sync_replay.loader import collect_replay_input, parse_camera_mapping
from scripts.storage_sync_replay.storage import (
    build_output_dir,
    write_detail_outputs,
    write_pairwise_output,
    write_window_sweep_output,
)


@dataclass(frozen=True)
class RunPlan:
    mode: str
    detail_window_ms: int | None
    sweep_windows_ms: list[int]


def parse_windows(raw_value: str | None, option_name: str) -> list[int]:
    if raw_value is None or not raw_value.strip():
        return []
    windows = []
    for raw_item in raw_value.split(","):
        item = raw_item.strip()
        if not item:
            continue
        window_ms = int(item)
        if window_ms <= 0:
            raise ValueError(f"{option_name} values must be greater than 0")
        windows.append(window_ms)
    return list(dict.fromkeys(windows))


def resolve_run_plan(args) -> RunPlan:
    requested_mode = args.mode
    legacy_sweep = requested_mode is None and args.window_sweep and not args.windows_ms
    mode = requested_mode or ("sweep" if args.windows_ms or args.window_sweep else "single")

    if mode == "single":
        if args.windows_ms or args.window_sweep:
            raise ValueError("single mode uses --window-ms only")
        return RunPlan(
            mode="single",
            detail_window_ms=args.window_ms,
            sweep_windows_ms=[],
        )

    windows = parse_windows(args.windows_ms, "--windows-ms")
    if not windows:
        windows = parse_windows(args.window_sweep, "--window-sweep")
    if not windows:
        raise ValueError("sweep mode requires --windows-ms")

    if legacy_sweep:
        detail_window_ms = args.window_ms
    elif args.sweep_detail == "first":
        detail_window_ms = windows[0]
    else:
        detail_window_ms = None

    return RunPlan(
        mode="sweep",
        detail_window_ms=detail_window_ms,
        sweep_windows_ms=windows,
    )


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
    parser.add_argument(
        "--mode",
        choices=["single", "sweep"],
        default=None,
        help="Run one sync window or compare multiple sync windows.",
    )
    parser.add_argument(
        "--window-ms",
        type=int,
        default=50,
        help="Single-mode sync window in milliseconds.",
    )
    parser.add_argument(
        "--windows-ms",
        default=None,
        help="Sweep-mode comma-separated sync windows, e.g. 30,50,100.",
    )
    parser.add_argument(
        "--sweep-detail",
        choices=["first", "none"],
        default="first",
        help="For sweep mode, write detailed JSONL for the first window or no detailed JSONL.",
    )
    parser.add_argument(
        "--window-sweep",
        default=None,
        help="Legacy alias for --windows-ms.",
    )
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
        help="Also replay every 2-camera combination.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT_DIR / "experiment_logs"),
        help="Base output directory for replay artifacts.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1000,
        help="Redraw the console progress bar every N replayed frames. Use 0 to show only start/end.",
    )
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    plan = resolve_run_plan(args)
    camera_mappings = [parse_camera_mapping(raw) for raw in args.camera]
    replay_input = collect_replay_input(
        camera_mappings,
        limit_per_camera=args.limit_per_camera,
        timestamp_align=args.timestamp_align,
        trim_overlap=args.trim_overlap,
    )
    output_dir = build_output_dir(Path(args.output_dir), args.run_id)
    setup_log(
        f"input loaded: frames={len(replay_input.frames)} "
        f"cameras={','.join(replay_input.expected_cameras)} "
        f"mode={plan.mode} trim_overlap={args.trim_overlap} "
        f"timestamp_align={args.timestamp_align}"
    )
    setup_log(f"output directory: {output_dir}\n")

    detail_result = None
    pairwise_summaries = []
    if plan.detail_window_ms is not None:
        detail_result = build_replay_summary(
            replay_input=replay_input,
            window_ms=plan.detail_window_ms,
            buffer_size=args.buffer_size,
            recent_limit=args.recent_limit,
            timestamp_align=args.timestamp_align,
            trim_overlap=args.trim_overlap,
            label=f"main {plan.detail_window_ms}ms",
            progress_interval=args.progress_interval,
        )
        write_detail_outputs(output_dir, detail_result)

        if args.pairwise:
            pairwise_summaries = build_pairwise_summaries(
                replay_input=replay_input,
                window_ms=plan.detail_window_ms,
                buffer_size=args.buffer_size,
                recent_limit=args.recent_limit,
                timestamp_align=args.timestamp_align,
                trim_overlap=args.trim_overlap,
                label_prefix=f"pairwise {plan.detail_window_ms}ms",
                progress_interval=args.progress_interval,
            )
            write_pairwise_output(output_dir, pairwise_summaries)

    window_sweep_summaries = []
    if plan.sweep_windows_ms:
        precomputed_summaries = {}
        precomputed_pairwise = {}
        if detail_result is not None:
            precomputed_summaries[detail_result.summary["window_ms"]] = detail_result.summary
            if pairwise_summaries:
                precomputed_pairwise[detail_result.summary["window_ms"]] = pairwise_summaries
        window_sweep_summaries = build_window_sweep_summaries(
            windows_ms=plan.sweep_windows_ms,
            replay_input=replay_input,
            buffer_size=args.buffer_size,
            recent_limit=args.recent_limit,
            timestamp_align=args.timestamp_align,
            trim_overlap=args.trim_overlap,
            include_pairwise=args.pairwise,
            progress_interval=args.progress_interval,
            precomputed_summaries=precomputed_summaries,
            precomputed_pairwise=precomputed_pairwise,
        )
        write_window_sweep_output(output_dir, window_sweep_summaries)

    print_summary_block(
        output_dir=output_dir,
        expected_cameras=replay_input.expected_cameras,
        summary=detail_result.summary if detail_result is not None else None,
        pairwise_summaries=pairwise_summaries,
        window_sweep_summaries=window_sweep_summaries,
    )

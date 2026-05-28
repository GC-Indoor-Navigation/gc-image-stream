from __future__ import annotations

import argparse
from pathlib import Path

from scripts.storage_processing_relay import ROOT_DIR
from scripts.storage_processing_relay.console import print_summary, setup_log
from scripts.storage_processing_relay.core import relay_frame_sets, relay_raw_frames
from scripts.storage_processing_relay.storage import build_output_dir, write_summary
from scripts.storage_sync_replay.loader import collect_replay_input, parse_camera_mapping


def parse_args():
    parser = argparse.ArgumentParser(
        description="Relay stored camera images to the Processing Server."
    )
    parser.add_argument(
        "--camera",
        action="append",
        required=True,
        help="Camera mapping in the form device_id=folder_path.",
    )
    parser.add_argument(
        "--mode",
        choices=["raw", "frame_set"],
        default="frame_set",
        help="Relay individual stored frames or synchronized stored frame sets.",
    )
    parser.add_argument(
        "--target",
        default="127.0.0.1:50051",
        help="Processing Server gRPC relay target.",
    )
    parser.add_argument("--timeout-sec", type=float, default=None)
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
        "--order-by",
        choices=["capture", "received"],
        default="capture",
        help="Send input by capture timestamp or server received timestamp.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT_DIR / "experiment_logs"),
        help="Base output directory for relay experiment artifacts.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1000,
        help="Redraw the console progress bar every N input frames. Use 0 to show only start/end.",
    )
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    camera_mappings = [parse_camera_mapping(raw) for raw in args.camera]
    replay_input = collect_replay_input(
        camera_mappings,
        limit_per_camera=args.limit_per_camera,
        timestamp_align=args.timestamp_align,
        trim_overlap=args.trim_overlap,
        order_by=args.order_by,
    )
    output_dir = build_output_dir(Path(args.output_dir), args.run_id)
    setup_log(
        f"input loaded: frames={len(replay_input.frames)} "
        f"cameras={','.join(replay_input.expected_cameras)} "
        f"mode={args.mode} trim_overlap={args.trim_overlap} "
        f"timestamp_align={args.timestamp_align} order_by={args.order_by}"
    )
    setup_log(f"output directory: {output_dir}\n")

    if args.mode == "raw":
        relay_result = relay_raw_frames(
            replay_input=replay_input,
            target=args.target,
            timeout_sec=args.timeout_sec,
            progress_interval=args.progress_interval,
        )
    else:
        relay_result = relay_frame_sets(
            replay_input=replay_input,
            target=args.target,
            timeout_sec=args.timeout_sec,
            window_ms=args.window_ms,
            buffer_size=args.buffer_size,
            recent_limit=args.recent_limit,
            progress_interval=args.progress_interval,
        )

    summary = {
        "output_dir": str(output_dir),
        "target": args.target,
        "mode": args.mode,
        "window_ms": args.window_ms,
        "buffer_size": args.buffer_size,
        "recent_limit": args.recent_limit,
        "timestamp_align": args.timestamp_align,
        "trim_overlap": args.trim_overlap,
        "order_by": args.order_by,
        "expected_cameras": replay_input.expected_cameras,
        "input_frame_count": len(replay_input.frames),
        "per_camera_counts": replay_input.per_camera_counts,
        "original_per_camera_counts": replay_input.original_per_camera_counts,
        "overlap": replay_input.overlap or {"enabled": False},
        **relay_result,
    }
    write_summary(output_dir, summary)
    print_summary(summary)

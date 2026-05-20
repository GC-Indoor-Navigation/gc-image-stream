from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def build_output_dir(base_dir: Path, run_id: str | None) -> Path:
    resolved_run_id = run_id or datetime.now().strftime("storage-sync-replay-%Y%m%d-%H%M%S")
    output_dir = unique_output_dir(base_dir, resolved_run_id)
    output_dir.mkdir(parents=True, exist_ok=False)
    return output_dir


def unique_output_dir(base_dir: Path, run_id: str) -> Path:
    output_dir = base_dir / run_id
    if not output_dir.exists():
        return output_dir
    for suffix in range(1, 1000):
        candidate = base_dir / f"{run_id}-{suffix:02d}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Could not allocate output directory for run id: {run_id}")


def write_json(path: Path, item):
    path.write_text(
        json.dumps(item, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_jsonl(path: Path, items: list[dict]):
    with path.open("w", encoding="utf-8") as file:
        for item in items:
            file.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            file.write("\n")


def write_detail_outputs(output_dir: Path, result):
    write_json(output_dir / "summary.json", result.summary)
    write_jsonl(output_dir / "matched_frame_sets.jsonl", result.matched_frame_sets)
    write_jsonl(output_dir / "no_set_frames.jsonl", result.no_set_frames)
    # Keep the old name for compatibility with existing local checks.
    write_jsonl(output_dir / "missed_frames.jsonl", result.no_set_frames)


def write_pairwise_output(output_dir: Path, pairwise_summaries: list[dict]):
    write_json(output_dir / "pairwise_summary.json", pairwise_summaries)


def write_window_sweep_output(output_dir: Path, window_sweep_summaries: list[dict]):
    write_json(output_dir / "window_sweep_summary.json", window_sweep_summaries)

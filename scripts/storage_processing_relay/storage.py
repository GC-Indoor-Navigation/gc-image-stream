from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def build_output_dir(base_dir: Path, run_id: str | None) -> Path:
    resolved_run_id = run_id or datetime.now().strftime("storage-processing-relay-%Y%m%d-%H%M%S")
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


def write_summary(output_dir: Path, summary: dict):
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

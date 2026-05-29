from __future__ import annotations

import re
import sys
from pathlib import Path


def setup_log(message: str):
    print(f"[setup] {message}", flush=True)


def console_bar_chars() -> tuple[str, str]:
    encoding = sys.stdout.encoding or "utf-8"
    for filled, empty in (("█", "░"), ("■", "□"), ("#", "-")):
        try:
            (filled + empty).encode(encoding)
        except UnicodeEncodeError:
            continue
        return filled, empty
    return "#", "-"


def split_progress_label(label: str) -> tuple[str, str]:
    if label.startswith("window-sweep "):
        return "window-sweep", compact_progress_label(label.removeprefix("window-sweep "))
    if label.startswith("pairwise "):
        return "pairwise", compact_progress_label(label.removeprefix("pairwise "))
    if label.startswith("main "):
        return "main", compact_progress_label(label.removeprefix("main "))
    return "replay", compact_progress_label(label)


def compact_progress_label(label: str) -> str:
    label = re.sub(r"android_device_(\d+)", r"cam\1", label)
    label = label.replace("pairwise ", "pair ")
    return label


class ProgressBar:
    BAR_WIDTH = 24
    LEFT_WIDTH = 52
    last_group_key: str | None = None

    def __init__(self, label: str, total: int, interval: int):
        self.prefix, self.label = split_progress_label(label)
        self.total = total
        self.interval = interval
        self.last_drawn = -1
        self.last_line_length = 0
        self.filled_char, self.empty_char = console_bar_chars()

    def start(self):
        group_key = self.group_key()
        if ProgressBar.last_group_key is not None and ProgressBar.last_group_key != group_key:
            print("", flush=True)
        ProgressBar.last_group_key = group_key
        self.draw(0, matched=0, ignored=0, force=True)

    def group_key(self) -> str:
        if self.prefix == "window-sweep":
            window = self.label.split(maxsplit=1)[0] if self.label else ""
            return f"{self.prefix} {window}"
        return self.prefix

    def draw(
        self,
        processed: int,
        *,
        matched: int,
        ignored: int,
        force: bool = False,
    ):
        if self.interval <= 0 and not force:
            return
        if not force and processed < self.total and processed % self.interval != 0:
            return
        if processed == self.last_drawn and not force:
            return

        self.last_drawn = processed
        percent = 100 if self.total == 0 else int((processed / self.total) * 100)
        filled = 0 if self.total == 0 else int((processed / self.total) * self.BAR_WIDTH)
        bar = self.filled_char * filled + self.empty_char * (self.BAR_WIDTH - filled)
        left = f"[{self.prefix}] {self.label}"
        if len(left) > self.LEFT_WIDTH:
            left = left[: self.LEFT_WIDTH - 3] + "..."
        line = (
            f"{left:<{self.LEFT_WIDTH}} "
            f"{bar} {percent:3d}% "
            f"{processed:>6,}/{self.total:<6,} "
            f"sets {matched:<5,} skip {ignored:<5,}"
        )
        padding = " " * max(self.last_line_length - len(line), 0)
        self.last_line_length = len(line)
        print("\r" + line + padding, end="", flush=True)

    def finish(self, *, matched: int, ignored: int):
        if self.last_drawn != self.total:
            self.draw(
                self.total,
                matched=matched,
                ignored=ignored,
                force=True,
            )
        print("", flush=True)


def format_ratio(value: float) -> str:
    return f"{value * 100:.2f}%"


def format_counts(counts: dict[str, int]) -> str:
    return ", ".join(
        f"{device_id}={count:,}"
        for device_id, count in sorted(counts.items())
    )


def format_delta(delta: dict) -> str:
    return (
        f"min={delta['min']} "
        f"avg={delta['avg']:.2f} "
        f"p95={delta['p95']} "
        f"max={delta['max']}"
        if delta["avg"] is not None
        else "n/a"
    )


def format_optional_float(value, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def format_fraction(numerator: int, denominator: int) -> str:
    return f"{numerator:,}/{denominator:,}"


def print_section(title: str):
    print("")
    print("=" * 72)
    print(title)
    print("-" * 72)


def print_summary_block(
    *,
    output_dir: Path,
    expected_cameras: list[str],
    summary: dict | None,
    pairwise_summaries: list[dict],
    window_sweep_summaries: list[dict],
):
    if summary is not None:
        print_section("Replay Run")
        print(f"output      : {output_dir}")
        print(f"cameras     : {', '.join(expected_cameras)}")
        print(f"frames      : {summary['input_frame_count']:,}")
        print(f"per camera  : {format_counts(summary['per_camera_counts'])}")
        print(f"original    : {format_counts(summary['original_per_camera_counts'])}")
        print(f"matcher     : {summary.get('matcher_mode', 'span')}")
        print(f"align       : {summary['timestamp_align']}")
        print(f"order by    : {summary['order_by']}")
        print(f"trim overlap: {summary['trim_overlap']}")
        if summary["overlap"].get("enabled"):
            overlap = summary["overlap"]
            print(
                "overlap     : "
                + f"{overlap['duration_ms']:,}ms "
                + f"({overlap['start_timestamp_ms']}..{overlap['end_timestamp_ms']})"
            )

        print_section("Main Window Result")
        print(f"window      : {summary['window_ms']}ms")
        print(
            "matched     : "
            + f"{summary['matched_frame_set_count']:,}/"
            + f"{summary['overlap_sync_opportunity_count']:,} "
            + f"({format_ratio(summary['matched_ratio_in_overlap'])})"
        )
        print(
            "events      : "
            + f"no_set={summary['no_set_event_count']:,} "
            + f"duplicate={summary['duplicate_count']:,} "
            + f"ignored={summary['ignored_count']:,}"
        )
        print(
            "event note  : "
            + "no_set is per-frame matcher state, not missing frame-set count"
        )
        print(f"span ms     : {format_delta(summary['span_ms'])}")
        print(f"stale drops : {summary['dropped_stale_count']:,}")
        print(f"last reason : {summary['last_reason']}")
    else:
        print_section("Replay Run")
        print(f"output      : {output_dir}")
        print(f"cameras     : {', '.join(expected_cameras)}")
        print("detail      : disabled for this sweep run")

    if pairwise_summaries:
        print_section("Pairwise Result")
        print(f"{'cameras':<43} {'ratio':>8} {'matched':>16} {'span p95':>8}")
        for item in pairwise_summaries:
            print(
                f"{' + '.join(item['cameras']):<43} "
                + f"{format_ratio(item['matched_ratio_in_overlap']):>8} "
                + f"{format_fraction(item['matched_frame_set_count'], item['overlap_sync_opportunity_count']):>16} "
                + f"{str(item['span_ms']['p95']):>8}"
            )

    if window_sweep_summaries:
        print_section("Window Sweep Result")
        print(f"{'window':>8} {'ratio':>9} {'matched':>15} {'span avg':>10} {'p95':>8} {'max':>8}")
        for item in window_sweep_summaries:
            print(
                f"{str(item['window_ms']) + 'ms':>8} "
                + f"{format_ratio(item['matched_ratio_in_overlap']):>9} "
                + f"{format_fraction(item['matched_frame_set_count'], item['overlap_sync_opportunity_count']):>15} "
                + f"{format_optional_float(item['span_ms']['avg']):>10} "
                + f"{str(item['span_ms']['p95']):>8} "
                + f"{str(item['span_ms']['max']):>8}"
            )

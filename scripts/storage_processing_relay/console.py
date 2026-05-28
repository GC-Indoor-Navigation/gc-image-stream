from __future__ import annotations

import sys


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


class RelayProgressBar:
    BAR_WIDTH = 24
    LEFT_WIDTH = 36

    def __init__(self, label: str, total: int, interval: int):
        self.label = label
        self.total = total
        self.interval = interval
        self.last_drawn = -1
        self.last_line_length = 0
        self.filled_char, self.empty_char = console_bar_chars()

    def start(self):
        self.draw(0, sent=0, force=True)

    def draw(self, processed: int, *, sent: int, force: bool = False):
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
        left = f"[relay] {self.label}"
        if len(left) > self.LEFT_WIDTH:
            left = left[: self.LEFT_WIDTH - 3] + "..."
        line = (
            f"{left:<{self.LEFT_WIDTH}} "
            f"{bar} {percent:3d}% "
            f"{processed:>6,}/{self.total:<6,} "
            f"sent {sent:<6,}"
        )
        padding = " " * max(self.last_line_length - len(line), 0)
        self.last_line_length = len(line)
        print("\r" + line + padding, end="", flush=True)

    def finish(self, *, sent: int):
        if self.last_drawn != self.total:
            self.draw(self.total, sent=sent, force=True)
        print("", flush=True)


def print_summary(summary: dict):
    print("")
    print("=" * 72)
    print("Storage Processing Relay")
    print("-" * 72)
    print(f"output      : {summary['output_dir']}")
    print(f"target      : {summary['target']}")
    print(f"mode        : {summary['mode']}")
    print(f"order by    : {summary['order_by']}")
    print(f"trim overlap: {summary['trim_overlap']}")
    print(f"frames      : {summary['input_frame_count']:,}")
    print(f"relay sent  : {summary['sent_count']:,}")
    print(f"bytes       : {summary['sent_image_bytes']:,}")
    if summary["mode"] == "frame_set":
        print(f"window      : {summary['window_ms']}ms")
        print(f"no-set      : {summary['no_set_count']:,}")
        print(f"stale drops : {summary['dropped_stale_count']:,}")
    print("")
    print("=" * 72)
    print("Processing Ack")
    print("-" * 72)
    print(f"success     : {summary['ack_success']}")
    print(f"received    : {summary['ack_received_count']:,}")
    print(f"message     : {summary['ack_message']}")
    print(f"elapsed     : {summary['elapsed_ms']}ms")

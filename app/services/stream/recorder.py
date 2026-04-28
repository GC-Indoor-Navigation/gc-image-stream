import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Any


@dataclass
class ExperimentContext:
    collector_type: str
    run_name: str
    experiment_log_dir: str | None
    experiment_id: str | None = None
    summary_fields: dict[str, Any] = field(default_factory=dict)


class ExperimentRecorder:
    def __init__(self, context: ExperimentContext):
        if not context.experiment_log_dir:
            raise ValueError("experiment_log_dir is required")

        self.context = context
        self.collector_type = context.collector_type
        self.started_at_monotonic = time.monotonic()
        self.started_at_ms = int(time.time() * 1000)
        self.run_id = sanitize_experiment_id(
            context.experiment_id
            or build_default_experiment_id(
                run_name=context.run_name,
                collector_type=context.collector_type,
            )
        )
        self.run_dir = os.path.join(context.experiment_log_dir, self.run_id)
        os.makedirs(self.run_dir, exist_ok=True)
        self.events_path = os.path.join(self.run_dir, "events.jsonl")
        self.summary_path = os.path.join(self.run_dir, "summary.json")
        self.lock = Lock()
        self.events_file = open(self.events_path, "a", encoding="utf-8")
        self.summary = {
            "experiment_id": self.run_id,
            "collector_type": context.collector_type,
            "run_name": context.run_name,
            **context.summary_fields,
            "started_at_ms": self.started_at_ms,
            "ended_at_ms": None,
            "duration_s": 0.0,
            "captured_count": 0,
            "registered_count": 0,
            "register_failed_count": 0,
            "register_error_count": 0,
            "relay_enqueued_count": 0,
            "relay_closed_count": 0,
            "relay_error_count": 0,
            "schedule_lag_count": 0,
            "schedule_lag_skipped_total": 0,
            "image_bytes_total": 0,
            "offset_ms_min": None,
            "offset_ms_max": None,
            "offset_ms_sum": 0.0,
            "offset_ms_avg": None,
            "capture_elapsed_s_sum": 0.0,
            "capture_elapsed_s_avg": None,
            "save_elapsed_s_sum": 0.0,
            "save_elapsed_s_avg": None,
        }
        self.record_event(
            "experiment_started",
            {
                "events_path": self.events_path,
                "summary_path": self.summary_path,
            },
        )

    def record_event(self, event_type: str, fields: dict | None = None):
        payload = {
            "event": event_type,
            "wall_time_ms": int(time.time() * 1000),
            "runtime_s": round(time.monotonic() - self.started_at_monotonic, 6),
        }
        if fields:
            payload.update(fields)

        with self.lock:
            self.events_file.write(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
            )
            self.events_file.flush()

    def record_capture(
        self,
        timestamp_ms: int,
        sequence: int,
        capture_label: str,
        capture_elapsed: float,
        save_elapsed: float,
        cycle_elapsed: float,
        queue_size: int,
        scheduled_at: float,
        captured_at: float,
        image_bytes_size: int,
        device_id: str | None = None,
    ):
        offset_ms = max(0.0, (captured_at - scheduled_at) * 1000)

        with self.lock:
            self.summary["captured_count"] += 1
            self.summary["image_bytes_total"] += image_bytes_size
            self.summary["offset_ms_sum"] += offset_ms
            self.summary["capture_elapsed_s_sum"] += capture_elapsed
            self.summary["save_elapsed_s_sum"] += save_elapsed

            if self.summary["offset_ms_min"] is None:
                self.summary["offset_ms_min"] = offset_ms
            else:
                self.summary["offset_ms_min"] = min(self.summary["offset_ms_min"], offset_ms)

            if self.summary["offset_ms_max"] is None:
                self.summary["offset_ms_max"] = offset_ms
            else:
                self.summary["offset_ms_max"] = max(self.summary["offset_ms_max"], offset_ms)

        self.record_event(
            "captured",
            {
                "device_id": device_id,
                "timestamp_ms": timestamp_ms,
                "sequence": sequence,
                "capture_label": capture_label,
                "capture_elapsed_s": round(capture_elapsed, 6),
                "save_elapsed_s": round(save_elapsed, 6),
                "cycle_elapsed_s": round(cycle_elapsed, 6),
                "offset_ms": round(offset_ms, 3),
                "register_queue_size": queue_size,
                "image_bytes": image_bytes_size,
            },
        )

    def record_registration(
        self,
        status: str,
        timestamp_ms: int,
        elapsed: float,
        queue_size: int,
        status_code: int | None = None,
        error: str | None = None,
        device_id: str | None = None,
    ):
        with self.lock:
            if status == "registered":
                self.summary["registered_count"] += 1
            elif status == "register_failed":
                self.summary["register_failed_count"] += 1
            elif status == "register_error":
                self.summary["register_error_count"] += 1

        self.record_event(
            status,
            {
                "device_id": device_id,
                "timestamp_ms": timestamp_ms,
                "elapsed_s": round(elapsed, 6),
                "queue_size": queue_size,
                "status_code": status_code,
                "error": error,
            },
        )

    def record_relay_enqueued(
        self,
        timestamp_ms: int,
        sequence: int,
        image_bytes_size: int,
        queue_size: int,
        device_id: str | None = None,
    ):
        with self.lock:
            self.summary["relay_enqueued_count"] += 1

        self.record_event(
            "relay_enqueued",
            {
                "device_id": device_id,
                "timestamp_ms": timestamp_ms,
                "sequence": sequence,
                "image_bytes": image_bytes_size,
                "relay_queue_size": queue_size,
            },
        )

    def record_relay_closed(self, success: bool, received_count: int, message: str):
        with self.lock:
            self.summary["relay_closed_count"] += 1

        self.record_event(
            "relay_closed",
            {
                "success": success,
                "received_count": received_count,
                "message": message,
            },
        )

    def record_relay_error(self, error: str):
        with self.lock:
            self.summary["relay_error_count"] += 1

        self.record_event("relay_error", {"error": error})

    def record_schedule_lag(
        self,
        skipped: int,
        loop_elapsed: float,
        device_id: str | None = None,
    ):
        with self.lock:
            self.summary["schedule_lag_count"] += 1
            self.summary["schedule_lag_skipped_total"] += skipped

        self.record_event(
            "schedule_lag",
            {
                "device_id": device_id,
                "skipped": skipped,
                "loop_elapsed_s": round(loop_elapsed, 6),
            },
        )

    def close(self):
        ended_at_ms = int(time.time() * 1000)
        duration_s = time.monotonic() - self.started_at_monotonic

        with self.lock:
            self.summary["ended_at_ms"] = ended_at_ms
            self.summary["duration_s"] = round(duration_s, 6)

            captured_count = self.summary["captured_count"]
            if captured_count:
                self.summary["offset_ms_avg"] = self.summary["offset_ms_sum"] / captured_count
                self.summary["capture_elapsed_s_avg"] = (
                    self.summary["capture_elapsed_s_sum"] / captured_count
                )
                self.summary["save_elapsed_s_avg"] = (
                    self.summary["save_elapsed_s_sum"] / captured_count
                )
                self.summary["average_fps"] = (
                    captured_count / duration_s if duration_s > 0 else 0.0
                )
            else:
                self.summary["average_fps"] = 0.0

            summary_payload = dict(self.summary)

        self.record_event(
            "experiment_finished",
            {
                "duration_s": summary_payload["duration_s"],
                "captured_count": summary_payload["captured_count"],
                "registered_count": summary_payload["registered_count"],
                "relay_enqueued_count": summary_payload["relay_enqueued_count"],
            },
        )

        with self.lock:
            self.events_file.close()

        with open(self.summary_path, "w", encoding="utf-8") as summary_file:
            json.dump(
                summary_payload,
                summary_file,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )


def sanitize_experiment_id(value: str) -> str:
    sanitized = []
    for char in value:
        if char.isalnum() or char in ("-", "_"):
            sanitized.append(char)
        else:
            sanitized.append("-")

    result = "".join(sanitized).strip("-")
    return result or "experiment"


def build_default_experiment_id(run_name: str, collector_type: str) -> str:
    timestamp_label = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    prefix_parts = [run_name]
    if collector_type != "stream_server":
        prefix_parts.append(collector_type)
    return "-".join(prefix_parts + [timestamp_label])


def start_generic_experiment_recorder(
    context: ExperimentContext,
) -> ExperimentRecorder | None:
    if not context.experiment_log_dir:
        return None

    recorder = ExperimentRecorder(context)
    print(f"[EXPERIMENT] events={recorder.events_path}")
    print(f"[EXPERIMENT] summary={recorder.summary_path}")
    return recorder


def close_experiment_recorder(recorder: ExperimentRecorder | None):
    if recorder is None:
        return

    recorder.close()
    print(f"[EXPERIMENT SAVED] summary={recorder.summary_path}")

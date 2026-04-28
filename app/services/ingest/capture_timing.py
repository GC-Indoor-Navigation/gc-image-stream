from app.services.stream.experiment_recorder import ExperimentRecorder


def calculate_next_capture_at(
    scheduled_at: float,
    interval_sec: float,
    now: float,
) -> float:
    next_capture_at = scheduled_at + interval_sec
    if next_capture_at > now:
        return next_capture_at

    missed_intervals = int((now - next_capture_at) // interval_sec) + 1
    return next_capture_at + (missed_intervals * interval_sec)


def log_capture(
    timestamp_ms: int,
    capture_label: str,
    capture_elapsed: float,
    save_elapsed: float,
    cycle_elapsed: float,
    queue_size: int,
    scheduled_at: float,
    captured_at: float,
    runtime_elapsed: float,
):
    offset_ms = max(0.0, (captured_at - scheduled_at) * 1000)

    print(
        "[CAPTURED] "
        f"runtime_s={runtime_elapsed:.3f} "
        f"timestamp={timestamp_ms} "
        f"{capture_label}={capture_elapsed:.3f}s "
        f"save={save_elapsed:.3f}s "
        f"cycle={cycle_elapsed:.3f}s "
        f"offset_ms={offset_ms:.1f} "
        f"queue={queue_size}"
    )


def log_schedule_lag(
    scheduled_at: float,
    interval_sec: float,
    loop_started_at: float,
    loop_finished_at: float,
    runtime_elapsed: float,
    experiment_recorder: ExperimentRecorder | None = None,
    device_id: str | None = None,
) -> float:
    adjusted_next_capture_at = calculate_next_capture_at(
        scheduled_at=scheduled_at,
        interval_sec=interval_sec,
        now=loop_finished_at,
    )

    if adjusted_next_capture_at > scheduled_at + interval_sec:
        skipped = int(
            round((adjusted_next_capture_at - (scheduled_at + interval_sec)) / interval_sec)
        )
        if skipped > 0:
            loop_elapsed = loop_finished_at - loop_started_at
            print(
                "[SCHEDULE LAG] "
                f"runtime_s={runtime_elapsed:.3f} "
                f"skipped={skipped} "
                f"loop={loop_elapsed:.3f}s"
            )
            if experiment_recorder is not None:
                experiment_recorder.record_schedule_lag(
                    skipped=skipped,
                    loop_elapsed=loop_elapsed,
                    device_id=device_id,
                )

    return adjusted_next_capture_at

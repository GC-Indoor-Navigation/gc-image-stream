import time
from threading import Event, Thread

import httpx

from app.db import SessionLocal
from app.services.ingest.adapters.base import CameraInputConfig, CameraInputRuntime
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.ingest.capture_timing import log_schedule_lag
from app.services.stream.stream_experiment import get_stream_experiment_recorder


def download_snapshot(
    session: httpx.Client,
    config: CameraInputConfig,
) -> bytes:
    response = session.get(config.source_url, timeout=config.capture_timeout_sec)
    response.raise_for_status()
    return response.content


def default_timestamp_factory(_sequence: int) -> int:
    return int(time.time() * 1000)


def run_snapshot_camera_session(
    config: CameraInputConfig,
    stop_event: Event,
    db_factory=SessionLocal,
    timestamp_factory=default_timestamp_factory,
    max_frames: int | None = None,
):
    if config.collect_interval_sec <= 0:
        raise ValueError("collect_interval_sec must be greater than 0")

    sequence = 0
    accepted_count = 0
    next_capture_at = time.monotonic()
    started_at = time.monotonic()

    with httpx.Client() as session:
        while not stop_event.is_set():
            sleep_sec = max(0.0, next_capture_at - time.monotonic())
            if sleep_sec > 0:
                time.sleep(sleep_sec)

            read_started_at = time.monotonic()
            scheduled_at = next_capture_at
            try:
                image_bytes = download_snapshot(session, config)
            except Exception:
                loop_finished_at = time.monotonic()
                next_capture_at = log_schedule_lag(
                    scheduled_at=scheduled_at,
                    interval_sec=config.collect_interval_sec,
                    loop_started_at=read_started_at,
                    loop_finished_at=loop_finished_at,
                    runtime_elapsed=loop_finished_at - started_at,
                    experiment_recorder=get_stream_experiment_recorder(),
                    device_id=config.device_id,
                )
                continue

            frame_ready_at = time.monotonic()
            sequence += 1
            accepted_count += 1
            timestamp_ms = timestamp_factory(sequence)

            db = db_factory()
            try:
                ingest_frame(
                    db,
                    device_id=config.device_id,
                    timestamp_ms=timestamp_ms,
                    sequence=sequence,
                    content_type=config.content_type,
                    image_bytes=image_bytes,
                )
            finally:
                db.close()

            ingested_at = time.monotonic()
            experiment_recorder = get_stream_experiment_recorder()
            if experiment_recorder is not None:
                experiment_recorder.record_capture(
                    device_id=config.device_id,
                    timestamp_ms=timestamp_ms,
                    sequence=sequence,
                    capture_label="snapshot",
                    capture_elapsed=frame_ready_at - read_started_at,
                    save_elapsed=ingested_at - frame_ready_at,
                    cycle_elapsed=ingested_at - read_started_at,
                    queue_size=0,
                    scheduled_at=scheduled_at,
                    captured_at=frame_ready_at,
                    image_bytes_size=len(image_bytes),
                )

            next_capture_at = log_schedule_lag(
                scheduled_at=scheduled_at,
                interval_sec=config.collect_interval_sec,
                loop_started_at=read_started_at,
                loop_finished_at=ingested_at,
                runtime_elapsed=ingested_at - started_at,
                experiment_recorder=experiment_recorder,
                device_id=config.device_id,
            )

            if max_frames is not None and accepted_count >= max_frames:
                break


def start_snapshot_camera_session(
    config: CameraInputConfig,
    db_factory=SessionLocal,
) -> CameraInputRuntime:
    stop_event = Event()
    worker = Thread(
        target=run_snapshot_camera_session,
        args=(config, stop_event, db_factory),
        daemon=True,
    )
    worker.start()
    return CameraInputRuntime(stop_event=stop_event, worker=worker)

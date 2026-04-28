import time
from threading import Event, Thread
from typing import Callable, Iterable

import httpx

from app.db import SessionLocal
from app.services.ingest.core import ingest_frame
from app.services.ingest.adapters.base import (
    CameraInputConfig,
    CameraInputRuntime,
    stop_camera_input,
)
from app.services.ingest.adapters.mjpeg_stream import iter_mjpeg_frames
from app.services.ingest.timing import log_schedule_lag
from app.services.stream.experiment import get_stream_experiment_recorder


FrameIteratorFactory = Callable[[httpx.Client, CameraInputConfig], Iterable[bytes]]
TimestampFactory = Callable[[int], int]


def default_frame_iterator_factory(
    session: httpx.Client,
    config: CameraInputConfig,
) -> Iterable[bytes]:
    return iter_mjpeg_frames(
        session=session,
        url=config.source_url,
        timeout_sec=config.capture_timeout_sec,
    )


def default_timestamp_factory(_sequence: int) -> int:
    return int(time.time() * 1000)


def run_mjpeg_camera_session(
    config: CameraInputConfig,
    stop_event: Event,
    db_factory=SessionLocal,
    frame_iterator_factory: FrameIteratorFactory = default_frame_iterator_factory,
    timestamp_factory: TimestampFactory = default_timestamp_factory,
    max_frames: int | None = None,
):
    if config.collect_interval_sec < 0:
        raise ValueError("collect_interval_sec must be greater than or equal to 0")

    sequence = 0
    accepted_count = 0
    next_capture_at = time.monotonic()
    started_at = time.monotonic()

    with httpx.Client() as session:
        frame_iter = iter(frame_iterator_factory(session, config))

        while not stop_event.is_set():
            read_started_at = time.monotonic()
            try:
                image_bytes = next(frame_iter)
            except StopIteration:
                break

            frame_ready_at = time.monotonic()
            if frame_ready_at < next_capture_at:
                continue

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
                    capture_label="stream_server",
                    capture_elapsed=frame_ready_at - read_started_at,
                    save_elapsed=ingested_at - frame_ready_at,
                    cycle_elapsed=ingested_at - read_started_at,
                    queue_size=0,
                    scheduled_at=next_capture_at,
                    captured_at=frame_ready_at,
                    image_bytes_size=len(image_bytes),
                )

            if config.collect_interval_sec == 0:
                next_capture_at = time.monotonic()
            else:
                next_capture_at = log_schedule_lag(
                    scheduled_at=next_capture_at,
                    interval_sec=config.collect_interval_sec,
                    loop_started_at=read_started_at,
                    loop_finished_at=ingested_at,
                    runtime_elapsed=ingested_at - started_at,
                    experiment_recorder=experiment_recorder,
                    device_id=config.device_id,
                )

            if max_frames is not None and accepted_count >= max_frames:
                break


def start_mjpeg_camera_session(
    config: CameraInputConfig,
    db_factory=SessionLocal,
) -> CameraInputRuntime:
    stop_event = Event()
    worker = Thread(
        target=run_mjpeg_camera_session,
        args=(config, stop_event, db_factory),
        daemon=True,
    )
    worker.start()
    return CameraInputRuntime(stop_event=stop_event, worker=worker)


CameraSessionConfig = CameraInputConfig
CameraSessionRuntime = CameraInputRuntime
stop_camera_session = stop_camera_input

import time
from dataclasses import dataclass
from threading import Event, Thread
from typing import Callable, Iterable

import httpx

from app.db import SessionLocal
from app.services.stream_ingest_service import ingest_frame
from app.services.stream_experiment_service import get_stream_experiment_recorder
from camera.collector.timing import log_schedule_lag
from camera.mjpeg_stream import iter_mjpeg_frames


@dataclass(frozen=True)
class CameraSessionConfig:
    device_id: str
    source_url: str
    collect_interval_sec: float
    capture_timeout_sec: float = 10.0
    content_type: str = "image/jpeg"


@dataclass(frozen=True)
class CameraSessionRuntime:
    stop_event: Event
    worker: Thread


FrameIteratorFactory = Callable[[httpx.Client, CameraSessionConfig], Iterable[bytes]]
TimestampFactory = Callable[[int], int]


def default_frame_iterator_factory(
    session: httpx.Client,
    config: CameraSessionConfig,
) -> Iterable[bytes]:
    return iter_mjpeg_frames(
        session=session,
        url=config.source_url,
        timeout_sec=config.capture_timeout_sec,
    )


def default_timestamp_factory(_sequence: int) -> int:
    return int(time.time() * 1000)


def run_mjpeg_camera_session(
    config: CameraSessionConfig,
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
    config: CameraSessionConfig,
    db_factory=SessionLocal,
) -> CameraSessionRuntime:
    stop_event = Event()
    worker = Thread(
        target=run_mjpeg_camera_session,
        args=(config, stop_event, db_factory),
        daemon=True,
    )
    worker.start()
    return CameraSessionRuntime(stop_event=stop_event, worker=worker)


def stop_camera_session(runtime: CameraSessionRuntime, timeout_sec: float = 2.0):
    runtime.stop_event.set()
    runtime.worker.join(timeout=timeout_sec)

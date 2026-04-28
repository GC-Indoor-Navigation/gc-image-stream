from dataclasses import dataclass
from threading import Event, Thread


@dataclass(frozen=True)
class CameraInputConfig:
    device_id: str
    source_url: str
    collect_interval_sec: float
    source_kind: str = "mjpeg"
    capture_timeout_sec: float = 10.0
    content_type: str = "image/jpeg"


@dataclass(frozen=True)
class CameraInputRuntime:
    stop_event: Event
    worker: Thread


def stop_camera_input(runtime: CameraInputRuntime, timeout_sec: float = 2.0):
    runtime.stop_event.set()
    runtime.worker.join(timeout=timeout_sec)

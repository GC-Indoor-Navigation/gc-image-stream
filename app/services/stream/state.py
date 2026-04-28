import time
from collections import deque
from dataclasses import dataclass, field
from threading import Lock


@dataclass
class StreamFrameState:
    frame_id: int
    device_id: str
    timestamp: int
    sequence: int | None
    file_path: str
    content_type: str
    image_bytes_size: int
    received_at_ms: int


@dataclass
class CameraStreamState:
    device_id: str
    latest_frame: StreamFrameState | None = None
    frame_count: int = 0
    sequence_gap_count: int = 0
    last_sequence: int | None = None
    recent_received_at_ms: deque[int] = field(default_factory=lambda: deque(maxlen=300))


class StreamState:
    def __init__(self):
        self._lock = Lock()
        self._cameras: dict[str, CameraStreamState] = {}

    def update_frame(
        self,
        frame_id: int,
        device_id: str,
        timestamp: int,
        sequence: int | None,
        file_path: str,
        content_type: str,
        image_bytes_size: int,
        received_at_ms: int | None = None,
    ) -> CameraStreamState:
        now_ms = received_at_ms if received_at_ms is not None else current_time_ms()

        with self._lock:
            camera = self._cameras.setdefault(
                device_id,
                CameraStreamState(device_id=device_id),
            )

            if (
                sequence is not None
                and camera.last_sequence is not None
                and sequence > camera.last_sequence + 1
            ):
                camera.sequence_gap_count += sequence - camera.last_sequence - 1

            if sequence is not None:
                camera.last_sequence = sequence

            camera.frame_count += 1
            camera.recent_received_at_ms.append(now_ms)
            camera.latest_frame = StreamFrameState(
                frame_id=frame_id,
                device_id=device_id,
                timestamp=timestamp,
                sequence=sequence,
                file_path=file_path,
                content_type=content_type,
                image_bytes_size=image_bytes_size,
                received_at_ms=now_ms,
            )
            return copy_camera_state(camera)

    def get_camera(self, device_id: str) -> CameraStreamState | None:
        with self._lock:
            camera = self._cameras.get(device_id)
            if camera is None:
                return None
            return copy_camera_state(camera)

    def list_cameras(self) -> list[CameraStreamState]:
        with self._lock:
            return [copy_camera_state(camera) for camera in self._cameras.values()]

    def clear(self):
        with self._lock:
            self._cameras.clear()


def current_time_ms() -> int:
    return int(time.time() * 1000)


def copy_camera_state(camera: CameraStreamState) -> CameraStreamState:
    copied = CameraStreamState(
        device_id=camera.device_id,
        latest_frame=camera.latest_frame,
        frame_count=camera.frame_count,
        sequence_gap_count=camera.sequence_gap_count,
        last_sequence=camera.last_sequence,
    )
    copied.recent_received_at_ms = deque(
        camera.recent_received_at_ms,
        maxlen=camera.recent_received_at_ms.maxlen,
    )
    return copied


stream_state = StreamState()

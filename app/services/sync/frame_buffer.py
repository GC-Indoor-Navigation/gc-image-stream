from collections import deque
from threading import Lock

from app.services.sync.models import StoredSyncFrame, SyncInputFrame


class CameraSyncBuffer:
    def __init__(self, max_frames: int):
        self.max_frames = max_frames
        self.frames: deque[StoredSyncFrame] = deque(maxlen=max_frames)
        self.received_count = 0
        self.last_sequence: int | None = None
        self.last_timestamp_ms: int | None = None
        self.sequence_gap_count = 0

    def append(self, frame: StoredSyncFrame):
        if (
            self.last_sequence is not None
            and frame.sequence is not None
            and frame.sequence > self.last_sequence + 1
        ):
            self.sequence_gap_count += frame.sequence - self.last_sequence - 1
        self.frames.append(frame)
        self.received_count += 1
        if frame.sequence is not None:
            self.last_sequence = frame.sequence
        self.last_timestamp_ms = frame.timestamp_ms

    def nearest_frame(
        self,
        anchor_timestamp_ms: int,
        window_ms: int,
    ) -> StoredSyncFrame | None:
        nearest: StoredSyncFrame | None = None
        nearest_delta: int | None = None
        for frame in self.frames:
            delta = abs(frame.timestamp_ms - anchor_timestamp_ms)
            if delta <= window_ms and (
                nearest_delta is None or delta < nearest_delta
            ):
                nearest = frame
                nearest_delta = delta
        return nearest

    def status(self, device_id: str) -> dict:
        return {
            "device_id": device_id,
            "buffered_count": len(self.frames),
            "received_count": self.received_count,
            "sequence_gap_count": self.sequence_gap_count,
            "last_sequence": self.last_sequence,
            "last_timestamp_ms": self.last_timestamp_ms,
        }


class SyncFrameBufferManager:
    def __init__(self, buffer_size: int = 120):
        self.buffer_size = buffer_size
        self._buffers: dict[str, CameraSyncBuffer] = {}
        self._frames_by_id: dict[int, StoredSyncFrame] = {}
        self._received_count = 0
        self._duplicate_frame_count = 0
        self._lock = Lock()

    def add_frame(self, frame: SyncInputFrame) -> StoredSyncFrame | None:
        stored = StoredSyncFrame(
            frame_id=frame.frame_id,
            device_id=frame.device_id,
            timestamp_ms=frame.timestamp_ms,
            sequence=frame.sequence,
            content_type=frame.content_type,
            image_bytes=frame.image_bytes,
            image_size=len(frame.image_bytes),
            file_path=frame.file_path,
        )

        with self._lock:
            if stored.frame_id in self._frames_by_id:
                self._duplicate_frame_count += 1
                return None

            buffer = self._buffers.setdefault(
                stored.device_id,
                CameraSyncBuffer(max_frames=self.buffer_size),
            )
            buffer.append(stored)
            self._frames_by_id[stored.frame_id] = stored
            self._received_count += 1
            return stored

    def nearest_frame(
        self,
        device_id: str,
        anchor_timestamp_ms: int,
        window_ms: int,
    ) -> StoredSyncFrame | None:
        with self._lock:
            buffer = self._buffers.get(device_id)
            if buffer is None:
                return None
            return buffer.nearest_frame(anchor_timestamp_ms, window_ms)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "camera_count": len(self._buffers),
                "received_count": self._received_count,
                "duplicate_frame_count": self._duplicate_frame_count,
                "buffer_size": self.buffer_size,
                "cameras": [
                    self._buffers[device_id].status(device_id)
                    for device_id in sorted(self._buffers)
                ],
            }

    def clear(self):
        with self._lock:
            self._buffers.clear()
            self._frames_by_id.clear()
            self._received_count = 0
            self._duplicate_frame_count = 0

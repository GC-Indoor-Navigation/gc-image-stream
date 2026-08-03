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
        exclude_frame_ids: set[int] | None = None,
    ) -> StoredSyncFrame | None:
        excluded = exclude_frame_ids or set()
        nearest: StoredSyncFrame | None = None
        nearest_delta: int | None = None
        for frame in self.frames:
            if frame.frame_id in excluded:
                continue
            delta = abs(frame.timestamp_ms - anchor_timestamp_ms)
            if delta <= window_ms and (
                nearest_delta is None or delta < nearest_delta
            ):
                nearest = frame
                nearest_delta = delta
        return nearest

    def available_frames(
        self,
        exclude_frame_ids: set[int] | None = None,
    ) -> list[StoredSyncFrame]:
        excluded = exclude_frame_ids or set()
        return sorted(
            (
                frame
                for frame in self.frames
                if frame.frame_id not in excluded
            ),
            key=lambda frame: (frame.timestamp_ms, frame.frame_id),
        )

    def drop_frames_older_than(
        self,
        timestamp_ms: int,
        counted_exclude_frame_ids: set[int] | None = None,
    ) -> int:
        counted_excluded = counted_exclude_frame_ids or set()
        kept: deque[StoredSyncFrame] = deque(maxlen=self.max_frames)
        dropped_count = 0
        for frame in self.frames:
            if frame.timestamp_ms < timestamp_ms:
                if frame.frame_id not in counted_excluded:
                    dropped_count += 1
                continue
            kept.append(frame)
        self.frames = kept
        return dropped_count

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
            source_session_id=frame.source_session_id,
            camera_stream_id=frame.camera_stream_id,
            source_frame_uid=frame.source_frame_uid,
            content_digest=frame.content_digest,
            identity_mode=frame.identity_mode,
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
        exclude_frame_ids: set[int] | None = None,
    ) -> StoredSyncFrame | None:
        with self._lock:
            buffer = self._buffers.get(device_id)
            if buffer is None:
                return None
            return buffer.nearest_frame(
                anchor_timestamp_ms,
                window_ms,
                exclude_frame_ids=exclude_frame_ids,
            )

    def available_frames(
        self,
        device_id: str,
        exclude_frame_ids: set[int] | None = None,
    ) -> list[StoredSyncFrame]:
        with self._lock:
            buffer = self._buffers.get(device_id)
            if buffer is None:
                return []
            return buffer.available_frames(exclude_frame_ids=exclude_frame_ids)

    def latest_timestamps(self, device_ids: list[str]) -> dict[str, int]:
        with self._lock:
            latest: dict[str, int] = {}
            for device_id in device_ids:
                buffer = self._buffers.get(device_id)
                if buffer is None or buffer.last_timestamp_ms is None:
                    continue
                latest[device_id] = buffer.last_timestamp_ms
            return latest

    def drop_frames_older_than(
        self,
        device_ids: list[str],
        timestamp_ms: int,
        counted_exclude_frame_ids: set[int] | None = None,
    ) -> int:
        with self._lock:
            dropped_count = 0
            for device_id in device_ids:
                buffer = self._buffers.get(device_id)
                if buffer is None:
                    continue
                dropped_count += buffer.drop_frames_older_than(
                    timestamp_ms,
                    counted_exclude_frame_ids=counted_exclude_frame_ids,
                )
            return dropped_count

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

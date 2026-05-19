from collections import deque

from app.services.sync.frame_buffer import SyncFrameBufferManager
from app.services.sync.models import StoredSyncFrame, SynchronizedFrameSet


class SyncMatcher:
    def __init__(
        self,
        buffer_manager: SyncFrameBufferManager,
        expected_cameras: list[str],
        window_ms: int,
        recent_limit: int = 20,
    ):
        self.buffer_manager = buffer_manager
        self.expected_cameras = list(expected_cameras)
        self.window_ms = window_ms
        self.recent_limit = recent_limit
        self._next_frame_set_id = 1
        self._emitted_keys: set[tuple[int, ...]] = set()
        self._recent_frame_sets: deque[SynchronizedFrameSet] = deque(
            maxlen=recent_limit
        )
        self.matched_count = 0
        self.missed_count = 0
        self.duplicate_count = 0
        self.ignored_count = 0
        self.last_frame_set_id: int | None = None
        self.last_anchor_timestamp_ms: int | None = None
        self.last_max_delta_ms: int | None = None
        self.last_missing_cameras: list[str] = []
        self.last_reason: str | None = None

    def try_match(self, anchor_frame: StoredSyncFrame) -> SynchronizedFrameSet | None:
        if not self.expected_cameras:
            self._record_miss(
                anchor_frame=anchor_frame,
                reason="expected cameras are not configured",
                missing_cameras=[],
            )
            return None
        if anchor_frame.device_id not in self.expected_cameras:
            self.ignored_count += 1
            self.last_anchor_timestamp_ms = anchor_frame.timestamp_ms
            self.last_missing_cameras = []
            self.last_reason = f"unexpected camera: {anchor_frame.device_id}"
            return None

        selected: dict[str, StoredSyncFrame] = {}
        missing_cameras: list[str] = []
        for device_id in self.expected_cameras:
            frame = self.buffer_manager.nearest_frame(
                device_id=device_id,
                anchor_timestamp_ms=anchor_frame.timestamp_ms,
                window_ms=self.window_ms,
            )
            if frame is None:
                missing_cameras.append(device_id)
                continue
            selected[device_id] = frame
        if missing_cameras:
            self._record_miss(
                anchor_frame=anchor_frame,
                reason="missing cameras inside sync window",
                missing_cameras=missing_cameras,
            )
            return None

        key = tuple(sorted(frame.frame_id for frame in selected.values()))
        if key in self._emitted_keys:
            self.duplicate_count += 1
            self.last_anchor_timestamp_ms = anchor_frame.timestamp_ms
            self.last_missing_cameras = []
            self.last_reason = "duplicate frame set"
            return None
        self._emitted_keys.add(key)

        max_delta_ms = max(
            abs(frame.timestamp_ms - anchor_frame.timestamp_ms)
            for frame in selected.values()
        )
        frame_set = SynchronizedFrameSet(
            frame_set_id=self._next_frame_set_id,
            anchor_timestamp_ms=anchor_frame.timestamp_ms,
            max_delta_ms=max_delta_ms,
            frames=selected,
        )
        self._next_frame_set_id += 1
        self.matched_count += 1
        self.last_frame_set_id = frame_set.frame_set_id
        self.last_anchor_timestamp_ms = anchor_frame.timestamp_ms
        self.last_max_delta_ms = max_delta_ms
        self.last_missing_cameras = []
        self.last_reason = "matched"
        self._recent_frame_sets.append(frame_set)
        return frame_set

    def status(self):
        return {
            "matched_count": self.matched_count,
            "missed_count": self.missed_count,
            "duplicate_count": self.duplicate_count,
            "ignored_count": self.ignored_count,
            "last_frame_set_id": self.last_frame_set_id,
            "last_anchor_timestamp_ms": self.last_anchor_timestamp_ms,
            "last_max_delta_ms": self.last_max_delta_ms,
            "last_missing_cameras": self.last_missing_cameras,
            "last_reason": self.last_reason,
        }

    def recent_frame_sets(self) -> list[SynchronizedFrameSet]:
        return list(self._recent_frame_sets)

    def _record_miss(
        self,
        anchor_frame: StoredSyncFrame,
        reason: str,
        missing_cameras: list[str],
    ):
        self.missed_count += 1
        self.last_anchor_timestamp_ms = anchor_frame.timestamp_ms
        self.last_missing_cameras = missing_cameras
        self.last_reason = reason

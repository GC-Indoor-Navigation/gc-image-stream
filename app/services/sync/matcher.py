from collections import deque
from heapq import heappop, heappush

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
        self._used_frame_ids: set[int] = set()
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
        self.last_span_ms: int | None = None
        self.last_watermark_timestamp_ms: int | None = None
        self.dropped_stale_count = 0
        self.last_missing_cameras: list[str] = []
        self.last_reason: str | None = None

    def try_match(self, trigger_frame: StoredSyncFrame) -> SynchronizedFrameSet | None:
        if not self.expected_cameras:
            self._record_miss(
                trigger_frame=trigger_frame,
                reason="expected cameras are not configured",
                missing_cameras=[],
            )
            return None
        if trigger_frame.device_id not in self.expected_cameras:
            self.ignored_count += 1
            self.last_anchor_timestamp_ms = trigger_frame.timestamp_ms
            self.last_missing_cameras = []
            self.last_reason = f"unexpected camera: {trigger_frame.device_id}"
            return None

        selected, span_ms, missing_cameras = self._select_min_span_candidate()
        if missing_cameras:
            self._record_miss(
                trigger_frame=trigger_frame,
                reason="missing cameras inside sync window",
                missing_cameras=missing_cameras,
            )
            self._drop_stale_frames()
            return None
        if selected is None or span_ms is None:
            self._record_miss(
                trigger_frame=trigger_frame,
                reason="no frame set inside sync window",
                missing_cameras=[],
            )
            self._drop_stale_frames()
            return None

        key = tuple(sorted(frame.frame_id for frame in selected.values()))
        if key in self._emitted_keys:
            self.duplicate_count += 1
            self.last_anchor_timestamp_ms = trigger_frame.timestamp_ms
            self.last_missing_cameras = []
            self.last_reason = "duplicate frame set"
            self._drop_stale_frames()
            return None
        self._emitted_keys.add(key)
        self._used_frame_ids.update(key)

        anchor_timestamp_ms = max(frame.timestamp_ms for frame in selected.values())
        frame_set = SynchronizedFrameSet(
            frame_set_id=self._next_frame_set_id,
            anchor_timestamp_ms=anchor_timestamp_ms,
            max_delta_ms=span_ms,
            frames=selected,
            span_ms=span_ms,
        )
        self._next_frame_set_id += 1
        self.matched_count += 1
        self.last_frame_set_id = frame_set.frame_set_id
        self.last_anchor_timestamp_ms = anchor_timestamp_ms
        self.last_max_delta_ms = span_ms
        self.last_span_ms = span_ms
        self.last_missing_cameras = []
        self.last_reason = "matched"
        self._recent_frame_sets.append(frame_set)
        self._drop_stale_frames()
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
            "last_span_ms": self.last_span_ms,
            "watermark_timestamp_ms": self.last_watermark_timestamp_ms,
            "dropped_stale_count": self.dropped_stale_count,
            "last_missing_cameras": self.last_missing_cameras,
            "last_reason": self.last_reason,
        }

    def recent_frame_sets(self) -> list[SynchronizedFrameSet]:
        return list(self._recent_frame_sets)

    def _record_miss(
        self,
        trigger_frame: StoredSyncFrame,
        reason: str,
        missing_cameras: list[str],
    ):
        self.missed_count += 1
        self.last_anchor_timestamp_ms = trigger_frame.timestamp_ms
        self.last_missing_cameras = missing_cameras
        self.last_reason = reason

    def _select_min_span_candidate(
        self,
    ) -> tuple[dict[str, StoredSyncFrame] | None, int | None, list[str]]:
        candidate_lists: dict[str, list[StoredSyncFrame]] = {}
        missing_cameras: list[str] = []
        for device_id in self.expected_cameras:
            frames = self.buffer_manager.available_frames(
                device_id,
                exclude_frame_ids=self._used_frame_ids,
            )
            if not frames:
                missing_cameras.append(device_id)
                continue
            candidate_lists[device_id] = frames
        if missing_cameras:
            return None, None, missing_cameras

        heap: list[tuple[int, int, str, int, StoredSyncFrame]] = []
        current_frames: dict[str, StoredSyncFrame] = {}
        current_max_timestamp_ms: int | None = None
        for device_id, frames in candidate_lists.items():
            frame = frames[0]
            current_frames[device_id] = frame
            current_max_timestamp_ms = (
                frame.timestamp_ms
                if current_max_timestamp_ms is None
                else max(current_max_timestamp_ms, frame.timestamp_ms)
            )
            heappush(heap, (frame.timestamp_ms, frame.frame_id, device_id, 0, frame))

        best_frames: dict[str, StoredSyncFrame] | None = None
        best_span_ms: int | None = None
        while heap and current_max_timestamp_ms is not None:
            min_timestamp_ms, _, device_id, index, frame = heappop(heap)
            span_ms = current_max_timestamp_ms - min_timestamp_ms
            if best_span_ms is None or span_ms < best_span_ms:
                best_span_ms = span_ms
                best_frames = dict(current_frames)
                if span_ms == 0:
                    break

            next_index = index + 1
            frames = candidate_lists[device_id]
            if next_index >= len(frames):
                break
            next_frame = frames[next_index]
            current_frames[device_id] = next_frame
            current_max_timestamp_ms = max(
                current_max_timestamp_ms,
                next_frame.timestamp_ms,
            )
            heappush(
                heap,
                (
                    next_frame.timestamp_ms,
                    next_frame.frame_id,
                    device_id,
                    next_index,
                    next_frame,
                ),
            )

        if best_frames is None or best_span_ms is None:
            return None, None, []
        if best_span_ms > self.window_ms:
            return None, None, []
        return best_frames, best_span_ms, []

    def _drop_stale_frames(self):
        latest_timestamps = self.buffer_manager.latest_timestamps(self.expected_cameras)
        if len(latest_timestamps) != len(self.expected_cameras):
            return
        watermark_timestamp_ms = min(latest_timestamps.values())
        self.last_watermark_timestamp_ms = watermark_timestamp_ms
        drop_before_timestamp_ms = watermark_timestamp_ms - self.window_ms
        dropped_count = self.buffer_manager.drop_frames_older_than(
            self.expected_cameras,
            drop_before_timestamp_ms,
            counted_exclude_frame_ids=self._used_frame_ids,
        )
        self.dropped_stale_count += dropped_count

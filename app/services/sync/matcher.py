from collections import deque
from dataclasses import replace
from heapq import heappop, heappush
import time
from uuid import uuid4

from app.services.sync.frame_buffer import SyncFrameBufferManager
from app.services.sync.models import StoredSyncFrame, SynchronizedFrameSet
from app.services.identity import (
    IDENTITY_MODE_LEGACY,
    IDENTITY_MODE_V2,
    build_capture_session_id,
    build_frame_set_uid,
    build_manifest_digest,
    canonical_json,
)


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
        self._capture_session_id: str | None = None
        self._capture_config_key: tuple[tuple[str, str], ...] | None = None
        self._capture_run_id = str(uuid4())
        self._last_synchronized_at_ms = 0
        self._emitted_keys: set[tuple[str, ...]] = set()
        self._used_frame_keys: set[str] = set()
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
        self.last_identity_mode: str | None = None
        self.legacy_identity_count = 0
        self.last_archive_state: str | None = None
        self.last_archive_error: str | None = None
        self.archive_degraded_count = 0

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

        key = tuple(sorted(frame.buffer_key for frame in selected.values()))
        if key in self._emitted_keys:
            self.duplicate_count += 1
            self.last_anchor_timestamp_ms = trigger_frame.timestamp_ms
            self.last_missing_cameras = []
            self.last_reason = "duplicate frame set"
            self._drop_stale_frames()
            return None
        self._emitted_keys.add(key)
        self._used_frame_keys.update(key)

        (
            identity_mode,
            capture_session_id,
            capture_config_key,
        ) = _resolve_capture_identity(selected)
        if (
            capture_session_id is not None
            and self._capture_session_id is not None
            and (
                capture_session_id != self._capture_session_id
                or capture_config_key != self._capture_config_key
            )
        ):
            self._capture_run_id = str(uuid4())
            self._next_frame_set_id = 1
        if capture_session_id is not None:
            self._capture_session_id = capture_session_id
            self._capture_config_key = capture_config_key

        anchor_timestamp_ms = max(frame.timestamp_ms for frame in selected.values())
        frame_set_id = self._next_frame_set_id
        manifest_payload = _build_manifest_payload(
            capture_session_id=capture_session_id,
            frames=selected,
            identity_mode=identity_mode,
            sync_window_ms=self.window_ms,
            synchronization_span_ms=span_ms,
            anchor_timestamp_ms=anchor_timestamp_ms,
        )
        manifest_digest = build_manifest_digest(manifest_payload)
        degraded_frames = [
            frame
            for frame in selected.values()
            if frame.archive_state == "ARCHIVE_DEGRADED_LIVE_ONLY"
        ]
        synchronized_at_ms = max(
            int(time.time() * 1000),
            self._last_synchronized_at_ms + 1,
        )
        self._last_synchronized_at_ms = synchronized_at_ms
        frame_set = SynchronizedFrameSet(
            frame_set_id=frame_set_id,
            anchor_timestamp_ms=anchor_timestamp_ms,
            max_delta_ms=span_ms,
            frames=selected,
            span_ms=span_ms,
            capture_session_id=capture_session_id,
            capture_run_id=self._capture_run_id,
            frame_set_uid=(
                build_frame_set_uid(manifest_digest)
                if identity_mode == IDENTITY_MODE_V2
                else None
            ),
            manifest_digest=manifest_digest,
            manifest_json=canonical_json(manifest_payload),
            identity_mode=identity_mode,
            archive_state=(
                "ARCHIVE_DEGRADED_LIVE_ONLY"
                if degraded_frames
                else "ARCHIVE_PENDING"
            ),
            archive_error=(
                "; ".join(
                    sorted(
                        {
                            frame.archive_error or "member archive unavailable"
                            for frame in degraded_frames
                        }
                    )
                )
                if degraded_frames
                else None
            ),
            sync_window_ms=self.window_ms,
            synchronized_at_ms=synchronized_at_ms,
            member_count=len(selected),
        )
        self._next_frame_set_id += 1
        self.matched_count += 1
        self.last_frame_set_id = frame_set.frame_set_id
        self.last_anchor_timestamp_ms = anchor_timestamp_ms
        self.last_max_delta_ms = span_ms
        self.last_span_ms = span_ms
        self.last_missing_cameras = []
        self.last_reason = "matched"
        self.last_identity_mode = identity_mode
        if identity_mode == IDENTITY_MODE_LEGACY:
            self.legacy_identity_count += 1
        self.last_archive_state = frame_set.archive_state
        self.last_archive_error = frame_set.archive_error
        if frame_set.archive_state == "ARCHIVE_DEGRADED_LIVE_ONLY":
            self.archive_degraded_count += 1
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
            "capture_session_id": self._capture_session_id,
            "capture_run_id": self._capture_run_id,
            "last_identity_mode": self.last_identity_mode,
            "legacy_identity_count": self.legacy_identity_count,
            "last_archive_state": self.last_archive_state,
            "last_archive_error": self.last_archive_error,
            "archive_degraded_count": self.archive_degraded_count,
        }

    def recent_frame_sets(self) -> list[SynchronizedFrameSet]:
        return list(self._recent_frame_sets)

    def finalize_archive_state(
        self,
        frame_set: SynchronizedFrameSet,
        *,
        state: str,
        error: str | None,
    ) -> SynchronizedFrameSet:
        updated = replace(frame_set, archive_state=state, archive_error=error)
        if (
            frame_set.archive_state != "ARCHIVE_DEGRADED_LIVE_ONLY"
            and state == "ARCHIVE_DEGRADED_LIVE_ONLY"
        ):
            self.archive_degraded_count += 1
        self.last_archive_state = state
        self.last_archive_error = error
        self._replace_recent_frame_set(updated)
        return updated

    def replace_frame_member(
        self,
        frame_set: SynchronizedFrameSet,
        frame: StoredSyncFrame,
    ) -> SynchronizedFrameSet:
        frames = dict(frame_set.frames)
        for device_id, member in frames.items():
            if member.buffer_key == frame.buffer_key:
                frames[device_id] = frame
                break
        degraded = [
            member
            for member in frames.values()
            if member.archive_state == "ARCHIVE_DEGRADED_LIVE_ONLY"
        ]
        updated = replace(
            frame_set,
            frames=frames,
            archive_state=(
                "ARCHIVE_DEGRADED_LIVE_ONLY" if degraded else "ARCHIVE_PENDING"
            ),
            archive_error=(
                "; ".join(
                    sorted(
                        {
                            member.archive_error or "member archive unavailable"
                            for member in degraded
                        }
                    )
                )
                if degraded
                else None
            ),
        )
        if (
            frame_set.archive_state != "ARCHIVE_DEGRADED_LIVE_ONLY"
            and updated.archive_state == "ARCHIVE_DEGRADED_LIVE_ONLY"
        ):
            self.archive_degraded_count += 1
        self.last_archive_state = updated.archive_state
        self.last_archive_error = updated.archive_error
        self._replace_recent_frame_set(updated)
        return updated

    def _replace_recent_frame_set(self, updated: SynchronizedFrameSet) -> None:
        self._recent_frame_sets = deque(
            [
                updated
                if item.capture_run_id == updated.capture_run_id
                and item.frame_set_id == updated.frame_set_id
                else item
                for item in self._recent_frame_sets
            ],
            maxlen=self.recent_limit,
        )

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
                exclude_frame_keys=self._used_frame_keys,
            )
            if not frames:
                missing_cameras.append(device_id)
                continue
            candidate_lists[device_id] = frames
        if missing_cameras:
            return None, None, missing_cameras

        heap: list[tuple[int, str, str, int, StoredSyncFrame]] = []
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
            heappush(
                heap,
                (frame.timestamp_ms, frame.buffer_key, device_id, 0, frame),
            )

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
                    next_frame.buffer_key,
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
            counted_exclude_frame_keys=self._used_frame_keys,
        )
        self.dropped_stale_count += dropped_count


def _resolve_capture_identity(
    frames: dict[str, StoredSyncFrame],
) -> tuple[str, str | None, tuple[tuple[str, str], ...] | None]:
    if not all(
        frame.identity_mode == IDENTITY_MODE_V2
        and frame.source_session_id
        and frame.camera_stream_id
        and frame.sequence is not None
        and frame.source_frame_uid
        and frame.content_digest
        for frame in frames.values()
    ):
        return IDENTITY_MODE_LEGACY, None, None
    return (
        IDENTITY_MODE_V2,
        build_capture_session_id(
            (frame.camera_stream_id, frame.source_session_id)
            for frame in frames.values()
        ),
        tuple(
            sorted(
                (
                    frame.camera_stream_id,
                    frame.capture_config_digest or "",
                )
                for frame in frames.values()
            )
        ),
    )


def _build_manifest_payload(
    *,
    capture_session_id: str | None,
    frames: dict[str, StoredSyncFrame],
    identity_mode: str,
    sync_window_ms: int,
    synchronization_span_ms: int,
    anchor_timestamp_ms: int,
) -> dict:
    return {
        "schema_version": 2,
        "identity_mode": identity_mode,
        "capture_session_id": capture_session_id,
        "synchronization": {
            "algorithm": "minimum-span-v1",
            "window_ms": sync_window_ms,
            "span_ms": synchronization_span_ms,
            "anchor_timestamp_ms": anchor_timestamp_ms,
            "freshness_origin_ms": min(
                frame.timestamp_ms for frame in frames.values()
            ),
        },
        "members": [
            {
                "device_id": frame.device_id,
                "source_session_id": frame.source_session_id,
                "camera_stream_id": frame.camera_stream_id,
                "frame_sequence": frame.sequence,
                "source_frame_uid": frame.source_frame_uid,
                "capture_timestamp_ms": frame.timestamp_ms,
                "content_type": frame.content_type,
                "image_size": frame.image_size,
                "content_digest": frame.content_digest,
                "capture_config_digest": frame.capture_config_digest,
                "capture_metadata_json": (
                    frame.capture_metadata_json
                    if frame.capture_metadata_json is not None
                    else "{}"
                ),
            }
            for frame in sorted(
                frames.values(),
                key=lambda item: (item.camera_stream_id or item.device_id),
            )
        ],
    }

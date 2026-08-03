from app.services.sync.frame_buffer import SyncFrameBufferManager
from app.services.sync.matcher import SyncMatcher
from app.services.sync.models import SyncInputFrame, SynchronizedFrameSet


class StreamSyncService:
    def __init__(self):
        self.enabled = False
        self.expected_cameras: list[str] = []
        self.window_ms = 50
        self.buffer_manager = SyncFrameBufferManager()
        self.matcher: SyncMatcher | None = None

    def configure(
        self,
        enabled: bool,
        expected_cameras: list[str] | tuple[str, ...],
        window_ms: int = 50,
        buffer_size: int = 120,
        recent_limit: int = 20,
    ):
        self.enabled = enabled
        self.expected_cameras = list(expected_cameras)
        self.window_ms = window_ms
        self.buffer_manager = SyncFrameBufferManager(buffer_size=buffer_size)
        self.matcher = (
            SyncMatcher(
                buffer_manager=self.buffer_manager,
                expected_cameras=self.expected_cameras,
                window_ms=window_ms,
                recent_limit=recent_limit,
            )
            if enabled
            else None
        )

    def handle_frame(self, frame: SyncInputFrame) -> SynchronizedFrameSet | None:
        if not self.enabled or self.matcher is None:
            return None

        stored = self.buffer_manager.add_frame(frame)
        if stored is None:
            return None
        return self.matcher.try_match(stored)

    def status(self) -> dict:
        buffer_snapshot = self.buffer_manager.snapshot()
        sync_status = (
            self.matcher.status()
            if self.matcher is not None
            else {
                "matched_count": 0,
                "missed_count": 0,
                "duplicate_count": 0,
                "ignored_count": 0,
                "last_frame_set_id": None,
                "last_anchor_timestamp_ms": None,
                "last_max_delta_ms": None,
                "last_span_ms": None,
                "watermark_timestamp_ms": None,
                "dropped_stale_count": 0,
                "last_missing_cameras": [],
                "last_reason": None,
                "capture_session_id": None,
                "capture_run_id": None,
                "last_identity_mode": None,
                "legacy_identity_count": 0,
            }
        )
        sync_progress = build_sync_progress(
            buffer_snapshot=buffer_snapshot,
            expected_cameras=self.expected_cameras,
            matched_count=sync_status["matched_count"],
        )
        return {
            "enabled": self.enabled,
            "expected_cameras": list(self.expected_cameras),
            "window_ms": self.window_ms,
            "buffer": buffer_snapshot,
            **sync_progress,
            **sync_status,
        }

    def recent_frame_sets(self) -> list[SynchronizedFrameSet]:
        if self.matcher is None:
            return []
        return self.matcher.recent_frame_sets()

    def clear(self):
        self.configure(
            enabled=False,
            expected_cameras=[],
            window_ms=50,
            buffer_size=120,
            recent_limit=20,
        )


stream_sync_service = StreamSyncService()


def build_sync_progress(
    *,
    buffer_snapshot: dict,
    expected_cameras: list[str],
    matched_count: int,
) -> dict:
    camera_counts = {
        camera["device_id"]: camera["received_count"]
        for camera in buffer_snapshot.get("cameras", [])
    }
    expected_counts = [
        camera_counts.get(device_id, 0)
        for device_id in expected_cameras
    ]
    expected_frame_set_count = (
        min(expected_counts)
        if expected_counts and all(count > 0 for count in expected_counts)
        else 0
    )
    matched_ratio = (
        matched_count / expected_frame_set_count
        if expected_frame_set_count > 0
        else 0.0
    )
    return {
        "expected_frame_set_count": expected_frame_set_count,
        "matched_ratio": matched_ratio,
        "per_expected_camera_received_count": dict(
            zip(expected_cameras, expected_counts, strict=False)
        ),
    }

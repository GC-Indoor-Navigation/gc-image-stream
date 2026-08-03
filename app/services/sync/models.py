from dataclasses import dataclass


@dataclass(frozen=True)
class SyncInputFrame:
    frame_id: int | None
    device_id: str
    timestamp_ms: int
    sequence: int | None
    content_type: str
    image_bytes: bytes
    file_path: str | None
    source_session_id: str | None = None
    camera_stream_id: str | None = None
    source_frame_uid: str | None = None
    content_digest: str | None = None
    identity_mode: str = "LEGACY"
    archive_state: str = "ARCHIVE_PENDING"
    archive_error: str | None = None


@dataclass(frozen=True)
class StoredSyncFrame:
    frame_id: int | None
    device_id: str
    timestamp_ms: int
    sequence: int | None
    content_type: str
    image_bytes: bytes
    image_size: int
    file_path: str | None
    buffer_key: str = ""
    source_session_id: str | None = None
    camera_stream_id: str | None = None
    source_frame_uid: str | None = None
    content_digest: str | None = None
    identity_mode: str = "LEGACY"
    archive_state: str = "ARCHIVE_PENDING"
    archive_error: str | None = None


@dataclass(frozen=True)
class SynchronizedFrameSet:
    frame_set_id: int
    anchor_timestamp_ms: int
    max_delta_ms: int
    frames: dict[str, StoredSyncFrame]
    span_ms: int | None = None
    capture_session_id: str | None = None
    capture_run_id: str | None = None
    frame_set_uid: str | None = None
    manifest_digest: str | None = None
    manifest_json: str | None = None
    identity_mode: str = "LEGACY"
    archive_state: str = "ARCHIVE_PENDING"
    archive_error: str | None = None

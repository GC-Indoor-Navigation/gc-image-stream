from dataclasses import dataclass


@dataclass(frozen=True)
class SyncInputFrame:
    frame_id: int
    device_id: str
    timestamp_ms: int
    sequence: int | None
    content_type: str
    image_bytes: bytes
    file_path: str


@dataclass(frozen=True)
class StoredSyncFrame:
    frame_id: int
    device_id: str
    timestamp_ms: int
    sequence: int | None
    content_type: str
    image_bytes: bytes
    image_size: int
    file_path: str


@dataclass(frozen=True)
class SynchronizedFrameSet:
    frame_set_id: int
    anchor_timestamp_ms: int
    max_delta_ms: int
    frames: dict[str, StoredSyncFrame]
    span_ms: int | None = None

import hashlib
import json
from collections.abc import Iterable
from uuid import NAMESPACE_URL, uuid5


SOURCE_FRAME_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "https://geocap.local/identity/source-frame/v2",
)
CAPTURE_SESSION_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "https://geocap.local/identity/capture-session/v2",
)
FRAME_SET_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "https://geocap.local/identity/frame-set/v2",
)

IDENTITY_MODE_V2 = "V2"
IDENTITY_MODE_LEGACY = "LEGACY"

CAPTURE_CONFIG_FIELDS = {
    "exposure_lock_requested",
    "exposure_time_ns_requested",
    "focal_length_mm",
    "focus_lock_requested",
    "focus_mode",
    "format",
    "fps_target",
    "height",
    "iso_requested",
    "manual_exposure_requested",
    "orientation_deg",
    "white_balance_lock_requested",
    "width",
    "zoom_disabled",
}


def canonical_camera_stream_id(device_id: str, camera_id: str | None = None) -> str:
    device = _required_text("device_id", device_id)
    camera = (camera_id or "").strip()
    return f"{device}/{camera}" if camera and camera != device else device


def build_source_frame_uid(
    source_session_id: str,
    camera_stream_id: str,
    frame_sequence: int,
) -> str:
    session = _required_text("source_session_id", source_session_id)
    camera = _required_text("camera_stream_id", camera_stream_id)
    sequence = _valid_sequence(frame_sequence)
    return str(uuid5(SOURCE_FRAME_NAMESPACE, canonical_json([session, camera, sequence])))


def build_capture_session_id(
    source_sessions: Iterable[tuple[str, str]],
) -> str:
    members = sorted(
        [
            [
                _required_text("camera_stream_id", camera_stream_id),
                _required_text("source_session_id", source_session_id),
            ]
            for camera_stream_id, source_session_id in source_sessions
        ]
    )
    if not members:
        raise ValueError("source_sessions must not be empty")
    return str(uuid5(CAPTURE_SESSION_NAMESPACE, canonical_json(members)))


def build_manifest_digest(payload: dict) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def build_capture_config_digest(metadata: dict | None) -> str:
    config = {
        key: value
        for key, value in (metadata or {}).items()
        if key in CAPTURE_CONFIG_FIELDS
    }
    return hashlib.sha256(canonical_json(config).encode("utf-8")).hexdigest()


def build_frame_set_uid(
    manifest_digest: str,
) -> str:
    digest = _required_text("manifest_digest", manifest_digest)
    return str(uuid5(FRAME_SET_NAMESPACE, digest))


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonical_json(value) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _required_text(name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _valid_sequence(value: int) -> int:
    sequence = int(value)
    if sequence < 0:
        raise ValueError("frame_sequence must be greater than or equal to 0")
    return sequence

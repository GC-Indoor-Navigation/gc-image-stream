from pathlib import Path

from app.models import Frame
from app.services.stream.relay import stream_relay_service
from app.services.stream.state import (
    CameraStreamState,
    StreamState,
    current_time_ms,
    stream_state,
)


STALE_THRESHOLD_MS = 3_000


def serialize_camera_state(
    camera: CameraStreamState,
    now_ms: int | None = None,
):
    current_ms = now_ms if now_ms is not None else current_time_ms()
    latest = camera.latest_frame
    last_received_age_ms = (
        current_ms - latest.received_at_ms
        if latest is not None
        else None
    )
    connected = latest is not None and last_received_age_ms is not None
    is_stale = connected and last_received_age_ms > STALE_THRESHOLD_MS
    status = "healthy" if connected and not is_stale else "stale" if is_stale else "disconnected"

    return {
        "device_id": camera.device_id,
        "connected": connected,
        "is_stale": is_stale,
        "status": status,
        "frame_count": camera.frame_count,
        "latest_frame_id": latest.frame_id if latest is not None else None,
        "latest_timestamp": latest.timestamp if latest is not None else None,
        "latest_sequence": latest.sequence if latest is not None else None,
        "latest_file_path": latest.file_path if latest is not None else None,
        "latest_content_type": latest.content_type if latest is not None else None,
        "latest_image_bytes": latest.image_bytes_size if latest is not None else None,
        "last_received_at": latest.received_at_ms if latest is not None else None,
        "last_received_age_ms": last_received_age_ms,
        "sequence_gap_count": camera.sequence_gap_count,
        "estimated_fps": estimate_fps(camera, now_ms=current_ms),
    }


def list_camera_states(state: StreamState = stream_state):
    cameras = sorted(state.list_cameras(), key=lambda camera: camera.device_id)
    return [serialize_camera_state(camera) for camera in cameras]


def get_camera_state(device_id: str, state: StreamState = stream_state):
    camera = state.get_camera(device_id)
    if camera is None:
        return None
    return serialize_camera_state(camera)


def estimate_fps(
    camera: CameraStreamState,
    now_ms: int | None = None,
    window_ms: int = 30_000,
) -> float:
    current_ms = now_ms if now_ms is not None else current_time_ms()
    recent = [
        received_at
        for received_at in camera.recent_received_at_ms
        if current_ms - received_at <= window_ms
    ]
    if len(recent) < 2:
        return 0.0

    elapsed_ms = max(recent[-1] - recent[0], 1)
    return (len(recent) - 1) * 1000 / elapsed_ms


def get_latest_frame_from_db(db, device_id: str) -> Frame | None:
    return (
        db.query(Frame)
        .filter(Frame.device_id == device_id)
        .order_by(Frame.timestamp.desc(), Frame.id.desc())
        .first()
    )


def get_latest_frame_path(db, device_id: str, state: StreamState = stream_state) -> str | None:
    camera = state.get_camera(device_id)
    if camera is not None and camera.latest_frame is not None:
        return camera.latest_frame.file_path

    frame = get_latest_frame_from_db(db, device_id)
    if frame is None:
        return None
    return frame.file_path


def latest_frame_file_exists(db, device_id: str, state: StreamState = stream_state) -> bool:
    path = get_latest_frame_path(db, device_id, state=state)
    return path is not None and Path(path).is_file()


def get_relay_status():
    return stream_relay_service.status()

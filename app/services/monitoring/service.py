from app.infrastructure.grpc.grpc_ingest_server import grpc_ingest_service
from pathlib import Path

from app.core.server import STREAM_RELAY_MODE
from app.infrastructure.grpc.processing_relay_client import (
    processing_frame_set_relay_service,
    processing_relay_service,
)
from app.infrastructure.grpc.live_relay_v2_client import (
    processing_live_relay_v2_client,
)
from app.models import Frame
from app.services.stream.state import (
    CameraStreamState,
    StreamState,
    current_time_ms,
    stream_state,
)
from app.services.sync import SynchronizedFrameSet, stream_sync_service


STALE_THRESHOLD_MS = 3_000


def serialize_camera_state(
    camera: CameraStreamState,
    now_ms: int | None = None,
    grpc_active_device_ids: set[str] | None = None,
    grpc_stream_closed_at_ms: dict[str, int] | None = None,
):
    current_ms = now_ms if now_ms is not None else current_time_ms()
    latest = camera.latest_frame
    is_grpc_camera = (
        grpc_active_device_ids is not None
        or (
            grpc_stream_closed_at_ms is not None
            and camera.device_id in grpc_stream_closed_at_ms
        )
    )
    grpc_is_active = (
        camera.device_id in grpc_active_device_ids
        if grpc_active_device_ids is not None
        else False
    )
    closed_at_ms = (
        grpc_stream_closed_at_ms.get(camera.device_id)
        if grpc_stream_closed_at_ms is not None
        else None
    )
    age_reference_ms = current_ms
    if latest is not None and is_grpc_camera and not grpc_is_active and closed_at_ms is not None:
        age_reference_ms = max(latest.received_at_ms, closed_at_ms)
    last_received_age_ms = (
        age_reference_ms - latest.received_at_ms
        if latest is not None
        else None
    )
    connected = (
        latest is not None
        and last_received_age_ms is not None
        and (not is_grpc_camera or grpc_is_active)
    )
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
        "latest_archive_state": latest.archive_state if latest is not None else None,
        "latest_archive_error": latest.archive_error if latest is not None else None,
        "last_received_at": latest.received_at_ms if latest is not None else None,
        "last_received_age_ms": last_received_age_ms,
        "sequence_gap_count": camera.sequence_gap_count,
        "estimated_fps": estimate_capture_fps(camera, now_ms=current_ms),
        "estimated_capture_fps": estimate_capture_fps(camera, now_ms=current_ms),
        "estimated_ingest_fps": estimate_ingest_fps(camera, now_ms=current_ms),
    }


def list_camera_states(state: StreamState = stream_state):
    cameras = sorted(state.list_cameras(), key=lambda camera: camera.device_id)
    grpc_status = grpc_ingest_service.status()
    grpc_device_ids = set(grpc_status["expected_device_ids"])
    grpc_device_ids.update(grpc_status["observed_device_ids"])
    grpc_device_ids.update(grpc_status["active_device_ids"])
    grpc_device_ids.update(grpc_status["stream_closed_at_ms"].keys())
    active_device_ids = set(grpc_status["active_device_ids"])
    stream_closed_at_ms = grpc_status["stream_closed_at_ms"]
    return [
        serialize_camera_state(
            camera,
            grpc_active_device_ids=active_device_ids if camera.device_id in grpc_device_ids else None,
            grpc_stream_closed_at_ms=stream_closed_at_ms,
        )
        for camera in cameras
    ]


def get_camera_state(device_id: str, state: StreamState = stream_state):
    camera = state.get_camera(device_id)
    if camera is None:
        return None
    grpc_status = grpc_ingest_service.status()
    grpc_device_ids = set(grpc_status["expected_device_ids"])
    grpc_device_ids.update(grpc_status["observed_device_ids"])
    grpc_device_ids.update(grpc_status["active_device_ids"])
    grpc_device_ids.update(grpc_status["stream_closed_at_ms"].keys())
    return serialize_camera_state(
        camera,
        grpc_active_device_ids=(
            set(grpc_status["active_device_ids"]) if camera.device_id in grpc_device_ids else None
        ),
        grpc_stream_closed_at_ms=grpc_status["stream_closed_at_ms"],
    )


def estimate_fps(
    camera: CameraStreamState,
    now_ms: int | None = None,
    window_ms: int = 30_000,
) -> float:
    return estimate_capture_fps(camera, now_ms=now_ms, window_ms=window_ms)


def estimate_capture_fps(
    camera: CameraStreamState,
    now_ms: int | None = None,
    window_ms: int = 30_000,
) -> float:
    if not camera.recent_timestamps_ms:
        return 0.0
    latest_timestamp_ms = camera.recent_timestamps_ms[-1]
    recent_timestamps = [
        timestamp
        for timestamp in camera.recent_timestamps_ms
        if latest_timestamp_ms - timestamp <= window_ms
    ]
    return estimate_rate(recent_timestamps)


def estimate_ingest_fps(
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
    return estimate_rate(recent)


def estimate_rate(timestamps_ms: list[int]) -> float:
    if len(timestamps_ms) < 2:
        return 0.0

    elapsed_ms = max(timestamps_ms[-1] - timestamps_ms[0], 1)
    return (len(timestamps_ms) - 1) * 1000 / elapsed_ms


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
    status = processing_relay_service.status()
    status["relay_mode"] = STREAM_RELAY_MODE
    status["selected"] = STREAM_RELAY_MODE == "raw"
    status["relay_v2"] = processing_live_relay_v2_client.status()
    return status


def get_frame_set_relay_status():
    status = processing_frame_set_relay_service.status()
    status["relay_mode"] = STREAM_RELAY_MODE
    status["selected"] = STREAM_RELAY_MODE == "frame_set"
    return status


def get_grpc_ingest_status():
    return grpc_ingest_service.status()


def get_sync_status():
    return stream_sync_service.status()


def serialize_sync_frame_set(frame_set: SynchronizedFrameSet) -> dict:
    return {
        "frame_set_id": frame_set.frame_set_id,
        "frame_set_uid": frame_set.frame_set_uid,
        "capture_session_id": frame_set.capture_session_id,
        "capture_run_id": frame_set.capture_run_id,
        "identity_mode": frame_set.identity_mode,
        "manifest_digest": frame_set.manifest_digest,
        "archive_state": frame_set.archive_state,
        "archive_error": frame_set.archive_error,
        "anchor_timestamp_ms": frame_set.anchor_timestamp_ms,
        "max_delta_ms": frame_set.max_delta_ms,
        "span_ms": frame_set.span_ms
        if frame_set.span_ms is not None
        else frame_set.max_delta_ms,
        "frames": {
            device_id: {
                "frame_id": frame.frame_id,
                "device_id": frame.device_id,
                "timestamp_ms": frame.timestamp_ms,
                "sequence": frame.sequence,
                "content_type": frame.content_type,
                "image_size": frame.image_size,
                "file_path": frame.file_path,
                "source_session_id": frame.source_session_id,
                "camera_stream_id": frame.camera_stream_id,
                "source_frame_uid": frame.source_frame_uid,
                "content_digest": frame.content_digest,
                "archive_state": frame.archive_state,
                "archive_error": frame.archive_error,
            }
            for device_id, frame in frame_set.frames.items()
        },
    }


def list_recent_sync_frame_sets():
    return [
        serialize_sync_frame_set(frame_set)
        for frame_set in stream_sync_service.recent_frame_sets()
    ]

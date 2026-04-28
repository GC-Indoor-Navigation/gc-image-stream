import time
from pathlib import Path

from sqlalchemy.orm import Session

from app.services.frame_service import create_frame
from app.services.stream.stream_experiment import get_stream_experiment_recorder
from app.services.stream.relay import StreamRelayService, stream_relay_service
from app.services.stream.state import StreamState, stream_state
from app.utils.file_utils import build_frame_path
from processing.grpc_relay import RelayFrame


def ingest_frame(
    db: Session,
    device_id: str,
    timestamp_ms: int,
    image_bytes: bytes,
    sequence: int | None = None,
    content_type: str = "image/jpeg",
    filename: str | None = None,
    state: StreamState = stream_state,
    relay_service: StreamRelayService = stream_relay_service,
):
    started_at = time.monotonic()
    target_filename = filename or f"{device_id}_{timestamp_ms}.jpg"
    save_path = build_frame_path(device_id, timestamp_ms, target_filename)
    Path(save_path).write_bytes(image_bytes)

    frame = create_frame(
        db,
        device_id=device_id,
        timestamp=timestamp_ms,
        file_path=save_path,
    )

    camera_state = state.update_frame(
        frame_id=frame.id,
        device_id=frame.device_id,
        timestamp=frame.timestamp,
        sequence=sequence,
        file_path=frame.file_path,
        content_type=content_type,
        image_bytes_size=len(image_bytes),
    )
    relay_enqueued = relay_service.enqueue(
        RelayFrame(
            device_id=frame.device_id,
            timestamp_ms=frame.timestamp,
            sequence=sequence or 0,
            content_type=content_type,
            image_bytes=image_bytes,
            file_path=frame.file_path,
        )
    )
    experiment_recorder = get_stream_experiment_recorder()
    if experiment_recorder is not None:
        experiment_recorder.record_registration(
            status="registered",
            device_id=frame.device_id,
            timestamp_ms=frame.timestamp,
            elapsed=time.monotonic() - started_at,
            queue_size=0,
        )

    return {
        "frame": frame,
        "camera_state": camera_state,
        "relay_enqueued": relay_enqueued,
    }

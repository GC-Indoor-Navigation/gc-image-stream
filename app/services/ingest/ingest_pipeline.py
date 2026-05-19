import time
from pathlib import Path

from sqlalchemy.orm import Session

from app.infrastructure.grpc.generated.processing_relay_pb2 import RelayFrame
from app.infrastructure.grpc.processing_relay_client import (
    ProcessingFrameSetRelayService,
    ProcessingRelayService,
    processing_frame_set_relay_service,
    processing_relay_service,
)
from app.infrastructure.storage.file_utils import build_frame_path
from app.services.frames.service import create_frame
from app.services.stream.stream_experiment import get_stream_experiment_recorder
from app.services.stream.state import StreamState, stream_state
from app.services.sync import StreamSyncService, SyncInputFrame, stream_sync_service


def ingest_frame(
    db: Session,
    device_id: str,
    timestamp_ms: int,
    image_bytes: bytes,
    sequence: int | None = None,
    content_type: str = "image/jpeg",
    filename: str | None = None,
    session_id: str | None = None,
    state: StreamState = stream_state,
    relay_service: ProcessingRelayService = processing_relay_service,
    sync_service: StreamSyncService = stream_sync_service,
    frame_set_relay_service: ProcessingFrameSetRelayService = processing_frame_set_relay_service,
):
    started_at = time.monotonic()
    target_filename = filename or f"{device_id}_{timestamp_ms}.jpg"
    save_path = build_frame_path(
        device_id,
        timestamp_ms,
        target_filename,
        session_id=session_id,
    )
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
    synchronized_frame_set = sync_service.handle_frame(
        SyncInputFrame(
            frame_id=frame.id,
            device_id=frame.device_id,
            timestamp_ms=frame.timestamp,
            sequence=sequence,
            content_type=content_type,
            image_bytes=image_bytes,
            file_path=frame.file_path,
        )
    )
    frame_set_relay_enqueued = (
        frame_set_relay_service.enqueue_synchronized_frame_set(synchronized_frame_set)
        if synchronized_frame_set is not None
        else False
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
        "synchronized_frame_set": synchronized_frame_set,
        "frame_set_relay_enqueued": frame_set_relay_enqueued,
    }

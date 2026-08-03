import time
from pathlib import Path

from sqlalchemy.orm import Session

from app.core.relay import (
    STREAM_RELAY_MODE_FRAME_SET,
    STREAM_RELAY_MODE_OFF,
    STREAM_RELAY_MODE_RAW,
    normalize_stream_relay_mode,
)
from app.infrastructure.grpc.generated.processing_relay_pb2 import RelayFrame
from app.infrastructure.grpc.processing_relay_client import (
    ProcessingFrameSetRelayService,
    ProcessingRelayService,
    processing_frame_set_relay_service,
    processing_relay_service,
)
from app.infrastructure.storage.file_utils import build_frame_path
from app.services.frames.service import create_frame
from app.services.identity import canonical_camera_stream_id, sha256_bytes
from app.services.stream.stream_experiment import get_stream_experiment_recorder
from app.services.stream.state import StreamState, stream_state
from app.services.sync import (
    StreamSyncService,
    SyncInputFrame,
    persist_frame_set_manifest,
    stream_sync_service,
)


def resolve_ingest_relay_mode(
    relay_mode: str | None,
    relay_service: ProcessingRelayService,
    frame_set_relay_service: ProcessingFrameSetRelayService,
) -> str:
    if relay_mode is not None:
        return normalize_stream_relay_mode(relay_mode)
    if frame_set_relay_service.enabled:
        return STREAM_RELAY_MODE_FRAME_SET
    if relay_service.enabled:
        return STREAM_RELAY_MODE_RAW
    return STREAM_RELAY_MODE_OFF


def ingest_frame(
    db: Session,
    device_id: str,
    timestamp_ms: int,
    image_bytes: bytes,
    sequence: int | None = None,
    content_type: str = "image/jpeg",
    filename: str | None = None,
    session_id: str | None = None,
    camera_stream_id: str | None = None,
    state: StreamState = stream_state,
    relay_service: ProcessingRelayService = processing_relay_service,
    sync_service: StreamSyncService = stream_sync_service,
    frame_set_relay_service: ProcessingFrameSetRelayService = processing_frame_set_relay_service,
    relay_mode: str | None = None,
    received_at_ms: int | None = None,
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

    resolved_camera_stream_id = (
        canonical_camera_stream_id(device_id, camera_stream_id)
        if session_id and sequence is not None
        else camera_stream_id
    )
    content_digest = sha256_bytes(image_bytes)

    frame = create_frame(
        db,
        device_id=device_id,
        timestamp=timestamp_ms,
        file_path=save_path,
        source_session_id=session_id,
        camera_stream_id=resolved_camera_stream_id,
        frame_sequence=sequence,
        content_digest=content_digest,
    )

    camera_state = state.update_frame(
        frame_id=frame.id,
        device_id=frame.device_id,
        timestamp=frame.timestamp,
        sequence=sequence,
        file_path=frame.file_path,
        content_type=content_type,
        image_bytes_size=len(image_bytes),
        received_at_ms=received_at_ms,
    )
    selected_relay_mode = resolve_ingest_relay_mode(
        relay_mode,
        relay_service,
        frame_set_relay_service,
    )
    relay_enqueued = (
        relay_service.enqueue(
            RelayFrame(
                device_id=frame.device_id,
                timestamp_ms=frame.timestamp,
                sequence=sequence or 0,
                content_type=content_type,
                image_bytes=image_bytes,
                file_path=frame.file_path,
            )
        )
        if selected_relay_mode == STREAM_RELAY_MODE_RAW
        else False
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
            source_session_id=frame.source_session_id,
            camera_stream_id=frame.camera_stream_id,
            source_frame_uid=frame.source_frame_uid,
            content_digest=frame.content_digest,
            identity_mode=frame.identity_mode,
        )
    )
    manifest_persisted = (
        persist_frame_set_manifest(db, synchronized_frame_set)
        if synchronized_frame_set is not None
        else False
    )
    frame_set_relay_enqueued = (
        frame_set_relay_service.enqueue_synchronized_frame_set(synchronized_frame_set)
        if (
            selected_relay_mode == STREAM_RELAY_MODE_FRAME_SET
            and synchronized_frame_set is not None
        )
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
        "relay_mode": selected_relay_mode,
        "synchronized_frame_set": synchronized_frame_set,
        "manifest_persisted": manifest_persisted,
        "frame_set_relay_enqueued": frame_set_relay_enqueued,
    }

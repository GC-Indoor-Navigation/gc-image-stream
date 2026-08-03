import logging
import time
from dataclasses import replace

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
from app.services.frames.service import resolve_frame_identity
from app.services.identity import canonical_camera_stream_id, sha256_bytes
from app.services.ingest.archive import (
    ARCHIVE_DEGRADED_LIVE_ONLY,
    ARCHIVE_DURABLE,
    ARCHIVE_PENDING,
    ArchiveWriter,
    persist_frame_archive,
)
from app.services.stream.stream_experiment import get_stream_experiment_recorder
from app.services.stream.state import StreamState, stream_state
from app.services.sync import (
    StreamSyncService,
    SyncInputFrame,
    persist_frame_set_manifest,
    stream_sync_service,
)


LOGGER = logging.getLogger("gc_image_stream.ingest")


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
    archive_writer: ArchiveWriter | None = None,
):
    started_at = time.monotonic()
    target_filename = filename or f"{device_id}_{timestamp_ms}.jpg"
    resolved_camera_stream_id = (
        canonical_camera_stream_id(device_id, camera_stream_id)
        if session_id and sequence is not None
        else camera_stream_id
    )
    content_digest = sha256_bytes(image_bytes)
    source_frame_uid, identity_mode = resolve_frame_identity(
        source_session_id=session_id,
        camera_stream_id=resolved_camera_stream_id,
        frame_sequence=sequence,
        content_digest=content_digest,
    )
    memory_frame = SyncInputFrame(
        frame_id=None,
        device_id=device_id,
        timestamp_ms=timestamp_ms,
        sequence=sequence,
        content_type=content_type,
        image_bytes=image_bytes,
        file_path=None,
        source_session_id=session_id,
        camera_stream_id=resolved_camera_stream_id,
        source_frame_uid=source_frame_uid,
        content_digest=content_digest,
        identity_mode=identity_mode,
        archive_state=ARCHIVE_PENDING,
    )

    selected_relay_mode = resolve_ingest_relay_mode(
        relay_mode,
        relay_service,
        frame_set_relay_service,
    )
    synchronized_frame_set = sync_service.handle_frame(memory_frame)
    frame_set_relay_enqueued = (
        frame_set_relay_service.enqueue_synchronized_frame_set(
            synchronized_frame_set
        )
        if (
            selected_relay_mode == STREAM_RELAY_MODE_FRAME_SET
            and synchronized_frame_set is not None
        )
        else False
    )

    save_path: str | None = None
    path_error: str | None = None
    try:
        save_path = build_frame_path(
            device_id,
            timestamp_ms,
            target_filename,
            session_id=session_id,
        )
    except Exception as exc:
        path_error = f"archive path failed: {type(exc).__name__}: {exc}"

    archive = persist_frame_archive(
        db=db,
        device_id=device_id,
        timestamp_ms=timestamp_ms,
        image_bytes=image_bytes,
        save_path=save_path,
        source_session_id=session_id,
        camera_stream_id=resolved_camera_stream_id,
        frame_sequence=sequence,
        content_digest=content_digest,
        initial_error=path_error,
        writer=archive_writer,
    )
    sync_input = replace(
        memory_frame,
        frame_id=archive.frame.id if archive.frame is not None else None,
        file_path=archive.file_path,
        archive_state=archive.state,
        archive_error=archive.error,
    )
    synchronized_frame_set = sync_service.finalize_frame_archive(
        memory_frame,
        synchronized_frame_set,
        frame_id=sync_input.frame_id,
        file_path=sync_input.file_path,
        archive_state=sync_input.archive_state,
        archive_error=sync_input.archive_error,
    )

    camera_state = state.update_frame(
        frame_id=sync_input.frame_id,
        device_id=sync_input.device_id,
        timestamp=sync_input.timestamp_ms,
        sequence=sequence,
        file_path=sync_input.file_path,
        content_type=content_type,
        image_bytes_size=len(image_bytes),
        received_at_ms=received_at_ms,
        archive_state=archive.state,
        archive_error=archive.error,
    )
    relay_enqueued = (
        relay_service.enqueue(_build_raw_relay_frame(sync_input))
        if selected_relay_mode == STREAM_RELAY_MODE_RAW
        else False
    )

    manifest_persisted = False
    if synchronized_frame_set is not None:
        final_archive_state = synchronized_frame_set.archive_state
        final_archive_error = synchronized_frame_set.archive_error
        if (
            synchronized_frame_set.identity_mode == "V2"
            and synchronized_frame_set.archive_state == ARCHIVE_PENDING
        ):
            try:
                manifest_persisted = persist_frame_set_manifest(
                    db,
                    synchronized_frame_set,
                )
                final_archive_state = ARCHIVE_DURABLE
                final_archive_error = None
            except Exception as exc:
                db.rollback()
                final_archive_state = ARCHIVE_DEGRADED_LIVE_ONLY
                final_archive_error = (
                    f"manifest archive failed: {type(exc).__name__}: {exc}"
                )
                LOGGER.exception("frame-set manifest archive failed; continuing LIVE")
        elif synchronized_frame_set.archive_state == ARCHIVE_PENDING:
            final_archive_state = ARCHIVE_DURABLE
        synchronized_frame_set = sync_service.finalize_archive_state(
            synchronized_frame_set,
            state=final_archive_state,
            error=final_archive_error,
        )
    experiment_recorder = get_stream_experiment_recorder()
    if experiment_recorder is not None:
        experiment_recorder.record_registration(
            status="registered",
            device_id=sync_input.device_id,
            timestamp_ms=sync_input.timestamp_ms,
            elapsed=time.monotonic() - started_at,
            queue_size=0,
        )

    return {
        "frame": archive.frame,
        "camera_state": camera_state,
        "archive_state": (
            synchronized_frame_set.archive_state
            if synchronized_frame_set
            else archive.state
        ),
        "archive_error": (
            synchronized_frame_set.archive_error
            if synchronized_frame_set
            else archive.error
        ),
        "relay_enqueued": relay_enqueued,
        "relay_mode": selected_relay_mode,
        "synchronized_frame_set": synchronized_frame_set,
        "manifest_persisted": manifest_persisted,
        "frame_set_relay_enqueued": frame_set_relay_enqueued,
    }


def _build_raw_relay_frame(frame: SyncInputFrame) -> RelayFrame:
    fields = {
        "device_id": frame.device_id,
        "timestamp_ms": frame.timestamp_ms,
        "sequence": frame.sequence or 0,
        "content_type": frame.content_type,
        "image_bytes": frame.image_bytes,
    }
    if frame.file_path is not None:
        fields["file_path"] = frame.file_path
    return RelayFrame(**fields)

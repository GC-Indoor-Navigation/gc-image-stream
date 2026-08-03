import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from sqlalchemy.orm import Session

from app.models import Frame
from app.services.frames.service import create_frame


ARCHIVE_DURABLE = "ARCHIVE_DURABLE"
ARCHIVE_PENDING = "ARCHIVE_PENDING"
ARCHIVE_DEGRADED_LIVE_ONLY = "ARCHIVE_DEGRADED_LIVE_ONLY"

ArchiveWriter = Callable[[Path, bytes], None]


@dataclass(frozen=True)
class FrameArchiveOutcome:
    frame: Frame | None
    state: str
    file_path: str | None
    error: str | None


def persist_frame_archive(
    *,
    db: Session,
    device_id: str,
    timestamp_ms: int,
    image_bytes: bytes,
    save_path: str | None,
    source_session_id: str | None,
    camera_stream_id: str | None,
    frame_sequence: int | None,
    content_digest: str,
    initial_error: str | None = None,
    writer: ArchiveWriter | None = None,
) -> FrameArchiveOutcome:
    errors = [initial_error] if initial_error else []
    resolved_writer = writer or durable_write_bytes
    file_durable = False

    if save_path is not None and not errors:
        try:
            resolved_writer(Path(save_path), image_bytes)
            file_durable = True
        except Exception as exc:
            errors.append(f"file archive failed: {type(exc).__name__}: {exc}")

    state = ARCHIVE_DURABLE if file_durable else ARCHIVE_DEGRADED_LIVE_ONLY
    error = "; ".join(errors) or None
    try:
        frame = create_frame(
            db,
            device_id=device_id,
            timestamp=timestamp_ms,
            file_path=save_path if file_durable else None,
            source_session_id=source_session_id,
            camera_stream_id=camera_stream_id,
            frame_sequence=frame_sequence,
            content_digest=content_digest,
            archive_state=state,
            archive_error=error,
            file_size=len(image_bytes) if file_durable else None,
        )
    except Exception as exc:
        db.rollback()
        errors.append(f"metadata archive failed: {type(exc).__name__}: {exc}")
        return FrameArchiveOutcome(
            frame=None,
            state=ARCHIVE_DEGRADED_LIVE_ONLY,
            file_path=None,
            error="; ".join(errors),
        )

    return FrameArchiveOutcome(
        frame=frame,
        state=frame.archive_state,
        file_path=(
            frame.file_path if frame.archive_state == ARCHIVE_DURABLE else None
        ),
        error=frame.archive_error,
    )


def durable_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    try:
        with temporary.open("xb") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _fsync_directory(path: Path) -> None:
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY)
        os.fsync(descriptor)
    except OSError:
        # Windows does not expose portable directory fsync. File fsync and
        # same-directory atomic replace remain mandatory on every platform.
        return
    finally:
        if descriptor is not None:
            os.close(descriptor)

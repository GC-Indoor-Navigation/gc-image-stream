import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from sqlalchemy.orm import Session

from app.models import (
    ArchiveReconciliationIssue,
    Frame,
    FrameSetManifest,
    FrameSetMember,
)


PAYLOAD_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".bin"}


@dataclass(frozen=True)
class ArchiveReconciliationReport:
    run_id: str
    checked_frames: int
    healthy_frames: int
    degraded_frames: int
    orphan_files: int
    partial_files: int


def reconcile_archive(
    db: Session,
    archive_root: str | Path,
    *,
    detected_at_ms: int | None = None,
) -> ArchiveReconciliationReport:
    root = Path(archive_root).resolve()
    run_id = str(uuid4())
    timestamp_ms = detected_at_ms or int(time.time() * 1000)
    checked = 0
    healthy = 0
    degraded = 0
    referenced_paths: set[Path] = set()

    frames = db.query(Frame).all()
    for frame in frames:
        if frame.file_path:
            referenced_paths.add(Path(frame.file_path).resolve())
        if frame.archive_state != "ARCHIVE_DURABLE":
            continue
        checked += 1
        issue = _validate_frame_file(frame, root)
        if issue is None:
            healthy += 1
            continue
        degraded += 1
        issue_type, detail = issue
        _degrade_frame_and_manifests(db, frame, issue_type)
        db.add(
            ArchiveReconciliationIssue(
                reconciliation_run_id=run_id,
                issue_type=issue_type,
                frame_id=frame.id,
                file_path=frame.file_path,
                detail=detail,
                detected_at_ms=timestamp_ms,
            )
        )

    orphan_count = 0
    partial_count = 0
    if root.is_dir():
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            resolved = path.resolve()
            if _is_partial_path(path):
                partial_count += 1
                db.add(
                    ArchiveReconciliationIssue(
                        reconciliation_run_id=run_id,
                        issue_type="PARTIAL_TEMP_FILE",
                        file_path=str(path),
                        detail="temporary archive file remained after restart",
                        detected_at_ms=timestamp_ms,
                    )
                )
            elif path.suffix.lower() in PAYLOAD_SUFFIXES and resolved not in referenced_paths:
                orphan_count += 1
                db.add(
                    ArchiveReconciliationIssue(
                        reconciliation_run_id=run_id,
                        issue_type="ORPHAN_FILE",
                        file_path=str(path),
                        detail="payload file has no frame metadata row",
                        detected_at_ms=timestamp_ms,
                    )
                )

    db.commit()
    return ArchiveReconciliationReport(
        run_id=run_id,
        checked_frames=checked,
        healthy_frames=healthy,
        degraded_frames=degraded,
        orphan_files=orphan_count,
        partial_files=partial_count,
    )


def _validate_frame_file(frame: Frame, root: Path) -> tuple[str, str] | None:
    if not frame.file_path:
        return "MISSING_FILE", "durable frame has no file path"
    path = Path(frame.file_path).resolve()
    if not path.is_relative_to(root):
        return "PATH_OUTSIDE_ARCHIVE_ROOT", f"path is outside archive root: {path}"
    if not path.is_file():
        return "MISSING_FILE", f"archive payload is missing: {path}"
    try:
        size = path.stat().st_size
        digest = _sha256_file(path)
    except OSError as exc:
        return "FILE_READ_ERROR", f"archive payload cannot be read: {exc}"
    if frame.file_size is not None and frame.file_size != size:
        return (
            "SIZE_MISMATCH",
            f"expected {frame.file_size} bytes but found {size}: {path}",
        )
    if frame.content_digest is not None and frame.content_digest != digest:
        return "DIGEST_MISMATCH", f"content SHA-256 mismatch: {path}"
    if frame.file_size is None:
        frame.file_size = size
    if frame.content_digest is None:
        frame.content_digest = digest
    return None


def _degrade_frame_and_manifests(
    db: Session,
    frame: Frame,
    issue_type: str,
) -> None:
    frame.archive_state = "ARCHIVE_DEGRADED_LIVE_ONLY"
    frame.archive_error = issue_type
    frame_set_uids = [
        value
        for (value,) in (
            db.query(FrameSetMember.frame_set_uid)
            .filter(FrameSetMember.frame_id == frame.id)
            .all()
        )
    ]
    if frame_set_uids:
        manifests = (
            db.query(FrameSetManifest)
            .filter(FrameSetManifest.frame_set_uid.in_(frame_set_uids))
            .all()
        )
        for manifest in manifests:
            manifest.archive_state = "ARCHIVE_DEGRADED_LIVE_ONLY"
            manifest.archive_error = issue_type


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_partial_path(path: Path) -> bool:
    return path.name.startswith(".") and path.name.endswith(".tmp")

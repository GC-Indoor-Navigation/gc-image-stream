from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Frame
from app.services.identity import (
    IDENTITY_MODE_LEGACY,
    IDENTITY_MODE_V2,
    build_source_frame_uid,
)


class FrameIntegrityError(ValueError):
    pass


# 프레임을 저장하고, 중복이면 기존 레코드를 반환한다.
def create_frame(
    db: Session,
    device_id: str,
    timestamp: int,
    file_path: str | None,
    *,
    source_session_id: str | None = None,
    camera_stream_id: str | None = None,
    frame_sequence: int | None = None,
    content_digest: str | None = None,
    archive_state: str = "ARCHIVE_DURABLE",
    archive_error: str | None = None,
    file_size: int | None = None,
    content_type: str | None = None,
    received_at_ms: int | None = None,
    capture_config_digest: str | None = None,
    capture_metadata_json: str | None = None,
    tenant_id: str | None = None,
    site_id: str | None = None,
    capture_session_id: str | None = None,
    processing_job_id: str | None = None,
    profile_digest: str | None = None,
    authorized_subject: str | None = None,
    session_token_jti: str | None = None,
    authorized_camera_id: str | None = None,
    camera_claim_id: str | None = None,
) -> Frame:
    source_frame_uid, identity_mode = resolve_frame_identity(
        source_session_id=source_session_id,
        camera_stream_id=camera_stream_id,
        frame_sequence=frame_sequence,
        content_digest=content_digest,
    )

    existing = _find_existing_frame(
        db,
        source_frame_uid=source_frame_uid,
        device_id=device_id,
        timestamp=timestamp,
        identity_mode=identity_mode,
    )
    if existing is not None:
        _verify_duplicate_digest(existing, content_digest)
        _verify_duplicate_scope(
            existing,
            tenant_id=tenant_id,
            site_id=site_id,
            capture_session_id=capture_session_id,
            processing_job_id=processing_job_id,
            profile_digest=profile_digest,
            authorized_subject=authorized_subject,
            session_token_jti=session_token_jti,
            authorized_camera_id=authorized_camera_id,
            camera_claim_id=camera_claim_id,
        )
        _apply_archive_recovery(
            db,
            existing,
            file_path=file_path,
            archive_state=archive_state,
            archive_error=archive_error,
            file_size=file_size,
        )
        return existing

    frame = Frame(
        device_id=device_id,
        timestamp=timestamp,
        file_path=file_path,
        source_session_id=source_session_id,
        camera_stream_id=camera_stream_id,
        frame_sequence=frame_sequence,
        source_frame_uid=source_frame_uid,
        content_digest=content_digest,
        identity_mode=identity_mode,
        archive_state=archive_state,
        archive_error=archive_error,
        file_size=file_size,
        content_type=content_type,
        received_at_ms=received_at_ms,
        capture_config_digest=capture_config_digest,
        capture_metadata_json=capture_metadata_json,
        tenant_id=tenant_id,
        site_id=site_id,
        capture_session_id=capture_session_id,
        processing_job_id=processing_job_id,
        profile_digest=profile_digest,
        authorized_subject=authorized_subject,
        session_token_jti=session_token_jti,
        authorized_camera_id=authorized_camera_id,
        camera_claim_id=camera_claim_id,
    )
    db.add(frame)
    try:
        db.commit()
        db.refresh(frame)
        return frame
    except IntegrityError:
        db.rollback()
        existing = _find_existing_frame(
            db,
            source_frame_uid=source_frame_uid,
            device_id=device_id,
            timestamp=timestamp,
            identity_mode=identity_mode,
        )
        if existing is not None:
            _verify_duplicate_digest(existing, content_digest)
            _verify_duplicate_scope(
                existing,
                tenant_id=tenant_id,
                site_id=site_id,
                capture_session_id=capture_session_id,
                processing_job_id=processing_job_id,
                profile_digest=profile_digest,
                authorized_subject=authorized_subject,
                session_token_jti=session_token_jti,
                authorized_camera_id=authorized_camera_id,
                camera_claim_id=camera_claim_id,
            )
            _apply_archive_recovery(
                db,
                existing,
                file_path=file_path,
                archive_state=archive_state,
                archive_error=archive_error,
                file_size=file_size,
            )
            return existing
        raise


def resolve_frame_identity(
    *,
    source_session_id: str | None,
    camera_stream_id: str | None,
    frame_sequence: int | None,
    content_digest: str | None,
) -> tuple[str | None, str]:
    has_natural_identity = (
        bool(source_session_id and source_session_id.strip())
        and bool(camera_stream_id and camera_stream_id.strip())
        and frame_sequence is not None
    )
    source_frame_uid = (
        build_source_frame_uid(
            source_session_id=source_session_id,
            camera_stream_id=camera_stream_id,
            frame_sequence=frame_sequence,
        )
        if has_natural_identity
        else None
    )
    mode = (
        IDENTITY_MODE_V2
        if has_natural_identity and bool(content_digest and content_digest.strip())
        else IDENTITY_MODE_LEGACY
    )
    return source_frame_uid, mode


def _find_existing_frame(
    db: Session,
    *,
    source_frame_uid: str | None,
    device_id: str,
    timestamp: int,
    identity_mode: str,
) -> Frame | None:
    if source_frame_uid is not None:
        return db.query(Frame).filter(Frame.source_frame_uid == source_frame_uid).first()
    return (
        db.query(Frame)
        .filter(
            Frame.device_id == device_id,
            Frame.timestamp == timestamp,
            Frame.identity_mode == identity_mode,
        )
        .first()
    )


def _verify_duplicate_digest(frame: Frame, content_digest: str | None) -> None:
    if (
        frame.source_frame_uid is not None
        and frame.content_digest is not None
        and content_digest is not None
        and frame.content_digest != content_digest
    ):
        raise FrameIntegrityError(
            "source_frame_uid was reused with a different content digest"
        )


def _verify_duplicate_scope(frame: Frame, **declared_scope) -> None:
    existing_scope = {
        name: getattr(frame, name)
        for name in declared_scope
    }
    if existing_scope != declared_scope:
        raise FrameIntegrityError(
            "source_frame_uid was reused with a different authorization scope"
        )


def _apply_archive_recovery(
    db: Session,
    frame: Frame,
    *,
    file_path: str | None,
    archive_state: str,
    archive_error: str | None,
    file_size: int | None,
) -> None:
    if (
        archive_state == "ARCHIVE_DURABLE"
        and frame.archive_state != "ARCHIVE_DURABLE"
    ):
        frame.file_path = file_path
        frame.archive_state = archive_state
        frame.archive_error = archive_error
        frame.file_size = file_size
        db.commit()
        db.refresh(frame)


# 최신 순으로 프레임 목록을 조회한다.
def get_frames(db: Session, limit: int = 50):
    return db.query(Frame).order_by(Frame.timestamp.desc()).limit(limit).all()


# 최근 프레임 조회용 자리 함수다.
def get_recent_frames(db: Session, window_ms: int = 100):
    return db.query(Frame).order_by(Frame.timestamp.asc()).all()

import json
import time

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    CaptureRun,
    CaptureSession,
    FrameSetDeliveryProjection,
    FrameSetManifest,
    FrameSetMember,
)
from app.services.identity import IDENTITY_MODE_V2
from app.services.sync.models import SynchronizedFrameSet


class ManifestIntegrityError(ValueError):
    pass


def persist_frame_set_manifest(
    db: Session,
    frame_set: SynchronizedFrameSet,
    *,
    created_at_ms: int | None = None,
) -> bool:
    if frame_set.identity_mode != IDENTITY_MODE_V2:
        return False
    _validate_v2_frame_set(frame_set)

    existing = db.get(FrameSetManifest, frame_set.frame_set_uid)
    if existing is not None:
        _verify_existing_manifest(existing, frame_set)
        if db.get(FrameSetDeliveryProjection, frame_set.frame_set_uid) is None:
            db.add(_new_delivery_projection(frame_set.frame_set_uid))
            db.commit()
        return False

    timestamp_ms = (
        created_at_ms if created_at_ms is not None else int(time.time() * 1000)
    )
    open_sessions = (
        db.query(CaptureSession)
        .filter(
            CaptureSession.state == "OPEN",
            CaptureSession.id != frame_set.capture_session_id,
        )
        .all()
    )
    for session in open_sessions:
        session.state = "CLOSED"
        session.closed_at_ms = timestamp_ms
        session.close_reason = "SOURCE_SESSION_SET_CHANGED"

    capture_session = db.get(CaptureSession, frame_set.capture_session_id)
    if capture_session is None:
        capture_session = CaptureSession(
            id=frame_set.capture_session_id,
            state="OPEN",
            source_sessions_json=_source_sessions_json(frame_set),
            started_at_ms=timestamp_ms,
        )
        db.add(capture_session)

    open_runs = (
        db.query(CaptureRun)
        .filter(
            CaptureRun.state == "OPEN",
            CaptureRun.id != frame_set.capture_run_id,
        )
        .all()
    )
    for run in open_runs:
        run.state = "CLOSED"
        run.closed_at_ms = timestamp_ms
        run.close_reason = "CAPTURE_RUN_ROTATED"

    run = db.get(CaptureRun, frame_set.capture_run_id)
    if run is None:
        run = CaptureRun(
            id=frame_set.capture_run_id,
            capture_session_id=frame_set.capture_session_id,
            identity_mode=frame_set.identity_mode,
            state="OPEN",
            started_at_ms=timestamp_ms,
        )
        db.add(run)
    elif run.capture_session_id != frame_set.capture_session_id:
        raise ManifestIntegrityError(
            "capture_run_id was reused with a different capture_session_id"
        )

    eligible_rows = (
        db.query(FrameSetDeliveryProjection, FrameSetManifest)
        .join(
            FrameSetManifest,
            FrameSetManifest.frame_set_uid
            == FrameSetDeliveryProjection.frame_set_uid,
        )
        .filter(FrameSetDeliveryProjection.live_state == "ELIGIBLE")
        .all()
    )
    newer_eligible_exists = any(
        existing_manifest.synchronized_at_ms > frame_set.synchronized_at_ms
        for _, existing_manifest in eligible_rows
    )
    if not newer_eligible_exists:
        for projection, _ in eligible_rows:
            projection.live_state = "SUPERSEDED_BEFORE_OFFER"
            projection.last_reason = "NEWER_FRAME_SET_AVAILABLE"
            projection.updated_at_ms = timestamp_ms

    freshness_origin_ms = min(
        frame.timestamp_ms for frame in frame_set.frames.values()
    )
    manifest = FrameSetManifest(
        frame_set_uid=frame_set.frame_set_uid,
        capture_session_id=frame_set.capture_session_id,
        capture_run_id=frame_set.capture_run_id,
        frame_set_id=frame_set.frame_set_id,
        anchor_timestamp_ms=frame_set.anchor_timestamp_ms,
        freshness_origin_ms=freshness_origin_ms,
        synchronization_span_ms=(frame_set.span_ms or frame_set.max_delta_ms),
        manifest_digest=frame_set.manifest_digest,
        manifest_json=frame_set.manifest_json,
        created_at_ms=timestamp_ms,
        sync_window_ms=frame_set.sync_window_ms,
        synchronized_at_ms=frame_set.synchronized_at_ms,
        member_count=frame_set.member_count,
    )
    db.add(manifest)
    db.add(
        _new_delivery_projection(
            frame_set.frame_set_uid,
            updated_at_ms=timestamp_ms,
            live_state=(
                "SUPERSEDED_BEFORE_OFFER"
                if newer_eligible_exists
                else "ELIGIBLE"
            ),
            reason=(
                "NEWER_FRAME_SET_AVAILABLE"
                if newer_eligible_exists
                else None
            ),
        )
    )
    db.add_all(
        [
            FrameSetMember(
                frame_set_uid=frame_set.frame_set_uid,
                frame_id=frame.frame_id,
                source_frame_uid=frame.source_frame_uid,
                source_session_id=frame.source_session_id,
                camera_stream_id=frame.camera_stream_id,
                frame_sequence=frame.sequence,
                capture_timestamp_ms=frame.timestamp_ms,
                content_type=frame.content_type,
                image_size=frame.image_size,
                content_digest=frame.content_digest,
                file_path=frame.file_path,
            )
            for frame in sorted(
                frame_set.frames.values(),
                key=lambda item: item.camera_stream_id,
            )
        ]
    )
    try:
        db.commit()
        return True
    except IntegrityError as exc:
        db.rollback()
        existing = db.get(FrameSetManifest, frame_set.frame_set_uid)
        if existing is not None:
            _verify_existing_manifest(existing, frame_set)
            return False
        raise ManifestIntegrityError(
            "manifest identity conflicts with existing durable data"
        ) from exc


def update_delivery_projection(
    db: Session,
    frame_set_uid: str,
    *,
    archive_state: str | None = None,
    live_state: str | None = None,
    legacy_relay_state: str | None = None,
    reason: str | None = None,
    updated_at_ms: int | None = None,
) -> bool:
    projection = db.get(FrameSetDeliveryProjection, frame_set_uid)
    if projection is None:
        if db.get(FrameSetManifest, frame_set_uid) is None:
            return False
        projection = _new_delivery_projection(frame_set_uid)
        db.add(projection)
    if archive_state is not None:
        projection.archive_state = archive_state
    if live_state is not None:
        projection.live_state = live_state
    if legacy_relay_state is not None:
        projection.legacy_relay_state = legacy_relay_state
    if reason is not None:
        projection.last_reason = reason
    projection.updated_at_ms = (
        updated_at_ms if updated_at_ms is not None else int(time.time() * 1000)
    )
    db.commit()
    return True


def get_newest_eligible_manifest(db: Session) -> FrameSetManifest | None:
    return (
        db.query(FrameSetManifest)
        .join(
            FrameSetDeliveryProjection,
            FrameSetDeliveryProjection.frame_set_uid
            == FrameSetManifest.frame_set_uid,
        )
        .filter(
            FrameSetDeliveryProjection.archive_state == "ARCHIVE_DURABLE",
            FrameSetDeliveryProjection.live_state == "ELIGIBLE",
        )
        .order_by(
            FrameSetManifest.synchronized_at_ms.desc(),
            FrameSetManifest.created_at_ms.desc(),
            FrameSetManifest.frame_set_uid.desc(),
        )
        .first()
    )


def _new_delivery_projection(
    frame_set_uid: str,
    *,
    updated_at_ms: int | None = None,
    live_state: str = "ELIGIBLE",
    reason: str | None = None,
) -> FrameSetDeliveryProjection:
    return FrameSetDeliveryProjection(
        frame_set_uid=frame_set_uid,
        archive_state="ARCHIVE_DURABLE",
        live_state=live_state,
        legacy_relay_state="NOT_ENQUEUED",
        last_reason=reason,
        updated_at_ms=(
            updated_at_ms
            if updated_at_ms is not None
            else int(time.time() * 1000)
        ),
    )


def _validate_v2_frame_set(frame_set: SynchronizedFrameSet) -> None:
    required = {
        "capture_session_id": frame_set.capture_session_id,
        "capture_run_id": frame_set.capture_run_id,
        "frame_set_uid": frame_set.frame_set_uid,
        "manifest_digest": frame_set.manifest_digest,
        "manifest_json": frame_set.manifest_json,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ManifestIntegrityError(
            "v2 frame set is missing " + ", ".join(sorted(missing))
        )
    for frame in frame_set.frames.values():
        if not (
            frame.source_frame_uid
            and frame.source_session_id
            and frame.camera_stream_id
            and frame.sequence is not None
            and frame.content_digest
            and frame.archive_state == "ARCHIVE_DURABLE"
            and frame.file_path
        ):
            raise ManifestIntegrityError(
                "v2 manifest member is not identity-complete and archive-durable"
            )


def _verify_existing_manifest(
    existing: FrameSetManifest,
    frame_set: SynchronizedFrameSet,
) -> None:
    if (
        existing.manifest_digest != frame_set.manifest_digest
        or existing.manifest_json != frame_set.manifest_json
    ):
        raise ManifestIntegrityError(
            "frame_set_uid was reused with different manifest content"
        )


def _source_sessions_json(frame_set: SynchronizedFrameSet) -> str:
    return json.dumps(
        [
            {
                "camera_stream_id": frame.camera_stream_id,
                "source_session_id": frame.source_session_id,
            }
            for frame in sorted(
                frame_set.frames.values(),
                key=lambda item: item.camera_stream_id,
            )
        ],
        separators=(",", ":"),
        sort_keys=True,
    )

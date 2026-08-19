import time
from dataclasses import dataclass

from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

from app.models import (
    FrameSetDeliveryProjection,
    FrameSetManifest,
    FrameSetMember,
    RelayV2ClientState,
)


@dataclass(frozen=True)
class FrameSetKey:
    capture_run_id: str
    frame_set_id: int
    frame_set_uid: str


@dataclass(frozen=True)
class CreditIdentity:
    processor_instance_id: str
    stream_epoch: str
    credit_id: str


@dataclass(frozen=True)
class LiveFrameMember:
    source_frame_uid: str
    source_session_id: str
    camera_stream_id: str
    frame_sequence: int
    capture_timestamp_ms: int
    content_type: str
    image_size: int
    content_digest: str
    file_path: str
    authorized_camera_id: str | None = None
    camera_claim_id: str | None = None
    authorized_subject: str | None = None
    session_token_jti: str | None = None


@dataclass(frozen=True)
class ClaimedFrameSet:
    key: FrameSetKey
    credit: CreditIdentity
    capture_session_id: str
    anchor_timestamp_ms: int
    freshness_origin_ms: int
    synchronization_span_ms: int
    synchronized_at_ms: int
    manifest_digest: str
    manifest_json: str
    offered_at_ms: int
    members: tuple[LiveFrameMember, ...]
    tenant_id: str | None = None
    site_id: str | None = None
    processing_job_id: str | None = None
    profile_digest: str | None = None
    authorized_subject: str | None = None
    session_token_jti: str | None = None

    @property
    def authorized_camera_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                member.authorized_camera_id
                for member in self.members
                if member.authorized_camera_id
            )
        )


class LatestLiveStore:
    """Durable latest-only scheduler state for the v2 shadow transport.

    The singleton row bounds unresolved LIVE work to one identity. Image bytes
    remain in the archive and are loaded only after a valid credit is claimed.
    """

    _SINGLETON_ID = 1

    def __init__(self, session_factory):
        self._session_factory = session_factory

    def claim_latest(
        self,
        credit: CreditIdentity,
        *,
        offered_at_ms: int | None = None,
    ) -> ClaimedFrameSet | None:
        timestamp_ms = (
            offered_at_ms
            if offered_at_ms is not None
            else int(time.time() * 1000)
        )
        with self._session_factory() as db:
            self._ensure_state_row(db, timestamp_ms)
            state = db.get(RelayV2ClientState, self._SINGLETON_ID)
            if state.in_flight_frame_set_uid is not None:
                db.commit()
                return None
            candidate = self._newest_eligible(db, state)
            if candidate is None:
                db.commit()
                return None

            claimed = db.execute(
                update(RelayV2ClientState)
                .where(
                    RelayV2ClientState.singleton_id == self._SINGLETON_ID,
                    RelayV2ClientState.in_flight_frame_set_uid.is_(None),
                )
                .values(
                    in_flight_frame_set_uid=candidate.frame_set_uid,
                    in_flight_credit_id=credit.credit_id,
                    in_flight_processor_instance_id=credit.processor_instance_id,
                    in_flight_stream_epoch=credit.stream_epoch,
                    in_flight_offered_at_ms=timestamp_ms,
                    watermark_capture_run_id=candidate.capture_run_id,
                    watermark_frame_set_id=candidate.frame_set_id,
                    watermark_frame_set_uid=candidate.frame_set_uid,
                    reoffer_frame_set_uid=None,
                    updated_at_ms=timestamp_ms,
                )
            )
            if claimed.rowcount != 1:
                db.rollback()
                return None

            projected = db.execute(
                update(FrameSetDeliveryProjection)
                .where(
                    FrameSetDeliveryProjection.frame_set_uid
                    == candidate.frame_set_uid,
                    FrameSetDeliveryProjection.archive_state == "ARCHIVE_DURABLE",
                    FrameSetDeliveryProjection.live_state == "ELIGIBLE",
                )
                .values(
                    live_state="OFFERED",
                    last_reason=None,
                    updated_at_ms=timestamp_ms,
                )
            )
            if projected.rowcount != 1:
                db.rollback()
                return None

            snapshot = self._snapshot(db, candidate, credit, timestamp_ms)
            db.commit()
            return snapshot

    def unresolved_keys(self) -> tuple[FrameSetKey, ...]:
        current = self.current_in_flight()
        return (current,) if current is not None else ()

    def processing_job_for(self, capture_run_id: str) -> str | None:
        with self._session_factory() as db:
            state = db.get(RelayV2ClientState, self._SINGLETON_ID)
            if (
                state is None
                or state.processing_job_capture_run_id != capture_run_id
            ):
                return None
            return state.processing_job_id

    def bind_processing_job(
        self,
        *,
        capture_run_id: str,
        processing_job_id: str,
        updated_at_ms: int | None = None,
    ) -> None:
        if not capture_run_id or not processing_job_id:
            raise ValueError("capture run and processing job IDs are required")
        timestamp_ms = updated_at_ms or int(time.time() * 1000)
        with self._session_factory() as db:
            self._ensure_state_row(db, timestamp_ms)
            state = db.get(RelayV2ClientState, self._SINGLETON_ID)
            if (
                state.processing_job_capture_run_id == capture_run_id
                and state.processing_job_id not in {None, processing_job_id}
            ):
                raise RuntimeError(
                    "processing job changed within one capture run"
                )
            state.processing_job_capture_run_id = capture_run_id
            state.processing_job_id = processing_job_id
            state.updated_at_ms = timestamp_ms
            db.commit()

    def snapshot_for_hello(self) -> ClaimedFrameSet | None:
        with self._session_factory() as db:
            state = db.get(RelayV2ClientState, self._SINGLETON_ID)
            if state is None:
                state = RelayV2ClientState(
                    singleton_id=self._SINGLETON_ID,
                    updated_at_ms=0,
                )
            if state.in_flight_frame_set_uid is not None:
                manifest = db.get(
                    FrameSetManifest,
                    state.in_flight_frame_set_uid,
                )
                if manifest is None:
                    raise RuntimeError("relay v2 in-flight manifest is missing")
                credit = CreditIdentity(
                    processor_instance_id=(
                        state.in_flight_processor_instance_id or ""
                    ),
                    stream_epoch=state.in_flight_stream_epoch or "",
                    credit_id=state.in_flight_credit_id or "",
                )
                return self._snapshot(
                    db,
                    manifest,
                    credit,
                    state.in_flight_offered_at_ms or 0,
                )
            manifest = self._newest_eligible(db, state)
            if manifest is None:
                return None
            return self._snapshot(
                db,
                manifest,
                CreditIdentity("", "", ""),
                0,
            )

    def has_newer_eligible(self, key: FrameSetKey) -> bool:
        with self._session_factory() as db:
            current = db.get(FrameSetManifest, key.frame_set_uid)
            if current is None or self._key(current) != key:
                return False
            candidate = db.execute(
                select(FrameSetManifest)
                .join(
                    FrameSetDeliveryProjection,
                    FrameSetDeliveryProjection.frame_set_uid
                    == FrameSetManifest.frame_set_uid,
                )
                .where(
                    FrameSetDeliveryProjection.archive_state
                    == "ARCHIVE_DURABLE",
                    FrameSetDeliveryProjection.live_state == "ELIGIBLE",
                )
                .order_by(
                    FrameSetManifest.synchronized_at_ms.desc(),
                    FrameSetManifest.created_at_ms.desc(),
                    FrameSetManifest.frame_set_uid.desc(),
                )
                .limit(1)
            ).scalar_one_or_none()
            if candidate is None:
                return False
            if candidate.capture_run_id == key.capture_run_id:
                return candidate.frame_set_id > key.frame_set_id
            return (
                candidate.synchronized_at_ms,
                candidate.created_at_ms,
                candidate.frame_set_uid,
            ) > (
                current.synchronized_at_ms,
                current.created_at_ms,
                current.frame_set_uid,
            )

    def mark_unresolved(
        self,
        key: FrameSetKey,
        *,
        reason: str = "TRANSPORT_DISCONNECTED",
        updated_at_ms: int | None = None,
    ) -> bool:
        return self._transition_current(
            key,
            live_state="UNRESOLVED",
            reason=reason,
            release=False,
            updated_at_ms=updated_at_ms,
        )

    def apply_remote_status(
        self,
        key: FrameSetKey,
        *,
        state: str,
        reason: str | None = None,
        updated_at_ms: int | None = None,
    ) -> bool:
        normalized = state.strip().upper()
        if normalized not in {
            "ACCEPTED",
            "STARTED",
            "REJECTED",
            "COMPLETED",
            "FAILED",
            "RECOVERY_REQUIRED",
        }:
            raise ValueError(f"unsupported relay v2 status: {state}")
        return self._transition_current(
            key,
            live_state=normalized,
            reason=reason,
            release=normalized
            in {"REJECTED", "COMPLETED", "FAILED", "RECOVERY_REQUIRED"},
            updated_at_ms=updated_at_ms,
        )

    def release_before_send(
        self,
        key: FrameSetKey,
        *,
        state: str,
        reason: str,
        updated_at_ms: int | None = None,
    ) -> bool:
        normalized = state.strip().upper()
        if normalized not in {"EXPIRED_BEFORE_OFFER", "REJECTED"}:
            raise ValueError(f"unsupported pre-send state: {state}")
        return self._transition_current(
            key,
            live_state=normalized,
            reason=reason,
            release=True,
            updated_at_ms=updated_at_ms,
        )

    def reconcile_not_found(
        self,
        key: FrameSetKey,
        *,
        retry_allowed: bool,
        updated_at_ms: int | None = None,
    ) -> bool:
        timestamp_ms = updated_at_ms or int(time.time() * 1000)
        with self._session_factory() as db:
            state = db.get(RelayV2ClientState, self._SINGLETON_ID)
            if not self._matches_current(db, state, key):
                return False
            projection = db.get(FrameSetDeliveryProjection, key.frame_set_uid)
            projection.live_state = (
                "ELIGIBLE" if retry_allowed else "SUPERSEDED_BEFORE_OFFER"
            )
            projection.last_reason = (
                "RECONCILED_NOT_FOUND_RETRY"
                if retry_allowed
                else "RECONCILED_NOT_FOUND_NEWER_AVAILABLE"
            )
            projection.updated_at_ms = timestamp_ms
            state.in_flight_frame_set_uid = None
            state.in_flight_credit_id = None
            state.in_flight_processor_instance_id = None
            state.in_flight_stream_epoch = None
            state.in_flight_offered_at_ms = None
            state.reoffer_frame_set_uid = (
                key.frame_set_uid if retry_allowed else None
            )
            state.updated_at_ms = timestamp_ms
            db.commit()
            return True

    def current_in_flight(self) -> FrameSetKey | None:
        with self._session_factory() as db:
            state = db.get(RelayV2ClientState, self._SINGLETON_ID)
            if state is None or state.in_flight_frame_set_uid is None:
                return None
            manifest = db.get(FrameSetManifest, state.in_flight_frame_set_uid)
            if manifest is None:
                raise RuntimeError("relay v2 in-flight manifest is missing")
            return self._key(manifest)

    def offered_watermark(self) -> FrameSetKey | None:
        with self._session_factory() as db:
            state = db.get(RelayV2ClientState, self._SINGLETON_ID)
            if state is None or state.watermark_frame_set_uid is None:
                return None
            return FrameSetKey(
                capture_run_id=state.watermark_capture_run_id,
                frame_set_id=state.watermark_frame_set_id,
                frame_set_uid=state.watermark_frame_set_uid,
            )

    def _transition_current(
        self,
        key: FrameSetKey,
        *,
        live_state: str,
        reason: str | None,
        release: bool,
        updated_at_ms: int | None,
    ) -> bool:
        timestamp_ms = updated_at_ms or int(time.time() * 1000)
        with self._session_factory() as db:
            state = db.get(RelayV2ClientState, self._SINGLETON_ID)
            if not self._matches_current(db, state, key):
                return False
            projection = db.get(FrameSetDeliveryProjection, key.frame_set_uid)
            projection.live_state = live_state
            projection.last_reason = reason
            projection.updated_at_ms = timestamp_ms
            if release:
                state.in_flight_frame_set_uid = None
                state.in_flight_credit_id = None
                state.in_flight_processor_instance_id = None
                state.in_flight_stream_epoch = None
                state.in_flight_offered_at_ms = None
            state.updated_at_ms = timestamp_ms
            db.commit()
            return True

    @staticmethod
    def _matches_current(
        db: Session,
        state: RelayV2ClientState | None,
        key: FrameSetKey,
    ) -> bool:
        if state is None or state.in_flight_frame_set_uid != key.frame_set_uid:
            return False
        manifest = db.get(FrameSetManifest, key.frame_set_uid)
        return manifest is not None and LatestLiveStore._key(manifest) == key

    @staticmethod
    def _ensure_state_row(db: Session, timestamp_ms: int) -> None:
        db.execute(
            text(
                """
                INSERT INTO relay_v2_client_state (singleton_id, updated_at_ms)
                SELECT 1, :updated_at_ms
                WHERE NOT EXISTS (
                    SELECT 1 FROM relay_v2_client_state WHERE singleton_id = 1
                )
                """
            ),
            {"updated_at_ms": timestamp_ms},
        )

    @staticmethod
    def _newest_eligible(
        db: Session,
        state: RelayV2ClientState,
    ) -> FrameSetManifest | None:
        candidate = db.execute(
            select(FrameSetManifest)
            .join(
                FrameSetDeliveryProjection,
                FrameSetDeliveryProjection.frame_set_uid
                == FrameSetManifest.frame_set_uid,
            )
            .where(
                FrameSetDeliveryProjection.archive_state == "ARCHIVE_DURABLE",
                FrameSetDeliveryProjection.live_state == "ELIGIBLE",
            )
            .order_by(
                FrameSetManifest.synchronized_at_ms.desc(),
                FrameSetManifest.created_at_ms.desc(),
                FrameSetManifest.frame_set_uid.desc(),
            )
            .limit(1)
        ).scalar_one_or_none()
        if candidate is None or state.watermark_frame_set_uid is None:
            return candidate
        if candidate.frame_set_uid == state.reoffer_frame_set_uid:
            return candidate

        watermark = db.get(FrameSetManifest, state.watermark_frame_set_uid)
        if watermark is None:
            raise RuntimeError("relay v2 offered watermark manifest is missing")
        if candidate.capture_run_id == watermark.capture_run_id:
            return (
                candidate
                if candidate.frame_set_id > watermark.frame_set_id
                else None
            )
        candidate_order = (
            candidate.synchronized_at_ms,
            candidate.created_at_ms,
            candidate.frame_set_uid,
        )
        watermark_order = (
            watermark.synchronized_at_ms,
            watermark.created_at_ms,
            watermark.frame_set_uid,
        )
        return candidate if candidate_order > watermark_order else None

    @staticmethod
    def _snapshot(
        db: Session,
        manifest: FrameSetManifest,
        credit: CreditIdentity,
        offered_at_ms: int,
    ) -> ClaimedFrameSet:
        members = db.execute(
            select(FrameSetMember)
            .where(FrameSetMember.frame_set_uid == manifest.frame_set_uid)
            .order_by(FrameSetMember.camera_stream_id)
        ).scalars()
        return ClaimedFrameSet(
            key=LatestLiveStore._key(manifest),
            credit=credit,
            capture_session_id=manifest.capture_session_id,
            anchor_timestamp_ms=manifest.anchor_timestamp_ms,
            freshness_origin_ms=manifest.freshness_origin_ms,
            synchronization_span_ms=manifest.synchronization_span_ms,
            synchronized_at_ms=manifest.synchronized_at_ms,
            manifest_digest=manifest.manifest_digest,
            manifest_json=manifest.manifest_json,
            offered_at_ms=offered_at_ms,
            members=tuple(
                LiveFrameMember(
                    source_frame_uid=member.source_frame_uid,
                    source_session_id=member.source_session_id,
                    camera_stream_id=member.camera_stream_id,
                    frame_sequence=member.frame_sequence,
                    capture_timestamp_ms=member.capture_timestamp_ms,
                    content_type=member.content_type,
                    image_size=member.image_size,
                    content_digest=member.content_digest,
                    file_path=member.file_path,
                    authorized_camera_id=member.authorized_camera_id,
                    camera_claim_id=member.camera_claim_id,
                    authorized_subject=member.authorized_subject,
                    session_token_jti=member.session_token_jti,
                )
                for member in members
            ),
            tenant_id=manifest.tenant_id,
            site_id=manifest.site_id,
            processing_job_id=manifest.processing_job_id,
            profile_digest=manifest.profile_digest,
            authorized_subject=manifest.authorized_subject,
            session_token_jti=manifest.session_token_jti,
        )

    @staticmethod
    def _key(manifest: FrameSetManifest) -> FrameSetKey:
        return FrameSetKey(
            capture_run_id=manifest.capture_run_id,
            frame_set_id=manifest.frame_set_id,
            frame_set_uid=manifest.frame_set_uid,
        )

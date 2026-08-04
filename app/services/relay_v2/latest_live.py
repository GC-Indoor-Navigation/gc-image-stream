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
                )
                for member in members
            ),
        )

    @staticmethod
    def _key(manifest: FrameSetManifest) -> FrameSetKey:
        return FrameSetKey(
            capture_run_id=manifest.capture_run_id,
            frame_set_id=manifest.frame_set_id,
            frame_set_uid=manifest.frame_set_uid,
        )

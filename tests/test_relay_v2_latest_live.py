from concurrent.futures import ThreadPoolExecutor

import pytest

from app.models import (
    CaptureRun,
    CaptureSession,
    FrameSetDeliveryProjection,
    FrameSetManifest,
    FrameSetMember,
)
from app.services.relay_v2 import CreditIdentity, FrameSetKey, LatestLiveStore


def _persist_candidate(
    session_factory,
    *,
    uid: str,
    frame_set_id: int,
    synchronized_at_ms: int,
) -> None:
    with session_factory() as db:
        if db.get(CaptureSession, "capture-session") is None:
            db.add(
                CaptureSession(
                    id="capture-session",
                    state="OPEN",
                    source_sessions_json="[]",
                    started_at_ms=1,
                )
            )
            db.add(
                CaptureRun(
                    id="capture-run",
                    capture_session_id="capture-session",
                    identity_mode="V2",
                    state="OPEN",
                    started_at_ms=1,
                )
            )
        db.add(
            FrameSetManifest(
                frame_set_uid=uid,
                capture_session_id="capture-session",
                capture_run_id="capture-run",
                frame_set_id=frame_set_id,
                anchor_timestamp_ms=synchronized_at_ms,
                freshness_origin_ms=synchronized_at_ms - 5,
                synchronization_span_ms=5,
                manifest_digest=f"manifest-{uid}",
                manifest_json=f'{{"uid":"{uid}"}}',
                created_at_ms=synchronized_at_ms,
                sync_window_ms=50,
                synchronized_at_ms=synchronized_at_ms,
                member_count=1,
            )
        )
        db.add(
            FrameSetMember(
                frame_set_uid=uid,
                frame_id=None,
                source_frame_uid=f"source-{uid}",
                source_session_id="source-session",
                camera_stream_id="camera-1",
                frame_sequence=frame_set_id,
                capture_timestamp_ms=synchronized_at_ms - 5,
                content_type="image/jpeg",
                image_size=5,
                content_digest=f"content-{uid}",
                file_path=f"archive/{uid}.jpg",
            )
        )
        db.add(
            FrameSetDeliveryProjection(
                frame_set_uid=uid,
                archive_state="ARCHIVE_DURABLE",
                live_state="ELIGIBLE",
                legacy_relay_state="NOT_ENQUEUED",
                last_reason=None,
                updated_at_ms=synchronized_at_ms,
            )
        )
        db.commit()


def _credit(value: str) -> CreditIdentity:
    return CreditIdentity(
        processor_instance_id="processor-1",
        stream_epoch="epoch-1",
        credit_id=value,
    )


def test_claims_only_newest_eligible_snapshot(session_factory):
    _persist_candidate(
        session_factory,
        uid="older",
        frame_set_id=1,
        synchronized_at_ms=1000,
    )
    _persist_candidate(
        session_factory,
        uid="newer",
        frame_set_id=2,
        synchronized_at_ms=1100,
    )
    store = LatestLiveStore(session_factory)

    claimed = store.claim_latest(_credit("credit-1"), offered_at_ms=1200)

    assert claimed is not None
    assert claimed.key == FrameSetKey("capture-run", 2, "newer")
    assert claimed.credit.credit_id == "credit-1"
    assert claimed.offered_at_ms == 1200
    assert [member.camera_stream_id for member in claimed.members] == ["camera-1"]
    assert store.current_in_flight() == claimed.key
    assert store.offered_watermark() == claimed.key

    with session_factory() as db:
        assert db.get(FrameSetDeliveryProjection, "older").live_state == "ELIGIBLE"
        assert db.get(FrameSetDeliveryProjection, "newer").live_state == "OFFERED"


def test_retires_unclaimed_manifest_for_inactive_session(session_factory):
    _persist_candidate(
        session_factory,
        uid="inactive",
        frame_set_id=1,
        synchronized_at_ms=1000,
    )
    store = LatestLiveStore(session_factory)
    snapshot = store.snapshot_for_hello()

    assert snapshot is not None
    assert store.retire_eligible(snapshot.key, updated_at_ms=1100) is True
    assert store.snapshot_for_hello() is None
    with session_factory() as db:
        projection = db.get(FrameSetDeliveryProjection, "inactive")
        assert projection.live_state == "SESSION_INACTIVE"
        assert projection.last_reason == "RELAY_CREDENTIAL_SESSION_INACTIVE"


def test_in_flight_identity_blocks_additional_claims(session_factory):
    _persist_candidate(
        session_factory,
        uid="first",
        frame_set_id=1,
        synchronized_at_ms=1000,
    )
    store = LatestLiveStore(session_factory)
    assert store.claim_latest(_credit("credit-1")) is not None
    _persist_candidate(
        session_factory,
        uid="second",
        frame_set_id=2,
        synchronized_at_ms=1100,
    )

    assert store.claim_latest(_credit("credit-2")) is None
    assert store.current_in_flight() == FrameSetKey("capture-run", 1, "first")


def test_concurrent_credits_can_claim_at_most_one_identity(session_factory):
    _persist_candidate(
        session_factory,
        uid="only",
        frame_set_id=1,
        synchronized_at_ms=1000,
    )
    store = LatestLiveStore(session_factory)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(
                lambda index: store.claim_latest(_credit(f"credit-{index}")),
                range(8),
            )
        )

    assert sum(result is not None for result in results) == 1
    assert store.current_in_flight() == FrameSetKey("capture-run", 1, "only")


def test_empty_store_does_not_create_an_in_flight_identity(session_factory):
    store = LatestLiveStore(session_factory)

    assert store.claim_latest(_credit("credit-1")) is None
    assert store.current_in_flight() is None
    assert store.offered_watermark() is None


def test_disconnect_and_remote_terminal_status_preserve_truth(session_factory):
    _persist_candidate(
        session_factory,
        uid="current",
        frame_set_id=1,
        synchronized_at_ms=1000,
    )
    store = LatestLiveStore(session_factory)
    claimed = store.claim_latest(_credit("credit-1"))
    assert claimed is not None

    assert store.mark_unresolved(claimed.key) is True
    assert store.current_in_flight() == claimed.key
    with session_factory() as db:
        assert db.get(FrameSetDeliveryProjection, "current").live_state == (
            "UNRESOLVED"
        )

    assert store.apply_remote_status(
        claimed.key,
        state="COMPLETED",
        reason="RECONCILED_AFTER_RECONNECT",
    ) is True
    assert store.current_in_flight() is None
    with session_factory() as db:
        projection = db.get(FrameSetDeliveryProjection, "current")
        assert projection.live_state == "COMPLETED"
        assert projection.last_reason == "RECONCILED_AFTER_RECONNECT"


def test_reconciled_not_found_can_reoffer_same_watermark_once(session_factory):
    _persist_candidate(
        session_factory,
        uid="retry",
        frame_set_id=1,
        synchronized_at_ms=1000,
    )
    store = LatestLiveStore(session_factory)
    first = store.claim_latest(_credit("credit-1"))
    assert first is not None
    store.mark_unresolved(first.key)

    assert store.reconcile_not_found(first.key, retry_allowed=True) is True
    retried = store.claim_latest(_credit("credit-2"))

    assert retried is not None
    assert retried.key == first.key
    assert retried.credit.credit_id == "credit-2"


def test_reconciled_not_found_does_not_reoffer_stale_identity(session_factory):
    _persist_candidate(
        session_factory,
        uid="old",
        frame_set_id=1,
        synchronized_at_ms=1000,
    )
    store = LatestLiveStore(session_factory)
    first = store.claim_latest(_credit("credit-1"))
    assert first is not None
    _persist_candidate(
        session_factory,
        uid="new",
        frame_set_id=2,
        synchronized_at_ms=1100,
    )

    assert store.reconcile_not_found(first.key, retry_allowed=False) is True
    next_claim = store.claim_latest(_credit("credit-2"))

    assert next_claim is not None
    assert next_claim.key.frame_set_uid == "new"


def test_processing_job_is_stable_within_capture_run(session_factory):
    store = LatestLiveStore(session_factory)

    store.bind_processing_job(
        capture_run_id="run-1",
        processing_job_id="job-1",
    )

    assert store.processing_job_for("run-1") == "job-1"
    assert store.processing_job_for("run-2") is None
    with pytest.raises(RuntimeError, match="changed within"):
        store.bind_processing_job(
            capture_run_id="run-1",
            processing_job_id="job-other",
        )

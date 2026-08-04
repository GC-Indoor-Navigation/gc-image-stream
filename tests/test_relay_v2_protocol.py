import json

import pytest

from app.infrastructure.grpc.generated import live_frame_relay_v2_pb2 as relay_pb2
from app.services.identity import canonical_json, sha256_bytes
from app.services.relay_v2 import (
    ArchiveIntegrityError,
    ClaimedFrameSet,
    CreditIdentity,
    CreditRejected,
    FrameSetExpired,
    FrameSetKey,
    LiveFrameMember,
    NegotiatedSession,
    ProtocolConfig,
    accept_hello,
    build_credited_frame_set,
    build_producer_hello,
    credit_identity,
)


def _claim(tmp_path, payload=b"frame"):
    path = tmp_path / "frame.jpg"
    path.write_bytes(payload)
    metadata = {
        "width": 1920,
        "height": 1080,
        "orientation_deg": 90,
        "mirrored": False,
        "encoding": "image/jpeg",
        "color_space": "sRGB",
    }
    manifest = {
        "members": [
            {
                "camera_stream_id": "device-1/camera-1",
                "capture_config_digest": "capture-config",
                "capture_metadata_json": canonical_json(metadata),
            }
        ]
    }
    return ClaimedFrameSet(
        key=FrameSetKey("run-1", 7, "set-7"),
        credit=CreditIdentity("processor-1", "epoch-1", "credit-1"),
        capture_session_id="capture-session-1",
        anchor_timestamp_ms=10_010,
        freshness_origin_ms=10_000,
        synchronization_span_ms=10,
        synchronized_at_ms=10_020,
        manifest_digest="manifest-digest",
        manifest_json=json.dumps(manifest),
        offered_at_ms=10_030,
        members=(
            LiveFrameMember(
                source_frame_uid="source-1",
                source_session_id="source-session-1",
                camera_stream_id="device-1/camera-1",
                frame_sequence=7,
                capture_timestamp_ms=10_000,
                content_type="image/jpeg",
                image_size=len(payload),
                content_digest=sha256_bytes(payload),
                file_path=str(path),
            ),
        ),
    )


def _config():
    return ProtocolConfig(
        producer_session_id="producer-1",
        processing_profile_digest="profile-1",
        producer_freshness_budget_ms=500,
        maximum_clock_uncertainty_ms=5,
    )


def _session():
    return NegotiatedSession(
        processing_job_id="job-1",
        processor_instance_id="processor-1",
        stream_epoch="epoch-1",
        maximum_payload_bytes=1_000_000,
        processing_profile_digest="profile-1",
    )


def _credit(**overrides):
    values = {
        "scope": relay_pb2.CreditScope(
            processor_instance_id="processor-1",
            stream_epoch="epoch-1",
            credit_id="credit-1",
        ),
        "credit_lease_duration_ms": 100,
        "earliest_acceptable_freshness_origin_utc_ms": 9_900,
        "ingress_reserve_ms": 20,
        "maximum_payload_bytes": 1_000_000,
        "schema_version": 1,
        "maximum_frame_age_ms": 400,
    }
    values.update(overrides)
    return relay_pb2.ProcessorCredit(**values)


def test_hello_includes_watermark_unresolved_and_capture_contract(tmp_path):
    claim = _claim(tmp_path)
    hello = build_producer_hello(
        config=_config(),
        claim=claim,
        watermark=claim.key,
        unresolved=(claim.key,),
        proposed_processing_job_id="job-1",
        measured_utc_ms=10_040,
    ).hello

    assert hello.capture_run_id == "run-1"
    assert hello.proposed_processing_job_id == "job-1"
    assert hello.last_offered_watermark.frame_set_uid == "set-7"
    assert hello.unresolved_frame_sets[0].frame_set_id == 7
    assert hello.capture_configs[0].width == 1920
    assert hello.clock_health.maximum_uncertainty_ms == 5


def test_accept_hello_and_credit_scope_are_strictly_fenced():
    accepted = relay_pb2.ProcessorEnvelope(
        hello_accepted=relay_pb2.HelloAccepted(
            protocol_version=2,
            schema_version=1,
            processing_job_id="job-1",
            processor_instance_id="processor-1",
            stream_epoch="epoch-1",
            maximum_payload_bytes=1_000_000,
            processing_profile_digest="profile-1",
        )
    )
    session = accept_hello(accepted, config=_config())
    assert credit_identity(_credit(), session).credit_id == "credit-1"

    fenced = _credit(
        scope=relay_pb2.CreditScope(
            processor_instance_id="processor-1",
            stream_epoch="old-epoch",
            credit_id="credit-old",
        )
    )
    with pytest.raises(CreditRejected, match="fenced"):
        credit_identity(fenced, session)


def test_payload_is_built_only_after_archive_and_deadline_checks(tmp_path):
    envelope = build_credited_frame_set(
        claim=_claim(tmp_path),
        credit=_credit(),
        session=_session(),
        config=_config(),
        credit_received_monotonic=5.0,
        now_monotonic=5.01,
        now_utc_ms=10_100,
    )
    frame_set = envelope.frame_set

    assert frame_set.key.frame_set_uid == "set-7"
    assert frame_set.effective_deadline_utc_ms == 10_395
    assert frame_set.frames[0].image_bytes == b"frame"
    assert frame_set.frames[0].content_digest == sha256_bytes(b"frame")
    assert frame_set.content_digest


def test_expired_credit_or_freshness_never_builds_payload(tmp_path):
    claim = _claim(tmp_path)
    with pytest.raises(CreditRejected, match="lease expired"):
        build_credited_frame_set(
            claim=claim,
            credit=_credit(),
            session=_session(),
            config=_config(),
            credit_received_monotonic=5.0,
            now_monotonic=5.1,
            now_utc_ms=10_100,
        )
    with pytest.raises(FrameSetExpired, match="deadline"):
        build_credited_frame_set(
            claim=claim,
            credit=_credit(),
            session=_session(),
            config=_config(),
            credit_received_monotonic=5.0,
            now_monotonic=5.01,
            now_utc_ms=10_380,
        )


def test_archive_digest_mismatch_is_terminal_integrity_error(tmp_path):
    claim = _claim(tmp_path)
    (tmp_path / "frame.jpg").write_bytes(b"other")

    with pytest.raises(ArchiveIntegrityError, match="digest mismatch"):
        build_credited_frame_set(
            claim=claim,
            credit=_credit(),
            session=_session(),
            config=_config(),
            credit_received_monotonic=5.0,
            now_monotonic=5.01,
            now_utc_ms=10_100,
        )

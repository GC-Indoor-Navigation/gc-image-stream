import hashlib
import json
import time
from dataclasses import dataclass, replace
from pathlib import Path

from app.infrastructure.grpc.generated import live_frame_relay_v2_pb2 as relay_pb2
from app.infrastructure.grpc.relay_v2_contract import serialize_with_limit
from app.services.identity import canonical_json, sha256_bytes
from app.services.relay_v2.latest_live import (
    ClaimedFrameSet,
    CreditIdentity,
    FrameSetKey,
)


class CreditRejected(ValueError):
    pass


class FrameSetExpired(ValueError):
    pass


class ArchiveIntegrityError(ValueError):
    pass


@dataclass(frozen=True)
class ProtocolConfig:
    producer_session_id: str
    processing_profile_digest: str | None
    producer_freshness_budget_ms: int
    maximum_clock_uncertainty_ms: int = 0
    protocol_version: int = 2
    schema_version: int = 1
    clock_source: str = "STREAM_SERVER_UTC"


@dataclass(frozen=True)
class NegotiatedSession:
    processing_job_id: str
    processor_instance_id: str
    stream_epoch: str
    maximum_payload_bytes: int
    processing_profile_digest: str


def bind_authorized_claim(
    config: ProtocolConfig,
    claim: ClaimedFrameSet,
) -> ProtocolConfig:
    if claim.profile_digest:
        if (
            config.processing_profile_digest
            and config.processing_profile_digest != claim.profile_digest
        ):
            raise CreditRejected(
                "configured profile digest conflicts with authorized manifest"
            )
        return replace(config, processing_profile_digest=claim.profile_digest)
    if not config.processing_profile_digest:
        raise CreditRejected("frame set is missing a processing profile digest")
    return config


def build_producer_hello(
    *,
    config: ProtocolConfig,
    claim: ClaimedFrameSet,
    watermark: FrameSetKey | None,
    unresolved: tuple[FrameSetKey, ...],
    proposed_processing_job_id: str | None = None,
    measured_utc_ms: int | None = None,
) -> relay_pb2.ProducerEnvelope:
    capture_configs, _ = _manifest_members(claim)
    fields = {
        "protocol_version": config.protocol_version,
        "schema_version": config.schema_version,
        "producer_session_id": config.producer_session_id,
        "capture_session_id": claim.capture_session_id,
        "capture_run_id": claim.key.capture_run_id,
        "mode": relay_pb2.LIVE,
        "processing_profile_digest": config.processing_profile_digest,
        "expected_camera_stream_ids": [
            item.camera_stream_id for item in capture_configs
        ],
        "capture_configs": capture_configs,
        "clock_health": relay_pb2.ProducerClockHealth(
            state=relay_pb2.READY,
            maximum_uncertainty_ms=config.maximum_clock_uncertainty_ms,
            measured_utc_ms=(
                measured_utc_ms
                if measured_utc_ms is not None
                else int(time.time() * 1000)
            ),
            clock_source=config.clock_source,
        ),
        "unresolved_frame_sets": [_proto_key(key) for key in unresolved],
    }
    if claim.tenant_id:
        fields.update(
            tenant_id=claim.tenant_id,
            site_id=claim.site_id,
            authorized_subject=claim.authorized_subject or "",
            session_token_jti=claim.session_token_jti or "",
        )
    if watermark is not None:
        fields["last_offered_watermark"] = _proto_key(watermark)
    if proposed_processing_job_id:
        fields["proposed_processing_job_id"] = proposed_processing_job_id
    return relay_pb2.ProducerEnvelope(
        hello=relay_pb2.ProducerHello(**fields)
    )


def accept_hello(
    envelope: relay_pb2.ProcessorEnvelope,
    *,
    config: ProtocolConfig,
) -> NegotiatedSession:
    if envelope.WhichOneof("body") == "hello_rejected":
        rejected = envelope.hello_rejected
        raise CreditRejected(
            f"hello rejected: {relay_pb2.Reason.Name(rejected.reason)} "
            f"({rejected.detail_code})"
        )
    if envelope.WhichOneof("body") != "hello_accepted":
        raise CreditRejected("first processor envelope must accept or reject hello")
    accepted = envelope.hello_accepted
    if accepted.protocol_version != config.protocol_version:
        raise CreditRejected("processor negotiated an unsupported protocol version")
    if accepted.schema_version != config.schema_version:
        raise CreditRejected("processor negotiated an unsupported schema version")
    if accepted.processing_profile_digest != config.processing_profile_digest:
        raise CreditRejected("processor profile digest does not match producer")
    if not (
        accepted.processing_job_id
        and accepted.processor_instance_id
        and accepted.stream_epoch
        and accepted.maximum_payload_bytes > 0
    ):
        raise CreditRejected("hello acceptance is missing required identity or bounds")
    return NegotiatedSession(
        processing_job_id=accepted.processing_job_id,
        processor_instance_id=accepted.processor_instance_id,
        stream_epoch=accepted.stream_epoch,
        maximum_payload_bytes=accepted.maximum_payload_bytes,
        processing_profile_digest=accepted.processing_profile_digest,
    )


def credit_identity(
    credit: relay_pb2.ProcessorCredit,
    session: NegotiatedSession,
) -> CreditIdentity:
    if credit.schema_version != 1:
        raise CreditRejected("credit schema version is unsupported")
    scope = credit.scope
    if (
        scope.processor_instance_id != session.processor_instance_id
        or scope.stream_epoch != session.stream_epoch
        or not scope.credit_id
    ):
        raise CreditRejected("credit scope is fenced or incomplete")
    if credit.credit_lease_duration_ms <= 0:
        raise CreditRejected("credit lease must be positive")
    return CreditIdentity(
        processor_instance_id=scope.processor_instance_id,
        stream_epoch=scope.stream_epoch,
        credit_id=scope.credit_id,
    )


def build_credited_frame_set(
    *,
    claim: ClaimedFrameSet,
    credit: relay_pb2.ProcessorCredit,
    session: NegotiatedSession,
    config: ProtocolConfig,
    credit_received_monotonic: float,
    now_monotonic: float | None = None,
    now_utc_ms: int | None = None,
) -> relay_pb2.ProducerEnvelope:
    current_monotonic = (
        now_monotonic if now_monotonic is not None else time.monotonic()
    )
    current_utc_ms = (
        now_utc_ms if now_utc_ms is not None else int(time.time() * 1000)
    )
    lease_deadline = (
        credit_received_monotonic
        + credit.credit_lease_duration_ms / 1000
    )
    if current_monotonic >= lease_deadline:
        raise CreditRejected("credit lease expired before payload send")
    if claim.freshness_origin_ms < credit.earliest_acceptable_freshness_origin_utc_ms:
        raise FrameSetExpired("frame set predates the credit freshness watermark")

    effective_deadline_utc_ms = min(
        claim.freshness_origin_ms + config.producer_freshness_budget_ms,
        claim.freshness_origin_ms + credit.maximum_frame_age_ms,
    ) - config.maximum_clock_uncertainty_ms
    if current_utc_ms + credit.ingress_reserve_ms >= effective_deadline_utc_ms:
        raise FrameSetExpired("frame set cannot reach admission before its deadline")

    capture_configs, metadata_by_camera = _manifest_members(claim)
    frames = []
    content_records = []
    for member in claim.members:
        payload = Path(member.file_path).read_bytes()
        if len(payload) != member.image_size:
            raise ArchiveIntegrityError(
                f"archive size mismatch for {member.source_frame_uid}"
            )
        if sha256_bytes(payload) != member.content_digest:
            raise ArchiveIntegrityError(
                f"archive digest mismatch for {member.source_frame_uid}"
            )
        metadata = metadata_by_camera[member.camera_stream_id]
        frames.append(
            relay_pb2.FramePayload(
                source_frame_uid=member.source_frame_uid,
                source_session_id=member.source_session_id,
                camera_stream_id=member.camera_stream_id,
                frame_sequence=member.frame_sequence,
                capture_timestamp_device_ms=member.capture_timestamp_ms,
                capture_timestamp_corrected_utc_ms=member.capture_timestamp_ms,
                clock_correction=relay_pb2.ClockCorrection(
                    source_clock_domain="CAPTURE_TIMESTAMP_MS",
                    estimated_offset_ms=0,
                    estimated_drift_ppb=0,
                    uncertainty_ms=config.maximum_clock_uncertainty_ms,
                    measured_utc_ms=current_utc_ms,
                ),
                stream_received_utc_ms=claim.synchronized_at_ms,
                width=_nonnegative_int(metadata.get("width")),
                height=_nonnegative_int(metadata.get("height")),
                orientation_deg=int(metadata.get("orientation_deg", 0)),
                mirrored=bool(metadata.get("mirrored", False)),
                encoding=str(metadata.get("encoding") or member.content_type),
                color_space=str(metadata.get("color_space") or "UNKNOWN"),
                capture_config_digest=str(metadata["capture_config_digest"]),
                content_size_bytes=len(payload),
                content_digest=member.content_digest,
                image_bytes=payload,
            )
        )
        content_records.append(
            {
                "camera_stream_id": member.camera_stream_id,
                "content_digest": member.content_digest,
                "content_size_bytes": len(payload),
                "source_frame_uid": member.source_frame_uid,
            }
        )

    frame_set = relay_pb2.CreditedFrameSet(
        credit_scope=credit.scope,
        processing_job_id=session.processing_job_id,
        producer_session_id=config.producer_session_id,
        capture_session_id=claim.capture_session_id,
        key=_proto_key(claim.key),
        mode=relay_pb2.LIVE,
        manifest_digest=claim.manifest_digest,
        content_digest=hashlib.sha256(
            canonical_json(content_records).encode("utf-8")
        ).hexdigest(),
        ordering_anchor_utc_ms=claim.anchor_timestamp_ms,
        freshness_origin_utc_ms=claim.freshness_origin_ms,
        synchronized_utc_ms=claim.synchronized_at_ms,
        offered_utc_ms=current_utc_ms,
        producer_freshness_budget_ms=config.producer_freshness_budget_ms,
        effective_deadline_utc_ms=effective_deadline_utc_ms,
        synchronization_span_ms=claim.synchronization_span_ms,
        clock_uncertainty_ms=config.maximum_clock_uncertainty_ms,
        capture_configs=capture_configs,
        frames=frames,
        alert_deadline_utc_ms=effective_deadline_utc_ms,
        reserved_delivery_bound_ms=0,
        processing_profile_digest=config.processing_profile_digest,
        tenant_id=claim.tenant_id or "",
        site_id=claim.site_id or "",
        authorized_subject=claim.authorized_subject or "",
        session_token_jti=claim.session_token_jti or "",
    )
    envelope = relay_pb2.ProducerEnvelope(frame_set=frame_set)
    serialize_with_limit(
        envelope,
        maximum_payload_bytes=min(
            session.maximum_payload_bytes,
            credit.maximum_payload_bytes,
        ),
    )
    if (
        (now_monotonic if now_monotonic is not None else time.monotonic())
        >= lease_deadline
    ):
        raise CreditRejected("credit lease expired while assembling payload")
    return envelope


def build_no_data(
    credit: relay_pb2.ProcessorCredit,
    *,
    reason: int = relay_pb2.NO_DATA,
    newest_known: FrameSetKey | None = None,
) -> relay_pb2.ProducerEnvelope:
    fields = {"scope": credit.scope, "reason": reason}
    if newest_known is not None:
        fields["newest_known_frame_set"] = _proto_key(newest_known)
    return relay_pb2.ProducerEnvelope(no_data=relay_pb2.NoData(**fields))


def build_reconciliation_request(
    *,
    session: NegotiatedSession,
    config: ProtocolConfig,
    unresolved: tuple[FrameSetKey, ...],
    resume_token: str | None = None,
) -> relay_pb2.ProducerEnvelope:
    fields = {
        "processing_job_id": session.processing_job_id,
        "producer_session_id": config.producer_session_id,
        "unresolved_frame_sets": [_proto_key(key) for key in unresolved],
    }
    if resume_token:
        fields["reconciliation_resume_token"] = resume_token
    return relay_pb2.ProducerEnvelope(
        reconciliation=relay_pb2.ReconciliationRequest(**fields)
    )


def _manifest_members(
    claim: ClaimedFrameSet,
) -> tuple[list[relay_pb2.CaptureConfigSnapshot], dict[str, dict]]:
    payload = json.loads(claim.manifest_json)
    raw_members = payload.get("members")
    if not isinstance(raw_members, list):
        raise ArchiveIntegrityError("manifest members are missing")
    metadata_by_camera = {}
    configs = []
    for member in raw_members:
        camera_stream_id = str(member.get("camera_stream_id") or "")
        digest = str(member.get("capture_config_digest") or "")
        if not camera_stream_id or not digest:
            raise ArchiveIntegrityError("manifest capture config is incomplete")
        raw_metadata = member.get("capture_metadata_json") or "{}"
        metadata = json.loads(raw_metadata)
        metadata["capture_config_digest"] = digest
        metadata_by_camera[camera_stream_id] = metadata
        configs.append(
            relay_pb2.CaptureConfigSnapshot(
                camera_stream_id=camera_stream_id,
                capture_config_digest=digest,
                width=_nonnegative_int(metadata.get("width")),
                height=_nonnegative_int(metadata.get("height")),
                orientation_deg=int(metadata.get("orientation_deg", 0)),
                mirrored=bool(metadata.get("mirrored", False)),
                encoding=str(metadata.get("encoding") or "image/jpeg"),
                color_space=str(metadata.get("color_space") or "UNKNOWN"),
                authorized_camera_id=str(
                    member.get("authorized_camera_id") or ""
                ),
            )
        )
    configs.sort(key=lambda item: item.camera_stream_id)
    if set(metadata_by_camera) != {
        member.camera_stream_id for member in claim.members
    }:
        raise ArchiveIntegrityError("manifest and archived member sets differ")
    return configs, metadata_by_camera


def _proto_key(key: FrameSetKey) -> relay_pb2.FrameSetKey:
    return relay_pb2.FrameSetKey(
        capture_run_id=key.capture_run_id,
        frame_set_id=key.frame_set_id,
        frame_set_uid=key.frame_set_uid,
    )


def _nonnegative_int(value) -> int:
    parsed = int(value or 0)
    if parsed < 0:
        raise ArchiveIntegrityError("capture dimensions must be nonnegative")
    return parsed

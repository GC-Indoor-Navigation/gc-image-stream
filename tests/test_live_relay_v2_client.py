import json
import queue

import pytest

from app.infrastructure.grpc.generated import live_frame_relay_v2_pb2 as relay_pb2
from app.infrastructure.grpc.live_relay_v2_client import (
    PermanentRelayError,
    ProcessingLiveRelayV2Client,
    ReconnectBackoff,
)
from app.models import (
    CaptureRun,
    CaptureSession,
    FrameSetDeliveryProjection,
    FrameSetManifest,
    FrameSetMember,
)
from app.services.identity import canonical_json, sha256_bytes
from app.services.relay_v2 import LatestLiveStore, NegotiatedSession, ProtocolConfig
from app.services.session_identity import (
    ActiveSessionCredentialStore,
    AuthorizedSessionScope,
)
from app.services.relay_credentials import (
    ActiveProcessingRelayCredential,
    ProcessingRelayScope,
)


def _config():
    return ProtocolConfig(
        producer_session_id="producer-1",
        processing_profile_digest="profile-1",
        producer_freshness_budget_ms=500,
    )


def _session():
    return NegotiatedSession(
        processing_job_id="job-1",
        processor_instance_id="processor-1",
        stream_epoch="epoch-1",
        maximum_payload_bytes=1_000_000,
        processing_profile_digest="profile-1",
    )


def _credit():
    return relay_pb2.ProcessorCredit(
        scope=relay_pb2.CreditScope(
            processor_instance_id="processor-1",
            stream_epoch="epoch-1",
            credit_id="credit-1",
        ),
        credit_lease_duration_ms=100,
        earliest_acceptable_freshness_origin_utc_ms=9_000,
        ingress_reserve_ms=20,
        maximum_payload_bytes=1_000_000,
        schema_version=1,
        maximum_frame_age_ms=2_000,
    )


def _persist_candidate(session_factory, tmp_path, *, authorized=False):
    payload = b"frame"
    frame_path = tmp_path / "frame.jpg"
    frame_path.write_bytes(payload)
    manifest_json = json.dumps(
        {
            "members": [
                {
                    "camera_stream_id": "camera-1",
                    "capture_config_digest": "capture-config",
                    "capture_metadata_json": canonical_json(
                        {"width": 640, "height": 480}
                    ),
                }
            ]
        }
    )
    with session_factory() as db:
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
                frame_set_uid="set-1",
                capture_session_id="capture-session",
                capture_run_id="capture-run",
                frame_set_id=1,
                anchor_timestamp_ms=10_010,
                freshness_origin_ms=10_000,
                synchronization_span_ms=10,
                manifest_digest="manifest-digest",
                manifest_json=manifest_json,
                created_at_ms=10_020,
                sync_window_ms=50,
                synchronized_at_ms=10_020,
                member_count=1,
                tenant_id="tenant-1" if authorized else None,
                site_id="site-1" if authorized else None,
                processing_job_id="job-1" if authorized else None,
                profile_digest="a" * 64 if authorized else None,
                authorized_subject="user-1" if authorized else None,
                session_token_jti="job-1" if authorized else None,
            )
        )
        db.add(
            FrameSetMember(
                frame_set_uid="set-1",
                frame_id=None,
                source_frame_uid="source-1",
                source_session_id="source-session",
                camera_stream_id="camera-1",
                frame_sequence=1,
                capture_timestamp_ms=10_000,
                content_type="image/jpeg",
                image_size=len(payload),
                content_digest=sha256_bytes(payload),
                file_path=str(frame_path),
                authorized_camera_id="camera-1" if authorized else None,
            )
        )
        db.add(
            FrameSetDeliveryProjection(
                frame_set_uid="set-1",
                archive_state="ARCHIVE_DURABLE",
                live_state="ELIGIBLE",
                legacy_relay_state="NOT_ENQUEUED",
                last_reason=None,
                updated_at_ms=10_020,
            )
        )
        db.commit()


def test_live_relay_v2_client_remains_disabled_by_default():
    client = ProcessingLiveRelayV2Client()

    assert client.start() is None
    assert client.status() == {
        "contract_registered": True,
        "enabled": False,
        "running": False,
        "target": "",
        "last_error": None,
        "connection_count": 0,
        "reconnect_count": 0,
        "offered_count": 0,
        "no_data_count": 0,
        "in_flight": False,
    }


def test_enabling_requires_all_shadow_dependencies(session_factory):
    client = ProcessingLiveRelayV2Client()

    with pytest.raises(ValueError, match="requires target"):
        client.configure(target="127.0.0.1:50051", enabled=True)

    client.configure(
        target="127.0.0.1:50051",
        enabled=True,
        session_factory=session_factory,
        protocol_config=_config(),
    )
    assert client.status()["enabled"] is True


def test_reconnect_backoff_is_bounded_and_jittered():
    backoff = ReconnectBackoff(
        initial_sec=0.1,
        maximum_sec=1.0,
        jitter_ratio=0.2,
    )

    assert backoff.delay(0, random_value=0.0) == pytest.approx(0.08)
    assert backoff.delay(0, random_value=1.0) == pytest.approx(0.12)
    assert backoff.delay(20, random_value=0.5) == pytest.approx(1.0)


def test_credit_sends_exactly_one_latest_payload(
    session_factory,
    tmp_path,
):
    _persist_candidate(session_factory, tmp_path)
    client = ProcessingLiveRelayV2Client(
        monotonic=lambda: 5.0,
        utc_now_ms=lambda: 10_100,
    )
    client.configure(
        target="127.0.0.1:50051",
        enabled=True,
        session_factory=session_factory,
        protocol_config=_config(),
    )
    outgoing = queue.Queue(maxsize=2)

    client._handle_credit(_credit(), _session(), outgoing)

    assert outgoing.get_nowait().frame_set.key.frame_set_uid == "set-1"
    assert outgoing.empty()
    assert client.status()["offered_count"] == 1
    assert client.status()["in_flight"] is True


def test_shutdown_fences_in_flight_as_unresolved(session_factory, tmp_path):
    _persist_candidate(session_factory, tmp_path)
    client = ProcessingLiveRelayV2Client(
        monotonic=lambda: 5.0,
        utc_now_ms=lambda: 10_100,
    )
    client.configure(
        target="127.0.0.1:50051",
        enabled=True,
        session_factory=session_factory,
        protocol_config=_config(),
    )
    client._handle_credit(_credit(), _session(), queue.Queue(maxsize=2))

    client.stop()

    store = LatestLiveStore(session_factory)
    assert store.current_in_flight().frame_set_uid == "set-1"
    with session_factory() as db:
        projection = db.get(FrameSetDeliveryProjection, "set-1")
        assert projection.live_state == "UNRESOLVED"
        assert projection.last_reason == "SHUTDOWN_INTERRUPTED"


def test_authorized_relay_forwards_token_as_grpc_metadata(
    session_factory,
    tmp_path,
    monkeypatch,
):
    _persist_candidate(session_factory, tmp_path, authorized=True)
    credentials = ActiveSessionCredentialStore(now=lambda: 1_000)
    credentials.register(
        "signed-session-token",
        AuthorizedSessionScope(
            tenant_id="tenant-1",
            site_id="site-1",
            capture_session_id="capture-session",
            processing_job_id="job-1",
            camera_ids=frozenset({"camera-1"}),
            profile_digest="a" * 64,
            authorized_subject="user-1",
            token_jti="job-1",
            expires_at=1_100,
        ),
    )
    observed = {}

    class Channel:
        def close(self):
            observed["closed"] = True

    def relay(requests, metadata=None):
        observed["metadata"] = metadata
        observed["hello"] = next(requests).hello
        yield relay_pb2.ProcessorEnvelope(
            hello_rejected=relay_pb2.HelloRejected(
                reason=relay_pb2.IDENTITY_CONFLICT,
                detail_code="TEST_STOP",
                retryable=False,
            )
        )

    client = ProcessingLiveRelayV2Client(
        channel_factory=lambda target: Channel(),
        credential_store=credentials,
    )
    monkeypatch.setattr(client, "build_stub", lambda channel: relay)
    client.configure(
        target="processing:50053",
        enabled=True,
        session_factory=session_factory,
        protocol_config=ProtocolConfig(
            producer_session_id="producer-1",
            processing_profile_digest=None,
            producer_freshness_budget_ms=500,
        ),
    )

    with pytest.raises(PermanentRelayError, match="TEST_STOP"):
        client._run_connection()

    assert observed["metadata"] == (
        ("authorization", "Bearer signed-session-token"),
    )
    assert observed["hello"].tenant_id == "tenant-1"
    assert observed["hello"].processing_profile_digest == "a" * 64
    assert observed["hello"].proposed_processing_job_id == "job-1"
    assert observed["closed"] is True


def test_workload_relay_credential_replaces_participant_identity(
    session_factory,
    tmp_path,
    monkeypatch,
):
    _persist_candidate(session_factory, tmp_path, authorized=True)
    relay_scope = _relay_scope()
    provider = _RelayCredentialProvider(relay_scope)
    observed = {}

    class Channel:
        def close(self):
            observed["closed"] = True

    def relay(requests, metadata=None):
        observed["metadata"] = metadata
        observed["hello"] = next(requests).hello
        yield relay_pb2.ProcessorEnvelope(
            hello_rejected=relay_pb2.HelloRejected(
                reason=relay_pb2.IDENTITY_CONFLICT,
                detail_code="TEST_STOP",
                retryable=False,
            )
        )

    client = ProcessingLiveRelayV2Client(
        channel_factory=lambda target: Channel(),
        relay_credential_provider=provider,
    )
    monkeypatch.setattr(client, "build_stub", lambda channel: relay)
    client.configure(
        target="processing:50053",
        enabled=True,
        session_factory=session_factory,
        protocol_config=ProtocolConfig(
            producer_session_id="producer-1",
            processing_profile_digest=None,
            producer_freshness_budget_ms=500,
        ),
        relay_credential_provider=provider,
    )

    with pytest.raises(PermanentRelayError, match="TEST_STOP"):
        client._run_connection()

    assert observed["metadata"] == (
        ("authorization", "Bearer signed-relay-token"),
    )
    assert observed["hello"].authorized_subject == "coordinator-1"
    assert observed["hello"].session_token_jti == "relay-jti-1"
    assert observed["hello"].proposed_processing_job_id == "job-1"
    assert provider.claims[0].authorized_subject == "user-1"


def test_workload_relay_identity_is_used_on_credited_frame_set(
    session_factory,
    tmp_path,
):
    _persist_candidate(session_factory, tmp_path, authorized=True)
    relay_scope = _relay_scope()
    outgoing = queue.Queue(maxsize=2)
    client = ProcessingLiveRelayV2Client(
        monotonic=lambda: 5.0,
        utc_now_ms=lambda: 10_100,
    )
    client.configure(
        target="processing:50053",
        enabled=True,
        session_factory=session_factory,
        protocol_config=ProtocolConfig(
            producer_session_id="producer-1",
            processing_profile_digest="a" * 64,
            producer_freshness_budget_ms=500,
        ),
    )

    client._handle_credit(
        _credit(),
        NegotiatedSession(
            processing_job_id="job-1",
            processor_instance_id="processor-1",
            stream_epoch="epoch-1",
            maximum_payload_bytes=1_000_000,
            processing_profile_digest="a" * 64,
        ),
        outgoing,
        config=client._required_config(),
        relay_scope=relay_scope,
    )

    frame_set = outgoing.get_nowait().frame_set
    assert frame_set.authorized_subject == "coordinator-1"
    assert frame_set.session_token_jti == "relay-jti-1"


def _relay_scope():
    return ProcessingRelayScope(
        tenant_id="tenant-1",
        site_id="site-1",
        capture_session_id="capture-session",
        processing_job_id="job-1",
        profile_digest="a" * 64,
        coordinator_subject="coordinator-1",
        camera_ids=frozenset({"camera-1"}),
        workload_subject="gc-image-stream:service-account",
        token_jti="relay-jti-1",
        expires_at=2_000_000_000,
    )


class _RelayCredentialProvider:
    def __init__(self, scope):
        self.scope = scope
        self.claims = []

    def resolve_for_claim(self, claim):
        self.claims.append(claim)
        return ActiveProcessingRelayCredential(
            token="signed-relay-token",
            scope=self.scope,
        )

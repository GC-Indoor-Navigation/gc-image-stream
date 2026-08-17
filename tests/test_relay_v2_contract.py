import hashlib
from pathlib import Path

import pytest
from google.protobuf import descriptor_pb2

from app.infrastructure.grpc.generated import live_frame_relay_v2_pb2
from app.infrastructure.grpc.relay_v2_contract import (
    PayloadLimitExceeded,
    parse_with_limit,
    serialize_with_limit,
)


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "proto" / "artifacts"
FIXTURES = ROOT / "tests" / "fixtures" / "relay_v2"


def test_generated_v2_client_contract_is_bidirectional():
    service = live_frame_relay_v2_pb2.DESCRIPTOR.services_by_name[
        "LiveFrameRelayService"
    ]
    method = service.methods_by_name["Relay"]

    assert live_frame_relay_v2_pb2.DESCRIPTOR.package == (
        "gc_image_stream.processing.v2"
    )
    assert method.client_streaming is True
    assert method.server_streaming is True
    assert method.input_type.name == "ProducerEnvelope"
    assert method.output_type.name == "ProcessorEnvelope"


def test_generated_v2_client_preserves_hello_and_credit_messages():
    hello = live_frame_relay_v2_pb2.ProducerEnvelope(
        hello=live_frame_relay_v2_pb2.ProducerHello(
            protocol_version=2,
            schema_version=1,
            producer_session_id="producer-1",
            capture_run_id="run-1",
            mode=live_frame_relay_v2_pb2.LIVE,
            processing_profile_digest="profile-sha256",
            expected_camera_stream_ids=["device-1/camera-1"],
        )
    )
    credit = live_frame_relay_v2_pb2.ProcessorEnvelope(
        credit=live_frame_relay_v2_pb2.ProcessorCredit(
            scope=live_frame_relay_v2_pb2.CreditScope(
                processor_instance_id="processor-1",
                stream_epoch="epoch-1",
                credit_id="credit-1",
            ),
            credit_lease_duration_ms=400,
            maximum_payload_bytes=8_000_000,
            schema_version=1,
            maximum_frame_age_ms=500,
        )
    )

    restored_hello = live_frame_relay_v2_pb2.ProducerEnvelope.FromString(
        hello.SerializeToString()
    )
    restored_credit = live_frame_relay_v2_pb2.ProcessorEnvelope.FromString(
        credit.SerializeToString()
    )

    assert restored_hello.WhichOneof("body") == "hello"
    assert restored_hello.hello.capture_run_id == "run-1"
    assert restored_credit.WhichOneof("body") == "credit"
    assert restored_credit.credit.scope.stream_epoch == "epoch-1"


def test_canonical_descriptor_and_binary_fixture_match_generated_client():
    descriptor_payload = (ARTIFACTS / "live_frame_relay_v2.desc").read_bytes()
    descriptor_hash = (
        ARTIFACTS / "live_frame_relay_v2.sha256"
    ).read_text(encoding="ascii")
    descriptor_set = descriptor_pb2.FileDescriptorSet.FromString(
        descriptor_payload
    )
    canonical = next(
        item
        for item in descriptor_set.file
        if item.name == "live_frame_relay_v2.proto"
    )
    generated = descriptor_pb2.FileDescriptorProto.FromString(
        live_frame_relay_v2_pb2.DESCRIPTOR.serialized_pb
    )
    _clear_json_names(canonical)
    _clear_json_names(generated)
    fixture = (FIXTURES / "producer_hello.bin").read_bytes()
    fixture_hash = (FIXTURES / "producer_hello.sha256").read_text(
        encoding="ascii"
    )

    assert hashlib.sha256(descriptor_payload).hexdigest() == descriptor_hash
    assert canonical.SerializeToString(deterministic=True) == (
        generated.SerializeToString(deterministic=True)
    )
    assert hashlib.sha256(fixture).hexdigest() == fixture_hash
    assert live_frame_relay_v2_pb2.ProducerEnvelope.FromString(
        fixture
    ).hello.capture_run_id == "fixture-capture-run"
    fixture_hello = live_frame_relay_v2_pb2.ProducerEnvelope.FromString(
        fixture
    ).hello
    assert fixture_hello.tenant_id == "fixture-tenant"
    assert fixture_hello.site_id == "fixture-site"
    assert fixture_hello.authorized_subject == "fixture-user"
    assert fixture_hello.session_token_jti == "fixture-processing-job"
    assert fixture_hello.capture_configs[0].authorized_camera_id == (
        "11111111-1111-1111-1111-111111111111"
    )


def test_unknown_fields_and_negotiated_payload_limit_are_safe():
    fixture = (FIXTURES / "producer_hello.bin").read_bytes()
    unknown_field = b"\xf8\x07\x01"
    with_unknown = fixture + unknown_field
    message = live_frame_relay_v2_pb2.ProducerEnvelope.FromString(with_unknown)

    assert message.SerializeToString(deterministic=True).endswith(unknown_field)
    with pytest.raises(PayloadLimitExceeded):
        parse_with_limit(
            live_frame_relay_v2_pb2.ProducerEnvelope,
            fixture,
            maximum_payload_bytes=len(fixture) - 1,
        )
    assert serialize_with_limit(
        message,
        maximum_payload_bytes=len(with_unknown),
    ) == with_unknown


def _clear_json_names(file_descriptor) -> None:
    pending = list(file_descriptor.message_type)
    while pending:
        message = pending.pop()
        pending.extend(message.nested_type)
        for field in message.field:
            field.ClearField("json_name")

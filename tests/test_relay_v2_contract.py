from app.infrastructure.grpc.generated import live_frame_relay_v2_pb2


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

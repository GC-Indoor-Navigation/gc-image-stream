from concurrent import futures

import pytest

from app.infrastructure.grpc.generated import (
    processing_alert_pb2,
    processing_alert_pb2_grpc,
)

AlertAck = processing_alert_pb2.AlertAck
AlertEvent = processing_alert_pb2.AlertEvent
AlertSource = processing_alert_pb2.AlertSource


def make_alert_event() -> AlertEvent:
    return AlertEvent(
        event_id="alert-1",
        frame_set_id=12,
        relay_run_id=3,
        timestamp_ms=1_780_502_472_361,
        severity="warning",
        distance_m=0.62,
        joint="pelvis",
        obstacle_id="unknown",
        ttl_ms=500,
        source=AlertSource(
            processor="mmpose_triangulation",
            camera_devices=[
                "android_device_001",
                "android_device_002",
            ],
            session_id="session-1",
        ),
    )


def test_alert_event_round_trip_preserves_payload():
    alert = make_alert_event()

    restored = AlertEvent()
    restored.ParseFromString(alert.SerializeToString())

    assert restored == alert
    assert restored.HasField("relay_run_id") is True
    assert restored.source.camera_devices == [
        "android_device_001",
        "android_device_002",
    ]
    assert restored.source.HasField("session_id") is True


def test_alert_ack_round_trip_preserves_fields():
    ack = AlertAck(
        accepted=True,
        duplicate=False,
        expired=False,
        event_id="alert-1",
        message="accepted",
        expires_at_ms=1_780_502_472_861,
    )

    restored = AlertAck()
    restored.ParseFromString(ack.SerializeToString())

    assert restored == ack


def test_grpc_processing_alert_publish_round_trip():
    grpc = pytest.importorskip("grpc")

    received_alerts = []
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

    class Servicer(processing_alert_pb2_grpc.ProcessingAlertServiceServicer):
        def PublishAlert(self, request, context):
            received_alerts.append(request)
            return AlertAck(
                accepted=True,
                duplicate=False,
                expired=False,
                event_id=request.event_id,
                message="accepted",
                expires_at_ms=request.timestamp_ms + request.ttl_ms,
            )

    processing_alert_pb2_grpc.add_ProcessingAlertServiceServicer_to_server(
        Servicer(),
        server,
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    try:
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        stub = processing_alert_pb2_grpc.ProcessingAlertServiceStub(channel).PublishAlert
        alert = make_alert_event()

        ack = stub(alert, timeout=5.0)

        assert ack.accepted is True
        assert ack.event_id == "alert-1"
        assert ack.expires_at_ms == alert.timestamp_ms + alert.ttl_ms
        assert received_alerts == [alert]
    finally:
        server.stop(0)

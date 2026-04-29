from concurrent import futures

import pytest

from app.infrastructure.grpc.generated import (
    processing_relay_pb2,
    processing_relay_pb2_grpc,
)

RelayAck = processing_relay_pb2.RelayAck
RelayFrame = processing_relay_pb2.RelayFrame


def test_relay_frame_round_trip_preserves_metadata_and_bytes():
    frame = RelayFrame(
        device_id="camera1",
        timestamp_ms=1_234,
        sequence=7,
        content_type="image/jpeg",
        image_bytes=b"\xff\xd8frame\xff\xd9",
        file_path="storage/camera1/frame.jpg",
    )

    restored = RelayFrame()
    restored.ParseFromString(frame.SerializeToString())

    assert restored == frame


def test_relay_ack_round_trip_preserves_fields():
    ack = RelayAck(success=True, received_count=3, message="ok")

    restored = RelayAck()
    restored.ParseFromString(ack.SerializeToString())

    assert restored == ack


def test_grpc_relay_stream_round_trip():
    grpc = pytest.importorskip("grpc")

    received_frames = []
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))

    class Servicer(processing_relay_pb2_grpc.FrameRelayServiceServicer):
        def StreamFrames(self, request_iterator, context):
            received_count = 0
            for frame in request_iterator:
                received_count += 1
                received_frames.append(frame)
            return RelayAck(success=True, received_count=received_count, message="ok")

    processing_relay_pb2_grpc.add_FrameRelayServiceServicer_to_server(Servicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    try:
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        stub = processing_relay_pb2_grpc.FrameRelayServiceStub(channel).StreamFrames

        frames = [
            RelayFrame(
                device_id="camera1",
                timestamp_ms=1_000,
                sequence=1,
                content_type="image/jpeg",
                image_bytes=b"frame-1",
                file_path="storage/camera1/1.jpg",
            ),
            RelayFrame(
                device_id="camera1",
                timestamp_ms=1_100,
                sequence=2,
                content_type="image/jpeg",
                image_bytes=b"frame-2",
                file_path="storage/camera1/2.jpg",
            ),
        ]

        ack = stub(iter(frames), timeout=5.0)

        assert ack.success is True
        assert ack.received_count == 2
        assert received_frames == frames
    finally:
        server.stop(0)

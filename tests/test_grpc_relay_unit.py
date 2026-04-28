from concurrent import futures

import pytest

from app.contracts.processing_relay import (
    RelayAck,
    RelayFrame,
    add_frame_relay_servicer,
    build_frame_relay_stub,
    deserialize_relay_ack,
    deserialize_relay_frame,
    serialize_relay_ack,
    serialize_relay_frame,
)


def test_relay_frame_round_trip_preserves_metadata_and_bytes():
    frame = RelayFrame(
        device_id="camera1",
        timestamp_ms=1_234,
        sequence=7,
        content_type="image/jpeg",
        image_bytes=b"\xff\xd8frame\xff\xd9",
        file_path="storage/camera1/frame.jpg",
    )

    restored = deserialize_relay_frame(serialize_relay_frame(frame))

    assert restored == frame


def test_relay_ack_round_trip_preserves_fields():
    ack = RelayAck(success=True, received_count=3, message="ok")

    restored = deserialize_relay_ack(serialize_relay_ack(ack))

    assert restored == ack


def test_grpc_relay_stream_round_trip():
    grpc = pytest.importorskip("grpc")

    received_frames = []
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    add_frame_relay_servicer(server, received_frames.append)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    try:
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        stub = build_frame_relay_stub(channel)

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

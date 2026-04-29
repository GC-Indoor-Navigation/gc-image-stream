from time import sleep

from app.infrastructure.grpc.generated.processing_relay_pb2 import RelayAck, RelayFrame
from app.infrastructure.grpc.processing_relay_client import ProcessingRelayService


def test_processing_relay_service_enqueue_is_noop_when_disabled():
    service = ProcessingRelayService()

    enqueued = service.enqueue(
        RelayFrame(
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            content_type="image/jpeg",
            image_bytes=b"frame",
        )
    )

    assert enqueued is False
    assert service.status()["queue_size"] == 0


def test_processing_relay_service_enqueue_tracks_queue_when_enabled():
    service = ProcessingRelayService()
    service.configure(target="127.0.0.1:50051", enabled=True)

    enqueued = service.enqueue(
        RelayFrame(
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            content_type="image/jpeg",
            image_bytes=b"frame",
        )
    )

    assert enqueued is True
    assert service.status()["queue_size"] == 1


def test_processing_relay_service_worker_sends_queued_frames():
    captured_frames = []

    def stub_factory(target):
        assert target == "127.0.0.1:50051"

        def stub(frame_iterator, timeout=None):
            assert timeout == 1.0
            captured_frames.extend(frame_iterator)
            return RelayAck(
                success=True,
                received_count=len(captured_frames),
                message="ok",
            )

        return stub

    service = ProcessingRelayService(stub_factory=stub_factory)
    service.configure(
        target="127.0.0.1:50051",
        timeout_sec=1.0,
        enabled=True,
    )

    service.start()
    service.enqueue(
        RelayFrame(
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            content_type="image/jpeg",
            image_bytes=b"frame-1",
        )
    )
    service.enqueue(
        RelayFrame(
            device_id="camera1",
            timestamp_ms=1010,
            sequence=2,
            content_type="image/jpeg",
            image_bytes=b"frame-2",
        )
    )
    service.stop(timeout_sec=1.0)

    status = service.status()
    assert [frame.sequence for frame in captured_frames] == [1, 2]
    assert status["sent_count"] == 2
    assert status["ack_received_count"] == 2
    assert status["error_count"] == 0
    assert status["last_ack_success"] is True


def test_processing_relay_service_worker_reconnects_after_error():
    captured_frames = []
    call_count = 0

    def stub_factory(target):
        assert target == "127.0.0.1:50051"

        def stub(frame_iterator, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient relay failure")
            assert timeout is None
            captured_frames.extend(frame_iterator)
            return RelayAck(
                success=True,
                received_count=len(captured_frames),
                message="ok",
            )

        return stub

    service = ProcessingRelayService(
        stub_factory=stub_factory,
        reconnect_delay_sec=0.01,
    )
    service.configure(
        target="127.0.0.1:50051",
        enabled=True,
    )

    service.start()
    service.enqueue(
        RelayFrame(
            device_id="camera1",
            timestamp_ms=1000,
            sequence=1,
            content_type="image/jpeg",
            image_bytes=b"frame-1",
        )
    )
    service.enqueue(
        RelayFrame(
            device_id="camera1",
            timestamp_ms=1010,
            sequence=2,
            content_type="image/jpeg",
            image_bytes=b"frame-2",
        )
    )
    for _ in range(100):
        if call_count >= 2:
            break
        sleep(0.01)
    service.stop(timeout_sec=1.0)

    status = service.status()
    assert call_count >= 2
    assert [frame.sequence for frame in captured_frames] == [1, 2]
    assert status["sent_count"] == 2
    assert status["ack_received_count"] == 2
    assert status["error_count"] == 1
    assert status["last_error"] is None
    assert status["last_ack_success"] is True

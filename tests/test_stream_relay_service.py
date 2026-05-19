from time import sleep

from app.infrastructure.grpc.generated.processing_relay_pb2 import (
    RelayAck,
    RelayFrame,
    RelayFrameSet,
)
from app.infrastructure.grpc.processing_relay_client import (
    ProcessingFrameSetRelayService,
    ProcessingRelayService,
    build_relay_frame_set,
)
from app.services.sync import StoredSyncFrame, SynchronizedFrameSet


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


def make_synchronized_frame_set():
    return SynchronizedFrameSet(
        frame_set_id=7,
        anchor_timestamp_ms=1010,
        max_delta_ms=10,
        frames={
            "camera1": StoredSyncFrame(
                frame_id=101,
                device_id="camera1",
                timestamp_ms=1000,
                sequence=1,
                content_type="image/jpeg",
                image_bytes=b"frame-1",
                image_size=len(b"frame-1"),
                file_path="storage/camera1/1.jpg",
            ),
            "camera2": StoredSyncFrame(
                frame_id=102,
                device_id="camera2",
                timestamp_ms=1010,
                sequence=1,
                content_type="image/jpeg",
                image_bytes=b"frame-2",
                image_size=len(b"frame-2"),
                file_path="storage/camera2/1.jpg",
            ),
        },
    )


def test_build_relay_frame_set_preserves_sync_metadata():
    relay_frame_set = build_relay_frame_set(make_synchronized_frame_set())

    assert relay_frame_set.frame_set_id == 7
    assert relay_frame_set.anchor_timestamp_ms == 1010
    assert relay_frame_set.max_delta_ms == 10
    assert [frame.device_id for frame in relay_frame_set.frames] == [
        "camera1",
        "camera2",
    ]
    assert relay_frame_set.frames[0].frame_id == 101
    assert relay_frame_set.frames[0].image_bytes == b"frame-1"


def test_processing_frame_set_relay_service_enqueue_is_noop_when_disabled():
    service = ProcessingFrameSetRelayService()

    enqueued = service.enqueue(RelayFrameSet(frame_set_id=1))

    assert enqueued is False
    assert service.status()["queue_size"] == 0


def test_processing_frame_set_relay_service_enqueue_tracks_queue_when_enabled():
    service = ProcessingFrameSetRelayService()
    service.configure(target="127.0.0.1:50051", enabled=True)

    enqueued = service.enqueue_synchronized_frame_set(make_synchronized_frame_set())

    assert enqueued is True
    status = service.status()
    assert status["queue_size"] == 1
    assert status["enqueued_count"] == 1
    assert status["last_frame_set_id"] == 7


def test_processing_frame_set_relay_service_worker_sends_queued_frame_sets():
    captured_frame_sets = []

    def stub_factory(target):
        assert target == "127.0.0.1:50051"

        def stub(frame_set_iterator, timeout=None):
            assert timeout == 1.0
            captured_frame_sets.extend(frame_set_iterator)
            return RelayAck(
                success=True,
                received_count=len(captured_frame_sets),
                message="ok",
            )

        return stub

    service = ProcessingFrameSetRelayService(stub_factory=stub_factory)
    service.configure(
        target="127.0.0.1:50051",
        timeout_sec=1.0,
        enabled=True,
    )

    service.start()
    service.enqueue_synchronized_frame_set(make_synchronized_frame_set())
    service.stop(timeout_sec=1.0)

    status = service.status()
    assert [frame_set.frame_set_id for frame_set in captured_frame_sets] == [7]
    assert status["sent_count"] == 1
    assert status["ack_received_count"] == 1
    assert status["error_count"] == 0
    assert status["last_ack_success"] is True

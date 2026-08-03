from collections.abc import Callable, Iterable
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Event, Lock, Thread

from app.infrastructure.grpc.generated import (
    processing_relay_pb2,
    processing_relay_pb2_grpc,
)
from app.services.stream.stream_experiment import get_stream_experiment_recorder
from app.services.sync import SynchronizedFrameSet

RelayFrame = processing_relay_pb2.RelayFrame
RelayFrameSet = processing_relay_pb2.RelayFrameSet
RelayFrameSetFrame = processing_relay_pb2.RelayFrameSetFrame
RelayAck = processing_relay_pb2.RelayAck


RelayStub = Callable[[Iterable[RelayFrame]], RelayAck]
RelayStubFactory = Callable[[str], Callable[..., RelayAck]]
FrameSetRelayStub = Callable[[Iterable[RelayFrameSet]], RelayAck]
FrameSetRelayStubFactory = Callable[[str], Callable[..., RelayAck]]


@dataclass(frozen=True)
class StreamRelayRuntime:
    stop_event: Event
    worker: Thread


class ProcessingRelayService:
    def __init__(
        self,
        stub_factory: RelayStubFactory | None = None,
        reconnect_delay_sec: float = 1.0,
    ):
        self.queue: Queue[RelayFrame] = Queue()
        self.runtime: StreamRelayRuntime | None = None
        self.target: str | None = None
        self.timeout_sec: float | None = None
        self.enabled = False
        self.enqueued_count = 0
        self.sent_count = 0
        self.ack_received_count = 0
        self.error_count = 0
        self.last_error: str | None = None
        self.last_ack_success: bool | None = None
        self.last_ack_received_count: int | None = None
        self._lock = Lock()
        self._stub_factory = stub_factory
        self._reconnect_delay_sec = reconnect_delay_sec

    def configure(
        self,
        target: str,
        timeout_sec: float | None = None,
        enabled: bool = True,
    ):
        self.target = target
        self.timeout_sec = timeout_sec
        self.enabled = enabled

    def enqueue(self, frame: RelayFrame):
        if not self.enabled:
            return False
        self.queue.put(frame)
        with self._lock:
            self.enqueued_count += 1
        experiment_recorder = get_stream_experiment_recorder()
        if experiment_recorder is not None:
            experiment_recorder.record_relay_enqueued(
                device_id=frame.device_id,
                timestamp_ms=frame.timestamp_ms,
                sequence=frame.sequence,
                image_bytes_size=len(frame.image_bytes),
                queue_size=self.queue.qsize(),
            )
        return True

    def start(self):
        if not self.enabled:
            return None
        if not self.target:
            raise RuntimeError("stream relay target is required")
        if self.runtime is not None and self.runtime.worker.is_alive():
            return self.runtime

        stop_event = Event()
        worker = Thread(
            target=self._run,
            args=(stop_event,),
            daemon=True,
        )
        self.last_error = None
        worker.start()
        self.runtime = StreamRelayRuntime(stop_event=stop_event, worker=worker)
        return self.runtime

    def stop(self, timeout_sec: float = 2.0):
        if self.runtime is None:
            return
        self.runtime.stop_event.set()
        self.runtime.worker.join(timeout=timeout_sec)
        self.runtime = None

    def status(self):
        with self._lock:
            counters = {
                "enqueued_count": self.enqueued_count,
                "sent_count": self.sent_count,
                "ack_received_count": self.ack_received_count,
                "error_count": self.error_count,
            }

        return {
            "enabled": self.enabled,
            "target": self.target,
            "queue_size": self.queue.qsize(),
            "running": self.runtime is not None and self.runtime.worker.is_alive(),
            "last_error": self.last_error,
            "last_ack_success": self.last_ack_success,
            "last_ack_received_count": self.last_ack_received_count,
            **counters,
        }

    def clear(self):
        self.stop()
        while True:
            try:
                self.queue.get_nowait()
            except Empty:
                break
            self.queue.task_done()
        self.enabled = False
        self.target = None
        self.timeout_sec = None
        self.enqueued_count = 0
        self.sent_count = 0
        self.ack_received_count = 0
        self.error_count = 0
        self.last_error = None
        self.last_ack_success = None
        self.last_ack_received_count = None

    def _iter_frames(self, stop_event: Event):
        while not stop_event.is_set() or not self.queue.empty():
            try:
                frame = self.queue.get(timeout=0.5)
            except Empty:
                continue

            try:
                with self._lock:
                    self.sent_count += 1
                yield frame
            finally:
                self.queue.task_done()

    def _run(self, stop_event: Event):
        while not stop_event.is_set() or not self.queue.empty():
            channel = None
            try:
                if self._stub_factory is not None:
                    stub = self._stub_factory(self.target or "")
                else:
                    try:
                        import grpc
                    except ImportError as exc:
                        raise RuntimeError("grpcio is required for stream relay") from exc

                    channel = grpc.insecure_channel(self.target)
                    stub = processing_relay_pb2_grpc.FrameRelayServiceStub(channel).StreamFrames

                ack = stub(
                    self._iter_frames(stop_event),
                    timeout=self.timeout_sec,
                )
                self.last_ack_success = ack.success
                self.last_ack_received_count = ack.received_count
                with self._lock:
                    self.ack_received_count += ack.received_count
                experiment_recorder = get_stream_experiment_recorder()
                if not ack.success:
                    self.last_error = ack.message
                    with self._lock:
                        self.error_count += 1
                else:
                    self.last_error = None
                if experiment_recorder is not None:
                    experiment_recorder.record_relay_closed(
                        success=ack.success,
                        received_count=ack.received_count,
                        message=ack.message,
                    )
                return
            except Exception as exc:
                self.last_error = str(exc)
                self.last_ack_success = False
                with self._lock:
                    self.error_count += 1
                experiment_recorder = get_stream_experiment_recorder()
                if experiment_recorder is not None:
                    experiment_recorder.record_relay_error(str(exc))
                if stop_event.wait(timeout=self._reconnect_delay_sec):
                    return
            finally:
                if channel is not None:
                    channel.close()


processing_relay_service = ProcessingRelayService()


def build_relay_frame_set(frame_set: SynchronizedFrameSet) -> RelayFrameSet:
    return RelayFrameSet(
        frame_set_id=frame_set.frame_set_id,
        anchor_timestamp_ms=frame_set.anchor_timestamp_ms,
        max_delta_ms=frame_set.max_delta_ms,
        frames=[
            _build_relay_frame_set_frame(frame)
            for device_id, frame in sorted(frame_set.frames.items())
        ],
    )


def _build_relay_frame_set_frame(frame) -> RelayFrameSetFrame:
    fields = {
        "device_id": frame.device_id,
        "timestamp_ms": frame.timestamp_ms,
        "sequence": frame.sequence or 0,
        "content_type": frame.content_type,
        "image_bytes": frame.image_bytes,
    }
    if frame.file_path is not None:
        fields["file_path"] = frame.file_path
    if frame.frame_id is not None:
        fields["frame_id"] = frame.frame_id
    return RelayFrameSetFrame(**fields)


class ProcessingFrameSetRelayService:
    def __init__(
        self,
        stub_factory: FrameSetRelayStubFactory | None = None,
        reconnect_delay_sec: float = 1.0,
    ):
        self.queue: Queue[RelayFrameSet] = Queue()
        self.runtime: StreamRelayRuntime | None = None
        self.target: str | None = None
        self.timeout_sec: float | None = None
        self.enabled = False
        self.enqueued_count = 0
        self.sent_count = 0
        self.ack_received_count = 0
        self.error_count = 0
        self.last_error: str | None = None
        self.last_ack_success: bool | None = None
        self.last_ack_received_count: int | None = None
        self.last_frame_set_id: int | None = None
        self._lock = Lock()
        self._stub_factory = stub_factory
        self._reconnect_delay_sec = reconnect_delay_sec

    def configure(
        self,
        target: str,
        timeout_sec: float | None = None,
        enabled: bool = True,
    ):
        self.target = target
        self.timeout_sec = timeout_sec
        self.enabled = enabled

    def enqueue(self, frame_set: RelayFrameSet):
        if not self.enabled:
            return False
        self.queue.put(frame_set)
        with self._lock:
            self.enqueued_count += 1
            self.last_frame_set_id = frame_set.frame_set_id
        return True

    def enqueue_synchronized_frame_set(self, frame_set: SynchronizedFrameSet):
        return self.enqueue(build_relay_frame_set(frame_set))

    def start(self):
        if not self.enabled:
            return None
        if not self.target:
            raise RuntimeError("stream frame-set relay target is required")
        if self.runtime is not None and self.runtime.worker.is_alive():
            return self.runtime

        stop_event = Event()
        worker = Thread(
            target=self._run,
            args=(stop_event,),
            daemon=True,
        )
        self.last_error = None
        worker.start()
        self.runtime = StreamRelayRuntime(stop_event=stop_event, worker=worker)
        return self.runtime

    def stop(self, timeout_sec: float = 2.0):
        if self.runtime is None:
            return
        self.runtime.stop_event.set()
        self.runtime.worker.join(timeout=timeout_sec)
        self.runtime = None

    def status(self):
        with self._lock:
            counters = {
                "enqueued_count": self.enqueued_count,
                "sent_count": self.sent_count,
                "ack_received_count": self.ack_received_count,
                "error_count": self.error_count,
                "last_frame_set_id": self.last_frame_set_id,
            }

        return {
            "enabled": self.enabled,
            "target": self.target,
            "queue_size": self.queue.qsize(),
            "running": self.runtime is not None and self.runtime.worker.is_alive(),
            "last_error": self.last_error,
            "last_ack_success": self.last_ack_success,
            "last_ack_received_count": self.last_ack_received_count,
            **counters,
        }

    def clear(self):
        self.stop()
        while True:
            try:
                self.queue.get_nowait()
            except Empty:
                break
            self.queue.task_done()
        self.enabled = False
        self.target = None
        self.timeout_sec = None
        self.enqueued_count = 0
        self.sent_count = 0
        self.ack_received_count = 0
        self.error_count = 0
        self.last_error = None
        self.last_ack_success = None
        self.last_ack_received_count = None
        self.last_frame_set_id = None

    def _iter_frame_sets(self, stop_event: Event):
        while not stop_event.is_set() or not self.queue.empty():
            try:
                frame_set = self.queue.get(timeout=0.5)
            except Empty:
                continue

            try:
                with self._lock:
                    self.sent_count += 1
                yield frame_set
            finally:
                self.queue.task_done()

    def _run(self, stop_event: Event):
        while not stop_event.is_set() or not self.queue.empty():
            channel = None
            try:
                if self._stub_factory is not None:
                    stub = self._stub_factory(self.target or "")
                else:
                    try:
                        import grpc
                    except ImportError as exc:
                        raise RuntimeError("grpcio is required for stream frame-set relay") from exc

                    channel = grpc.insecure_channel(self.target)
                    stub = processing_relay_pb2_grpc.FrameRelayServiceStub(channel).StreamFrameSets

                ack = stub(
                    self._iter_frame_sets(stop_event),
                    timeout=self.timeout_sec,
                )
                self.last_ack_success = ack.success
                self.last_ack_received_count = ack.received_count
                with self._lock:
                    self.ack_received_count += ack.received_count
                if not ack.success:
                    self.last_error = ack.message
                    with self._lock:
                        self.error_count += 1
                else:
                    self.last_error = None
                return
            except Exception as exc:
                self.last_error = str(exc)
                self.last_ack_success = False
                with self._lock:
                    self.error_count += 1
                if stop_event.wait(timeout=self._reconnect_delay_sec):
                    return
            finally:
                if channel is not None:
                    channel.close()


processing_frame_set_relay_service = ProcessingFrameSetRelayService()

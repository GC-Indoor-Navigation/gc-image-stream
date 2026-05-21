import json
import time
from concurrent import futures
from pathlib import Path
from threading import Lock
from typing import Callable, Iterable

from google.protobuf.json_format import MessageToDict

from app.db import SessionLocal
from app.infrastructure.grpc.generated import (
    frame_ingest_pb2,
    frame_ingest_pb2_grpc,
)
from app.infrastructure.grpc.processing_relay_client import processing_relay_service
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.stream_experiment import get_stream_experiment_recorder
from app.services.stream.state import stream_state

FrameMetadata = frame_ingest_pb2.FrameMetadata
FramePacket = frame_ingest_pb2.FramePacket
StreamFramesResponse = frame_ingest_pb2.StreamFramesResponse


def serialize_frame_packet(frame: FramePacket) -> bytes:
    return frame.SerializeToString()


def deserialize_frame_packet(payload: bytes) -> FramePacket:
    message = FramePacket()
    message.ParseFromString(payload)
    return message


def build_frame_ingest_stub(channel):
    return frame_ingest_pb2_grpc.FrameIngestServiceStub(channel).StreamFrames


class _CollectorFrameIngestServicer(frame_ingest_pb2_grpc.FrameIngestServiceServicer):
    def __init__(
        self,
        handler: Callable[[Iterable[FramePacket], object], StreamFramesResponse],
    ):
        self._handler = handler

    def StreamFrames(self, request_iterator, context):
        return self._handler(request_iterator, context)


def resolve_content_type(image_format: str) -> str:
    normalized = image_format.strip().lower()
    if "/" in normalized:
        return normalized
    if normalized in {"jpg", "jpeg"}:
        return "image/jpeg"
    if normalized == "png":
        return "image/png"
    if normalized == "webp":
        return "image/webp"
    return "application/octet-stream"


def resolve_file_extension(image_format: str) -> str:
    normalized = image_format.strip().lower()
    if normalized in {"jpg", "jpeg", "image/jpeg"}:
        return "jpg"
    if normalized in {"png", "image/png"}:
        return "png"
    if normalized in {"webp", "image/webp"}:
        return "webp"
    return "bin"


def write_ingest_metadata_sidecar(frame_path: str, metadata_payload: dict):
    sidecar_path = f"{frame_path}.metadata.json"
    Path(sidecar_path).write_text(
        json.dumps(metadata_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


class GrpcIngestService:
    def __init__(
        self,
        db_factory: Callable[[], object] = SessionLocal,
        ingest_func: Callable[..., dict] = ingest_frame,
        state=stream_state,
        relay_service=processing_relay_service,
    ):
        self.bind = ""
        self.enabled = False
        self.runtime_server = None
        self.runtime_bind = ""
        self.expected_device_count: int | None = None
        self.expected_device_ids: set[str] = set()
        self.gate_open = True
        self.observed_device_ids: set[str] = set()
        self.active_device_ids: set[str] = set()
        self.active_device_stream_counts: dict[str, int] = {}
        self.latest_device_timestamp_ms: dict[str, int] = {}
        self.gate_start_timestamp_ms: int | None = None
        self.first_accepted_timestamp_ms: int | None = None
        self.pre_gate_dropped_count = 0
        self.stale_after_gate_dropped_count = 0
        self.collection_started = False
        self.collection_stopped = False
        self.collection_stop_reason: str | None = None
        self.gate_lock = Lock()
        self.db_factory = db_factory
        self.ingest_func = ingest_func
        self.state = state
        self.relay_service = relay_service

    def configure(
        self,
        bind: str,
        enabled: bool = True,
        expected_device_count: int | None = None,
        expected_device_ids: Iterable[str] | None = None,
    ):
        normalized_expected_ids = {
            device_id.strip()
            for device_id in (expected_device_ids or [])
            if device_id and device_id.strip()
        }
        self.bind = bind
        self.enabled = enabled
        self.expected_device_ids = normalized_expected_ids
        if self.expected_device_ids:
            self.expected_device_count = (
                len(self.expected_device_ids) if len(self.expected_device_ids) > 1 else None
            )
        else:
            self.expected_device_count = (
                expected_device_count if expected_device_count and expected_device_count > 1 else None
            )
        self.gate_open = not self._gate_enabled()
        self.observed_device_ids = set()
        self.active_device_ids = set()
        self.active_device_stream_counts = {}
        self.latest_device_timestamp_ms = {}
        self.gate_start_timestamp_ms = None
        self.first_accepted_timestamp_ms = None
        self.pre_gate_dropped_count = 0
        self.stale_after_gate_dropped_count = 0
        self.collection_started = not self._gate_enabled()
        self.collection_stopped = False
        self.collection_stop_reason = None

    def start(self):
        if not self.enabled:
            return None
        if self.runtime_server is not None:
            return self.runtime_server
        if not self.bind:
            raise RuntimeError("grpc ingest bind is required")

        try:
            import grpc
        except ImportError as exc:
            raise RuntimeError("grpcio is required for grpc ingest") from exc

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        frame_ingest_pb2_grpc.add_FrameIngestServiceServicer_to_server(
            _CollectorFrameIngestServicer(self._stream_frames),
            server,
        )
        port = server.add_insecure_port(self.bind)
        if port <= 0:
            raise RuntimeError(f"failed to bind grpc ingest server: {self.bind}")
        server.start()
        self.runtime_server = server
        self.runtime_bind = self.bind.replace(":0", f":{port}")
        return server

    def stop(self, grace_sec: float = 2.0):
        if self.runtime_server is None:
            return
        self.runtime_server.stop(grace_sec).wait()
        self.runtime_server = None
        self.runtime_bind = ""

    def status(self):
        expected_device_ids = sorted(self.expected_device_ids)
        missing_device_ids = sorted(self.expected_device_ids - self.active_device_ids)
        unexpected_device_ids = sorted(self.observed_device_ids - self.expected_device_ids)
        return {
            "enabled": self.enabled,
            "bind": self.runtime_bind or self.bind,
            "running": self.runtime_server is not None,
            "gate_enabled": self._gate_enabled(),
            "gate_open": self.gate_open,
            "expected_device_count": self.expected_device_count,
            "expected_device_ids": expected_device_ids,
            "observed_device_ids": sorted(self.observed_device_ids),
            "active_device_ids": sorted(self.active_device_ids),
            "latest_device_timestamp_ms": dict(sorted(self.latest_device_timestamp_ms.items())),
            "missing_device_ids": missing_device_ids,
            "unexpected_device_ids": unexpected_device_ids if self.expected_device_ids else [],
            "gate_start_timestamp_ms": self.gate_start_timestamp_ms,
            "first_accepted_timestamp_ms": self.first_accepted_timestamp_ms,
            "pre_gate_dropped_count": self.pre_gate_dropped_count,
            "stale_after_gate_dropped_count": self.stale_after_gate_dropped_count,
            "collection_started": self.collection_started,
            "collection_stopped": self.collection_stopped,
            "collection_stop_reason": self.collection_stop_reason,
        }

    def _gate_enabled(self) -> bool:
        return self.expected_device_count is not None

    def _allow_ingest(self, device_id: str, timestamp_ms: int | None = None) -> bool:
        with self.gate_lock:
            if self.collection_stopped:
                return False

            if self.expected_device_ids and device_id not in self.expected_device_ids:
                return False

            if self.gate_open:
                if (
                    self._gate_enabled()
                    and self.gate_start_timestamp_ms is not None
                    and timestamp_ms is not None
                    and timestamp_ms < self.gate_start_timestamp_ms
                ):
                    self.stale_after_gate_dropped_count += 1
                    return False
                if self.first_accepted_timestamp_ms is None and timestamp_ms is not None:
                    self.first_accepted_timestamp_ms = timestamp_ms
                return True

            if self.expected_device_ids:
                expected_devices_seen = self.expected_device_ids.issubset(
                    self.active_device_ids
                )
                if expected_devices_seen:
                    self._open_gate_locked()
                self.pre_gate_dropped_count += 1
                return False

            if len(self.active_device_ids) >= (self.expected_device_count or 0):
                self._open_gate_locked()
            self.pre_gate_dropped_count += 1
            return False

    def _mark_device_active(self, device_id: str):
        with self.gate_lock:
            self.observed_device_ids.add(device_id)
            self.active_device_stream_counts[device_id] = (
                self.active_device_stream_counts.get(device_id, 0) + 1
            )
            self.active_device_ids.add(device_id)

    def _record_device_timestamp(self, device_id: str, timestamp_ms: int):
        with self.gate_lock:
            previous_timestamp_ms = self.latest_device_timestamp_ms.get(device_id)
            if previous_timestamp_ms is None or timestamp_ms > previous_timestamp_ms:
                self.latest_device_timestamp_ms[device_id] = timestamp_ms

    def _open_gate_locked(self):
        self.gate_open = True
        self.collection_started = True
        if self.gate_start_timestamp_ms is not None:
            return

        if self.expected_device_ids:
            timestamps = [
                self.latest_device_timestamp_ms[device_id]
                for device_id in self.expected_device_ids
                if device_id in self.latest_device_timestamp_ms
            ]
        else:
            timestamps = [
                self.latest_device_timestamp_ms[device_id]
                for device_id in self.active_device_ids
                if device_id in self.latest_device_timestamp_ms
            ]
        if timestamps:
            self.gate_start_timestamp_ms = max(timestamps)

    def _mark_stream_closed(self, device_ids: set[str]):
        if not device_ids:
            return

        with self.gate_lock:
            for device_id in device_ids:
                stream_count = self.active_device_stream_counts.get(device_id, 0) - 1
                if stream_count > 0:
                    self.active_device_stream_counts[device_id] = stream_count
                else:
                    self.active_device_stream_counts.pop(device_id, None)
                    self.active_device_ids.discard(device_id)
            if (
                not self.collection_started
                or self.collection_stopped
                or not self._gate_enabled()
            ):
                return

            if self.expected_device_ids:
                missing_device_ids = sorted(
                    self.expected_device_ids - self.active_device_ids
                )
                if not missing_device_ids:
                    return
                self.gate_open = False
                self.collection_stopped = True
                self.collection_stop_reason = (
                    "expected device disconnected: "
                    + ",".join(missing_device_ids)
                )
                return

            if len(self.active_device_ids) < (self.expected_device_count or 0):
                self.gate_open = False
                self.collection_stopped = True
                self.collection_stop_reason = "expected device count disconnected"

    def _stream_frames(
        self,
        request_iterator: Iterable[FramePacket],
        context,
    ) -> StreamFramesResponse:
        received_count = 0
        stream_device_ids: set[str] = set()

        try:
            for request in request_iterator:
                metadata = request.metadata
                internal_device_id = metadata.device_id or metadata.camera_id
                if not internal_device_id:
                    return StreamFramesResponse(
                        received_frames=received_count,
                        message="device_id or camera_id is required",
                    )
                if metadata.device_timestamp_ms <= 0:
                    return StreamFramesResponse(
                        received_frames=received_count,
                        message="device_timestamp_ms must be greater than 0",
                    )

                if internal_device_id not in stream_device_ids:
                    stream_device_ids.add(internal_device_id)
                    self._mark_device_active(internal_device_id)
                self._record_device_timestamp(
                    internal_device_id,
                    metadata.device_timestamp_ms,
                )

                content_type = resolve_content_type(metadata.format or "jpeg")
                camera_id = metadata.camera_id or "unknown"
                filename = f"{internal_device_id}_{camera_id}_{metadata.frame_sequence}.jpg"
                session_id = metadata.session_id if metadata.HasField("session_id") else None
                received_at = time.monotonic()
                experiment_recorder = get_stream_experiment_recorder()
                if experiment_recorder is not None:
                    experiment_recorder.observe_device(internal_device_id)
                if not self._allow_ingest(
                    internal_device_id,
                    timestamp_ms=metadata.device_timestamp_ms,
                ):
                    continue

                db = self.db_factory()
                try:
                    result = self.ingest_func(
                        db,
                        device_id=internal_device_id,
                        timestamp_ms=metadata.device_timestamp_ms,
                        image_bytes=request.jpeg,
                        sequence=metadata.frame_sequence or None,
                        content_type=content_type,
                        filename=filename,
                        session_id=session_id,
                        state=self.state,
                        relay_service=self.relay_service,
                    )
                    ingested_at = time.monotonic()
                    if experiment_recorder is not None:
                        experiment_recorder.record_capture(
                            device_id=internal_device_id,
                            timestamp_ms=metadata.device_timestamp_ms,
                            sequence=metadata.frame_sequence or 0,
                            capture_label="grpc_ingest",
                            capture_elapsed=0.0,
                            save_elapsed=ingested_at - received_at,
                            cycle_elapsed=ingested_at - received_at,
                            queue_size=0,
                            scheduled_at=received_at,
                            captured_at=received_at,
                            image_bytes_size=len(request.jpeg),
                        )
                    write_ingest_metadata_sidecar(
                        result["frame"].file_path,
                        {
                            "service": "gc.collector.v1.FrameIngestService",
                            "metadata": MessageToDict(
                                metadata,
                                preserving_proto_field_name=True,
                                always_print_fields_with_no_presence=True,
                            ),
                        },
                    )
                finally:
                    db.close()

                received_count += 1
        finally:
            self._mark_stream_closed(stream_device_ids)

        return StreamFramesResponse(
            received_frames=received_count,
            message="collector ingest stream completed",
        )


grpc_ingest_service = GrpcIngestService()

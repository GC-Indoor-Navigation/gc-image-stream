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
        self.gate_open = True
        self.observed_device_ids: set[str] = set()
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
    ):
        self.bind = bind
        self.enabled = enabled
        self.expected_device_count = (
            expected_device_count if expected_device_count and expected_device_count > 1 else None
        )
        self.gate_open = self.expected_device_count is None
        self.observed_device_ids = set()

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
        return {
            "enabled": self.enabled,
            "bind": self.runtime_bind or self.bind,
            "running": self.runtime_server is not None,
            "gate_enabled": self.expected_device_count is not None,
            "gate_open": self.gate_open,
            "expected_device_count": self.expected_device_count,
            "observed_device_ids": sorted(self.observed_device_ids),
        }

    def _allow_ingest(self, device_id: str) -> bool:
        with self.gate_lock:
            if self.gate_open:
                self.observed_device_ids.add(device_id)
                return True

            self.observed_device_ids.add(device_id)
            if len(self.observed_device_ids) >= (self.expected_device_count or 0):
                self.gate_open = True
            return False

    def _stream_frames(
        self,
        request_iterator: Iterable[FramePacket],
        context,
    ) -> StreamFramesResponse:
        received_count = 0

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

            content_type = resolve_content_type(metadata.format or "jpeg")
            camera_id = metadata.camera_id or "unknown"
            filename = f"{internal_device_id}_{camera_id}_{metadata.frame_sequence}.jpg"
            received_at = time.monotonic()
            experiment_recorder = get_stream_experiment_recorder()
            if experiment_recorder is not None:
                experiment_recorder.observe_device(internal_device_id)
            if not self._allow_ingest(internal_device_id):
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

        return StreamFramesResponse(
            received_frames=received_count,
            message="collector ingest stream completed",
        )


grpc_ingest_service = GrpcIngestService()

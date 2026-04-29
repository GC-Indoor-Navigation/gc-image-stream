import json
import time
from concurrent import futures
from pathlib import Path
from typing import Callable, Iterable

from google.protobuf.json_format import MessageToDict

from app.db import SessionLocal
from app.infrastructure.grpc.generated import (
    frame_ingest_pb2,
    frame_ingest_pb2_grpc,
    stream_ingest_pb2,
    stream_ingest_pb2_grpc,
)
from app.infrastructure.grpc.processing_relay_client import processing_relay_service
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.state import stream_state

IngestMetadata = stream_ingest_pb2.FrameMetadata
IngestFrame = stream_ingest_pb2.IngestFrame
IngestAck = stream_ingest_pb2.IngestAck
CollectorFrameMetadata = frame_ingest_pb2.FrameMetadata
CollectorFramePacket = frame_ingest_pb2.FramePacket
CollectorStreamFramesResponse = frame_ingest_pb2.StreamFramesResponse


def serialize_ingest_frame(frame: IngestFrame) -> bytes:
    return frame.SerializeToString()


def deserialize_ingest_frame(payload: bytes) -> IngestFrame:
    message = IngestFrame()
    message.ParseFromString(payload)
    return message


def serialize_ingest_ack(ack: IngestAck) -> bytes:
    return ack.SerializeToString()


def deserialize_ingest_ack(payload: bytes) -> IngestAck:
    message = IngestAck()
    message.ParseFromString(payload)
    return message


def build_frame_ingest_stub(channel):
    return stream_ingest_pb2_grpc.FrameIngestServiceStub(channel).StreamFrames


class _FrameIngestServicer(stream_ingest_pb2_grpc.FrameIngestServiceServicer):
    def __init__(self, handler: Callable[[Iterable[IngestFrame], object], IngestAck]):
        self._handler = handler

    def StreamFrames(self, request_iterator, context):
        return self._handler(request_iterator, context)


class _CollectorFrameIngestServicer(frame_ingest_pb2_grpc.FrameIngestServiceServicer):
    def __init__(
        self,
        handler: Callable[[Iterable[CollectorFramePacket], object], CollectorStreamFramesResponse],
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


def build_collector_frame_ingest_stub(channel):
    return frame_ingest_pb2_grpc.FrameIngestServiceStub(channel).StreamFrames


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
        self.db_factory = db_factory
        self.ingest_func = ingest_func
        self.state = state
        self.relay_service = relay_service

    def configure(self, bind: str, enabled: bool = True):
        self.bind = bind
        self.enabled = enabled

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
        stream_ingest_pb2_grpc.add_FrameIngestServiceServicer_to_server(
            _FrameIngestServicer(self._stream_frames),
            server,
        )
        frame_ingest_pb2_grpc.add_FrameIngestServiceServicer_to_server(
            _CollectorFrameIngestServicer(self._stream_collector_frames),
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
        }

    def _stream_frames(self, request_iterator: Iterable[IngestFrame], context) -> IngestAck:
        received_count = 0
        warnings: list[str] = []

        for request in request_iterator:
            metadata = request.metadata
            internal_device_id = metadata.camera_id or metadata.device_id
            if not internal_device_id:
                return IngestAck(
                    success=False,
                    received_count=received_count,
                    message="camera_id or device_id is required",
                    server_ack_timestamp_ms=current_time_ms(),
                    warnings=warnings,
                )
            if metadata.device_timestamp_ms <= 0:
                return IngestAck(
                    success=False,
                    received_count=received_count,
                    message="device_timestamp_ms must be greater than 0",
                    server_ack_timestamp_ms=current_time_ms(),
                    warnings=warnings,
                )

            if request.content_length and request.content_length != len(request.image_bytes):
                warnings.append(
                    f"content_length mismatch for sequence {metadata.frame_sequence}"
                )

            content_type = resolve_content_type(metadata.format)
            extension = resolve_file_extension(metadata.format)
            filename = f"{internal_device_id}_{metadata.device_timestamp_ms}.{extension}"

            db = self.db_factory()
            try:
                result = self.ingest_func(
                    db,
                    device_id=internal_device_id,
                    timestamp_ms=metadata.device_timestamp_ms,
                    image_bytes=request.image_bytes,
                    sequence=metadata.frame_sequence or None,
                    content_type=content_type,
                    filename=filename,
                    state=self.state,
                    relay_service=self.relay_service,
                )
                write_ingest_metadata_sidecar(
                    result["frame"].file_path,
                    {
                        "service": "gc_image_stream.ingest.v1.FrameIngestService",
                        "metadata": MessageToDict(
                            metadata,
                            preserving_proto_field_name=True,
                            always_print_fields_with_no_presence=True,
                        ),
                        "content_length": int(request.content_length),
                        "app_sent_at_ms": int(request.app_sent_at_ms),
                    },
                )
            finally:
                db.close()

            received_count += 1

        return IngestAck(
            success=True,
            received_count=received_count,
            message="grpc ingest stream completed",
            server_ack_timestamp_ms=current_time_ms(),
            warnings=warnings,
        )

    def _stream_collector_frames(
        self,
        request_iterator: Iterable[CollectorFramePacket],
        context,
    ) -> CollectorStreamFramesResponse:
        received_count = 0

        for request in request_iterator:
            metadata = request.metadata
            internal_device_id = metadata.device_id or metadata.camera_id
            if not internal_device_id:
                return CollectorStreamFramesResponse(
                    received_frames=received_count,
                    message="device_id or camera_id is required",
                )
            if metadata.device_timestamp_ms <= 0:
                return CollectorStreamFramesResponse(
                    received_frames=received_count,
                    message="device_timestamp_ms must be greater than 0",
                )

            content_type = resolve_content_type(metadata.format or "jpeg")
            camera_id = metadata.camera_id or "unknown"
            filename = f"{internal_device_id}_{camera_id}_{metadata.frame_sequence}.jpg"

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

        return CollectorStreamFramesResponse(
            received_frames=received_count,
            message="collector ingest stream completed",
        )


def current_time_ms() -> int:
    return int(time.time() * 1000)


grpc_ingest_service = GrpcIngestService()

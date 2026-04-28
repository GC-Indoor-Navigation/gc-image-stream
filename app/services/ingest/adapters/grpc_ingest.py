import json
import time
from concurrent import futures
from dataclasses import asdict, dataclass, field
from typing import Callable, Iterable

from app.db import SessionLocal
from app.services.ingest.ingest_pipeline import ingest_frame
from app.services.stream.processing_relay_client import processing_relay_service
from app.services.stream.state import stream_state


SERVICE_NAME = "gc_image_stream.ingest.v1.FrameIngestService"
METHOD_NAME = "StreamFrames"
METHOD_PATH = f"/{SERVICE_NAME}/{METHOD_NAME}"


@dataclass(frozen=True)
class IngestMetadata:
    session_id: str = ""
    camera_id: str = ""
    device_id: str = ""
    frame_sequence: int = 0
    device_timestamp_ms: int = 0
    device_monotonic_ns: int = 0
    width: int = 0
    height: int = 0
    format: str = "jpeg"
    orientation_deg: int = 0
    fps_target: int = 0
    focus_mode: str = ""
    exposure_locked: bool = False
    white_balance_locked: bool = False
    iso: int | None = None
    exposure_time_us: int | None = None
    focal_length_mm: float | None = None
    lens_facing: str | None = None
    sensor_timestamp_ns: int | None = None
    battery_level: float | None = None
    network_status: str | None = None
    app_version: str | None = None


@dataclass(frozen=True)
class IngestFrame:
    metadata: IngestMetadata
    image_bytes: bytes
    content_length: int = 0
    app_sent_at_ms: int = 0


@dataclass(frozen=True)
class IngestAck:
    success: bool
    received_count: int
    message: str = ""
    server_ack_timestamp_ms: int = 0
    warnings: list[str] = field(default_factory=list)


def serialize_ingest_frame(frame: IngestFrame) -> bytes:
    metadata = {
        "metadata": asdict(frame.metadata),
        "content_length": frame.content_length,
        "app_sent_at_ms": frame.app_sent_at_ms,
    }
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    return len(metadata_bytes).to_bytes(8, "big") + metadata_bytes + frame.image_bytes


def deserialize_ingest_frame(payload: bytes) -> IngestFrame:
    metadata_length = int.from_bytes(payload[:8], "big")
    metadata_start = 8
    metadata_end = metadata_start + metadata_length
    raw = json.loads(payload[metadata_start:metadata_end].decode("utf-8"))
    image_bytes = payload[metadata_end:]
    metadata = IngestMetadata(**raw["metadata"])
    return IngestFrame(
        metadata=metadata,
        image_bytes=image_bytes,
        content_length=int(raw.get("content_length") or 0),
        app_sent_at_ms=int(raw.get("app_sent_at_ms") or 0),
    )


def serialize_ingest_ack(ack: IngestAck) -> bytes:
    return json.dumps(
        {
            "success": ack.success,
            "received_count": ack.received_count,
            "message": ack.message,
            "server_ack_timestamp_ms": ack.server_ack_timestamp_ms,
            "warnings": ack.warnings,
        },
        separators=(",", ":"),
    ).encode("utf-8")


def deserialize_ingest_ack(payload: bytes) -> IngestAck:
    data = json.loads(payload.decode("utf-8"))
    return IngestAck(
        success=bool(data["success"]),
        received_count=int(data["received_count"]),
        message=data.get("message", ""),
        server_ack_timestamp_ms=int(data.get("server_ack_timestamp_ms") or 0),
        warnings=list(data.get("warnings") or []),
    )


def build_frame_ingest_stub(channel):
    return channel.stream_unary(
        METHOD_PATH,
        request_serializer=serialize_ingest_frame,
        response_deserializer=deserialize_ingest_ack,
    )


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
        generic_handler = grpc.method_handlers_generic_handler(
            SERVICE_NAME,
            {
                METHOD_NAME: grpc.stream_unary_rpc_method_handler(
                    self._stream_frames,
                    request_deserializer=deserialize_ingest_frame,
                    response_serializer=serialize_ingest_ack,
                )
            },
        )
        server.add_generic_rpc_handlers((generic_handler,))
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
                self.ingest_func(
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


def current_time_ms() -> int:
    return int(time.time() * 1000)


grpc_ingest_service = GrpcIngestService()

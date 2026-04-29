from collections.abc import Callable
from dataclasses import dataclass, field

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


PACKAGE_NAME = "gc_image_stream.ingest.v1"
SERVICE_NAME = f"{PACKAGE_NAME}.FrameIngestService"
METHOD_NAME = "StreamFrames"
METHOD_PATH = f"/{SERVICE_NAME}/{METHOD_NAME}"


def _add_optional_field(message_proto, name: str, number: int, field_type: int):
    optional_oneof = message_proto.oneof_decl.add()
    optional_oneof.name = f"_{name}"

    field = message_proto.field.add()
    field.name = name
    field.number = number
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field.type = field_type
    field.proto3_optional = True
    field.oneof_index = len(message_proto.oneof_decl) - 1


def _build_file_descriptor() -> bytes:
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = "stream_ingest.proto"
    file_proto.package = PACKAGE_NAME
    file_proto.syntax = "proto3"

    frame_metadata = file_proto.message_type.add()
    frame_metadata.name = "FrameMetadata"

    metadata_fields = [
        ("session_id", 1, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("camera_id", 2, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("device_id", 3, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("frame_sequence", 4, descriptor_pb2.FieldDescriptorProto.TYPE_UINT64),
        ("device_timestamp_ms", 5, descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
        ("device_monotonic_ns", 6, descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
        ("width", 7, descriptor_pb2.FieldDescriptorProto.TYPE_UINT32),
        ("height", 8, descriptor_pb2.FieldDescriptorProto.TYPE_UINT32),
        ("format", 9, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("orientation_deg", 10, descriptor_pb2.FieldDescriptorProto.TYPE_UINT32),
        ("fps_target", 11, descriptor_pb2.FieldDescriptorProto.TYPE_UINT32),
        ("focus_mode", 12, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("exposure_locked", 13, descriptor_pb2.FieldDescriptorProto.TYPE_BOOL),
        ("white_balance_locked", 14, descriptor_pb2.FieldDescriptorProto.TYPE_BOOL),
    ]
    for name, number, field_type in metadata_fields:
        field = frame_metadata.field.add()
        field.name = name
        field.number = number
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        field.type = field_type

    optional_metadata_fields = [
        ("iso", 15, descriptor_pb2.FieldDescriptorProto.TYPE_UINT32),
        ("exposure_time_us", 16, descriptor_pb2.FieldDescriptorProto.TYPE_UINT64),
        ("focal_length_mm", 17, descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT),
        ("lens_facing", 18, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("sensor_timestamp_ns", 19, descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
        ("battery_level", 20, descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT),
        ("network_status", 21, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("app_version", 22, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
    ]
    for name, number, field_type in optional_metadata_fields:
        _add_optional_field(frame_metadata, name, number, field_type)

    ingest_frame = file_proto.message_type.add()
    ingest_frame.name = "IngestFrame"
    metadata_field = ingest_frame.field.add()
    metadata_field.name = "metadata"
    metadata_field.number = 1
    metadata_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    metadata_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    metadata_field.type_name = f".{PACKAGE_NAME}.FrameMetadata"

    image_bytes_field = ingest_frame.field.add()
    image_bytes_field.name = "image_bytes"
    image_bytes_field.number = 2
    image_bytes_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    image_bytes_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_BYTES

    content_length_field = ingest_frame.field.add()
    content_length_field.name = "content_length"
    content_length_field.number = 3
    content_length_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    content_length_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_UINT32

    app_sent_at_field = ingest_frame.field.add()
    app_sent_at_field.name = "app_sent_at_ms"
    app_sent_at_field.number = 4
    app_sent_at_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    app_sent_at_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT64

    ingest_ack = file_proto.message_type.add()
    ingest_ack.name = "IngestAck"
    ack_fields = [
        ("success", 1, descriptor_pb2.FieldDescriptorProto.TYPE_BOOL),
        ("received_count", 2, descriptor_pb2.FieldDescriptorProto.TYPE_UINT64),
        ("message", 3, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("server_ack_timestamp_ms", 4, descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
    ]
    for name, number, field_type in ack_fields:
        field = ingest_ack.field.add()
        field.name = name
        field.number = number
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        field.type = field_type

    warnings_field = ingest_ack.field.add()
    warnings_field.name = "warnings"
    warnings_field.number = 5
    warnings_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
    warnings_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    service = file_proto.service.add()
    service.name = "FrameIngestService"
    method = service.method.add()
    method.name = METHOD_NAME
    method.input_type = f".{PACKAGE_NAME}.IngestFrame"
    method.output_type = f".{PACKAGE_NAME}.IngestAck"
    method.client_streaming = True

    return file_proto.SerializeToString()


_POOL = descriptor_pool.DescriptorPool()
_POOL.AddSerializedFile(_build_file_descriptor())
_FrameMetadataMessage = message_factory.GetMessageClass(
    _POOL.FindMessageTypeByName(f"{PACKAGE_NAME}.FrameMetadata")
)
_IngestFrameMessage = message_factory.GetMessageClass(
    _POOL.FindMessageTypeByName(f"{PACKAGE_NAME}.IngestFrame")
)
_IngestAckMessage = message_factory.GetMessageClass(
    _POOL.FindMessageTypeByName(f"{PACKAGE_NAME}.IngestAck")
)


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


def _metadata_to_proto(metadata: IngestMetadata):
    message = _FrameMetadataMessage(
        session_id=metadata.session_id,
        camera_id=metadata.camera_id,
        device_id=metadata.device_id,
        frame_sequence=metadata.frame_sequence,
        device_timestamp_ms=metadata.device_timestamp_ms,
        device_monotonic_ns=metadata.device_monotonic_ns,
        width=metadata.width,
        height=metadata.height,
        format=metadata.format,
        orientation_deg=metadata.orientation_deg,
        fps_target=metadata.fps_target,
        focus_mode=metadata.focus_mode,
        exposure_locked=metadata.exposure_locked,
        white_balance_locked=metadata.white_balance_locked,
    )
    optional_fields = [
        ("iso", metadata.iso),
        ("exposure_time_us", metadata.exposure_time_us),
        ("focal_length_mm", metadata.focal_length_mm),
        ("lens_facing", metadata.lens_facing),
        ("sensor_timestamp_ns", metadata.sensor_timestamp_ns),
        ("battery_level", metadata.battery_level),
        ("network_status", metadata.network_status),
        ("app_version", metadata.app_version),
    ]
    for field_name, value in optional_fields:
        if value is not None:
            setattr(message, field_name, value)
    return message


def _frame_to_proto(frame: IngestFrame):
    return _IngestFrameMessage(
        metadata=_metadata_to_proto(frame.metadata),
        image_bytes=frame.image_bytes,
        content_length=frame.content_length,
        app_sent_at_ms=frame.app_sent_at_ms,
    )


def _ack_to_proto(ack: IngestAck):
    return _IngestAckMessage(
        success=ack.success,
        received_count=ack.received_count,
        message=ack.message,
        server_ack_timestamp_ms=ack.server_ack_timestamp_ms,
        warnings=ack.warnings,
    )


def _proto_to_metadata(message) -> IngestMetadata:
    optional_fields: dict[str, object | None] = {}
    for field_name in (
        "iso",
        "exposure_time_us",
        "focal_length_mm",
        "lens_facing",
        "sensor_timestamp_ns",
        "battery_level",
        "network_status",
        "app_version",
    ):
        optional_fields[field_name] = getattr(message, field_name) if message.HasField(field_name) else None

    return IngestMetadata(
        session_id=message.session_id,
        camera_id=message.camera_id,
        device_id=message.device_id,
        frame_sequence=int(message.frame_sequence),
        device_timestamp_ms=int(message.device_timestamp_ms),
        device_monotonic_ns=int(message.device_monotonic_ns),
        width=int(message.width),
        height=int(message.height),
        format=message.format,
        orientation_deg=int(message.orientation_deg),
        fps_target=int(message.fps_target),
        focus_mode=message.focus_mode,
        exposure_locked=bool(message.exposure_locked),
        white_balance_locked=bool(message.white_balance_locked),
        **optional_fields,
    )


def _proto_to_frame(message) -> IngestFrame:
    return IngestFrame(
        metadata=_proto_to_metadata(message.metadata),
        image_bytes=message.image_bytes,
        content_length=int(message.content_length),
        app_sent_at_ms=int(message.app_sent_at_ms),
    )


def _proto_to_ack(message) -> IngestAck:
    return IngestAck(
        success=bool(message.success),
        received_count=int(message.received_count),
        message=message.message,
        server_ack_timestamp_ms=int(message.server_ack_timestamp_ms),
        warnings=list(message.warnings),
    )


def serialize_ingest_frame(frame: IngestFrame) -> bytes:
    return _frame_to_proto(frame).SerializeToString()


def deserialize_ingest_frame(payload: bytes) -> IngestFrame:
    message = _IngestFrameMessage()
    message.ParseFromString(payload)
    return _proto_to_frame(message)


def serialize_ingest_ack(ack: IngestAck) -> bytes:
    return _ack_to_proto(ack).SerializeToString()


def deserialize_ingest_ack(payload: bytes) -> IngestAck:
    message = _IngestAckMessage()
    message.ParseFromString(payload)
    return _proto_to_ack(message)


def build_frame_ingest_stub(channel):
    return channel.stream_unary(
        METHOD_PATH,
        request_serializer=serialize_ingest_frame,
        response_deserializer=deserialize_ingest_ack,
    )


def add_frame_ingest_servicer(server, frame_handler: Callable[[IngestFrame], None]):
    import grpc

    def stream_frames(request_iterator, context):
        received_count = 0
        warnings: list[str] = []
        for frame in request_iterator:
            received_count += 1
            frame_handler(frame)

        return IngestAck(
            success=True,
            received_count=received_count,
            message="grpc ingest stream completed",
            warnings=warnings,
        )

    generic_handler = grpc.method_handlers_generic_handler(
        SERVICE_NAME,
        {
            METHOD_NAME: grpc.stream_unary_rpc_method_handler(
                stream_frames,
                request_deserializer=deserialize_ingest_frame,
                response_serializer=serialize_ingest_ack,
            )
        },
    )
    server.add_generic_rpc_handlers((generic_handler,))


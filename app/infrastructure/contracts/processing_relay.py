from collections.abc import Callable, Iterable
from dataclasses import dataclass

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


PACKAGE_NAME = "gc_image_stream.processing.v1"
SERVICE_NAME = f"{PACKAGE_NAME}.FrameRelayService"
METHOD_NAME = "StreamFrames"
METHOD_PATH = f"/{SERVICE_NAME}/{METHOD_NAME}"


def _build_file_descriptor() -> bytes:
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = "processing_relay.proto"
    file_proto.package = PACKAGE_NAME
    file_proto.syntax = "proto3"

    relay_frame = file_proto.message_type.add()
    relay_frame.name = "RelayFrame"

    file_path_oneof = relay_frame.oneof_decl.add()
    file_path_oneof.name = "_file_path"

    fields = [
        ("device_id", 1, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("timestamp_ms", 2, descriptor_pb2.FieldDescriptorProto.TYPE_INT64),
        ("sequence", 3, descriptor_pb2.FieldDescriptorProto.TYPE_UINT64),
        ("content_type", 4, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
        ("image_bytes", 5, descriptor_pb2.FieldDescriptorProto.TYPE_BYTES),
    ]
    for name, number, field_type in fields:
        field = relay_frame.field.add()
        field.name = name
        field.number = number
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        field.type = field_type

    file_path_field = relay_frame.field.add()
    file_path_field.name = "file_path"
    file_path_field.number = 6
    file_path_field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    file_path_field.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING
    file_path_field.proto3_optional = True
    file_path_field.oneof_index = 0

    relay_ack = file_proto.message_type.add()
    relay_ack.name = "RelayAck"
    ack_fields = [
        ("success", 1, descriptor_pb2.FieldDescriptorProto.TYPE_BOOL),
        ("received_count", 2, descriptor_pb2.FieldDescriptorProto.TYPE_UINT64),
        ("message", 3, descriptor_pb2.FieldDescriptorProto.TYPE_STRING),
    ]
    for name, number, field_type in ack_fields:
        field = relay_ack.field.add()
        field.name = name
        field.number = number
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        field.type = field_type

    service = file_proto.service.add()
    service.name = "FrameRelayService"
    method = service.method.add()
    method.name = METHOD_NAME
    method.input_type = f".{PACKAGE_NAME}.RelayFrame"
    method.output_type = f".{PACKAGE_NAME}.RelayAck"
    method.client_streaming = True

    return file_proto.SerializeToString()


_POOL = descriptor_pool.DescriptorPool()
_POOL.AddSerializedFile(_build_file_descriptor())
_RelayFrameMessage = message_factory.GetMessageClass(
    _POOL.FindMessageTypeByName(f"{PACKAGE_NAME}.RelayFrame")
)
_RelayAckMessage = message_factory.GetMessageClass(
    _POOL.FindMessageTypeByName(f"{PACKAGE_NAME}.RelayAck")
)


@dataclass(frozen=True)
class RelayFrame:
    device_id: str
    timestamp_ms: int
    sequence: int
    content_type: str
    image_bytes: bytes
    file_path: str | None = None


@dataclass(frozen=True)
class RelayAck:
    success: bool
    received_count: int
    message: str = ""


def _frame_to_proto(frame: RelayFrame):
    message = _RelayFrameMessage(
        device_id=frame.device_id,
        timestamp_ms=frame.timestamp_ms,
        sequence=frame.sequence,
        content_type=frame.content_type,
        image_bytes=frame.image_bytes,
    )
    if frame.file_path is not None:
        message.file_path = frame.file_path
    return message


def _ack_to_proto(ack: RelayAck):
    return _RelayAckMessage(
        success=ack.success,
        received_count=ack.received_count,
        message=ack.message,
    )


def _proto_to_frame(message) -> RelayFrame:
    file_path = message.file_path if message.HasField("file_path") else None
    return RelayFrame(
        device_id=message.device_id,
        timestamp_ms=message.timestamp_ms,
        sequence=message.sequence,
        content_type=message.content_type,
        image_bytes=message.image_bytes,
        file_path=file_path,
    )


def _proto_to_ack(message) -> RelayAck:
    return RelayAck(
        success=bool(message.success),
        received_count=int(message.received_count),
        message=message.message,
    )


def serialize_relay_frame(frame: RelayFrame) -> bytes:
    return _frame_to_proto(frame).SerializeToString()


def deserialize_relay_frame(payload: bytes) -> RelayFrame:
    message = _RelayFrameMessage()
    message.ParseFromString(payload)
    return _proto_to_frame(message)


def serialize_relay_ack(ack: RelayAck) -> bytes:
    return _ack_to_proto(ack).SerializeToString()


def deserialize_relay_ack(payload: bytes) -> RelayAck:
    message = _RelayAckMessage()
    message.ParseFromString(payload)
    return _proto_to_ack(message)


def build_frame_relay_stub(channel):
    return channel.stream_unary(
        METHOD_PATH,
        request_serializer=serialize_relay_frame,
        response_deserializer=deserialize_relay_ack,
    )


def add_frame_relay_servicer(server, frame_handler: Callable[[RelayFrame], None]):
    import grpc

    def stream_frames(request_iterator, context):
        received_count = 0
        for frame in request_iterator:
            received_count += 1
            frame_handler(frame)

        return RelayAck(
            success=True,
            received_count=received_count,
            message="relay stream completed",
        )

    generic_handler = grpc.method_handlers_generic_handler(
        SERVICE_NAME,
        {
            METHOD_NAME: grpc.stream_unary_rpc_method_handler(
                stream_frames,
                request_deserializer=deserialize_relay_frame,
                response_serializer=serialize_relay_ack,
            )
        },
    )
    server.add_generic_rpc_handlers((generic_handler,))

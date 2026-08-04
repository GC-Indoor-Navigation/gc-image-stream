from google.protobuf.message import Message


class PayloadLimitExceeded(ValueError):
    pass


def serialize_with_limit(
    message: Message,
    *,
    maximum_payload_bytes: int,
) -> bytes:
    _validate_limit(maximum_payload_bytes)
    payload = message.SerializeToString(deterministic=True)
    _validate_size(len(payload), maximum_payload_bytes)
    return payload


def parse_with_limit(
    message_type,
    payload: bytes,
    *,
    maximum_payload_bytes: int,
):
    _validate_limit(maximum_payload_bytes)
    _validate_size(len(payload), maximum_payload_bytes)
    return message_type.FromString(payload)


def _validate_limit(maximum_payload_bytes: int) -> None:
    if maximum_payload_bytes <= 0:
        raise ValueError("maximum_payload_bytes must be greater than zero")


def _validate_size(payload_size: int, maximum_payload_bytes: int) -> None:
    if payload_size > maximum_payload_bytes:
        raise PayloadLimitExceeded(
            f"payload is {payload_size} bytes; limit is {maximum_payload_bytes}"
        )

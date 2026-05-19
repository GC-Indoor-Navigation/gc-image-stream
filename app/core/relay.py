STREAM_RELAY_MODE_OFF = "off"
STREAM_RELAY_MODE_RAW = "raw"
STREAM_RELAY_MODE_FRAME_SET = "frame_set"

VALID_STREAM_RELAY_MODES = {
    STREAM_RELAY_MODE_OFF,
    STREAM_RELAY_MODE_RAW,
    STREAM_RELAY_MODE_FRAME_SET,
}


def normalize_stream_relay_mode(mode: str) -> str:
    normalized = mode.strip().lower()
    if normalized not in VALID_STREAM_RELAY_MODES:
        raise RuntimeError(
            "STREAM_RELAY_MODE must be one of: off, raw, frame_set"
        )
    return normalized


def resolve_stream_relay_mode(
    configured_mode: str,
    raw_relay_enabled: bool,
    frame_set_relay_enabled: bool,
) -> str:
    if configured_mode.strip():
        return normalize_stream_relay_mode(configured_mode)
    if frame_set_relay_enabled:
        return STREAM_RELAY_MODE_FRAME_SET
    if raw_relay_enabled:
        return STREAM_RELAY_MODE_RAW
    return STREAM_RELAY_MODE_OFF

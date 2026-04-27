from app.config.env import (
    get_optional_bool_env,
    get_optional_float_env,
    get_optional_int_env,
    get_optional_str_env,
    get_required_env,
)


DATABASE_URL = get_required_env("DATABASE_URL")
STORAGE_DIR = get_required_env("STORAGE_DIR")
PROCESSING_SERVER_URL = get_required_env("PROCESSING_SERVER_URL")
AUTO_SYNC_ENABLED = get_optional_bool_env("AUTO_SYNC_ENABLED", False)
STREAM_RELAY_ENABLED = get_optional_bool_env("STREAM_RELAY_ENABLED", False)
STREAM_RELAY_TARGET = get_required_env("STREAM_RELAY_TARGET") if STREAM_RELAY_ENABLED else ""
_stream_relay_timeout_sec = get_optional_float_env("STREAM_RELAY_TIMEOUT_SEC", 0.0)
STREAM_RELAY_TIMEOUT_SEC = (
    _stream_relay_timeout_sec
    if _stream_relay_timeout_sec > 0
    else None
)
EXPERIMENT_ENABLED = get_optional_bool_env("EXPERIMENT_ENABLED", True)
EXPERIMENT_LOG_DIR = (
    get_optional_str_env("EXPERIMENT_LOG_DIR")
    if EXPERIMENT_ENABLED
    else ""
)
EXPERIMENT_ID = get_optional_str_env("EXPERIMENT_ID")
FRAME_MAINTENANCE_INTERVAL_SEC = get_optional_float_env(
    "FRAME_MAINTENANCE_INTERVAL_SEC",
    60.0,
)
FRAME_COMPRESS_AFTER_SEC = get_optional_float_env(
    "FRAME_COMPRESS_AFTER_SEC",
    300.0,
)
FRAME_COMPRESS_JPEG_QUALITY = get_optional_int_env(
    "FRAME_COMPRESS_JPEG_QUALITY",
    60,
)
FRAME_COMPRESS_BATCH_SIZE = get_optional_int_env(
    "FRAME_COMPRESS_BATCH_SIZE",
    100,
)

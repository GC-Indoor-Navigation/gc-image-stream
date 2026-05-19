from app.core.env import (
    get_optional_bool_env,
    get_optional_csv_env,
    get_optional_float_env,
    get_optional_int_env,
    get_optional_str_env,
    get_required_env,
)


DATABASE_URL = get_required_env("DATABASE_URL")
STORAGE_DIR = get_required_env("STORAGE_DIR")
GRPC_INGEST_BIND = get_optional_str_env("GRPC_INGEST_BIND", "127.0.0.1:50052")
STREAM_RELAY_ENABLED = get_optional_bool_env("STREAM_RELAY_ENABLED", False)
STREAM_RELAY_TARGET = get_required_env("STREAM_RELAY_TARGET") if STREAM_RELAY_ENABLED else ""
_stream_relay_timeout_sec = get_optional_float_env("STREAM_RELAY_TIMEOUT_SEC", 0.0)
STREAM_RELAY_TIMEOUT_SEC = (
    _stream_relay_timeout_sec
    if _stream_relay_timeout_sec > 0
    else None
)
STREAM_FRAME_SET_RELAY_ENABLED = get_optional_bool_env(
    "STREAM_FRAME_SET_RELAY_ENABLED",
    False,
)
STREAM_FRAME_SET_RELAY_TARGET = (
    get_required_env("STREAM_FRAME_SET_RELAY_TARGET")
    if STREAM_FRAME_SET_RELAY_ENABLED
    else ""
)
_stream_frame_set_relay_timeout_sec = get_optional_float_env(
    "STREAM_FRAME_SET_RELAY_TIMEOUT_SEC",
    0.0,
)
STREAM_FRAME_SET_RELAY_TIMEOUT_SEC = (
    _stream_frame_set_relay_timeout_sec
    if _stream_frame_set_relay_timeout_sec > 0
    else None
)
STREAM_SYNC_ENABLED = get_optional_bool_env("STREAM_SYNC_ENABLED", False)
STREAM_SYNC_WINDOW_MS = get_optional_int_env("STREAM_SYNC_WINDOW_MS", 50)
STREAM_SYNC_EXPECTED_CAMERAS = get_optional_csv_env("STREAM_SYNC_EXPECTED_CAMERAS")
STREAM_SYNC_BUFFER_SIZE = get_optional_int_env("STREAM_SYNC_BUFFER_SIZE", 120)
STREAM_SYNC_RECENT_LIMIT = get_optional_int_env("STREAM_SYNC_RECENT_LIMIT", 20)
EXPERIMENT_ENABLED = get_optional_bool_env("EXPERIMENT_ENABLED", True)
EXPERIMENT_LOG_DIR = (
    get_optional_str_env("EXPERIMENT_LOG_DIR")
    if EXPERIMENT_ENABLED
    else ""
)
EXPERIMENT_ID = get_optional_str_env("EXPERIMENT_ID")
_experiment_duration_sec = get_optional_float_env("EXPERIMENT_DURATION_SEC", 0.0)
EXPERIMENT_DURATION_SEC = (
    _experiment_duration_sec
    if _experiment_duration_sec > 0
    else None
)

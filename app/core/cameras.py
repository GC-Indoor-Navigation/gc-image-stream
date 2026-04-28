import os

from app.core.env import (
    get_optional_bool_env,
    get_optional_csv_env,
    get_optional_str_env,
)
from app.services.ingest.adapters.adapter_runtime import CameraInputConfig


CAMERA_SESSIONS_ENABLED = get_optional_bool_env("CAMERA_SESSIONS_ENABLED", False)


def build_camera_session_configs_from_env() -> list[CameraInputConfig]:
    camera_ids = get_optional_csv_env("CAMERA_SESSIONS", [])
    return [
        build_camera_session_config(camera_id)
        for camera_id in camera_ids
    ]


def build_camera_session_config(camera_id: str) -> CameraInputConfig:
    prefix = camera_id.upper()
    source_kind = get_camera_env(
        prefix,
        "INPUT_TYPE",
        default=get_optional_str_env("CAMERA_INPUT_TYPE", "mjpeg").lower(),
    ).lower()
    if source_kind == "mjpeg":
        source_url = get_camera_env(prefix, "STREAM_URL", required=True)
    elif source_kind == "snapshot":
        source_url = get_camera_env(prefix, "SNAPSHOT_URL", required=True)
    elif source_kind == "grpc":
        source_url = ""
    else:
        raise RuntimeError(f"Unsupported camera input type: {prefix}_INPUT_TYPE={source_kind}")
    interval_raw = get_camera_env(prefix, "COLLECT_INTERVAL_SEC", default="1.0")
    timeout_raw = get_camera_env(prefix, "CAPTURE_TIMEOUT_SEC", default="10.0")

    return CameraInputConfig(
        device_id=camera_id,
        source_kind=source_kind,
        source_url=source_url,
        collect_interval_sec=parse_positive_float(
            f"{prefix}_COLLECT_INTERVAL_SEC",
            interval_raw,
        ),
        capture_timeout_sec=parse_positive_float(
            f"{prefix}_CAPTURE_TIMEOUT_SEC",
            timeout_raw,
        ),
    )


def get_camera_env(
    prefix: str,
    suffix: str,
    required: bool = False,
    default: str | None = None,
) -> str:
    name = f"{prefix}_{suffix}"
    value = os.getenv(name)
    if value is None or not value.strip():
        if required:
            raise RuntimeError(f"Missing required environment variable: {name}")
        if default is None:
            return ""
        return default
    return value.strip()


def parse_positive_float(name: str, raw_value: str) -> float:
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"Invalid float value for {name}: {raw_value}") from exc

    if value <= 0:
        raise RuntimeError(f"{name} must be greater than 0")
    return value

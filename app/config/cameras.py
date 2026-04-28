import os

from app.config.env import (
    get_optional_bool_env,
    get_optional_csv_env,
)
from app.services.ingest.adapters.mjpeg import CameraSessionConfig


CAMERA_SESSIONS_ENABLED = get_optional_bool_env("CAMERA_SESSIONS_ENABLED", False)


def build_camera_session_configs_from_env() -> list[CameraSessionConfig]:
    camera_ids = get_optional_csv_env("CAMERA_SESSIONS", [])
    return [
        build_camera_session_config(camera_id)
        for camera_id in camera_ids
    ]


def build_camera_session_config(camera_id: str) -> CameraSessionConfig:
    prefix = camera_id.upper()
    source_url = get_camera_env(prefix, "STREAM_URL", required=True)
    interval_raw = get_camera_env(prefix, "COLLECT_INTERVAL_SEC", default="1.0")
    timeout_raw = get_camera_env(prefix, "CAPTURE_TIMEOUT_SEC", default="10.0")

    return CameraSessionConfig(
        device_id=camera_id,
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

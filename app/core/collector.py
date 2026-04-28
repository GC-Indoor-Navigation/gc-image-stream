from app.core.env import get_float_env, get_required_env


CAMERA_NAME = get_required_env("CAMERA_NAME")
CAMERA_SNAPSHOT_URL = get_required_env("CAMERA_SNAPSHOT_URL")
CAMERA_STREAM_URL = get_required_env("CAMERA_STREAM_URL")
COLLECT_INTERVAL_SEC = get_float_env("COLLECT_INTERVAL_SEC")
REGISTER_API_URL = get_required_env("REGISTER_API_URL")

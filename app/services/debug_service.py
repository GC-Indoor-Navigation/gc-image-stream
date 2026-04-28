from app.services.monitoring_service import list_camera_states
from app.services.stream.state import StreamState, stream_state


def get_latest_timestamp_delta(state: StreamState = stream_state):
    cameras = [
        camera
        for camera in list_camera_states(state)
        if camera["latest_timestamp"] is not None
    ]
    if not cameras:
        return {
            "base_device_id": None,
            "base_timestamp": None,
            "items": [],
        }

    base = max(cameras, key=lambda camera: camera["latest_timestamp"])
    base_timestamp = base["latest_timestamp"]

    return {
        "base_device_id": base["device_id"],
        "base_timestamp": base_timestamp,
        "items": [
            {
                "device_id": camera["device_id"],
                "latest_timestamp": camera["latest_timestamp"],
                "delta_ms": camera["latest_timestamp"] - base_timestamp,
            }
            for camera in cameras
        ],
    }

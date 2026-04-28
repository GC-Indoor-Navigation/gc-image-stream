from dataclasses import dataclass

from app.services.ingest.adapters.mjpeg import (
    CameraSessionConfig,
    CameraSessionRuntime,
    start_mjpeg_camera_session,
    stop_camera_session,
)


@dataclass(frozen=True)
class ManagedCameraSession:
    config: CameraSessionConfig
    runtime: CameraSessionRuntime


class CameraSessionManager:
    def __init__(self):
        self._sessions: dict[str, ManagedCameraSession] = {}

    def start_all(self, configs: list[CameraSessionConfig]):
        for config in configs:
            self.start(config)

    def start(self, config: CameraSessionConfig):
        if config.device_id in self._sessions:
            raise RuntimeError(f"Camera session already running: {config.device_id}")

        runtime = start_mjpeg_camera_session(config)
        self._sessions[config.device_id] = ManagedCameraSession(
            config=config,
            runtime=runtime,
        )
        return runtime

    def stop_all(self):
        for device_id in list(self._sessions.keys()):
            self.stop(device_id)

    def stop(self, device_id: str):
        session = self._sessions.pop(device_id, None)
        if session is None:
            return
        stop_camera_session(session.runtime)

    def list_sessions(self):
        return [
            {
                "device_id": session.config.device_id,
                "source_url": session.config.source_url,
                "collect_interval_sec": session.config.collect_interval_sec,
                "capture_timeout_sec": session.config.capture_timeout_sec,
                "running": session.runtime.worker.is_alive(),
            }
            for session in self._sessions.values()
        ]

    def clear(self):
        self.stop_all()


camera_session_manager = CameraSessionManager()

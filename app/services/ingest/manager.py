from dataclasses import dataclass

from app.services.ingest.adapters.base import CameraInputConfig, CameraInputRuntime, stop_camera_input
from app.services.ingest.adapters.mjpeg_ingest import start_mjpeg_camera_session
from app.services.ingest.adapters.snapshot_ingest import start_snapshot_camera_session


@dataclass(frozen=True)
class ManagedCameraSession:
    config: CameraInputConfig
    runtime: CameraInputRuntime


class CameraSessionManager:
    def __init__(self):
        self._sessions: dict[str, ManagedCameraSession] = {}

    def start_all(self, configs: list[CameraInputConfig]):
        for config in configs:
            self.start(config)

    def start(self, config: CameraInputConfig):
        if config.device_id in self._sessions:
            raise RuntimeError(f"Camera session already running: {config.device_id}")

        if config.source_kind == "mjpeg":
            runtime = start_mjpeg_camera_session(config)
        elif config.source_kind == "snapshot":
            runtime = start_snapshot_camera_session(config)
        else:
            raise RuntimeError(f"Unsupported camera input type: {config.source_kind}")

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
        stop_camera_input(session.runtime)

    def list_sessions(self):
        return [
            {
                "device_id": session.config.device_id,
                "source_kind": session.config.source_kind,
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

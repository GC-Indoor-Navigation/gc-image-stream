from app.services.ingest.adapters.mjpeg import CameraSessionConfig, CameraSessionRuntime
from app.services.ingest.mjpeg_manager import CameraSessionManager


class FakeWorker:
    def __init__(self):
        self.stopped = False

    def is_alive(self):
        return not self.stopped

    def join(self, timeout=None):
        self.stopped = True


class FakeStopEvent:
    def __init__(self):
        self.was_set = False

    def set(self):
        self.was_set = True


def test_camera_session_manager_starts_and_stops_sessions(monkeypatch):
    manager = CameraSessionManager()
    started = []

    def fake_start(config):
        started.append(config.device_id)
        return CameraSessionRuntime(
            stop_event=FakeStopEvent(),
            worker=FakeWorker(),
        )

    monkeypatch.setattr(
        "app.services.ingest.mjpeg_manager.start_mjpeg_camera_session",
        fake_start,
    )

    configs = [
        CameraSessionConfig("camera1", "http://camera1.local/video", 0.1),
        CameraSessionConfig("camera2", "http://camera2.local/video", 0.1),
    ]

    manager.start_all(configs)

    sessions = manager.list_sessions()
    assert started == ["camera1", "camera2"]
    assert [session["device_id"] for session in sessions] == ["camera1", "camera2"]
    assert all(session["running"] for session in sessions)

    manager.stop_all()

    assert manager.list_sessions() == []

from app.services.ingest.adapters.mjpeg import CameraSessionConfig, CameraSessionRuntime
from app.services.ingest.manager import CameraSessionManager


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
        "app.services.ingest.manager.start_mjpeg_camera_session",
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


def test_camera_session_manager_starts_snapshot_sessions(monkeypatch):
    manager = CameraSessionManager()
    started = []

    def fake_start(config):
        started.append((config.device_id, config.source_kind))
        return CameraSessionRuntime(
            stop_event=FakeStopEvent(),
            worker=FakeWorker(),
        )

    monkeypatch.setattr(
        "app.services.ingest.manager.start_snapshot_camera_session",
        fake_start,
    )

    config = CameraSessionConfig(
        device_id="camera1",
        source_url="http://camera1.local/shot.jpg",
        collect_interval_sec=0.5,
        source_kind="snapshot",
    )

    manager.start(config)

    sessions = manager.list_sessions()
    assert started == [("camera1", "snapshot")]
    assert sessions[0]["source_kind"] == "snapshot"

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.api.routes.capture import router as capture_router
from app.db import Base, get_db
from app.api.routes.debug import router as debug_router
from app.api.routes.frames import router as frames_router
from app.api.routes.internal import router as internal_router
from app.api.routes.monitoring import router as monitoring_router
from app.infrastructure.grpc.processing_relay_client import (
    processing_frame_set_relay_service,
    processing_relay_service,
)
from app.infrastructure.storage import file_utils
from app.services.stream.stream_experiment import clear_stream_experiment_recorder
from app.services.stream.state import stream_state
from app.services.sync import stream_sync_service
from app.services.alerts import processing_alert_store


@pytest.fixture
def storage_dir(tmp_path, monkeypatch):
    path = tmp_path / "storage"
    monkeypatch.setattr(file_utils, "STORAGE_DIR", str(path))
    return path


@pytest.fixture
def session_factory(tmp_path):
    db_path = tmp_path / "test.db"
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    try:
        yield factory
    finally:
        engine.dispose()


@pytest.fixture
def app(session_factory, storage_dir):
    stream_state.clear()
    processing_alert_store.clear()
    processing_relay_service.clear()
    processing_frame_set_relay_service.clear()
    stream_sync_service.clear()
    clear_stream_experiment_recorder()
    test_app = FastAPI()
    test_app.include_router(capture_router)
    test_app.include_router(frames_router)
    test_app.include_router(internal_router)
    test_app.include_router(monitoring_router)
    test_app.include_router(debug_router)

    def override_get_db():
        db = session_factory()
        try:
            yield db
        finally:
            db.close()

    test_app.dependency_overrides[get_db] = override_get_db
    yield test_app
    stream_state.clear()
    processing_alert_store.clear()
    processing_relay_service.clear()
    processing_frame_set_relay_service.clear()
    stream_sync_service.clear()
    clear_stream_experiment_recorder()


@pytest.fixture
def client(app):
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def read_file_bytes():
    def _read(path: str) -> bytes:
        return Path(path).read_bytes()

    return _read

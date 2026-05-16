import logging

from app.core.cameras import (
    CAMERA_SESSIONS_ENABLED,
    build_camera_session_configs_from_env,
)
from app.core.logging import format_log_event
from app.core.server import (
    EXPERIMENT_ID,
    EXPERIMENT_DURATION_SEC,
    EXPERIMENT_LOG_DIR,
    GRPC_INGEST_BIND,
    STORAGE_DIR,
    STREAM_RELAY_ENABLED,
    STREAM_RELAY_TARGET,
    STREAM_RELAY_TIMEOUT_SEC,
)
from app.infrastructure.grpc.grpc_ingest_server import grpc_ingest_service
from app.infrastructure.grpc.processing_relay_client import processing_relay_service
from app.services.ingest.camera_session_manager import camera_session_manager
from app.services.stream.stream_experiment import (
    clear_stream_experiment_recorder,
    configure_stream_experiment_recorder,
)


logger = logging.getLogger("gc_image_stream.app")


async def startup_application():
    camera_configs = []
    if CAMERA_SESSIONS_ENABLED:
        camera_configs = build_camera_session_configs_from_env()
    worker_camera_configs = [
        config
        for config in camera_configs
        if config.source_kind in {"mjpeg", "snapshot"}
    ]
    grpc_camera_configs = [
        config
        for config in camera_configs
        if config.source_kind == "grpc"
    ]
    grpc_ingest_enabled = any(
        config.source_kind == "grpc"
        for config in camera_configs
    )

    configure_stream_experiment_recorder(
        experiment_log_dir=EXPERIMENT_LOG_DIR,
        experiment_id=EXPERIMENT_ID,
        duration_sec=EXPERIMENT_DURATION_SEC,
        expected_device_count=len(grpc_camera_configs) if len(grpc_camera_configs) > 1 else None,
        storage_dir=STORAGE_DIR,
        relay_target=STREAM_RELAY_TARGET if STREAM_RELAY_ENABLED else "",
        camera_ids=[config.device_id for config in camera_configs],
    )

    if STREAM_RELAY_ENABLED:
        processing_relay_service.configure(
            target=STREAM_RELAY_TARGET,
            timeout_sec=STREAM_RELAY_TIMEOUT_SEC,
            enabled=True,
        )
        processing_relay_service.start()
        logger.info(
            format_log_event(
                "stream_relay_started",
                target=STREAM_RELAY_TARGET,
            )
        )
    else:
        processing_relay_service.configure(
            target="",
            timeout_sec=STREAM_RELAY_TIMEOUT_SEC,
            enabled=False,
        )
        logger.info(format_log_event("stream_relay_disabled"))

    if grpc_ingest_enabled:
        grpc_ingest_service.configure(
            bind=GRPC_INGEST_BIND,
            enabled=True,
            expected_device_count=len(grpc_camera_configs),
        )
        grpc_ingest_service.start()
        logger.info(
            format_log_event(
                "grpc_ingest_started",
                bind=grpc_ingest_service.status()["bind"],
            )
        )
    else:
        grpc_ingest_service.configure(bind="", enabled=False)
        logger.info(format_log_event("grpc_ingest_disabled"))

    if worker_camera_configs:
        camera_session_manager.start_all(worker_camera_configs)
        logger.info(
            format_log_event(
                "camera_sessions_started",
                count=len(worker_camera_configs),
            )
        )
    else:
        logger.info(format_log_event("camera_sessions_disabled"))


async def shutdown_application():
    camera_session_manager.stop_all()
    grpc_ingest_service.stop()
    processing_relay_service.stop()
    clear_stream_experiment_recorder()
    logger.info(format_log_event("camera_sessions_stopped"))

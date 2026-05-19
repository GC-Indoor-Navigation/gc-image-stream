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
    STREAM_FRAME_SET_RELAY_ENABLED,
    STREAM_FRAME_SET_RELAY_TARGET,
    STREAM_FRAME_SET_RELAY_TIMEOUT_SEC,
    STREAM_RELAY_ENABLED,
    STREAM_RELAY_TARGET,
    STREAM_RELAY_TIMEOUT_SEC,
    STREAM_SYNC_BUFFER_SIZE,
    STREAM_SYNC_ENABLED,
    STREAM_SYNC_EXPECTED_CAMERAS,
    STREAM_SYNC_RECENT_LIMIT,
    STREAM_SYNC_WINDOW_MS,
)
from app.services.ingest.adapters.adapter_runtime import CameraInputConfig
from app.infrastructure.grpc.grpc_ingest_server import grpc_ingest_service
from app.infrastructure.grpc.processing_relay_client import (
    processing_frame_set_relay_service,
    processing_relay_service,
)
from app.services.ingest.camera_session_manager import camera_session_manager
from app.services.stream.stream_experiment import (
    clear_stream_experiment_recorder,
    configure_stream_experiment_recorder,
)
from app.services.sync import stream_sync_service


logger = logging.getLogger("gc_image_stream.app")


def resolve_sync_expected_cameras(
    configured_expected_cameras: list[str] | tuple[str, ...],
    grpc_camera_configs: list[CameraInputConfig],
) -> list[str]:
    if configured_expected_cameras:
        return list(configured_expected_cameras)
    return [config.device_id for config in grpc_camera_configs]


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

    if STREAM_FRAME_SET_RELAY_ENABLED:
        processing_frame_set_relay_service.configure(
            target=STREAM_FRAME_SET_RELAY_TARGET,
            timeout_sec=STREAM_FRAME_SET_RELAY_TIMEOUT_SEC,
            enabled=True,
        )
        processing_frame_set_relay_service.start()
        logger.info(
            format_log_event(
                "stream_frame_set_relay_started",
                target=STREAM_FRAME_SET_RELAY_TARGET,
            )
        )
    else:
        processing_frame_set_relay_service.configure(
            target="",
            timeout_sec=STREAM_FRAME_SET_RELAY_TIMEOUT_SEC,
            enabled=False,
        )
        logger.info(format_log_event("stream_frame_set_relay_disabled"))

    sync_expected_cameras = resolve_sync_expected_cameras(
        STREAM_SYNC_EXPECTED_CAMERAS,
        grpc_camera_configs,
    )
    stream_sync_service.configure(
        enabled=STREAM_SYNC_ENABLED,
        expected_cameras=sync_expected_cameras,
        window_ms=STREAM_SYNC_WINDOW_MS,
        buffer_size=STREAM_SYNC_BUFFER_SIZE,
        recent_limit=STREAM_SYNC_RECENT_LIMIT,
    )
    logger.info(
        format_log_event(
            "stream_sync_configured",
            enabled=STREAM_SYNC_ENABLED,
            expected_cameras=",".join(sync_expected_cameras),
            window_ms=STREAM_SYNC_WINDOW_MS,
        )
    )

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
    processing_frame_set_relay_service.stop()
    stream_sync_service.clear()
    clear_stream_experiment_recorder()
    logger.info(format_log_event("camera_sessions_stopped"))

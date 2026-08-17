import logging
from uuid import uuid4

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
    STREAM_RELAY_ENABLED,
    STREAM_RELAY_MODE,
    STREAM_RELAY_TARGET,
    STREAM_RELAY_TIMEOUT_SEC,
    STREAM_RELAY_V2_MAXIMUM_CLOCK_UNCERTAINTY_MS,
    STREAM_RELAY_V2_PROCESSING_PROFILE_DIGEST,
    STREAM_RELAY_V2_PRODUCER_FRESHNESS_BUDGET_MS,
    STREAM_RELAY_V2_SHADOW_ENABLED,
    STREAM_RELAY_V2_TARGET,
    STREAM_SYNC_BUFFER_SIZE,
    STREAM_SYNC_ENABLED,
    STREAM_SYNC_EXPECTED_CAMERAS,
    STREAM_SYNC_RECENT_LIMIT,
    STREAM_SYNC_WINDOW_MS,
    STREAM_SESSION_AUTH_ENABLED,
    STREAM_SESSION_JWKS_URL,
    STREAM_SESSION_TOKEN_AUDIENCE,
    STREAM_SESSION_TOKEN_ISSUER,
    STREAM_SESSION_STATUS_URL_TEMPLATE,
)
from app.services.ingest.adapters.adapter_runtime import CameraInputConfig
from app.infrastructure.grpc.grpc_ingest_server import grpc_ingest_service
from app.infrastructure.grpc.processing_relay_client import (
    processing_frame_set_relay_service,
    processing_relay_service,
)
from app.infrastructure.grpc.live_relay_v2_client import (
    processing_live_relay_v2_client,
)
from app.services.ingest.camera_session_manager import camera_session_manager
from app.services.ingest.reconciliation import reconcile_archive
from app.db import SessionLocal
from app.services.relay_v2 import ProtocolConfig
from app.services.stream.stream_experiment import (
    clear_stream_experiment_recorder,
    configure_stream_experiment_recorder,
)
from app.services.sync import stream_sync_service
from app.services.session_identity import (
    JwksKeyCache,
    SessionStatusCache,
    SessionTokenVerifier,
)


logger = logging.getLogger("gc_image_stream.app")


def resolve_sync_expected_cameras(
    configured_expected_cameras: list[str] | tuple[str, ...],
    grpc_camera_configs: list[CameraInputConfig],
) -> list[str]:
    if configured_expected_cameras:
        return list(configured_expected_cameras)
    return [config.device_id for config in grpc_camera_configs]


def resolve_selected_relay_target() -> str:
    if STREAM_RELAY_ENABLED or STREAM_FRAME_SET_RELAY_ENABLED:
        return STREAM_RELAY_TARGET
    if STREAM_RELAY_V2_SHADOW_ENABLED:
        return STREAM_RELAY_V2_TARGET
    return ""


async def startup_application():
    reconciliation_db = SessionLocal()
    try:
        reconciliation = reconcile_archive(reconciliation_db, STORAGE_DIR)
    finally:
        reconciliation_db.close()
    reconciliation_fields = {
        "run_id": reconciliation.run_id,
        "checked_frames": reconciliation.checked_frames,
        "healthy_frames": reconciliation.healthy_frames,
        "degraded_frames": reconciliation.degraded_frames,
        "orphan_files": reconciliation.orphan_files,
        "partial_files": reconciliation.partial_files,
    }
    processing_live_relay_v2_client.configure(
        target=STREAM_RELAY_V2_TARGET,
        enabled=STREAM_RELAY_V2_SHADOW_ENABLED,
        session_factory=(SessionLocal if STREAM_RELAY_V2_SHADOW_ENABLED else None),
        protocol_config=(
            ProtocolConfig(
                producer_session_id=str(uuid4()),
                processing_profile_digest=(
                    STREAM_RELAY_V2_PROCESSING_PROFILE_DIGEST
                ),
                producer_freshness_budget_ms=(
                    STREAM_RELAY_V2_PRODUCER_FRESHNESS_BUDGET_MS
                ),
                maximum_clock_uncertainty_ms=(
                    STREAM_RELAY_V2_MAXIMUM_CLOCK_UNCERTAINTY_MS
                ),
            )
            if STREAM_RELAY_V2_SHADOW_ENABLED
            else None
        ),
    )
    if reconciliation.degraded_frames or reconciliation.orphan_files or reconciliation.partial_files:
        logger.warning(format_log_event("archive_reconciliation_degraded", **reconciliation_fields))
    else:
        logger.info(format_log_event("archive_reconciliation_healthy", **reconciliation_fields))

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
        relay_target=resolve_selected_relay_target(),
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
                mode=STREAM_RELAY_MODE,
                target=STREAM_RELAY_TARGET,
            )
        )
    else:
        processing_relay_service.configure(
            target="",
            timeout_sec=STREAM_RELAY_TIMEOUT_SEC,
            enabled=False,
        )
        logger.info(
            format_log_event(
                "stream_relay_disabled",
                mode=STREAM_RELAY_MODE,
            )
        )

    if STREAM_FRAME_SET_RELAY_ENABLED:
        processing_frame_set_relay_service.configure(
            target=STREAM_RELAY_TARGET,
            timeout_sec=STREAM_RELAY_TIMEOUT_SEC,
            enabled=True,
        )
        processing_frame_set_relay_service.start()
        logger.info(
            format_log_event(
                "stream_frame_set_relay_started",
                mode=STREAM_RELAY_MODE,
                target=STREAM_RELAY_TARGET,
            )
        )
    else:
        processing_frame_set_relay_service.configure(
            target="",
            timeout_sec=STREAM_RELAY_TIMEOUT_SEC,
            enabled=False,
        )
        logger.info(
            format_log_event(
                "stream_frame_set_relay_disabled",
                mode=STREAM_RELAY_MODE,
            )
        )

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

    if STREAM_RELAY_V2_SHADOW_ENABLED:
        processing_live_relay_v2_client.start()
        logger.info(
            format_log_event(
                "stream_relay_v2_shadow_started",
                target=STREAM_RELAY_V2_TARGET,
                alerts_enabled=False,
            )
        )
    else:
        logger.info(format_log_event("stream_relay_v2_shadow_disabled"))

    if grpc_ingest_enabled:
        session_token_verifier = (
            SessionTokenVerifier(
                issuer=STREAM_SESSION_TOKEN_ISSUER,
                audience=STREAM_SESSION_TOKEN_AUDIENCE,
                key_cache=JwksKeyCache(STREAM_SESSION_JWKS_URL),
                status_cache=SessionStatusCache(
                    STREAM_SESSION_STATUS_URL_TEMPLATE
                ),
            )
            if STREAM_SESSION_AUTH_ENABLED
            else None
        )
        grpc_ingest_service.configure(
            bind=GRPC_INGEST_BIND,
            enabled=True,
            expected_device_ids=[config.device_id for config in grpc_camera_configs],
            session_token_verifier=session_token_verifier,
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
    processing_live_relay_v2_client.stop()
    stream_sync_service.clear()
    clear_stream_experiment_recorder()
    logger.info(format_log_event("camera_sessions_stopped"))

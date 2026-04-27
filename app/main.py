import asyncio
import logging
from fastapi import FastAPI

from app.config.cameras import (
    CAMERA_SESSIONS_ENABLED,
    build_camera_session_configs_from_env,
)
from app.db import Base, engine, SessionLocal, ensure_database_schema
from app.config.server import (
    AUTO_SYNC_ENABLED,
    EXPERIMENT_ID,
    EXPERIMENT_LOG_DIR,
    FRAME_COMPRESS_AFTER_SEC,
    FRAME_COMPRESS_BATCH_SIZE,
    FRAME_COMPRESS_JPEG_QUALITY,
    FRAME_MAINTENANCE_INTERVAL_SEC,
    PROCESSING_SERVER_URL,
    STORAGE_DIR,
    STREAM_RELAY_ENABLED,
    STREAM_RELAY_TARGET,
    STREAM_RELAY_TIMEOUT_SEC,
)
from app.logging_config import configure_logging, format_log_event
from app.routes.debug import router as debug_router
from app.routes.frames import router as frames_router
from app.routes.monitoring import router as monitoring_router
from app.routes.sync import router as sync_router
from app.services.camera_session_manager import camera_session_manager
from app.services.frame_maintenance_service import compress_old_dispatched_frames
from app.services.stream_relay_service import stream_relay_service
from app.services.stream_experiment_service import (
    clear_stream_experiment_recorder,
    configure_stream_experiment_recorder,
)
from app.services.sync_service import (
    build_sync_groups,
    dispatch_sync_group,
    get_groups_ready_for_retry,
    get_sync_group_by_id,
    record_sync_group_dispatch_result,
)

configure_logging()

logger = logging.getLogger("gc_image_stream.app")

Base.metadata.create_all(bind=engine)
ensure_database_schema()

app = FastAPI(
    title="GC Image Stream",
    summary="멀티 카메라 프레임 수집 및 스트림 릴레이를 담당하는 컬렉션 백엔드",
    description=(
        "GC Image Stream은 카메라 앱 또는 수집기에서 전달된 프레임 데이터를 받아 파일과 "
        "메타데이터를 저장하고, gRPC relay를 통해 외부 processing server로 프레임 스트림을 "
        "전달하는 수집 서버입니다. 기존 sync group 기반 HTTP dispatch는 fallback 및 "
        "디버깅 경로로 유지됩니다."
    ),
)

app.include_router(frames_router)
app.include_router(sync_router)
app.include_router(monitoring_router)
app.include_router(debug_router)

AUTO_SYNC_THRESHOLD_MS = 200
AUTO_SYNC_INTERVAL_SEC = 1.0
FRAME_COMPRESS_AFTER_MS = int(FRAME_COMPRESS_AFTER_SEC * 1000)


# 주기적으로 sync group 생성과 자동 dispatch/retry를 수행한다.
async def auto_sync_loop():
    while True:
        db = SessionLocal()
        try:
            groups = build_sync_groups(db, threshold_ms=AUTO_SYNC_THRESHOLD_MS)
            retry_groups = get_groups_ready_for_retry(db)

            if groups:
                logger.info(
                    format_log_event(
                        "sync_groups_created",
                        source="auto",
                        count=len(groups),
                    )
                )

            if retry_groups:
                logger.info(
                    format_log_event(
                        "sync_groups_retrying",
                        source="auto",
                        count=len(retry_groups),
                    )
                )

            dispatch_targets = [
                {"id": group.id}
                for group in groups
            ] + [
                {"id": group["id"]}
                for group in retry_groups
            ]

            for target in dispatch_targets:
                group_id = target["id"]
                try:
                    group_data = get_sync_group_by_id(db, group_id)

                    if group_data is None:
                        logger.warning(
                            format_log_event(
                                "sync_group_dispatch_skipped",
                                source="auto",
                                group_id=group_id,
                                reason="group_not_found",
                            )
                        )
                        continue

                    result = await dispatch_sync_group(group_data, PROCESSING_SERVER_URL)
                    record_sync_group_dispatch_result(db, group_id, result, source="auto")

                    if result.get("success"):
                        logger.info(
                            format_log_event(
                                "sync_group_dispatch_succeeded",
                                source="auto",
                                group_id=group_id,
                                status_code=result.get("status_code"),
                            )
                        )
                    else:
                        logger.warning(
                            format_log_event(
                                "sync_group_dispatch_failed",
                                source="auto",
                                group_id=group_id,
                                status_code=result.get("status_code"),
                                error=result.get("error"),
                            )
                        )

                except Exception as e:
                    logger.exception(
                        format_log_event(
                            "sync_group_dispatch_error",
                            source="auto",
                            group_id=group_id,
                            error=str(e),
                        )
                    )

        except Exception as e:
            logger.exception(
                format_log_event(
                    "auto_sync_loop_error",
                    error=str(e),
                )
            )
        finally:
            db.close()

        await asyncio.sleep(AUTO_SYNC_INTERVAL_SEC)


# 주기적으로 성공 dispatch 이후 오래된 프레임을 재압축한다.
async def frame_maintenance_loop():
    while True:
        db = SessionLocal()
        try:
            compress_old_dispatched_frames(
                db,
                compress_after_ms=FRAME_COMPRESS_AFTER_MS,
                quality=FRAME_COMPRESS_JPEG_QUALITY,
                limit=FRAME_COMPRESS_BATCH_SIZE,
            )
        except Exception as e:
            logger.exception(
                format_log_event(
                    "frame_maintenance_loop_error",
                    error=str(e),
                )
            )
        finally:
            db.close()

        await asyncio.sleep(FRAME_MAINTENANCE_INTERVAL_SEC)


# 서버 시작 시 필요한 백그라운드 루프를 띄운다.
@app.on_event("startup")
async def startup_event():
    camera_configs = []
    if CAMERA_SESSIONS_ENABLED:
        camera_configs = build_camera_session_configs_from_env()

    configure_stream_experiment_recorder(
        experiment_log_dir=EXPERIMENT_LOG_DIR,
        experiment_id=EXPERIMENT_ID,
        storage_dir=STORAGE_DIR,
        relay_target=STREAM_RELAY_TARGET if STREAM_RELAY_ENABLED else "",
        camera_ids=[config.device_id for config in camera_configs],
    )

    if STREAM_RELAY_ENABLED:
        stream_relay_service.configure(
            target=STREAM_RELAY_TARGET,
            timeout_sec=STREAM_RELAY_TIMEOUT_SEC,
            enabled=True,
        )
        stream_relay_service.start()
        logger.info(
            format_log_event(
                "stream_relay_started",
                target=STREAM_RELAY_TARGET,
            )
        )
    else:
        stream_relay_service.configure(
            target="",
            timeout_sec=STREAM_RELAY_TIMEOUT_SEC,
            enabled=False,
        )
        logger.info(format_log_event("stream_relay_disabled"))

    if CAMERA_SESSIONS_ENABLED:
        camera_session_manager.start_all(camera_configs)
        logger.info(
            format_log_event(
                "camera_sessions_started",
                count=len(camera_configs),
            )
        )
    else:
        logger.info(
            format_log_event(
                "camera_sessions_disabled",
            )
        )

    if AUTO_SYNC_ENABLED:
        asyncio.create_task(auto_sync_loop())
        logger.info(
            format_log_event(
                "auto_sync_started",
                interval_sec=AUTO_SYNC_INTERVAL_SEC,
                threshold_ms=AUTO_SYNC_THRESHOLD_MS,
                dispatch_url=PROCESSING_SERVER_URL,
            )
        )
    else:
        logger.info(
            format_log_event(
                "auto_sync_disabled",
                reason="grpc_relay_primary_path",
            )
        )

    asyncio.create_task(frame_maintenance_loop())
    logger.info(
        format_log_event(
            "frame_maintenance_started",
            interval_sec=FRAME_MAINTENANCE_INTERVAL_SEC,
            compress_after_sec=FRAME_COMPRESS_AFTER_SEC,
            quality=FRAME_COMPRESS_JPEG_QUALITY,
            batch_size=FRAME_COMPRESS_BATCH_SIZE,
        )
    )


@app.on_event("shutdown")
async def shutdown_event():
    camera_session_manager.stop_all()
    stream_relay_service.stop()
    clear_stream_experiment_recorder()
    logger.info(format_log_event("camera_sessions_stopped"))


# 간단한 서버 상태 확인 응답을 반환한다.
@app.get(
    "/",
    summary="서비스 상태 확인",
    description="수집 서버가 실행 중인지 간단히 확인하기 위한 루트 엔드포인트입니다.",
)
def root():
    return {"message": "GC Image Stream server is running"}

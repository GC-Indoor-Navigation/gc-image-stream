import asyncio
import logging

from app.core.logging import format_log_event
from app.core.server import (
    FRAME_COMPRESS_AFTER_SEC,
    FRAME_COMPRESS_BATCH_SIZE,
    FRAME_COMPRESS_JPEG_QUALITY,
    FRAME_MAINTENANCE_INTERVAL_SEC,
    PROCESSING_SERVER_URL,
)
from app.db import SessionLocal
from app.services.frame_maintenance_service import compress_old_dispatched_frames
from app.services.sync_service import (
    build_sync_groups,
    dispatch_sync_group,
    get_groups_ready_for_retry,
    get_sync_group_by_id,
    record_sync_group_dispatch_result,
)


logger = logging.getLogger("gc_image_stream.app")

AUTO_SYNC_THRESHOLD_MS = 200
AUTO_SYNC_INTERVAL_SEC = 1.0
FRAME_COMPRESS_AFTER_MS = int(FRAME_COMPRESS_AFTER_SEC * 1000)


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

                except Exception as exc:
                    logger.exception(
                        format_log_event(
                            "sync_group_dispatch_error",
                            source="auto",
                            group_id=group_id,
                            error=str(exc),
                        )
                    )

        except Exception as exc:
            logger.exception(
                format_log_event(
                    "auto_sync_loop_error",
                    error=str(exc),
                )
            )
        finally:
            db.close()

        await asyncio.sleep(AUTO_SYNC_INTERVAL_SEC)


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
        except Exception as exc:
            logger.exception(
                format_log_event(
                    "frame_maintenance_loop_error",
                    error=str(exc),
                )
            )
        finally:
            db.close()

        await asyncio.sleep(FRAME_MAINTENANCE_INTERVAL_SEC)


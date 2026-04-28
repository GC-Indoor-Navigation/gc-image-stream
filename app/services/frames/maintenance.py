import logging
import os
import time
from pathlib import Path

from sqlalchemy.orm import Session

from app.core.logging import format_log_event
from app.models import Frame, SyncFrame, SyncGroup
from app.services.sync.service import DISPATCH_STATUS_SUCCESS


logger = logging.getLogger("gc_image_stream.frame_maintenance")


# 압축 대상이 되는 오래된 성공 dispatch 프레임을 조회한다.
def get_frames_ready_for_compression(
    db: Session,
    older_than_ms: int,
    limit: int = 100,
):
    return (
        db.query(Frame)
        .join(SyncFrame, SyncFrame.frame_id == Frame.id)
        .join(SyncGroup, SyncGroup.id == SyncFrame.sync_group_id)
        .filter(SyncGroup.dispatch_status == DISPATCH_STATUS_SUCCESS)
        .filter(Frame.timestamp <= older_than_ms)
        .filter(Frame.compressed_at.is_(None))
        .order_by(Frame.timestamp.asc(), Frame.id.asc())
        .limit(limit)
        .all()
    )


# JPEG 파일을 같은 경로에 더 낮은 품질로 다시 저장한다.
def compress_frame_file(file_path: str, quality: int = 60):
    try:
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError(
            "Pillow is required for frame compression"
        ) from exc

    path = Path(file_path)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")

    with Image.open(path) as image:
        image.convert("RGB").save(
            temp_path,
            format="JPEG",
            optimize=True,
            quality=quality,
        )

    os.replace(temp_path, path)


# 오래된 성공 dispatch 프레임을 재압축하고 메타데이터를 갱신한다.
def compress_old_dispatched_frames(
    db: Session,
    compress_after_ms: int,
    quality: int = 60,
    limit: int = 100,
    now_ms: int | None = None,
):
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    older_than_ms = now_ms - compress_after_ms
    candidates = get_frames_ready_for_compression(
        db,
        older_than_ms=older_than_ms,
        limit=limit,
    )

    compressed_count = 0

    for frame in candidates:
        try:
            compress_frame_file(frame.file_path, quality=quality)
        except FileNotFoundError:
            logger.warning(
                format_log_event(
                    "frame_compression_skipped",
                    frame_id=frame.id,
                    file_path=frame.file_path,
                    reason="file_not_found",
                )
            )
            continue
        except RuntimeError as exc:
            logger.warning(
                format_log_event(
                    "frame_compression_unavailable",
                    frame_id=frame.id,
                    error=str(exc),
                )
            )
            break
        except Exception as exc:
            logger.warning(
                format_log_event(
                    "frame_compression_failed",
                    frame_id=frame.id,
                    file_path=frame.file_path,
                    error=str(exc),
                )
            )
            continue

        frame.compressed_at = now_ms
        compressed_count += 1

    if compressed_count:
        db.commit()
        logger.info(
            format_log_event(
                "frame_compression_completed",
                count=compressed_count,
                quality=quality,
                cutoff_ms=older_than_ms,
            )
        )
    else:
        db.rollback()

    return compressed_count

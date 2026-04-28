import logging
import time
from typing import Literal

import httpx
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.core.logging import format_log_event
from app.models import Frame, SyncGroup, SyncFrame


DISPATCH_STATUS_PENDING = "pending"
DISPATCH_STATUS_RETRY_SCHEDULED = "retry_scheduled"
DISPATCH_STATUS_SUCCESS = "success"
DISPATCH_STATUS_FAILED = "failed"
DISPATCH_STATUS_EXHAUSTED = "exhausted"
RETRYABLE_HTTP_STATUS_CODES = {502, 503, 504}
MAX_DISPATCH_RETRIES = 3
RETRY_DELAYS_MS = (5_000, 15_000, 30_000)


logger = logging.getLogger("gc_image_stream.sync")

SYNC_GROUP_SORT_FIELDS = {
    "id": SyncGroup.id,
    "group_timestamp": SyncGroup.group_timestamp,
    "last_dispatch_at": SyncGroup.last_dispatch_at,
    "next_retry_at": SyncGroup.next_retry_at,
    "retry_count": SyncGroup.retry_count,
}

# 지정한 frame ID들로 sync group 하나를 생성한다.
def create_sync_group(db: Session, group_timestamp: int, frame_ids: list[int]) -> SyncGroup:
    group = SyncGroup(
        group_timestamp=group_timestamp,
        dispatch_status=DISPATCH_STATUS_PENDING,
    )
    db.add(group)
    db.flush()

    for frame_id in frame_ids:
        item = SyncFrame(sync_group_id=group.id, frame_id=frame_id)
        db.add(item)

    db.commit()
    db.refresh(group)
    return group


# ORM sync group을 API 응답용 dict로 직렬화한다.
def serialize_sync_group(group: SyncGroup):
    return {
        "id": group.id,
        "group_timestamp": group.group_timestamp,
        "dispatch_status": group.dispatch_status,
        "last_dispatch_at": group.last_dispatch_at,
        "last_dispatch_status_code": group.last_dispatch_status_code,
        "last_dispatch_error": group.last_dispatch_error,
        "dispatched_at": group.dispatched_at,
        "retry_count": group.retry_count,
        "next_retry_at": group.next_retry_at,
        "frames": [
            {
                "id": item.frame.id,
                "device_id": item.frame.device_id,
                "timestamp": item.frame.timestamp,
                "file_path": item.frame.file_path,
            }
            for item in group.frames
        ],
    }


# sync group 기본 조회 쿼리를 구성한다.
def build_sync_group_query(db: Session):
    return (
        db.query(SyncGroup)
        .options(joinedload(SyncGroup.frames).joinedload(SyncFrame.frame))
    )


# 필터, 정렬, 페이지네이션 조건으로 sync group 목록을 조회한다.
def get_sync_groups(
    db: Session,
    limit: int = 20,
    offset: int = 0,
    dispatch_status: str | None = None,
    retry_ready: bool | None = None,
    exhausted: bool | None = None,
    sort_by: Literal[
        "id",
        "group_timestamp",
        "last_dispatch_at",
        "next_retry_at",
        "retry_count",
    ] = "group_timestamp",
    sort_order: Literal["asc", "desc"] = "desc",
    now_ms: int | None = None,
):
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    query = build_sync_group_query(db)

    if dispatch_status is not None:
        query = query.filter(SyncGroup.dispatch_status == dispatch_status)

    if retry_ready is True:
        query = (
            query
            .filter(SyncGroup.dispatch_status == DISPATCH_STATUS_RETRY_SCHEDULED)
            .filter(SyncGroup.next_retry_at.is_not(None))
            .filter(SyncGroup.next_retry_at <= now_ms)
        )
    elif retry_ready is False:
        query = (
            query
            .filter(
                (SyncGroup.next_retry_at.is_(None)) |
                (SyncGroup.next_retry_at > now_ms)
            )
        )

    if exhausted is True:
        query = (
            query
            .filter(SyncGroup.dispatch_status == DISPATCH_STATUS_EXHAUSTED)
        )
    elif exhausted is False:
        query = (
            query
            .filter(SyncGroup.dispatch_status != DISPATCH_STATUS_EXHAUSTED)
        )

    total = query.count()

    sort_column = SYNC_GROUP_SORT_FIELDS[sort_by]
    ordered_column = (
        sort_column.asc() if sort_order == "asc" else sort_column.desc()
    )
    ordered_group_id = (
        SyncGroup.id.asc() if sort_order == "asc" else SyncGroup.id.desc()
    )

    groups = (
        query
        .order_by(sort_column.is_(None), ordered_column, ordered_group_id)
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [serialize_sync_group(group) for group in groups],
    }


# 현재 sync group 상태를 집계해서 운영 요약 정보를 만든다.
def get_sync_summary(db: Session, now_ms: int | None = None):
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    status_counts = {
        status: count
        for status, count in (
            db.query(SyncGroup.dispatch_status, func.count(SyncGroup.id))
            .group_by(SyncGroup.dispatch_status)
            .all()
        )
    }

    retry_ready = (
        db.query(func.count(SyncGroup.id))
        .filter(SyncGroup.dispatch_status == DISPATCH_STATUS_RETRY_SCHEDULED)
        .filter(SyncGroup.next_retry_at.is_not(None))
        .filter(SyncGroup.next_retry_at <= now_ms)
        .scalar()
    ) or 0

    return {
        "total_groups": sum(status_counts.values()),
        "pending": status_counts.get(DISPATCH_STATUS_PENDING, 0),
        "retry_scheduled": status_counts.get(DISPATCH_STATUS_RETRY_SCHEDULED, 0),
        "success": status_counts.get(DISPATCH_STATUS_SUCCESS, 0),
        "failed": status_counts.get(DISPATCH_STATUS_FAILED, 0),
        "exhausted": status_counts.get(DISPATCH_STATUS_EXHAUSTED, 0),
        "retry_ready": retry_ready,
    }


# ID로 sync group 하나를 조회한다.
def get_sync_group_by_id(db: Session, group_id: int):
    group = (
        db.query(SyncGroup)
        .options(joinedload(SyncGroup.frames).joinedload(SyncFrame.frame))
        .filter(SyncGroup.id == group_id)
        .first()
    )

    if not group:
        return None

    return serialize_sync_group(group)


# 아직 어떤 sync group에도 속하지 않은 프레임만 조회한다.
def get_unsynced_frames(db: Session):
    return (
        db.query(Frame)
        .outerjoin(SyncFrame, SyncFrame.frame_id == Frame.id)
        .filter(SyncFrame.frame_id.is_(None))
        .order_by(Frame.timestamp.asc())
        .all()
    )


# threshold 안의 프레임들을 묶어 새로운 sync group을 만든다.
def build_sync_groups(db: Session, threshold_ms: int = 50):
    all_frames = get_unsynced_frames(db)

    used_frame_ids = set()
    created_groups = []
    window_start = 0
    window_end = 0

    for base_frame in all_frames:
        if base_frame.id in used_frame_ids:
            continue

        lower_bound = base_frame.timestamp - threshold_ms
        upper_bound = base_frame.timestamp + threshold_ms

        while window_start < len(all_frames) and all_frames[window_start].timestamp < lower_bound:
            window_start += 1

        if window_end < window_start:
            window_end = window_start

        while window_end < len(all_frames) and all_frames[window_end].timestamp <= upper_bound:
            window_end += 1

        selected = [base_frame]
        used_devices = {base_frame.device_id}

        for candidate in all_frames[window_start:window_end]:
            if candidate.id in used_frame_ids:
                continue
            if candidate.id == base_frame.id:
                continue
            if candidate.device_id in used_devices:
                continue

            if abs(candidate.timestamp - base_frame.timestamp) <= threshold_ms:
                selected.append(candidate)
                used_devices.add(candidate.device_id)

        if len(selected) >= 2:
            group = SyncGroup(
                group_timestamp=base_frame.timestamp,
                dispatch_status=DISPATCH_STATUS_PENDING,
            )
            db.add(group)
            db.flush()

            for frame in selected:
                db.add(SyncFrame(sync_group_id=group.id, frame_id=frame.id))
                used_frame_ids.add(frame.id)

            created_groups.append(group)

    if created_groups:
        db.commit()
        for group in created_groups:
            db.refresh(group)

    return created_groups


# 지금 시점에 재시도 가능한 sync group 목록을 조회한다.
def get_groups_ready_for_retry(
    db: Session,
    now_ms: int | None = None,
    limit: int = 20,
):
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    groups = (
        build_sync_group_query(db)
        .filter(SyncGroup.dispatch_status == DISPATCH_STATUS_RETRY_SCHEDULED)
        .filter(SyncGroup.next_retry_at.is_not(None))
        .filter(SyncGroup.next_retry_at <= now_ms)
        .order_by(SyncGroup.next_retry_at.asc(), SyncGroup.group_timestamp.asc())
        .limit(limit)
        .all()
    )

    return [serialize_sync_group(group) for group in groups]


# dispatch 실패가 자동 재시도 대상인지 판단한다.
def is_retryable_dispatch_result(result: dict):
    if result.get("success"):
        return False

    status_code = result.get("status_code")
    if status_code in RETRYABLE_HTTP_STATUS_CODES:
        return True

    error = result.get("error")
    return error in {
        "Processing server connection failed",
        "Processing server timeout",
    }


# 재시도 횟수에 맞는 다음 지연 시간을 계산한다.
def get_retry_delay_ms(retry_count: int):
    index = min(max(retry_count - 1, 0), len(RETRY_DELAYS_MS) - 1)
    return RETRY_DELAYS_MS[index]


# 수동 재시도가 허용되는 상태인지 판단한다.
def can_manually_retry_group(group: dict):
    return group["dispatch_status"] != DISPATCH_STATUS_SUCCESS


# dispatch 결과를 sync group 상태와 재시도 정보에 반영한다.
def record_sync_group_dispatch_result(
    db: Session,
    group_id: int,
    result: dict,
    source: str = "manual",
):
    group = db.query(SyncGroup).filter(SyncGroup.id == group_id).first()

    if group is None:
        return None

    attempted_at = int(time.time() * 1000)
    success = bool(result.get("success"))
    retryable = is_retryable_dispatch_result(result)

    group.dispatch_status = (
        DISPATCH_STATUS_SUCCESS if success else DISPATCH_STATUS_FAILED
    )
    group.last_dispatch_at = attempted_at
    group.last_dispatch_status_code = result.get("status_code")
    group.last_dispatch_error = None if success else result.get("error")

    if success:
        group.dispatched_at = attempted_at
        group.retry_count = 0
        group.next_retry_at = None
    else:
        group.retry_count += 1
        if retryable and group.retry_count < MAX_DISPATCH_RETRIES:
            group.dispatch_status = DISPATCH_STATUS_RETRY_SCHEDULED
            group.next_retry_at = attempted_at + get_retry_delay_ms(group.retry_count)
        elif retryable:
            group.dispatch_status = DISPATCH_STATUS_EXHAUSTED
            group.next_retry_at = None
        else:
            group.dispatch_status = DISPATCH_STATUS_FAILED
            group.next_retry_at = None

    db.commit()
    db.refresh(group)

    logger.info(
        format_log_event(
            "sync_group_dispatch_recorded",
            source=source,
            group_id=group.id,
            dispatch_status=group.dispatch_status,
            status_code=group.last_dispatch_status_code,
            retry_count=group.retry_count,
            next_retry_at=group.next_retry_at,
            error=group.last_dispatch_error,
        )
    )

    return group


# 처리 서버로 보낼 payload 형태를 만든다.
def build_dispatch_payload(group: dict):
    return {
        "syncGroupId": group["id"],
        "groupTimestamp": group["group_timestamp"],
        "frames": [
            {
                "frameId": frame["id"],
                "deviceId": frame["device_id"],
                "timestamp": frame["timestamp"],
                "filePath": frame["file_path"],
            }
            for frame in group["frames"]
        ]
    }


# HTTP 응답 본문을 JSON 또는 문자열로 정리한다.
def parse_response_body(response: httpx.Response):
    if not response.content:
        return None

    try:
        return response.json()
    except ValueError:
        return response.text


# sync group을 처리 서버로 전송하고 결과를 표준 dict로 반환한다.
async def dispatch_sync_group(group: dict, processing_server_url: str):
    payload = build_dispatch_payload(group)

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(processing_server_url, json=payload)
            response_body = parse_response_body(response)
            response.raise_for_status()

        return {
            "success": True,
            "status_code": response.status_code,
            "response_body": response_body,
            "payload": payload
        }

    except httpx.HTTPStatusError as exc:
        return {
            "success": False,
            "error": f"Processing server returned HTTP {exc.response.status_code}",
            "status_code": exc.response.status_code,
            "response_body": parse_response_body(exc.response),
            "payload": payload,
        }

    except httpx.ConnectError:
        return {
            "success": False,
            "error": "Processing server connection failed",
            "payload": payload
        }

    except httpx.ReadTimeout:
        return {
            "success": False,
            "error": "Processing server timeout",
            "payload": payload
        }

    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "payload": payload
        }

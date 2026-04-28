from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.server import PROCESSING_SERVER_URL
from app.db import get_db
from app.schemas import SyncGroupListResponse, SyncGroupResponse, SyncSummaryResponse
from app.services.sync_service import (
    build_sync_groups,
    can_manually_retry_group,
    dispatch_sync_group,
    get_sync_group_by_id,
    get_sync_groups,
    get_sync_summary,
    record_sync_group_dispatch_result,
)

router = APIRouter(prefix="/sync", tags=["sync"])


# 아직 묶이지 않은 프레임으로 새 sync group을 만든다.
@router.post(
    "/build",
    summary="Sync 그룹 생성",
    description=(
        "아직 sync되지 않은 프레임을 스캔해서, 서로 다른 디바이스의 프레임 중 timestamp 차이가 "
        "주어진 threshold 범위 안에 들어오는 것들을 하나의 sync group으로 묶습니다. 새로 "
        "생성된 그룹은 `pending` 상태로 시작합니다."
    ),
    response_description="이번 요청으로 생성된 sync group 개수입니다.",
)
def build_groups(
    threshold_ms: int = Query(
        default=50,
        ge=0,
        description="하나의 sync group 안에서 허용할 최대 timestamp 차이입니다. 밀리초 단위입니다.",
    ),
    db: Session = Depends(get_db),
):
    groups = build_sync_groups(db, threshold_ms=threshold_ms)
    return {
        "created_count": len(groups),
    }


# 필터, 정렬, 페이지네이션 조건으로 sync group 목록을 조회한다.
@router.get(
    "/groups",
    response_model=SyncGroupListResponse,
    summary="Sync 그룹 목록 조회",
    description=(
        "운영용 필터, 페이지네이션, 정렬 옵션과 함께 sync group 목록을 조회합니다. 대기열 상태, "
        "재시도 상태, dispatch 이력을 확인할 때 사용합니다."
    ),
    response_description="총 개수와 dispatch 메타데이터를 포함한 페이지 단위 sync group 목록입니다.",
)
def list_groups(
    limit: int = Query(default=20, ge=1, le=100, description="반환할 sync group의 최대 개수입니다."),
    offset: int = Query(default=0, ge=0, description="페이지네이션을 위한 시작 offset입니다."),
    status: str | None = Query(
        default=None,
        description="dispatch 상태 필터입니다. pending, retry_scheduled, success, failed, exhausted 중 하나를 사용합니다.",
    ),
    retry_ready: bool | None = Query(
        default=None,
        description="true면 retry 예정 시각이 이미 도래한 retry_scheduled 그룹만 조회합니다.",
    ),
    exhausted: bool | None = Query(
        default=None,
        description="true면 재시도 한도에 도달한 exhausted 그룹만 조회합니다.",
    ),
    sort_by: Literal[
        "id",
        "group_timestamp",
        "last_dispatch_at",
        "next_retry_at",
        "retry_count",
    ] = Query(default="group_timestamp", description="sync group 목록 정렬에 사용할 필드입니다."),
    sort_order: Literal["asc", "desc"] = Query(
        default="desc",
        description="선택한 정렬 필드에 대한 정렬 방향입니다.",
    ),
    db: Session = Depends(get_db),
):
    return get_sync_groups(
        db,
        limit=limit,
        offset=offset,
        dispatch_status=status,
        retry_ready=retry_ready,
        exhausted=exhausted,
        sort_by=sort_by,
        sort_order=sort_order,
    )


# dispatch 상태 기준 운영 요약 정보를 반환한다.
@router.get(
    "/summary",
    response_model=SyncSummaryResponse,
    summary="Sync 큐 요약 조회",
    description=(
        "dispatch 상태별 sync group 개수를 요약해서 반환합니다. 현재 시점에 재시도 가능한 "
        "retry_scheduled 그룹 수까지 함께 제공합니다."
    ),
    response_description="sync group 상태에 대한 운영 요약 정보입니다.",
)
def get_summary(
    db: Session = Depends(get_db),
):
    return get_sync_summary(db)


# 특정 sync group의 상세 정보를 조회한다.
@router.get(
    "/groups/{group_id}",
    response_model=SyncGroupResponse,
    summary="Sync 그룹 상세 조회",
    description=(
        "하나의 sync group을 조회하며, 포함된 프레임 목록과 마지막 dispatch 시도 시각, "
        "재시도 횟수, 다음 재시도 예정 시각, 최종 전달 상태 같은 메타데이터를 함께 반환합니다."
    ),
    response_description="상세 sync group 정보입니다.",
    responses={404: {"description": "요청한 sync group이 존재하지 않습니다."}},
)
def get_group(
    group_id: int,
    db: Session = Depends(get_db),
):
    group = get_sync_group_by_id(db, group_id)

    if group is None:
        raise HTTPException(status_code=404, detail="Sync group not found")

    return group


# 선택한 sync group을 즉시 처리 서버로 전송한다.
@router.post(
    "/groups/{group_id}/dispatch",
    summary="Sync 그룹 전송",
    description=(
        "선택한 sync group을 설정된 외부 처리 서버로 즉시 전송합니다. 전송 결과는 상태, "
        "상태 코드, 에러 메시지, 재시도 스케줄과 함께 기록됩니다."
    ),
    response_description="처리 서버 연동 결과입니다.",
    responses={404: {"description": "요청한 sync group이 존재하지 않습니다."}},
)
async def dispatch_group(
    group_id: int,
    db: Session = Depends(get_db),
):
    group = get_sync_group_by_id(db, group_id)

    if group is None:
        raise HTTPException(status_code=404, detail="Sync group not found")

    result = await dispatch_sync_group(group, PROCESSING_SERVER_URL)
    record_sync_group_dispatch_result(db, group_id, result, source="manual_dispatch")
    return result


# 성공하지 않은 sync group에 대해 수동 재시도를 수행한다.
@router.post(
    "/groups/{group_id}/retry",
    summary="Sync 그룹 수동 재시도",
    description=(
        "failed, exhausted, pending, retry_scheduled 상태의 sync group에 대해 수동으로 "
        "dispatch를 다시 시도합니다. 이미 성공한 그룹은 다시 전송하지 않습니다."
    ),
    response_description="수동 재시도 결과입니다.",
    responses={
        400: {"description": "이미 성공적으로 전송된 sync group입니다."},
        404: {"description": "요청한 sync group이 존재하지 않습니다."},
    },
)
async def retry_group(
    group_id: int,
    db: Session = Depends(get_db),
):
    group = get_sync_group_by_id(db, group_id)

    if group is None:
        raise HTTPException(status_code=404, detail="Sync group not found")

    if not can_manually_retry_group(group):
        raise HTTPException(
            status_code=400,
            detail="Successful sync groups cannot be retried manually",
        )

    result = await dispatch_sync_group(group, PROCESSING_SERVER_URL)
    record_sync_group_dispatch_result(db, group_id, result, source="manual_retry")
    return result

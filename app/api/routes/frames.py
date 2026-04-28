import os
import shutil

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from app.db import get_db
from app.schemas import FrameResponse
from app.services.frame_service import create_frame, get_frames
from app.utils.file_utils import build_frame_path, parse_filename

router = APIRouter(prefix="/frames", tags=["frames"])


# 업로드 실패 시 남은 파일을 정리한다.
def remove_file_if_exists(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


# 업로드된 이미지 파일을 저장하고 프레임 메타데이터를 등록한다.
@router.post(
    "/upload",
    response_model=FrameResponse,
    summary="프레임 이미지 업로드",
    description=(
        "카메라 클라이언트가 업로드한 이미지 파일을 받아 파일명에서 device ID와 촬영 "
        "timestamp를 추출하고, 파일을 디스크에 저장한 뒤 프레임 메타데이터를 DB에 기록합니다. "
        "메타데이터 저장에 실패하면 고아 파일이 남지 않도록 저장된 파일을 삭제합니다."
    ),
    response_description="업로드된 이미지에 대한 저장된 프레임 메타데이터입니다.",
    responses={
        400: {
            "description": (
                "파일명이 없거나 `<device_id>_<timestamp>.<ext>` 형식을 따르지 않습니다."
            )
        },
        500: {"description": "프레임 파일 저장 후 메타데이터를 DB에 기록하지 못했습니다."},
    },
)
async def upload_frame(
    file: UploadFile = File(
        ...,
        description=(
            "프레임 이미지 파일입니다. 파일명에는 device ID와 촬영 timestamp가 포함되어야 하며, "
            "예시는 `camera1_1712321562400.jpg` 입니다."
        ),
    ),
    db: Session = Depends(get_db),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required.")

    try:
        device_id, timestamp = parse_filename(file.filename)
    except ValueError:
        raise HTTPException(status_code=400, detail="Filename format is invalid.")

    save_path = build_frame_path(device_id, timestamp, file.filename)

    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        frame = create_frame(
            db=db,
            device_id=device_id,
            timestamp=timestamp,
            file_path=save_path,
        )
    except Exception as exc:
        db.rollback()
        remove_file_if_exists(save_path)
        raise HTTPException(
            status_code=500,
            detail="Failed to persist uploaded frame.",
        ) from exc

    return frame


# 파일 업로드 없이 프레임 메타데이터만 등록한다.
@router.post(
    "/register",
    response_model=FrameResponse,
    summary="프레임 메타데이터 등록",
    description=(
        "파일 업로드 없이 프레임 메타데이터만 등록합니다. 수집기가 이미 이미지를 저장해 둔 상태에서 "
        "device ID, timestamp, file path만 기록해야 할 때 사용합니다. 같은 device와 "
        "timestamp 조합에 대한 중복 등록은 idempotent하게 처리됩니다."
    ),
    response_description="등록된 프레임 메타데이터입니다.",
)
def register_frame(
    device_id: str = Form(..., description="프레임을 생성한 카메라 또는 디바이스 식별자입니다."),
    timestamp: int = Form(..., description="프레임 촬영 시각입니다. 밀리초 단위입니다."),
    file_path: str = Form(
        ...,
        description="수집기가 이미 저장한 이미지 파일 경로입니다.",
    ),
    db: Session = Depends(get_db),
):
    frame = create_frame(
        db=db,
        device_id=device_id,
        timestamp=timestamp,
        file_path=file_path,
    )
    return frame


# 최근 저장된 프레임 목록을 조회한다.
@router.get(
    "",
    response_model=list[FrameResponse],
    summary="수집된 프레임 목록 조회",
    description=(
        "로컬 DB에 저장된 최근 프레임 메타데이터를 조회합니다. 수집 서버에 어떤 프레임이 저장되어 "
        "있는지 운영 관점에서 확인할 때 사용합니다."
    ),
    response_description="수집 서버에 저장된 최근 프레임 레코드입니다.",
)
def list_frames(
    limit: int = Query(
        default=50,
        ge=1,
        le=200,
        description="반환할 프레임 레코드의 최대 개수입니다.",
    ),
    db: Session = Depends(get_db),
):
    return get_frames(db, limit)

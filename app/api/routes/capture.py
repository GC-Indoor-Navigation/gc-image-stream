import json
import os
import shutil
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.db import get_db
from app.infrastructure.storage.file_utils import build_calibration_frame_path
from app.schemas import FrameResponse
from app.services.frames.service import create_frame

router = APIRouter(prefix="/capture", tags=["capture"])


def remove_file_if_exists(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


@router.post(
    "/internal-calibration",
    response_model=FrameResponse,
    summary="Upload one internal calibration frame",
    description=(
        "Accept a single calibration JPEG over HTTP and store it under the "
        "device-specific calibration directory without using the gRPC relay path."
    ),
)
async def upload_internal_calibration_frame(
    file: UploadFile = File(...),
    device_id: str = Form(...),
    camera_id: str = Form(...),
    frame_sequence: int = Form(...),
    device_timestamp_ms: int = Form(...),
    db: Session = Depends(get_db),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required.")
    if device_timestamp_ms <= 0:
        raise HTTPException(status_code=400, detail="device_timestamp_ms must be greater than 0.")

    filename = f"{device_id}_{camera_id}_{frame_sequence}.jpg"
    save_path = build_calibration_frame_path(
        device_id=device_id,
        timestamp=device_timestamp_ms,
        filename=filename,
    )

    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    sidecar_path = f"{save_path}.metadata.json"
    sidecar_payload = {
        "capture_type": "internal_calibration",
        "device_id": device_id,
        "camera_id": camera_id,
        "frame_sequence": frame_sequence,
        "device_timestamp_ms": device_timestamp_ms,
        "content_type": file.content_type or "image/jpeg",
    }
    Path(sidecar_path).write_text(
        json.dumps(sidecar_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    try:
        frame = create_frame(
            db=db,
            device_id=device_id,
            timestamp=device_timestamp_ms,
            file_path=save_path,
        )
    except Exception as exc:
        db.rollback()
        remove_file_if_exists(save_path)
        remove_file_if_exists(sidecar_path)
        raise HTTPException(
            status_code=500,
            detail="Failed to persist calibration frame.",
        ) from exc

    return frame

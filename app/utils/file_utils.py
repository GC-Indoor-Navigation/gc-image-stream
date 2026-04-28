import os
import re
from datetime import datetime
from pathlib import Path

from app.core.server import STORAGE_DIR


# 디렉터리가 없으면 생성한다.
def ensure_dir(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)


# device와 timestamp 기준으로 프레임 저장 경로를 만든다.
def build_frame_path(device_id: str, timestamp: int, filename: str) -> str:
    dt = datetime.fromtimestamp(timestamp / 1000)

    folder = os.path.join(
        STORAGE_DIR,
        device_id,
        str(dt.year),
        f"{dt.month:02d}",
        f"{dt.day:02d}",
    )
    ensure_dir(folder)

    safe_name = f"{timestamp}_{filename}"
    return os.path.join(folder, safe_name)


# 업로드 파일명에서 camera 번호와 timestamp를 추출한다.
def parse_filename(filename: str):
    """
    camera1_1712321562400.jpg
    -> device_id: camera1
    -> timestamp: 1712321562400
    """
    pattern = r"^(camera\d+)_(\d+)\.jpg$"
    match = re.match(pattern, filename)

    if not match:
        raise ValueError("Filename format is invalid.")

    device_id = match.group(1)
    timestamp = int(match.group(2))

    return device_id, timestamp

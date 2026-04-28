from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Frame


# 프레임을 저장하고, 중복이면 기존 레코드를 반환한다.
def create_frame(db: Session, device_id: str, timestamp: int, file_path: str) -> Frame:
    frame = Frame(
        device_id=device_id,
        timestamp=timestamp,
        file_path=file_path,
    )
    db.add(frame)
    try:
        db.commit()
        db.refresh(frame)
        return frame
    except IntegrityError:
        db.rollback()
        existing = (
            db.query(Frame)
            .filter(Frame.device_id == device_id, Frame.timestamp == timestamp)
            .first()
        )
        if existing is not None:
            return existing
        raise


# 최신 순으로 프레임 목록을 조회한다.
def get_frames(db: Session, limit: int = 50):
    return db.query(Frame).order_by(Frame.timestamp.desc()).limit(limit).all()


# 최근 프레임 조회용 자리 함수다.
def get_recent_frames(db: Session, window_ms: int = 100):
    return db.query(Frame).order_by(Frame.timestamp.asc()).all()

from sqlalchemy import BigInteger, Column, Integer, String, UniqueConstraint

from app.db import Base


class Frame(Base):
    __tablename__ = "frames"
    __table_args__ = (
        UniqueConstraint("device_id", "timestamp", name="uq_frame_device_timestamp"),
    )

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, index=True, nullable=False)
    timestamp = Column(BigInteger, index=True, nullable=False)
    file_path = Column(String, nullable=False)

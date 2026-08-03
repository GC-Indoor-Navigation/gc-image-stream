from sqlalchemy import (
    BigInteger,
    Column,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)

from app.db import Base


class Frame(Base):
    __tablename__ = "frames"
    __table_args__ = (
        UniqueConstraint("source_frame_uid", name="uq_frame_source_uid"),
        UniqueConstraint(
            "source_session_id",
            "camera_stream_id",
            "frame_sequence",
            name="uq_frame_source_identity",
        ),
        Index(
            "uq_frame_legacy_device_timestamp",
            "device_id",
            "timestamp",
            unique=True,
            sqlite_where=text("identity_mode = 'LEGACY'"),
        ),
    )

    id = Column(Integer, primary_key=True, index=True)
    device_id = Column(String, index=True, nullable=False)
    timestamp = Column(BigInteger, index=True, nullable=False)
    file_path = Column(String, nullable=True)
    source_session_id = Column(String, nullable=True)
    camera_stream_id = Column(String, nullable=True)
    frame_sequence = Column(BigInteger, nullable=True)
    source_frame_uid = Column(String, nullable=True)
    content_digest = Column(String, nullable=True)
    identity_mode = Column(String, nullable=False, default="LEGACY", server_default="LEGACY")
    archive_state = Column(
        String,
        nullable=False,
        default="ARCHIVE_DURABLE",
        server_default="ARCHIVE_DURABLE",
    )
    archive_error = Column(String, nullable=True)
    file_size = Column(BigInteger, nullable=True)
    content_type = Column(String, nullable=True)
    received_at_ms = Column(BigInteger, nullable=True)
    capture_config_digest = Column(String, nullable=True)
    capture_metadata_json = Column(Text, nullable=True)

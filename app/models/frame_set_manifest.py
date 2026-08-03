from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)

from app.db import Base


class CaptureSession(Base):
    __tablename__ = "capture_sessions"

    id = Column(String, primary_key=True)
    state = Column(String, nullable=False, index=True)
    source_sessions_json = Column(Text, nullable=False)
    started_at_ms = Column(BigInteger, nullable=False)
    closed_at_ms = Column(BigInteger, nullable=True)
    close_reason = Column(String, nullable=True)


class CaptureRun(Base):
    __tablename__ = "capture_runs"

    id = Column(String, primary_key=True)
    capture_session_id = Column(
        String,
        ForeignKey("capture_sessions.id"),
        nullable=False,
        index=True,
    )
    identity_mode = Column(String, nullable=False)
    state = Column(String, nullable=False, index=True)
    started_at_ms = Column(BigInteger, nullable=False)
    closed_at_ms = Column(BigInteger, nullable=True)
    close_reason = Column(String, nullable=True)


class FrameSetManifest(Base):
    __tablename__ = "frame_set_manifests"
    __table_args__ = (
        UniqueConstraint(
            "capture_run_id",
            "frame_set_id",
            name="uq_manifest_capture_run_sequence",
        ),
    )

    frame_set_uid = Column(String, primary_key=True)
    capture_session_id = Column(String, nullable=False, index=True)
    capture_run_id = Column(
        String,
        ForeignKey("capture_runs.id"),
        nullable=False,
        index=True,
    )
    frame_set_id = Column(BigInteger, nullable=False)
    anchor_timestamp_ms = Column(BigInteger, nullable=False)
    freshness_origin_ms = Column(BigInteger, nullable=False)
    synchronization_span_ms = Column(BigInteger, nullable=False)
    manifest_digest = Column(String, nullable=False, unique=True)
    manifest_json = Column(Text, nullable=False)
    created_at_ms = Column(BigInteger, nullable=False)


class FrameSetMember(Base):
    __tablename__ = "frame_set_members"
    __table_args__ = (
        UniqueConstraint(
            "frame_set_uid",
            "camera_stream_id",
            name="uq_manifest_member_camera",
        ),
        UniqueConstraint("source_frame_uid", name="uq_manifest_source_frame"),
    )

    id = Column(Integer, primary_key=True)
    frame_set_uid = Column(
        String,
        ForeignKey("frame_set_manifests.frame_set_uid"),
        nullable=False,
        index=True,
    )
    frame_id = Column(Integer, nullable=True)
    source_frame_uid = Column(String, nullable=False)
    source_session_id = Column(String, nullable=False)
    camera_stream_id = Column(String, nullable=False)
    frame_sequence = Column(BigInteger, nullable=False)
    capture_timestamp_ms = Column(BigInteger, nullable=False)
    content_type = Column(String, nullable=False)
    image_size = Column(BigInteger, nullable=False)
    content_digest = Column(String, nullable=False)
    file_path = Column(String, nullable=False)

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
    archive_state = Column(
        String,
        nullable=False,
        default="ARCHIVE_DURABLE",
        server_default="ARCHIVE_DURABLE",
    )
    archive_error = Column(String, nullable=True)
    sync_window_ms = Column(BigInteger, nullable=False, default=0, server_default="0")
    synchronized_at_ms = Column(
        BigInteger,
        nullable=False,
        default=0,
        server_default="0",
    )
    member_count = Column(Integer, nullable=False, default=0, server_default="0")


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


class ArchiveReconciliationIssue(Base):
    __tablename__ = "archive_reconciliation_issues"

    id = Column(Integer, primary_key=True)
    reconciliation_run_id = Column(String, nullable=False, index=True)
    issue_type = Column(String, nullable=False, index=True)
    frame_id = Column(Integer, nullable=True, index=True)
    frame_set_uid = Column(String, nullable=True, index=True)
    file_path = Column(String, nullable=True)
    detail = Column(Text, nullable=False)
    detected_at_ms = Column(BigInteger, nullable=False)


class FrameSetDeliveryProjection(Base):
    __tablename__ = "frame_set_delivery_projections"

    frame_set_uid = Column(
        String,
        ForeignKey("frame_set_manifests.frame_set_uid"),
        primary_key=True,
    )
    archive_state = Column(String, nullable=False)
    live_state = Column(String, nullable=False)
    legacy_relay_state = Column(String, nullable=False)
    last_reason = Column(String, nullable=True)
    updated_at_ms = Column(BigInteger, nullable=False)


class RelayV2ClientState(Base):
    __tablename__ = "relay_v2_client_state"

    singleton_id = Column(Integer, primary_key=True)
    in_flight_frame_set_uid = Column(String, nullable=True)
    in_flight_credit_id = Column(String, nullable=True)
    in_flight_processor_instance_id = Column(String, nullable=True)
    in_flight_stream_epoch = Column(String, nullable=True)
    in_flight_offered_at_ms = Column(BigInteger, nullable=True)
    watermark_capture_run_id = Column(String, nullable=True)
    watermark_frame_set_id = Column(BigInteger, nullable=True)
    watermark_frame_set_uid = Column(String, nullable=True)
    reoffer_frame_set_uid = Column(String, nullable=True)
    processing_job_id = Column(String, nullable=True)
    processing_job_capture_run_id = Column(String, nullable=True)
    updated_at_ms = Column(BigInteger, nullable=False)

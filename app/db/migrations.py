import time

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


FRAME_V2_COLUMNS = {
    "source_session_id",
    "camera_stream_id",
    "frame_sequence",
    "source_frame_uid",
    "content_digest",
    "identity_mode",
    "archive_state",
    "archive_error",
    "file_size",
    "content_type",
    "received_at_ms",
    "capture_config_digest",
    "capture_metadata_json",
}


def migrate_frame_identity_schema(engine: Engine) -> bool:
    inspector = inspect(engine)
    if "frames" not in inspector.get_table_names():
        return False

    column_metadata = {
        column["name"]: column for column in inspector.get_columns("frames")
    }
    columns = set(column_metadata)
    unique_names = {
        constraint.get("name")
        for constraint in inspector.get_unique_constraints("frames")
    }
    if (
        FRAME_V2_COLUMNS.issubset(columns)
        and "uq_frame_device_timestamp" not in unique_names
        and column_metadata["file_path"].get("nullable", False)
    ):
        return False
    if engine.dialect.name != "sqlite":
        raise RuntimeError(
            "automatic frame identity migration currently supports SQLite only"
        )

    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE frames_v2_migration (
                id INTEGER NOT NULL PRIMARY KEY,
                device_id VARCHAR NOT NULL,
                timestamp BIGINT NOT NULL,
                file_path VARCHAR,
                source_session_id VARCHAR,
                camera_stream_id VARCHAR,
                frame_sequence BIGINT,
                source_frame_uid VARCHAR,
                content_digest VARCHAR,
                identity_mode VARCHAR NOT NULL DEFAULT 'LEGACY',
                archive_state VARCHAR NOT NULL DEFAULT 'ARCHIVE_DURABLE',
                archive_error VARCHAR,
                file_size BIGINT,
                content_type VARCHAR,
                received_at_ms BIGINT,
                capture_config_digest VARCHAR,
                capture_metadata_json TEXT,
                CONSTRAINT uq_frame_source_uid UNIQUE (source_frame_uid),
                CONSTRAINT uq_frame_source_identity UNIQUE (
                    source_session_id,
                    camera_stream_id,
                    frame_sequence
                )
            )
            """
        )
        available = FRAME_V2_COLUMNS.intersection(columns)
        select_values = {
            "source_session_id": (
                "source_session_id" if "source_session_id" in available else "NULL"
            ),
            "camera_stream_id": (
                "camera_stream_id" if "camera_stream_id" in available else "NULL"
            ),
            "frame_sequence": (
                "frame_sequence" if "frame_sequence" in available else "NULL"
            ),
            "source_frame_uid": (
                "source_frame_uid" if "source_frame_uid" in available else "NULL"
            ),
            "content_digest": (
                "content_digest" if "content_digest" in available else "NULL"
            ),
            "identity_mode": (
                "COALESCE(identity_mode, 'LEGACY')"
                if "identity_mode" in available
                else "'LEGACY'"
            ),
            "archive_state": (
                "COALESCE(archive_state, 'ARCHIVE_DURABLE')"
                if "archive_state" in available
                else "'ARCHIVE_DURABLE'"
            ),
            "archive_error": (
                "archive_error" if "archive_error" in available else "NULL"
            ),
            "file_size": "file_size" if "file_size" in available else "NULL",
            "content_type": (
                "content_type" if "content_type" in available else "NULL"
            ),
            "received_at_ms": (
                "received_at_ms" if "received_at_ms" in available else "NULL"
            ),
            "capture_config_digest": (
                "capture_config_digest"
                if "capture_config_digest" in available
                else "NULL"
            ),
            "capture_metadata_json": (
                "capture_metadata_json"
                if "capture_metadata_json" in available
                else "NULL"
            ),
        }
        connection.exec_driver_sql(
            f"""
            INSERT INTO frames_v2_migration (
                id,
                device_id,
                timestamp,
                file_path,
                source_session_id,
                camera_stream_id,
                frame_sequence,
                source_frame_uid,
                content_digest,
                identity_mode,
                archive_state,
                archive_error,
                file_size,
                content_type,
                received_at_ms,
                capture_config_digest,
                capture_metadata_json
            )
            SELECT
                id,
                device_id,
                timestamp,
                file_path,
                {select_values['source_session_id']},
                {select_values['camera_stream_id']},
                {select_values['frame_sequence']},
                {select_values['source_frame_uid']},
                {select_values['content_digest']},
                {select_values['identity_mode']},
                {select_values['archive_state']},
                {select_values['archive_error']},
                {select_values['file_size']},
                {select_values['content_type']},
                {select_values['received_at_ms']},
                {select_values['capture_config_digest']},
                {select_values['capture_metadata_json']}
            FROM frames
            """
        )
        connection.exec_driver_sql("DROP TABLE frames")
        connection.exec_driver_sql(
            "ALTER TABLE frames_v2_migration RENAME TO frames"
        )
        connection.exec_driver_sql(
            "CREATE INDEX ix_frames_device_id ON frames (device_id)"
        )
        connection.exec_driver_sql(
            "CREATE INDEX ix_frames_timestamp ON frames (timestamp)"
        )
        connection.exec_driver_sql(
            """
            CREATE UNIQUE INDEX uq_frame_legacy_device_timestamp
            ON frames (device_id, timestamp)
            WHERE identity_mode = 'LEGACY'
            """
        )
    return True


def migrate_manifest_archive_schema(engine: Engine) -> bool:
    inspector = inspect(engine)
    if "frame_set_manifests" not in inspector.get_table_names():
        return False
    columns = {
        column["name"]
        for column in inspector.get_columns("frame_set_manifests")
    }
    required = {
        "archive_state": "VARCHAR NOT NULL DEFAULT 'ARCHIVE_DURABLE'",
        "archive_error": "VARCHAR",
        "sync_window_ms": "BIGINT NOT NULL DEFAULT 0",
        "synchronized_at_ms": "BIGINT NOT NULL DEFAULT 0",
        "member_count": "INTEGER NOT NULL DEFAULT 0",
    }
    missing = set(required) - columns
    if not missing:
        return False
    with engine.begin() as connection:
        for name in sorted(missing):
            connection.exec_driver_sql(
                f"ALTER TABLE frame_set_manifests ADD COLUMN {name} {required[name]}"
            )
    return True


def migrate_relay_v2_client_state_schema(engine: Engine) -> bool:
    inspector = inspect(engine)
    if "relay_v2_client_state" not in inspector.get_table_names():
        return False
    columns = {
        column["name"]
        for column in inspector.get_columns("relay_v2_client_state")
    }
    required = {
        "reoffer_frame_set_uid": "VARCHAR",
        "processing_job_id": "VARCHAR",
        "processing_job_capture_run_id": "VARCHAR",
    }
    missing = set(required) - columns
    if not missing:
        return False
    with engine.begin() as connection:
        for name in sorted(missing):
            connection.exec_driver_sql(
                f"ALTER TABLE relay_v2_client_state ADD COLUMN {name} {required[name]}"
            )
    return True


def close_open_capture_runs_after_restart(engine: Engine) -> int:
    inspector = inspect(engine)
    if "capture_runs" not in inspector.get_table_names():
        return 0
    closed_at_ms = int(time.time() * 1000)
    with engine.begin() as connection:
        result = connection.execute(
            text(
                """
                UPDATE capture_runs
                SET state = 'CLOSED',
                    closed_at_ms = :closed_at_ms,
                    close_reason = 'PROCESS_RESTART'
                WHERE state = 'OPEN'
                """
            ),
            {"closed_at_ms": closed_at_ms},
        )
    return int(result.rowcount or 0)

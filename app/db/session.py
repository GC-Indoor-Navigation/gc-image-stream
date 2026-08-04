from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

from app.core.server import DATABASE_URL
from app.db.migrations import (
    close_open_capture_runs_after_restart,
    migrate_frame_identity_schema,
    migrate_manifest_archive_schema,
    migrate_relay_v2_client_state_schema,
)


engine_kwargs = {}
if DATABASE_URL.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, **engine_kwargs)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def ensure_database_schema(bind=engine):
    inspect(bind)
    migrate_frame_identity_schema(bind)
    migrate_manifest_archive_schema(bind)
    migrate_relay_v2_client_state_schema(bind)
    close_open_capture_runs_after_restart(bind)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

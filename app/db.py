from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker

from app.core.server import DATABASE_URL


engine_kwargs = {}
if DATABASE_URL.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, **engine_kwargs)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

Base = declarative_base()


FRAME_SCHEMA_UPDATES = {
    "compressed_at": (
        "ALTER TABLE frames "
        "ADD COLUMN compressed_at BIGINT"
    ),
}


SYNC_GROUP_SCHEMA_UPDATES = {
    "dispatch_status": (
        "ALTER TABLE sync_groups "
        "ADD COLUMN dispatch_status VARCHAR NOT NULL DEFAULT 'pending'"
    ),
    "last_dispatch_at": (
        "ALTER TABLE sync_groups "
        "ADD COLUMN last_dispatch_at BIGINT"
    ),
    "last_dispatch_status_code": (
        "ALTER TABLE sync_groups "
        "ADD COLUMN last_dispatch_status_code INTEGER"
    ),
    "last_dispatch_error": (
        "ALTER TABLE sync_groups "
        "ADD COLUMN last_dispatch_error VARCHAR"
    ),
    "dispatched_at": (
        "ALTER TABLE sync_groups "
        "ADD COLUMN dispatched_at BIGINT"
    ),
    "retry_count": (
        "ALTER TABLE sync_groups "
        "ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0"
    ),
    "next_retry_at": (
        "ALTER TABLE sync_groups "
        "ADD COLUMN next_retry_at BIGINT"
    ),
}


# 기존 DB에 필요한 sync_group 컬럼이 없으면 보강한다.
def ensure_database_schema():
    inspector = inspect(engine)

    table_names = set(inspector.get_table_names())

    if "frames" in table_names:
        existing_frame_columns = {
            column["name"]
            for column in inspector.get_columns("frames")
        }
        missing_frame_updates = [
            sql
            for column_name, sql in FRAME_SCHEMA_UPDATES.items()
            if column_name not in existing_frame_columns
        ]

        if missing_frame_updates:
            with engine.begin() as connection:
                for sql in missing_frame_updates:
                    connection.execute(text(sql))

    if "sync_groups" not in table_names:
        return

    existing_columns = {
        column["name"]
        for column in inspector.get_columns("sync_groups")
    }

    missing_updates = [
        sql
        for column_name, sql in SYNC_GROUP_SCHEMA_UPDATES.items()
        if column_name not in existing_columns
    ]

    if not missing_updates:
        return

    with engine.begin() as connection:
        for sql in missing_updates:
            connection.execute(text(sql))


# 요청 단위 DB 세션을 생성하고 종료한다.
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

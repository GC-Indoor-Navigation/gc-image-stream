from app.runtime.app_factory import create_app
from app.core.logging import configure_logging
from app.db import Base, engine, ensure_database_schema


configure_logging()

Base.metadata.create_all(bind=engine)
ensure_database_schema()

app = create_app()

from fastapi import FastAPI

from app.api.routes.debug import router as debug_router
from app.api.routes.frames import router as frames_router
from app.api.routes.monitoring import router as monitoring_router
from app.api.routes.sync import router as sync_router
from app.runtime.lifecycle import shutdown_application, startup_application


def create_app() -> FastAPI:
    app = FastAPI(
        title="GC Image Stream",
        summary="Camera frame ingest, monitoring, and processing relay server",
        description=(
            "GC Image Stream receives frames from camera input adapters, stores image and "
            "metadata locally, exposes monitoring and debug views, and relays frames to "
            "the downstream processing server."
        ),
    )

    app.include_router(frames_router)
    app.include_router(sync_router)
    app.include_router(monitoring_router)
    app.include_router(debug_router)

    @app.on_event("startup")
    async def on_startup():
        await startup_application()

    @app.on_event("shutdown")
    async def on_shutdown():
        await shutdown_application()

    @app.get(
        "/",
        summary="Service health",
        description="Simple root endpoint for quick process liveness checks.",
    )
    def root():
        return {"message": "GC Image Stream server is running"}

    return app

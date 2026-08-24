from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db import get_db
from app.infrastructure.grpc.grpc_ingest_server import grpc_ingest_service


router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
def liveness():
    return {"status": "UP"}


@router.get("/readiness")
def readiness(db: Session = Depends(get_db)):
    checks = {
        "database": "DOWN",
        "grpcIngest": "DISABLED",
    }
    try:
        db.execute(text("SELECT 1"))
        checks["database"] = "UP"
    except Exception:
        return JSONResponse(
            status_code=503,
            content={"status": "OUT_OF_SERVICE", "checks": checks},
        )

    grpc_status = grpc_ingest_service.status()
    if grpc_status["enabled"]:
        checks["grpcIngest"] = "UP" if grpc_status["running"] else "DOWN"
    ready = checks["grpcIngest"] != "DOWN"
    return JSONResponse(
        status_code=200 if ready else 503,
        content={
            "status": "UP" if ready else "OUT_OF_SERVICE",
            "checks": checks,
        },
    )

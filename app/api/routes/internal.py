from fastapi import APIRouter, Query, status

from app.schemas.processing_alerts import (
    ProcessingAlertEvent,
    ProcessingAlertIngestResponse,
    RecentProcessingAlertsResponse,
)
from app.services.alerts import phone_alert_delivery_hub, processing_alert_store


router = APIRouter(prefix="/internal", tags=["internal"])


@router.post(
    "/processing-alerts",
    response_model=ProcessingAlertIngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Receive Processing Server alert event",
    description="Accept proximity alert events published by the Processing Server.",
)
def receive_processing_alert(alert: ProcessingAlertEvent):
    record, result = processing_alert_store.add_alert(alert)
    if result == "accepted":
        phone_alert_delivery_hub.publish(record)
    return ProcessingAlertIngestResponse(
        accepted=result == "accepted",
        duplicate=result == "duplicate",
        expired=result == "expired",
        event_id=alert.event_id,
        expires_at_ms=record.expires_at_ms,
        routing=record.routing,
    )


@router.get(
    "/processing-alerts/recent",
    response_model=RecentProcessingAlertsResponse,
    summary="List recent Processing Server alerts",
    description="Return non-expired recent alert events retained by the Stream Server.",
)
def list_recent_processing_alerts(
    limit: int = Query(default=20, ge=1, le=200),
):
    items = [
        record.to_dict()
        for record in processing_alert_store.recent(limit=limit)
    ]
    return RecentProcessingAlertsResponse(
        items=items,
        status=processing_alert_store.status(),
    )

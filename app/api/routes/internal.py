from dataclasses import asdict
from functools import lru_cache

from fastapi import APIRouter, HTTPException, Query, status

from app.core import server as server_config

from app.schemas.processing_alerts import (
    ProcessingAlertEvent,
    ProcessingAlertIngestResponse,
    RecentProcessingAlertsResponse,
    RelayV2AlertReceiverRequest,
    RelayV2AlertReceiverResponse,
)
from app.services.alerts import phone_alert_delivery_hub, processing_alert_store
from app.services.alerts.processing_alerts import current_time_ms
from app.services.alerts.relay_v2_receiver import (
    RelayV2AlertEnvelope,
    RelayV2AlertReceiver,
)


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


@router.post(
    "/relay-v2/alerts",
    response_model=RelayV2AlertReceiverResponse,
    summary="Receive a versioned relay v2 hazard edge",
    description=(
        "Apply the alert idempotency key and hazard version atomically. "
        "The endpoint is unavailable unless its shadow receiver flag is enabled."
    ),
)
def receive_relay_v2_alert(request: RelayV2AlertReceiverRequest):
    if not server_config.STREAM_RELAY_V2_ALERT_RECEIVER_ENABLED:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    receiver = _relay_v2_receiver(
        server_config.STREAM_RELAY_V2_ALERT_RECEIVER_DATABASE_PATH
    )
    envelope = RelayV2AlertEnvelope(**request.model_dump(mode="python"))
    result = receiver.receive(envelope)
    response = asdict(result)
    response.pop("delivery_deadline_met")
    return RelayV2AlertReceiverResponse(**response)


@lru_cache(maxsize=4)
def _relay_v2_receiver(database_path: str) -> RelayV2AlertReceiver:
    return RelayV2AlertReceiver(database_path, current_time_ms=current_time_ms)

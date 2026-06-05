import asyncio
import json

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.services.alerts import (
    phone_alert_delivery_hub,
    processing_alert_store,
)


router = APIRouter(prefix="/phone", tags=["phone"])


def _sse_event(payload: dict) -> str:
    body = json.dumps(payload, separators=(",", ":"))
    return f"event: processing_alert\ndata: {body}\n\n"


@router.get(
    "/alerts/events",
    summary="Phone alert event stream",
    description="SSE stream for Android user-mode proximity alerts.",
)
async def stream_phone_alert_events(
    request: Request,
    device_id: list[str] = Query(default=[]),
    session_id: str | None = Query(default=None),
    once: bool = False,
):
    device_ids = [item for item in device_id if item]
    if not device_ids and not session_id:
        raise HTTPException(
            status_code=400,
            detail="device_id or session_id is required",
        )

    subscription = phone_alert_delivery_hub.subscribe(
        device_ids=device_ids,
        session_id=session_id,
    )

    async def event_generator():
        try:
            for record in processing_alert_store.recent(limit=processing_alert_store.max_alerts):
                if phone_alert_delivery_hub.matches(record, subscription):
                    yield _sse_event(record.to_dict())
                    if once:
                        return

            if once:
                return

            while True:
                if await request.is_disconnected():
                    break
                payload = await asyncio.to_thread(subscription.next_event, 1.0)
                if payload is None:
                    yield ": keep-alive\n\n"
                    continue
                yield _sse_event(payload)
        finally:
            phone_alert_delivery_hub.unsubscribe(subscription.subscription_id)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@router.get(
    "/alerts/status",
    summary="Phone alert delivery status",
    description="Return active phone alert subscribers and delivery counters.",
)
def get_phone_alert_status():
    return phone_alert_delivery_hub.status()

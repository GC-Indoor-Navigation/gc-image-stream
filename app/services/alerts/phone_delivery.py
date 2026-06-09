from __future__ import annotations

import queue
from dataclasses import dataclass, field
from threading import Lock
from uuid import uuid4

from app.services.alerts.processing_alerts import (
    ProcessingAlertRecord,
    current_time_ms,
)


@dataclass
class PhoneAlertSubscription:
    subscription_id: str
    device_ids: tuple[str, ...]
    camera_device_ids: tuple[str, ...]
    session_id: str | None
    connected_at_ms: int
    queue: queue.Queue[dict] = field(default_factory=lambda: queue.Queue(maxsize=50))
    delivered_count: int = 0
    dropped_count: int = 0
    last_sent_event_id: str | None = None

    def enqueue(self, payload: dict) -> None:
        if self.queue.full():
            try:
                self.queue.get_nowait()
                self.dropped_count += 1
            except queue.Empty:
                pass
        self.queue.put_nowait(payload)
        self.delivered_count += 1
        self.last_sent_event_id = payload.get("event_id")

    def next_event(self, timeout_sec: float = 1.0) -> dict | None:
        try:
            return self.queue.get(timeout=timeout_sec)
        except queue.Empty:
            return None

    def to_status(self) -> dict:
        return {
            "subscription_id": self.subscription_id,
            "device_ids": list(self.device_ids),
            "camera_device_ids": list(self.camera_device_ids),
            "session_id": self.session_id,
            "connected_at_ms": self.connected_at_ms,
            "queue_size": self.queue.qsize(),
            "delivered_count": self.delivered_count,
            "dropped_count": self.dropped_count,
            "last_sent_event_id": self.last_sent_event_id,
        }


class PhoneAlertDeliveryHub:
    def __init__(self):
        self._lock = Lock()
        self._subscriptions: dict[str, PhoneAlertSubscription] = {}
        self.published_count = 0
        self.delivered_count = 0
        self.skipped_expired_count = 0
        self.skipped_unmatched_count = 0

    def subscribe(
        self,
        *,
        device_ids: list[str],
        camera_device_ids: list[str] | None = None,
        session_id: str | None = None,
    ) -> PhoneAlertSubscription:
        subscription = PhoneAlertSubscription(
            subscription_id=str(uuid4()),
            device_ids=tuple(dict.fromkeys(device_ids)),
            camera_device_ids=tuple(dict.fromkeys(camera_device_ids or device_ids)),
            session_id=session_id,
            connected_at_ms=current_time_ms(),
        )
        with self._lock:
            self._subscriptions[subscription.subscription_id] = subscription
        return subscription

    def unsubscribe(self, subscription_id: str) -> None:
        with self._lock:
            self._subscriptions.pop(subscription_id, None)

    def publish(self, record: ProcessingAlertRecord, now_ms: int | None = None) -> int:
        current_ms = now_ms if now_ms is not None else current_time_ms()
        if record.is_expired(current_ms):
            with self._lock:
                self.skipped_expired_count += 1
            return 0

        payload = record.to_dict()
        with self._lock:
            self.published_count += 1
            subscriptions = list(self._subscriptions.values())

        matched = 0
        for subscription in subscriptions:
            if not self.matches(record, subscription):
                continue
            subscription.enqueue(payload)
            matched += 1

        with self._lock:
            self.delivered_count += matched
            if matched == 0:
                self.skipped_unmatched_count += 1
        return matched

    def matches(
        self,
        record: ProcessingAlertRecord,
        subscription: PhoneAlertSubscription,
    ) -> bool:
        routing = record.routing or {}
        if subscription.session_id and routing.get("session_id") == subscription.session_id:
            return True
        alert_devices = set(record.alert.source.camera_devices)
        return bool(alert_devices.intersection(subscription.camera_device_ids))

    def status(self) -> dict:
        with self._lock:
            subscriptions = list(self._subscriptions.values())
            return {
                "subscriber_count": len(subscriptions),
                "published_count": self.published_count,
                "delivered_count": self.delivered_count,
                "skipped_expired_count": self.skipped_expired_count,
                "skipped_unmatched_count": self.skipped_unmatched_count,
                "subscriptions": [
                    subscription.to_status()
                    for subscription in subscriptions
                ],
            }

    def clear(self) -> None:
        with self._lock:
            self._subscriptions.clear()
            self.published_count = 0
            self.delivered_count = 0
            self.skipped_expired_count = 0
            self.skipped_unmatched_count = 0


phone_alert_delivery_hub = PhoneAlertDeliveryHub()

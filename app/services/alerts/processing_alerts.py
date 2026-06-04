from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from threading import Lock

from app.schemas.processing_alerts import ProcessingAlertEvent


def current_time_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class ProcessingAlertRecord:
    alert: ProcessingAlertEvent
    received_at_ms: int
    expires_at_ms: int
    routing: dict

    def is_expired(self, now_ms: int) -> bool:
        return now_ms > self.expires_at_ms

    def to_dict(self) -> dict:
        payload = self.alert.model_dump(mode="json")
        payload["received_at_ms"] = self.received_at_ms
        payload["expires_at_ms"] = self.expires_at_ms
        payload["routing"] = self.routing
        return payload


class ProcessingAlertStore:
    def __init__(self, max_alerts: int = 200):
        self.max_alerts = max_alerts
        self._lock = Lock()
        self._records: deque[ProcessingAlertRecord] = deque(maxlen=max_alerts)
        self._event_ids: set[str] = set()
        self.received_count = 0
        self.duplicate_count = 0
        self.expired_count = 0

    def add_alert(
        self,
        alert: ProcessingAlertEvent,
        now_ms: int | None = None,
    ) -> tuple[ProcessingAlertRecord, str]:
        received_at_ms = now_ms if now_ms is not None else current_time_ms()
        expires_at_ms = alert.timestamp_ms + alert.ttl_ms
        routing = build_alert_routing(alert)
        record = ProcessingAlertRecord(
            alert=alert,
            received_at_ms=received_at_ms,
            expires_at_ms=expires_at_ms,
            routing=routing,
        )

        with self._lock:
            self.received_count += 1
            if alert.event_id in self._event_ids:
                self.duplicate_count += 1
                return record, "duplicate"

            if record.is_expired(received_at_ms):
                self.expired_count += 1
                return record, "expired"

            self._records.append(record)
            self._event_ids.add(alert.event_id)
            return record, "accepted"

    def recent(
        self,
        limit: int = 20,
        now_ms: int | None = None,
    ) -> list[ProcessingAlertRecord]:
        current_ms = now_ms if now_ms is not None else current_time_ms()
        with self._lock:
            active = [
                record
                for record in self._records
                if not record.is_expired(current_ms)
            ]
        return list(reversed(active[-limit:]))

    def status(self, now_ms: int | None = None) -> dict:
        current_ms = now_ms if now_ms is not None else current_time_ms()
        active_count = len(self.recent(limit=self.max_alerts, now_ms=current_ms))
        with self._lock:
            retained_count = len(self._records)
            return {
                "received_count": self.received_count,
                "duplicate_count": self.duplicate_count,
                "expired_count": self.expired_count,
                "active_count": active_count,
                "retained_count": retained_count,
            }

    def clear(self):
        with self._lock:
            self._records.clear()
            self._event_ids.clear()
            self.received_count = 0
            self.duplicate_count = 0
            self.expired_count = 0


def build_alert_routing(alert: ProcessingAlertEvent) -> dict:
    return {
        "camera_devices": list(alert.source.camera_devices),
        "session_id": None,
        "delivery_status": "not_delivered",
    }


processing_alert_store = ProcessingAlertStore()

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ProcessingAlertSource(BaseModel):
    model_config = ConfigDict(extra="allow")

    processor: str = Field(min_length=1)
    camera_devices: list[str] = Field(default_factory=list)


class ProcessingAlertEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: str = Field(min_length=1)
    frame_set_id: int = Field(ge=0)
    relay_run_id: int | None = Field(default=None, ge=0)
    timestamp_ms: int = Field(gt=0)
    severity: Literal["info", "warning", "danger"]
    distance_m: float | None = Field(default=None, ge=0)
    joint: str | None = None
    obstacle_id: str | None = None
    ttl_ms: int = Field(gt=0)
    source: ProcessingAlertSource


class ProcessingAlertIngestResponse(BaseModel):
    accepted: bool
    duplicate: bool
    expired: bool
    event_id: str
    expires_at_ms: int
    routing: dict


class RecentProcessingAlertsResponse(BaseModel):
    items: list[dict]
    status: dict


class RelayV2AlertReceiverRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract_version: int = Field(ge=1)
    idempotency_key: str = Field(min_length=1)
    payload_digest: str = Field(min_length=64, max_length=64)
    processing_job_id: str = Field(min_length=1)
    frame_set_uid: str = Field(min_length=1)
    transition_event_id: str = Field(min_length=1)
    hazard_key: str = Field(min_length=1)
    from_state: str = Field(min_length=1)
    to_state: str = Field(min_length=1)
    from_version: int = Field(ge=0)
    to_version: int = Field(ge=1)
    severity: Literal["info", "warning", "danger"]
    observation_event_utc_ms: int = Field(gt=0)
    delivery_deadline_utc_ms: int = Field(gt=0)
    alert_payload: dict[str, Any]


class RelayV2AlertReceiverResponse(BaseModel):
    contract_version: int
    idempotency_key: str
    payload_digest: str
    status: Literal[
        "APPLIED",
        "DUPLICATE",
        "RETRYABLE_FAILURE",
        "VERSION_GAP",
        "CONFLICT",
        "REJECTED",
    ]
    receiver_hazard_version: int = Field(ge=0)
    user_visible_effect_applied: bool
    received_at_utc_ms: int = Field(gt=0)

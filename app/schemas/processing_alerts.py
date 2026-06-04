from typing import Literal

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
    distance_m: float = Field(ge=0)
    joint: str = Field(min_length=1)
    obstacle_id: str = Field(default="unknown")
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

from pydantic import BaseModel, ConfigDict, Field


class FrameResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Collected frame record ID")
    device_id: str = Field(description="Camera or device identifier that produced the frame")
    timestamp: int = Field(description="Frame capture timestamp in milliseconds")
    file_path: str = Field(description="Stored image file path for the frame")
    source_session_id: str | None = Field(default=None)
    camera_stream_id: str | None = Field(default=None)
    frame_sequence: int | None = Field(default=None)
    source_frame_uid: str | None = Field(default=None)
    content_digest: str | None = Field(default=None)
    identity_mode: str = Field(description="V2 stable identity or LEGACY fallback")

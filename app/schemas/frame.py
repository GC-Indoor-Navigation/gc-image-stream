from pydantic import BaseModel, ConfigDict, Field


class FrameResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Collected frame record ID")
    device_id: str = Field(description="Camera or device identifier that produced the frame")
    timestamp: int = Field(description="Frame capture timestamp in milliseconds")
    file_path: str = Field(description="Stored image file path for the frame")


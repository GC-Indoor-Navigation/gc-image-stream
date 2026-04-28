from pydantic import BaseModel, ConfigDict, Field


class SyncFrameDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Collected frame record ID")
    device_id: str = Field(description="Camera or device identifier for the frame")
    timestamp: int = Field(description="Frame capture timestamp in milliseconds")
    file_path: str = Field(description="Stored image file path for the frame")


class SyncGroupResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(description="Sync group ID")
    group_timestamp: int = Field(description="Base timestamp for the sync group in milliseconds")
    dispatch_status: str = Field(
        description="Dispatch state such as pending, retry_scheduled, success, failed, or exhausted"
    )
    last_dispatch_at: int | None = Field(default=None, description="Most recent dispatch attempt time in milliseconds")
    last_dispatch_status_code: int | None = Field(default=None, description="Most recent processing server HTTP status code")
    last_dispatch_error: str | None = Field(default=None, description="Most recent dispatch error message")
    dispatched_at: int | None = Field(default=None, description="Successful dispatch completion time in milliseconds")
    retry_count: int = Field(description="Retry attempt count for the sync group")
    next_retry_at: int | None = Field(default=None, description="Scheduled next retry time in milliseconds")
    frames: list[SyncFrameDetail] = Field(description="Frames included in the sync group")


class SyncGroupListResponse(BaseModel):
    total: int = Field(description="Total sync group count for the current filter")
    limit: int = Field(description="Applied page size")
    offset: int = Field(description="Applied page offset")
    items: list[SyncGroupResponse] = Field(description="Filtered sync groups for the current page")


class SyncSummaryResponse(BaseModel):
    total_groups: int = Field(description="Total sync group count in the database")
    pending: int = Field(description="Pending sync group count")
    retry_scheduled: int = Field(description="Retry scheduled sync group count")
    success: int = Field(description="Successful sync group count")
    failed: int = Field(description="Failed non-retryable sync group count")
    exhausted: int = Field(description="Retry exhausted sync group count")
    retry_ready: int = Field(description="Retry-ready sync group count")

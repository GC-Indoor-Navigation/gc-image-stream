# GC Image Stream

`GC Image Stream` is a collection backend for multi-camera image pipelines.

This repository is responsible for:

- collecting frame data from camera apps or camera streams
- storing image files and frame metadata
- relaying frame streams to an external processing server
- keeping timestamp-based sync grouping as a fallback batch path

This repository does **not** implement the processing server itself. Its job is to supply clean, stable input to that external system.

## Scope

The current scope of this repository is limited to the collection pipeline:

1. ingest frames from multiple cameras
2. save files and metadata safely
3. relay frame bytes and metadata to an external processing server
4. keep grouped HTTP dispatch available for fallback and debugging

Out of scope:

- processing server internals
- 3D reconstruction
- pose estimation
- skeleton analysis
- risk analysis or guidance generation

## Current Features

- FastAPI-based frame upload and registration endpoints
- local file storage for collected frames
- SQLite-based metadata management
- gRPC frame relay from collectors to a processing server
- stream state monitoring and debug endpoints for latest frames
- internal camera session worker path for Stream Server ingestion
- server-side gRPC relay queue from Stream Server to Processing Server
- timestamp-based sync grouping as a fallback/debug path
- manual grouped HTTP dispatch
- optional automatic grouped HTTP dispatch
- dispatch state tracking per sync group
- retry scheduling for retryable dispatch failures
- operational retry controls for failed sync groups
- automated tests for core collection/grouping/dispatch logic

## Repository Structure

```text
app/
  config/           configuration loading and separation
  routes/           API routes
  services/         frame/group/dispatch business logic
  utils/            file/path utilities
camera/
  mjpeg_stream.py       MJPEG parsing and frame iterator
  snapshot_collector.py snapshot collector
  mjpeg_collector.py    legacy standalone MJPEG collector
  core.py               shared collector logic
fake_camera_generator.py test data generator for fake cameras
tests/              automated tests
```

## Quick Start

### 1. Create a virtual environment

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 2. Configure environment variables

Create a root `.env` file for server settings.

Example:

```env
DATABASE_URL=sqlite:///./frames.db
STORAGE_DIR=storage
PROCESSING_SERVER_URL=http://127.0.0.1:9000/process
AUTO_SYNC_ENABLED=false
STREAM_RELAY_ENABLED=false
STREAM_RELAY_TARGET=127.0.0.1:50051
STREAM_RELAY_TIMEOUT_SEC=
EXPERIMENT_ENABLED=false
EXPERIMENT_ID=
EXPERIMENT_LOG_DIR=
CAMERA_SESSIONS_ENABLED=false
```

If you use collector scripts, prepare per-camera env files such as `.env.camera1`.
Standalone collector scripts are legacy/transitional runners. Their direct gRPC
relay settings are fallback-only; the primary processing path is Stream Server
relay through `STREAM_RELAY_ENABLED` and `STREAM_RELAY_TARGET`.
Their local save and `REGISTER_API_URL` flow are also fallback-only; primary
storage and DB registration happen inside Stream Server `ingest_frame()`.

Example:

```env
CAMERA_NAME=camera1
CAMERA_SNAPSHOT_URL=http://127.0.0.1:8080/shot.jpg
CAMERA_STREAM_URL=http://127.0.0.1:8080/video
COLLECT_INTERVAL_SEC=1.0
REGISTER_API_URL=http://127.0.0.1:8000/frames/register
EXPERIMENT_ID=camera1-mjpeg-relay
EXPERIMENT_LOG_DIR=experiment_logs
```

Optional legacy direct relay for standalone collector experiments:

```env
EXPERIMENT_ENABLED=true
GRPC_RELAY_TARGET=127.0.0.1:50051
GRPC_RELAY_TIMEOUT_SEC=60
```

For internal Stream Server camera workers, configure a camera list in the server `.env`.

Example:

```env
CAMERA_SESSIONS_ENABLED=true
CAMERA_SESSIONS=camera1,camera2
CAMERA1_STREAM_URL=http://127.0.0.1:8080/video
CAMERA1_COLLECT_INTERVAL_SEC=0.1
CAMERA2_STREAM_URL=http://127.0.0.1:8081/video
CAMERA2_COLLECT_INTERVAL_SEC=0.1
```

Primary Stream Server experiment logging uses the root `.env` values
`EXPERIMENT_ENABLED`, `EXPERIMENT_ID`, and `EXPERIMENT_LOG_DIR`. When
`EXPERIMENT_ENABLED=true` and `EXPERIMENT_LOG_DIR` is set, camera session
capture, ingest registration, and relay events are written under
`experiment_logs/<experiment-id>/`. Leave `EXPERIMENT_ID=` empty when you want
the server to create a new timestamped folder automatically for each run.

### 3. Run the server

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

### 4. Run tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## Collector Experiment Logs

Collector runs save experiment records by default under `experiment_logs/<experiment-id>/`.

- `events.jsonl`: capture, register, relay, and schedule-lag events
- `summary.json`: aggregate counts, average fps, offsets, byte totals, and error counts

Set `EXPERIMENT_ID` in each camera env file to pin a stable folder name when
you want to compare repeated runs. Leave `EXPERIMENT_ID=` empty to create a new
timestamped folder automatically each time. Set `EXPERIMENT_LOG_DIR=` to
disable file logging for a run.

## API Summary

### Frames

- `POST /frames/upload`
- `POST /frames/register`
- `GET /frames`

### Sync

- `POST /sync/build`
- `GET /sync/summary`
- `GET /sync/groups`
- `GET /sync/groups/{group_id}`
- `POST /sync/groups/{group_id}/dispatch`
- `POST /sync/groups/{group_id}/retry`

### Monitoring

- `GET /monitoring/viewer`
- `GET /monitoring/cameras`
- `GET /monitoring/cameras/{device_id}`
- `GET /monitoring/relay`

### Debug

- `GET /debug/viewer`
- `GET /debug/cameras/{device_id}/latest-frame`
- `GET /debug/timestamp-delta`

`GET /sync/summary` returns an operational summary across sync groups:

- `total_groups`
- `pending`
- `retry_scheduled`
- `success`
- `failed`
- `exhausted`
- `retry_ready`

`GET /sync/groups` supports operational filters:

- `status=pending|retry_scheduled|success|failed|exhausted`
- `retry_ready=true|false`
- `exhausted=true|false`
- `limit=1..100`
- `offset=0..`
- `sort_by=id|group_timestamp|last_dispatch_at|next_retry_at|retry_count`
- `sort_order=asc|desc`

`GET /sync/groups` returns:

- `total`
- `limit`
- `offset`
- `items`

Each sync group response includes dispatch state metadata such as:

- `dispatch_status`
- `last_dispatch_at`
- `last_dispatch_status_code`
- `last_dispatch_error`
- `dispatched_at`
- `retry_count`
- `next_retry_at`

Dispatch status meanings:

- `pending`: the group has not been dispatched yet
- `retry_scheduled`: the last dispatch failed, but the server will retry it later
- `success`: the group was dispatched successfully
- `failed`: the dispatch failed and is not retryable
- `exhausted`: the dispatch failed repeatedly and has reached the retry limit

Automatic sync grouping and grouped HTTP dispatch are disabled by default because gRPC frame relay is the primary processing path. Set `AUTO_SYNC_ENABLED=true` only when you want to run the legacy grouped dispatch loop.

Server-side gRPC relay is controlled by `STREAM_RELAY_ENABLED`. When enabled, frames accepted by `ingest_frame()` are saved locally, tracked in StreamState, and queued for relay to `STREAM_RELAY_TARGET`. `STREAM_RELAY_TIMEOUT_SEC` is optional; leave it empty to keep the streaming RPC open without a deadline.

## Dispatch Retry Policy

Retry is scheduled only for failures that may succeed on a later attempt.

Retryable cases:

- connection failures
- request timeouts
- HTTP `502`, `503`, `504`

Non-retryable cases:

- client-side request errors such as `400`
- payload or endpoint problems that will fail again without code/data changes

Status behavior:

- retryable failures become `retry_scheduled`
- non-retryable failures become `failed`
- retryable failures that hit the retry limit become `exhausted`

Current retry schedule:

- 1st retry after 5 seconds
- 2nd retry after 15 seconds
- 3rd retry after 30 seconds

Automatic retry is handled by the server loop, and failed groups can also be retried manually through the retry endpoint.

## Reliability Goals

- remain stable under duplicate frame registration
- minimize file/DB inconsistency
- keep sync grouping deterministic
- never treat dispatch failure as success
- persist dispatch outcome and retry state for later inspection

## Key Files

- [`app/main.py`](app/main.py)
- [`app/routes/frames.py`](app/routes/frames.py)
- [`app/routes/sync.py`](app/routes/sync.py)
- [`app/services/frame_service.py`](app/services/frame_service.py)
- [`app/services/sync_service.py`](app/services/sync_service.py)
- [`camera/snapshot_collector.py`](camera/snapshot_collector.py)
- [`camera/mjpeg_collector.py`](camera/mjpeg_collector.py)
- [`camera/mjpeg_stream.py`](camera/mjpeg_stream.py)
- [`fake_camera_generator.py`](fake_camera_generator.py)

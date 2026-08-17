# GC Image Stream

`gc-image-stream` is a Stream Server for multi-camera inputs. It accepts frames,
stores files and metadata, exposes monitoring/debug state, and relays frames to
an external Processing Server.

## Responsibilities

- accept frames from camera sources or app clients
- store image files and frame metadata
- optionally build synchronized frame sets in-process
- expose Monitoring / Debug Viewer state
- relay frames to the Processing Server over gRPC

This repository does not implement AI processing logic. Stream-side sync is an
opt-in migration path while the raw Processing Server relay remains available.

## Current Shape

```text
Collector / App Client
  -> Stream Server
  -> Processing Server
```

Primary path:

```text
MJPEG worker or gRPC ingest
  -> ingest_pipeline
  -> local storage + frame DB
  -> stream state
  -> gRPC relay
  -> Processing Server
```

## Input Modes

- internal MJPEG camera session worker
- direct gRPC ingest from app clients
- single-shot HTTP calibration upload

MJPEG and gRPC inputs converge on the same ingest pipeline.
Calibration upload is stored locally as a separate single-shot capture path.

The Stream Server gRPC ingest contract is:

- `proto/frame_ingest.proto`
  - `gc.collector.v1.FrameIngestService`
  - Android collector path

## Quick Start

### 1. Create a virtual environment

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 2. Example `.env`

```env
DATABASE_URL=sqlite:///./frames.db
STORAGE_DIR=storage

GRPC_INGEST_BIND=127.0.0.1:50052

STREAM_RELAY_ENABLED=true
STREAM_RELAY_TARGET=127.0.0.1:50051
STREAM_RELAY_TIMEOUT_SEC=

STREAM_RELAY_V2_SHADOW_ENABLED=false
STREAM_RELAY_V2_TARGET=127.0.0.1:50053
STREAM_RELAY_V2_PROCESSING_PROFILE_DIGEST=

STREAM_SYNC_ENABLED=false
STREAM_SYNC_WINDOW_MS=50
STREAM_SYNC_EXPECTED_CAMERAS=camera1,camera2,camera3,camera4
STREAM_SYNC_BUFFER_SIZE=120
STREAM_SYNC_RECENT_LIMIT=20

EXPERIMENT_ENABLED=false
EXPERIMENT_ID=
EXPERIMENT_LOG_DIR=experiment_logs
EXPERIMENT_DURATION_SEC=

CAMERA_SESSIONS_ENABLED=true
CAMERA_SESSIONS=camera1
CAMERA_INPUT_TYPE=mjpeg

CAMERA1_STREAM_URL=http://127.0.0.1:8080/video
CAMERA1_COLLECT_INTERVAL_SEC=0.1
CAMERA1_CAPTURE_TIMEOUT_SEC=10
```

Relay v2 never falls back to `STREAM_RELAY_TARGET`. When enabled, it requires
the explicit `STREAM_RELAY_V2_TARGET` for the Processing Server's independent
v2 endpoint. If a legacy relay and v2 shadow run together, their targets must
be different. Disabling v2 leaves the legacy/raw route unchanged.

### 3. Run the server

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

### 4. Run tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## Main Views / APIs

- Monitoring Viewer: `/monitoring/viewer`
- Debug Viewer: `/debug/viewer`
- Relay status: `/monitoring/relay`
- Sync status: `/monitoring/sync`
- Internal calibration upload: `POST /capture/internal-calibration`

## Docs

Detailed architecture, design notes, experiment records, and operational notes
live under `docs/`.

- Overview: `docs/overview/`
- Design: `docs/design/`
- Experiments: `docs/experiments/`
- Operations: `docs/operations/`

See [`docs/README.md`](docs/README.md) for the local docs index.

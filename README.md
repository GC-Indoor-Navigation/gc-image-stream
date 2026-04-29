# GC Image Stream

`gc-image-stream` is a Stream Server for multi-camera inputs. It accepts frames,
stores files and metadata, exposes monitoring/debug state, and relays frames to
an external Processing Server.

## Responsibilities

- accept frames from camera sources or app clients
- store image files and frame metadata
- expose Monitoring / Debug Viewer state
- relay frames to the Processing Server over gRPC
- keep sync-group HTTP dispatch as a fallback path

This repository does not implement Processing Server sync or AI logic.

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

Both paths converge on the same ingest pipeline.

The gRPC ingest contract is defined in `proto/stream_ingest.proto` and is meant
to stay wire-compatible with the Android client proto.

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

PROCESSING_SERVER_URL=http://127.0.0.1:9000/process
AUTO_SYNC_ENABLED=false

GRPC_INGEST_BIND=127.0.0.1:50052

STREAM_RELAY_ENABLED=true
STREAM_RELAY_TARGET=127.0.0.1:50051
STREAM_RELAY_TIMEOUT_SEC=

EXPERIMENT_ENABLED=false
EXPERIMENT_ID=
EXPERIMENT_LOG_DIR=experiment_logs

CAMERA_SESSIONS_ENABLED=true
CAMERA_SESSIONS=camera1
CAMERA_INPUT_TYPE=mjpeg

CAMERA1_STREAM_URL=http://127.0.0.1:8080/video
CAMERA1_COLLECT_INTERVAL_SEC=0.1
CAMERA1_CAPTURE_TIMEOUT_SEC=10
```

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

## Docs

Detailed architecture, design notes, experiment records, and operational notes
live under `docs/`.

- Overview: `docs/overview/`
- Design: `docs/design/`
- Experiments: `docs/experiments/`
- Operations: `docs/operations/`

See [`docs/README.md`](docs/README.md) for the local docs index.

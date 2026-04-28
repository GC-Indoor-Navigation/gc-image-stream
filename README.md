# GC Image Stream

`gc-image-stream`은 멀티 카메라 입력을 받아 저장하고, 상태를 노출하고,
외부 Processing Server로 릴레이하는 Stream Server입니다.

## 역할

- 카메라 또는 앱 클라이언트에서 프레임 입력 받기
- 이미지 파일 및 메타데이터 저장
- Monitoring / Debug Viewer 상태 제공
- Processing Server로 gRPC 릴레이
- sync-group 기반 HTTP dispatch 경로는 fallback으로 유지

이 저장소는 Processing Server의 sync/AI 처리 자체는 담당하지 않습니다.

## 현재 구조

```text
Collector / App Client
  -> Stream Server
  -> Processing Server
```

현재 primary path:

```text
MJPEG worker or gRPC ingest
  -> ingest_pipeline
  -> local storage + frame DB
  -> stream state
  -> gRPC relay
  -> Processing Server
```

## 입력 방식

- 내부 MJPEG camera session worker
- 앱 클라이언트의 direct gRPC ingest

두 경로 모두 같은 ingest pipeline으로 합류합니다.

## 빠른 실행

### 1. 가상환경 및 의존성 설치

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 2. `.env` 예시

```env
DATABASE_URL=sqlite:///./frames.db
STORAGE_DIR=storage

PROCESSING_SERVER_URL=http://127.0.0.1:9000/process
AUTO_SYNC_ENABLED=false

GRPC_INGEST_ENABLED=false
GRPC_INGEST_BIND=127.0.0.1:50052

STREAM_RELAY_ENABLED=true
STREAM_RELAY_TARGET=127.0.0.1:50051
STREAM_RELAY_TIMEOUT_SEC=

EXPERIMENT_ENABLED=false
EXPERIMENT_ID=
EXPERIMENT_LOG_DIR=experiment_logs

CAMERA_SESSIONS_ENABLED=true
CAMERA_SESSIONS=camera1

CAMERA1_STREAM_URL=http://127.0.0.1:8080/video
CAMERA1_COLLECT_INTERVAL_SEC=0.1
CAMERA1_CAPTURE_TIMEOUT_SEC=10
```

### 3. 서버 실행

```powershell
.\.venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

### 4. 테스트 실행

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## 주요 화면 / API

- Monitoring Viewer: `/monitoring/viewer`
- Debug Viewer: `/debug/viewer`
- Relay 상태: `/monitoring/relay`

## 문서

자세한 구조, 설계, 실험 기록, 운영 메모는 `docs/`에 정리합니다.

- 개요: `docs/overview/`
- 설계: `docs/design/`
- 실험 기록: `docs/experiments/`
- 운영 메모: `docs/operations/`

로컬 문서 인덱스는 [`docs/README.md`](docs/README.md)에서 확인할 수 있습니다.

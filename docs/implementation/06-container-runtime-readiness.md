# Stream Server 컨테이너 런타임과 readiness

## 목적

통합 E2E Compose가 Stream Server를 실제 프로세스로 실행하고, HTTP process가 열렸다는 사실뿐 아니라 SQLite와 gRPC ingest가 사용할 준비가 됐는지를 구분해서 기다릴 수 있게 했다.

## 런타임 이미지

- Python 3.12 slim 이미지를 고정된 minor/patch 태그로 사용한다.
- `requirements.txt`를 먼저 복사해 dependency layer cache를 재사용한다.
- 애플리케이션은 root가 아닌 `gc` system user로 실행한다.
- HTTP 8000과 Android ingest gRPC 50052를 노출한다.
- `.env`, SQLite DB, storage, output, 실험 로그는 image build context에서 제외한다.

컨테이너는 설정을 image에 하드코딩하지 않는다. `DATABASE_URL`, `STORAGE_DIR`, Main endpoint와 relay endpoint는 Compose 환경 변수로 주입한다.

## health 경계

- `GET /health/live`: Python/FastAPI process 생존 확인
- `GET /health/readiness`: DB `SELECT 1`과, 활성화된 경우 gRPC ingest server의 실행 상태 확인

gRPC ingest가 설정상 비활성화되어 있으면 `DISABLED`는 정상 상태다. 반대로 활성화됐지만 아직 bind되지 않았다면 HTTP가 응답하더라도 503을 반환한다. 이 구분 덕분에 E2E camera simulator가 너무 일찍 연결해서 발생하는 시작 순서 race를 피할 수 있다.

Processing 연결 여부는 Stream process readiness와 분리한다. relay는 재접속 가능한 외부 의존성이므로, 전체 E2E runner가 별도 monitoring/relay assertion으로 검증한다.

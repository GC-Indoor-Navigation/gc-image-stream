# gRPC 카메라 수신 경계 연결

## 구현 목적

JWT 형식만 검증해도 실제 프레임의 카메라나 기기 값과 결합하지 않으면 다른
카메라 ID를 넣어 보내는 권한 대체가 가능하다. 이번 구현은 v2 카메라 자격
증명 검증을 실제 gRPC 스트림의 가장 앞단에 연결해 저장, 동기화, relay보다
먼저 권한을 확정한다.

## 버전 라우팅

Stream은 마이그레이션 기간에 기존 v1 세션 토큰과 v2 카메라 토큰을 함께
받을 수 있어야 한다. 서명 검증 전 payload는 오직 검증기 선택에만 사용한다.
`credential_kind=CAMERA_INGEST`이면 v2 검증기로 보내고, 이 claim이 없으면
기존 검증기로 보낸다. 선택된 검증기는 이후 서명, issuer, audience, 만료와
모든 claim을 독립적으로 다시 검증한다.

`PROCESSING_RELAY`처럼 Android 수신 경계에 올 수 없는 credential kind는
legacy 검증기로 우회시키지 않고 즉시 거부한다. 이 선택은 보안 판정이 아니라
서로 다른 audience 계약을 섞지 않기 위한 라우팅이다.

## 프레임 입장 조건

v2 스트림은 다음 값이 모두 토큰과 같아야 첫 프레임을 저장할 수 있다.

- tenant ID, site ID
- capture session ID, processing job ID
- processing profile digest
- camera ID
- Android device ID

특히 device ID가 비어 있으면 camera ID fallback으로 대신 인증하지 않는다.
토큰의 기기 ID와 프레임 metadata의 명시적인 device ID가 일치해야 한다.

## 장시간 스트림과 갱신

서버는 각 프레임 처리 전에 토큰 만료와 Main의 세션 활성 상태를 다시 확인한다.
또한 메모리에 등록된 현재 카메라 자격 증명과 비교한다. 같은 클레임에서 새
토큰이 등록되면 기존 스트림은 다음 프레임부터 `UNAUTHENTICATED`로 종료된다.
이렇게 해야 갱신 뒤에도 이전 토큰 스트림이 동시에 계속 쓰이는 일을 줄일 수
있다.

## 설정과 호환성

- `STREAM_CAMERA_INGEST_AUTH_ENABLED=false`가 기본값이라 기존 실행은 변하지
  않는다.
- v2 audience 기본값은 `gc-stream-ingest`이다.
- 기존 `STREAM_SESSION_AUTH_ENABLED`를 함께 켜면 두 버전을 동시에 수용한다.
- 두 기능 모두 같은 Main issuer, JWKS 캐시, 작업 상태 캐시를 공유한다.

## 검증한 내용

- v1/v2 토큰이 서로 올바른 검증기로만 전달되는지 확인
- relay credential kind가 ingest 경계에서 거부되는지 확인
- 정상 camera/device 조합이 저장 파이프라인까지 전달되는지 확인
- 다른 device ID가 저장 전에 `PERMISSION_DENIED`로 거부되는지 확인

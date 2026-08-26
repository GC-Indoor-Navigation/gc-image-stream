# 카메라 credential 폐기의 data-plane 반영

## 실제 E2E에서 확인한 문제

Main은 camera credential refresh 시 이전 JWT의 jti를 REVOKED로 바꾸지만,
Stream은 session/job 상태만 확인하고 있었다. 따라서 새 토큰이 Stream에 접속하기
전에는 폐기된 토큰도 gRPC 연결을 열 수 있었다. 기존 in-memory current-token
검사는 다른 인스턴스 또는 Stream 재시작 이후의 폐기를 증명하지 못한다.

## 수정한 경계

- CAMERA_INGEST 전용 credential-status 검증을 연결 및 매 프레임 admission에 추가했다.
- credentialId, processingJobId, credentialKind, known, active를 모두 검증한다.
- 긍정 상태만 1초 캐시하고, TTL 이후 조회 실패·알 수 없는 ID·폐기 상태는 fail closed한다.
- 캐시 키는 (credential ID, job ID)이며 최대 1024개의 긍정 결과만 보관한다.
- 새 토큰 접속 여부나 JWT iat 순서에 의존하지 않는다.
- legacy 인증과 raw frame relay는 변경하지 않았다.

camera 인증을 켠 배포에는 다음 환경변수가 필요하다. 누락되면 시작을 거부한다.

`STREAM_CAMERA_CREDENTIAL_STATUS_URL_TEMPLATE=http://main-server:8080/.well-known/credential-status/{credential_id}`

## 검증

정확한 상태 바인딩, 기존 연결의 폐기 전파, 새 토큰 독립성, 잘못된 응답, TTL 이후
authority 장애, 캐시 보관 한도를 회귀 테스트로 확인했다. 실제 Docker E2E에서는
refresh 후 새 토큰이 접속하기 전에도 이전 토큰이 UNAUTHENTICATED인지 확인한다.

1초는 캐시가 허용하는 최대 상태 지연이다. 만료된 캐시를 연장하지 않으며,
Main 응답을 기다리는 동안에는 프레임을 승인하지 않는다.

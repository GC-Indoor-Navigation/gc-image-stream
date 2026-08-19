# Main relay 자격 증명 클라이언트

## 책임 분리

Android의 `CAMERA_INGEST` 토큰은 한 참여자와 카메라 한 대만 증명한다. Stream이
Processing에 전체 프레임 세트를 보낼 때 이 토큰을 전달하면 Android 권한이
서비스 간 권한처럼 사용되고, 여러 카메라 중 어떤 토큰을 대표로 선택할지도
정의할 수 없다.

따라서 Stream은 자신의 workload identity로 Main에 인증하고, 전체 Processing
작업 범위를 가진 `PROCESSING_RELAY` 토큰을 별도로 받아야 한다.

## 2단계 발급 흐름

1. Stream의 confidential client ID와 secret으로 OIDC token endpoint의
   client-credentials grant를 호출한다.
2. 받은 workload access token으로 Main 내부 relay credential API를 호출한다.

workload access token은 `expires_in`과 안전 여유 시간을 기준으로 메모리에서
재사용한다. client secret, access token, relay token은 로그나 DB에 기록하지
않는다.

## One-time relay token 처리

Main은 relay JWT 원문을 `X-GC-Processing-Relay-Token` 응답 헤더에 한 번만
반환한다. Stream은 매 발급 요청에 새 Idempotency-Key를 사용하고, 헤더가 없는
응답은 성공 본문이 있더라도 실패로 처리한다.

응답을 잃은 경우 같은 요청의 token을 복구하려 하지 않는다. 다음 연결 시 새
Idempotency-Key로 회전을 요청하고 Main이 기존 credential을 폐기한 뒤 새
credential을 발급한다.

## Stream의 독립 검증

받은 relay JWT는 전달 전에 Stream에서도 다음을 검증한다.

- RS256, Main issuer, `gc-processing-relay` audience, 시간 조건
- `credential_kind=PROCESSING_RELAY`
- tenant/site/session/job/profile identity
- coordinator subject와 Stream workload subject
- 중복 없는 전체 camera ID 집합
- canonical credential jti

서명된 JWT, Main JSON 응답, 현재 manifest의 작업 범위와 카메라 집합이 모두
일치해야 메모리 캐시에 등록한다. 이 삼중 결합은 잘못된 작업 토큰이나 응답
혼선을 Processing 연결에 전달하지 않게 한다.

## 캐시 정책

캐시는 Processing job ID별로 유지한다. 유효 기간의 안전 여유가 남고 manifest
범위가 같을 때만 재사용한다. 만료가 가까워지거나 범위가 달라지면 Main에 새
발급을 요청한다. 프로세스가 재시작되면 캐시는 비어 있으므로 자연스럽게 새
relay credential 회전이 일어난다.

## 검증한 내용

- workload access token 발급과 만료 전 캐시 재사용
- relay JWT의 audience, kind, camera set, coordinator 검증
- Main URL path, workload Bearer, Idempotency-Key 구성
- one-time 헤더, JSON 본문, 서명 JWT, manifest 범위의 일치 검증
- one-time 헤더가 없는 응답의 fail-closed 처리

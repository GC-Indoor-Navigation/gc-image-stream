# 카메라별 수신 자격 증명 검증

## 왜 이 구현이 필요했는가

기존 세션 토큰은 한 작업에 속한 전체 카메라 목록을 한 토큰에 담고,
토큰 ID(`jti`)도 Processing 작업 ID와 같았다. 이 구조는 한 사용자가 모든
카메라를 직접 제어하는 초기 흐름에는 맞지만, 여러 Android 사용자가 각자
카메라 한 대를 맡는 참여형 촬영에서는 권한 범위가 너무 넓다.

Main Server v2는 참여자가 카메라를 클레임할 때 해당 카메라 한 대에만 유효한
짧은 수명의 `CAMERA_INGEST` JWT를 발급한다. Stream Server는 이 토큰을 기존
세션 토큰으로 해석하지 않고 독립된 보안 계약으로 검증해야 한다.

## 핵심 구현

- RS256 서명, issuer, `gc-stream-ingest` audience, 시간 조건을 검증한다.
- tenant, site, capture session, processing job, profile digest를 모두 읽는다.
- `camera_claim_id`, `camera_id`, `device_id`를 필수로 검증한다.
- `credential_kind`가 정확히 `CAMERA_INGEST`인지 확인한다.
- 모든 식별 UUID는 축약이나 임의 표기 없이 canonical UUID 형식만 허용한다.
- 프레임이 선언한 세션 범위뿐 아니라 실제 `camera_id`와 `device_id`도 토큰과
  정확히 일치해야 통과할 수 있도록 scope 객체가 비교 책임을 가진다.

## 메모리 등록 정책

검증된 원문 토큰은 로그나 DB에 저장하지 않고 메모리에만 둔다. 키는
`(processing_job_id, camera_id)`라서 한 작업 안의 여러 카메라 자격 증명이
서로 덮어쓰지 않는다.

같은 카메라 클레임의 갱신 토큰은 더 최신 `iat`일 때 교체할 수 있다. 반면
같은 작업과 카메라 키에 다른 `camera_claim_id`가 들어오면 활성 클레임 탈취나
경합 가능성이 있으므로 거부한다. 만료된 자격 증명은 조회 시 즉시 제거한다.

## 기존 흐름과의 관계

기존 `gc-data-plane` 세션 토큰 검증기는 그대로 유지한다. 이번 구현은 v2
계약과 저장 경계를 먼저 추가한 것이고, 다음 구현에서 gRPC ingest가 설정에
따라 두 계약을 구분해 적용한다. 따라서 기존 v1 트래픽의 동작은 이 단계에서
변하지 않는다.

## 검증한 내용

- Main Server가 발급하는 정상 claim 조합 수락
- 잘못된 audience, credential kind, UUID, device ID, profile digest 거부
- 선언된 세션 범위와 실제 카메라/기기 식별자 결합
- 같은 클레임의 토큰 갱신 허용과 다른 클레임의 교체 거부

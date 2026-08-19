# Processing relay 전용 자격 증명 경계

## 최종 연결 구조

v2 relay 연결은 프레임을 보낸 Android 토큰을 더 이상 gRPC metadata로 전달하지
않는다. 프레임 세트가 준비되면 Stream은 해당 manifest의 tenant와 Processing
job을 기준으로 Main relay credential provider를 호출한다.

provider가 반환한 `PROCESSING_RELAY` JWT만 다음 연결에 사용한다.

```text
Android CAMERA_INGEST JWT
  -> Stream gRPC ingest 검증
  -> 프레임 멤버 감사 정보

Stream workload access token
  -> Main relay credential 발급
  -> PROCESSING_RELAY JWT
  -> Processing v2 gRPC Authorization metadata
```

두 토큰은 audience와 사용 위치가 완전히 분리된다.

## Hello와 frame-set identity

다중 참여자 manifest의 최상위에는 participant subject나 Android credential jti를
대표값으로 두지 않는다. Processing으로 보내는 hello와 credited frame set에는
relay JWT에서 검증한 다음 값을 넣는다.

- `authorized_subject`: 촬영 coordinator subject
- `session_token_jti`: Processing relay credential jti

tenant, site, capture session, Processing job, profile digest, 전체 camera set은
relay JWT와 현재 manifest가 정확히 일치해야 한다. 연결 도중 더 최신 frame set을
고를 때도 같은 검사를 다시 수행해 다른 작업의 프레임이 기존 relay credential
아래로 들어가지 않게 한다.

## 호환 경로

`STREAM_RELAY_CREDENTIAL_ENABLED=false`이면 기존 session-token forwarding
경로를 그대로 유지한다. 이는 기존 v1/shadow 실험을 깨지 않기 위한 한시적
fallback이다. 운영 v2에서는 relay credential 옵션을 켜야 한다.

relay credential 옵션은 v2 relay가 켜져 있을 때만 허용한다. 필요한 Main URL,
OIDC client 설정, JWKS, issuer가 하나라도 없으면 서버 시작 단계에서 실패한다.

## 보안상 중요한 점

- client secret과 두 종류의 토큰은 status 응답이나 로그에 노출하지 않는다.
- Processing gRPC metadata에는 오직 relay JWT만 들어간다.
- Android participant subject/jti는 멤버 감사 정보로만 남는다.
- relay JWT 범위가 현재 frame set과 달라지면 연결을 영구 오류로 종료한다.
- credential 캐시는 메모리에만 있으므로 Stream 재시작 시 Main 회전 API를 다시
  호출한다.

## 검증한 내용

- gRPC Authorization metadata가 relay JWT로 교체됨
- hello의 subject/jti가 coordinator와 relay credential ID로 교체됨
- credit 이후 실제 frame set envelope도 같은 relay identity 사용
- provider가 받은 원본 manifest에는 participant 감사 정보가 유지됨
- 기존 session-token relay 테스트와 전체 회귀 유지

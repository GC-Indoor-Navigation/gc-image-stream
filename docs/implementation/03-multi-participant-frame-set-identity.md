# 다중 참여자 프레임 세트 식별자

## 기존 구현의 문제

기존 동기화 matcher는 한 프레임 세트의 모든 프레임이 다음 값을 전부 공유해야
한다고 판단했다.

- tenant, site, capture session, processing job, profile digest
- authorized subject
- session token jti

v1에서는 하나의 세션 토큰을 여러 카메라가 공유했기 때문에 맞는 조건이었다.
하지만 v2에서는 카메라마다 참여자와 credential jti가 다르다. 공통 촬영 작업에
속한 정상 프레임도 subject나 jti가 다르다는 이유로 scope conflict가 발생했다.

## 식별자를 두 층으로 분리

프레임 세트의 권한 식별자를 다음 두 층으로 나눴다.

1. 프레임 세트 공통 작업 범위
   - tenant ID
   - site ID
   - capture session ID
   - processing job ID
   - processing profile digest
2. 프레임 멤버별 참여 범위
   - authorized camera ID
   - camera claim ID
   - participant subject
   - camera ingest credential jti

공통 작업 범위는 모든 멤버가 정확히 같아야 한다. 카메라 ID는 프레임 세트
안에서 중복될 수 없다. 반면 참여자와 credential jti는 카메라마다 달라도 된다.
이 구분으로 서로 다른 사용자의 카메라가 하나의 Processing 작업에 안전하게
합쳐질 수 있다.

## 자격 증명 갱신과 촬영 run

`camera_claim_id`가 있는 v2 프레임은 subject와 jti를 프레임 세트 최상위
식별자에 넣지 않는다. 이를 최상위에 두면 짧은 수명의 토큰을 갱신할 때마다
동일한 촬영의 capture run이 새로 생성되기 때문이다.

카메라 클레임과 공통 작업 범위가 유지되는 한 토큰 갱신은 촬영 run을 바꾸지
않는다. 대신 실제로 어느 토큰이 어떤 프레임을 보냈는지는 각 멤버의 감사
정보에 남는다.

## 영속화

원본 `frames`에는 `camera_claim_id`를 추가했다. `frame_set_members`에는 다음
정보를 추가해 manifest를 다시 읽는 relay 단계에서도 참여 이력을 잃지 않게
했다.

- `authorized_camera_id`
- `camera_claim_id`
- `authorized_subject`
- `session_token_jti`

기존 SQLite DB는 시작 시 additive migration으로 새 컬럼을 추가한다. 기존
manifest 최상위 subject/jti 컬럼은 v1 호환성을 위해 유지하고, v2 다중 참여자
세트에서는 null로 둔다.

## 실패 조건

- session/job/profile 등 공통 작업 범위가 하나라도 다름
- 권한 정보가 일부 프레임에만 존재함
- 같은 authorized camera ID가 두 번 사용됨
- 한 프레임 세트에 camera-claim 방식과 기존 세션-token 방식이 섞임
- 멤버별 subject, credential jti, camera ID가 누락됨

## 검증한 내용

- 서로 다른 두 참여자와 두 credential jti가 하나의 프레임 세트를 생성
- 최상위 manifest에는 공통 작업 범위만 유지
- DB 멤버와 canonical manifest JSON에 카메라별 감사 정보 보존
- 기존 단일 세션 토큰 manifest와 relay protocol 회귀 유지
- 기존 DB에 새 감사 컬럼을 반복 실행 가능한 방식으로 추가

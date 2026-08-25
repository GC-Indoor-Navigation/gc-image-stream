# Relay v2 idle polling과 reconnect backoff 분리

## 발견된 문제

Relay v2 thread가 아직 durable manifest를 찾지 못한 상태는 정상적인 idle이다. 기존
loop는 이를 연결 실패와 동일하게 처리해 reconnect 횟수를 올리고 exponential
backoff를 적용했다. 서버 시작 후 카메라가 늦게 연결되면 backoff가 최대 5초까지
증가했고, 첫 frame-set이 생겨도 relay가 깨어나기 전에 500ms freshness window를
넘겨 `EXPIRED_BEFORE_OFFER`가 됐다.

## 구현

`_run_connection()`의 결과에서 idle을 별도로 표현한다. manifest가 없을 때는 기존
50ms event wait 뒤 즉시 다시 확인하고 backoff attempt를 0으로 유지한다. gRPC,
credential, protocol 오류처럼 실제 연결 시도가 실패한 경우에만 exponential
backoff와 reconnect counter를 적용한다.

latest-only 정책과 freshness 기준은 완화하지 않았다. 새 데이터 감지만 빠르게 해
오래된 프레임을 억지로 처리하는 대신 최신 frame-set이 유효 시간 안에 credit을 받을
수 있게 했다.

## 검증

빈 store의 `_run_connection()`이 idle 결과를 반환하고 reconnect failure counter를
증가시키지 않는 회귀 테스트를 추가했다.

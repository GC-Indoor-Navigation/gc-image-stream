# Relay v2 client 상태 관측

## 기존 문제

`/monitoring/relay`은 legacy raw relay worker만 반환했다. Relay v2 shadow client가
활성화돼도 실행 여부, 연결 및 재연결 횟수, offer 수, 마지막 오류를 HTTP 상태에서
확인할 수 없었다. 그 결과 Stream에 durable manifest가 쌓여도 Processing 연결이
없는 상황을 legacy relay 상태만으로 진단해야 했다.

## 구현

기존 응답 계약은 유지하고 `relay_v2` 하위 객체를 추가했다. 여기에는 활성화 여부,
thread 실행 상태, target, 마지막 오류, 연결/재연결/offer/no-data 횟수와 in-flight
상태가 포함된다.

이 값에는 relay credential 원문을 포함하지 않는다. 운영자는 secret 노출 없이
manifest 생성 문제와 transport/handshake 문제를 구분할 수 있다.

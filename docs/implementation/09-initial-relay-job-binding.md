# 최초 Relay v2 processing job 제안

## 발견된 문제

Relay v2 client는 Processing이 이전 연결에서 확정해 저장한 job ID만 producer hello의
`proposed_processing_job_id`로 보냈다. 새 capture run에는 아직 저장된 확정값이
없으므로 필드가 비었고, Main이 발급한 `PROCESSING_RELAY` credential에는 이미 job
scope가 존재해 Processing의 hello 검증에서 `SESSION_TOKEN_SCOPE_INVALID`로
거절됐다.

## 구현

재연결에서는 기존처럼 Processing이 확정한 job binding을 우선 사용한다. 최초
연결처럼 확정값이 없을 때는 durable manifest에 Main이 부여한
`processing_job_id`를 제안한다.

이 순서는 Processing의 재연결 fencing 권한을 유지하면서, 새 run도 Main의 control
plane job identity로 인증된 handshake를 시작하게 한다. 임의의 새 ID를 Stream에서
생성하지 않는다.

## 검증

기존 participant credential 경로와 workload relay credential 경로 양쪽에서 첫
hello가 manifest의 job ID를 제안하는지 회귀 테스트로 확인한다.

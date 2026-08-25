# 동시 카메라 manifest archive 완료 처리

## 발견된 문제

세 카메라 gRPC stream이 동시에 들어오면 live matcher는 파일 archive가 끝나기 전에
frame-set을 만들 수 있다. 이는 디스크 I/O가 실시간 동기화를 막지 않게 하기 위한
의도된 순서다.

기존 구현은 매칭을 발생시킨 요청의 archive 완료만 해당 frame-set snapshot에
반영했다. 먼저 들어온 다른 카메라 요청은 `handle_frame()`에서 frame-set을 받지
못했기 때문에, 나중에 archive가 완료돼도 이미 만들어진 snapshot을 갱신하지
않았다. DB의 개별 frame은 모두 durable인데 manifest 검증에서는 멤버가 여전히
pending으로 보여 모든 세트가 live-only로 강등됐다.

## 구현

archive 완료 시 caller가 가진 frame-set만 갱신하지 않고, 해당 frame의 buffer key를
포함하는 최근 frame-set을 matcher에서 찾는다. 어느 카메라 요청이 마지막으로
archive를 마치든 같은 snapshot을 갱신해 모든 멤버가 durable이 되는 요청이 manifest
저장과 Relay v2 전달을 이어갈 수 있다.

live matching은 archive를 기다리지 않으므로 실시간성 경계는 바뀌지 않는다.
durable manifest와 Relay v2만 모든 멤버의 archive 완료 뒤 진행한다.

## 검증

세 멤버를 먼저 pending 상태로 매칭한 뒤, 매칭 trigger와 다른 두 요청의 archive를
서로 다른 순서로 완료하는 회귀 테스트를 추가했다. frame-set을 직접 받지 않았던
요청도 최근 snapshot을 갱신하고 마지막 완료 시 세 멤버가 모두 durable인지
확인한다.

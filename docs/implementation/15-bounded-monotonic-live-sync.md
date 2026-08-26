# LIVE 동기화의 순서와 메모리 상한 정리

## 기존 문제

중복·역순·카메라 누락 E2E를 준비하면서 두 가지 문제를 재현했다. 같은 source stream에서 sequence 3 다음에 도착한 sequence 2가 새 frame-set으로 만들어질 수 있었고, 카메라 deque는 bounded여도 payload index와 matcher의 used/emitted key 집합은 계속 커졌다. 따라서 deque 길이만 확인해서는 장시간 실행의 메모리 상한을 설명할 수 없었다.

기존 동시성 테스트에도 별도 문제가 있었다. ThreadPoolExecutor에 넣은 100개 작업이 제출 순서대로 실행된다고 가정하고 정확히 50개 frame-set을 기대했다. 하지만 앞선 timestamp가 먼저 처리되면 watermark가 오래된 후보를 버릴 수 있으므로 이 기대는 실행 스케줄에 따라 달라졌다.

## 변경한 정책

같은 V2 source session/camera stream에서는 sequence가 증가하고 timestamp가 역행하지 않는 프레임만 LIVE 동기화 버퍼에 넣는다. 재정렬을 위해 무한히 기다리는 대신 늦은 프레임을 버리고 최신 완전한 묶음으로 진행한다. source session이 바뀌면 새 스트림으로 간주하므로 sequence 재시작은 허용한다. 동일 스트림의 재접속은 기존 source session과 sequence를 이어야 한다.

중복 판정은 현재 보관 중인 source frame key를 먼저 확인한다. 오래전에 퇴출되어 key가 없어진 프레임이라도 같은 V2 스트림의 sequence high-water mark보다 낮으면 다시 매칭 후보로 들어오지 않는다. 이 경우 중복 카운터 대신 out-of-order 카운터에 기록된다.

minimum-span 후보 선택 방식은 유지했다. 다만 한 묶음을 내보낸 뒤에는 각 카메라에서 선택된 timestamp 이하 후보를 소비·폐기한다. 먼저 내보낸 최신 묶음 뒤로 오래된 완전 후보가 뒤늦게 출력되는 것을 막으며, 매칭에 사용한 모든 key를 영구 보관할 필요도 없어졌다.

## 무엇을 bounded로 만들었나

- 카메라별 동기화 후보: 기존 buffer_size 이내
- payload/key index: 카메라별 최근 buffer_size 이내
- 최근 frame-set: 기존 recent_limit 이내
- matcher의 전체 실행 이력 used/emitted key 집합: 제거

따라서 고정된 카메라 수 C, 버퍼 크기 B, 최근 묶음 수 R에 대해 이 동기화 계층이 유지하는 프레임 참조는 O(C × (B + R)) 범위다. 이는 프로세스 전체 RSS나 디스크 archive의 상한을 측정했다는 의미는 아니다.

payload index 퇴출 이후 archive 저장이 완료되는 경우에는 최근 frame-set의 해당 멤버를 찾아 완료 상태를 반영한다. 최근 목록에서도 빠진 오래된 묶음은 호출자가 보유한 frame-set을 갱신한다. 이를 위해 전체 이미지 이력을 다시 보관하지 않는다.

관측값에는 retained_frame_count, evicted_frame_count, out_of_order_frame_count, dropped_superseded_count를 추가했다. raw relay 전송과 영속 archive 저장 경로는 바꾸지 않았으며, 이 변경은 LIVE 동기화 후보의 수명과 순서에 적용된다.

## 검증

동시성 검사는 카메라당 한 producer를 두고 같은 tick의 두 요청만 동시에 처리하도록 barrier로 경계를 명시했다. 역순 도착과 중복 폭주는 별도 테스트에서 검사한다. 따라서 기대 frame-set 수를 낮춰 간헐 실패를 숨긴 것이 아니다.

- sequence 역행과 timestamp 역행 거부
- 100개 동시 중복 요청에서 단 하나의 frame-set 출력
- 누락 카메라가 있는 동안 후보 및 payload index 상한 유지
- 퇴출된 오래된 프레임의 재진입 방지
- 더 좋은 최신 후보 선택 후 오래된 완전 후보 폐기
- 새 source session의 sequence 재시작과 늦은 archive 완료 처리
- Stream 전체 테스트 290개 통과, 제외 없음
- 동시성 회귀 테스트 100회 연속 통과

실제 Compose E2E에서는 B=120인 상태에서 한 카메라를 125틱 동안 조용하게 유지했다. 다른 두 카메라가 계속 진행해도 불완전 묶음을 출력하지 않았으며, 복귀 후 8 → 134 → 135 순서로 최신 완전 묶음을 만들었다. Processing의 ACCEPTED/STARTED/COMPLETED 기록도 frame-set ID가 역행하지 않았다.

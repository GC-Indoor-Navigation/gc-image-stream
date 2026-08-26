# 프레임 scope 검증과 수집 게이트의 순서

## 발견한 문제

gRPC ingest는 프레임을 저장하기 전에 JWT와 frame scope를 비교했지만, 비교하기
전에 장치를 active로 표시하고 최신 device timestamp를 갱신했다. 이 때문에
PERMISSION_DENIED로 거절된 요청도 장치 목록·타임스탬프·연결 종료 상태를 바꿨다.
보안 E2E에서 archive/sync는 그대로인데 gate snapshot만 달라지는 것으로 재현했다.

## 변경

서명 credential에 바인딩된 tenant/site/session/job/profile/camera/device를 먼저
검증하고, 통과한 프레임만 device active와 timestamp 상태를 갱신하도록 순서를
바꿨다. scope 누락도 같은 경계에서 거절한다. 수신 시각과 수신 횟수 측정은
유지하고, 실제 수집 게이트나 저장 경로는 조작된 프레임의 영향을 받지 않게 했다.

## 검증

8가지 scope 누락·대체에 대해 storage 호출이 없고 기존 gate snapshot이 유지되는지
회귀 테스트를 추가했다. 이미 연결된 정상 장치가 있는 상황과 과도하게 미래인
조작 타임스탬프를 함께 사용한다. Docker E2E에서도 거절 응답뿐 아니라 frame
목록, sync 카운터, gate 상태가 함께 유지돼야 PASS로 판정한다.

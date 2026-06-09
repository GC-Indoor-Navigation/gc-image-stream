import argparse
import json
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def request_json(method: str, url: str, payload: dict | None = None, timeout_sec: float = 5.0):
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else None
    except HTTPError as exc:
        raw = exc.read().decode("utf-8")
        detail = json.loads(raw) if raw else {"detail": exc.reason}
        return exc.code, detail
    except URLError as exc:
        raise RuntimeError(f"request failed: {exc.reason}") from exc


def normalize_base_url(value: str) -> str:
    return value.rstrip("/")


def find_matching_subscriptions(status: dict, device_id: str) -> list[dict]:
    return [
        subscription
        for subscription in status.get("subscriptions", [])
        if device_id in subscription.get("device_ids", [])
    ]


def build_payload(args) -> dict:
    now_ms = int(time.time() * 1000)
    event_id = args.event_id or f"android-user-alert-test-{now_ms}"
    distance_m = None if args.nullable_fields else args.distance_m
    joint = None if args.nullable_fields else args.joint
    obstacle_id = None if args.nullable_fields else args.obstacle_id
    camera_device_ids = args.camera_device_id or [args.device_id]

    return {
        "event_id": event_id,
        "frame_set_id": args.frame_set_id,
        "relay_run_id": args.relay_run_id,
        "timestamp_ms": now_ms,
        "severity": args.severity,
        "distance_m": distance_m,
        "joint": joint,
        "obstacle_id": obstacle_id,
        "ttl_ms": args.ttl_ms,
        "source": {
            "processor": args.processor,
            "camera_devices": camera_device_ids,
        },
    }


def print_status_summary(label: str, status: dict, device_id: str):
    matches = find_matching_subscriptions(status, device_id)
    print(f"\n== {label} ==")
    print(f"subscriber_count : {status.get('subscriber_count', 0)}")
    print(f"published_count  : {status.get('published_count', 0)}")
    print(f"delivered_count  : {status.get('delivered_count', 0)}")
    print(f"unmatched_count  : {status.get('skipped_unmatched_count', 0)}")
    print(f"matching phones  : {len(matches)}")
    for item in matches:
        print(
            "  - "
            f"phone={','.join(item.get('device_ids', [])) or '-'} "
            f"cameras={','.join(item.get('camera_device_ids', [])) or '-'} "
            f"queue={item.get('queue_size', 0)} "
            f"delivered={item.get('delivered_count', 0)} "
            f"last={item.get('last_sent_event_id') or '-'}"
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Send a manual Processing alert to test Android user-mode SSE delivery.",
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="Stream Server base URL.",
    )
    parser.add_argument(
        "--device-id",
        required=True,
        help="Android user-mode device id.",
    )
    parser.add_argument(
        "--camera-device-id",
        action="append",
        default=[],
        help="Camera device id to include in alert source. Repeat for multiple cameras.",
    )
    parser.add_argument("--event-id", default="", help="Alert event id. Defaults to a timestamped id.")
    parser.add_argument("--severity", default="warning", choices=["info", "warning", "danger"])
    parser.add_argument("--distance-m", type=float, default=0.62)
    parser.add_argument("--joint", default="pelvis")
    parser.add_argument("--obstacle-id", default="unknown")
    parser.add_argument("--nullable-fields", action="store_true", help="Send distance/joint/obstacle as null.")
    parser.add_argument("--frame-set-id", type=int, default=100)
    parser.add_argument("--relay-run-id", type=int, default=1)
    parser.add_argument("--ttl-ms", type=int, default=60_000)
    parser.add_argument("--processor", default="manual_test")
    parser.add_argument("--timeout-sec", type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_url = normalize_base_url(args.base_url)
    status_url = f"{base_url}/phone/alerts/status"
    ingest_url = f"{base_url}/internal/processing-alerts"

    status_code, status_before = request_json("GET", status_url, timeout_sec=args.timeout_sec)
    if status_code != 200:
        print(f"status check failed: HTTP {status_code} {status_before}")
        return 1

    print_status_summary("Before", status_before, args.device_id)
    if not find_matching_subscriptions(status_before, args.device_id):
        print(f"\nwarning: no active phone subscriber for device_id={args.device_id}")

    payload = build_payload(args)
    post_code, post_body = request_json("POST", ingest_url, payload=payload, timeout_sec=args.timeout_sec)
    print("\n== Alert POST ==")
    print(f"status_code : {post_code}")
    print(f"event_id    : {payload['event_id']}")
    print(f"accepted    : {post_body.get('accepted') if isinstance(post_body, dict) else '-'}")
    print(f"duplicate   : {post_body.get('duplicate') if isinstance(post_body, dict) else '-'}")
    print(f"expired     : {post_body.get('expired') if isinstance(post_body, dict) else '-'}")
    if post_code != 202 or not post_body.get("accepted"):
        print(json.dumps(post_body, indent=2, ensure_ascii=False))
        return 1

    time.sleep(0.2)
    status_code, status_after = request_json("GET", status_url, timeout_sec=args.timeout_sec)
    if status_code != 200:
        print(f"status check failed: HTTP {status_code} {status_after}")
        return 1

    print_status_summary("After", status_after, args.device_id)
    delivered_delta = status_after.get("delivered_count", 0) - status_before.get("delivered_count", 0)
    print("\n== Result ==")
    if delivered_delta > 0:
        print(f"delivery: OK ({delivered_delta} event delivered)")
        return 0

    print("delivery: NOT CONFIRMED")
    print("check device_id matching, Android SSE logs, and user mode connection state")
    return 2


if __name__ == "__main__":
    sys.exit(main())

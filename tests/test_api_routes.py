from pathlib import Path

from app.api.routes import sync as sync_routes
from app.services.sync import service as sync_service


def test_register_frame_is_idempotent(client):
    first = client.post(
        "/frames/register",
        data={
            "device_id": "camera1",
            "timestamp": 1000,
            "file_path": "storage/camera1/1000.jpg",
        },
    )
    second = client.post(
        "/frames/register",
        data={
            "device_id": "camera1",
            "timestamp": 1000,
            "file_path": "storage/camera1/other.jpg",
        },
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert second.json()["file_path"] == "storage/camera1/1000.jpg"


def test_upload_frame_saves_file_and_metadata(client, read_file_bytes):
    response = client.post(
        "/frames/upload",
        files={
            "file": (
                "camera1_1712321562400.jpg",
                b"frame-bytes",
                "image/jpeg",
            )
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["device_id"] == "camera1"
    assert body["timestamp"] == 1712321562400
    assert Path(body["file_path"]).is_file()
    assert read_file_bytes(body["file_path"]) == b"frame-bytes"


def test_sync_build_and_group_listing_flow(client):
    frame_inputs = [
        ("camera1", 1000, "storage/camera1/1000.jpg"),
        ("camera2", 1030, "storage/camera2/1030.jpg"),
        ("camera3", 1200, "storage/camera3/1200.jpg"),
    ]

    for device_id, timestamp, file_path in frame_inputs:
        response = client.post(
            "/frames/register",
            data={
                "device_id": device_id,
                "timestamp": timestamp,
                "file_path": file_path,
            },
        )
        assert response.status_code == 200

    build_response = client.post("/sync/build", params={"threshold_ms": 50})
    groups_response = client.get("/sync/groups")

    assert build_response.status_code == 200
    assert build_response.json() == {"created_count": 1}
    assert groups_response.status_code == 200

    groups = groups_response.json()
    assert groups["total"] == 1
    assert groups["limit"] == 20
    assert groups["offset"] == 0
    assert len(groups["items"]) == 1
    assert groups["items"][0]["group_timestamp"] == 1000
    assert groups["items"][0]["dispatch_status"] == "pending"
    assert groups["items"][0]["last_dispatch_at"] is None
    assert groups["items"][0]["last_dispatch_status_code"] is None
    assert groups["items"][0]["last_dispatch_error"] is None
    assert groups["items"][0]["dispatched_at"] is None
    assert groups["items"][0]["retry_count"] == 0
    assert groups["items"][0]["next_retry_at"] is None
    assert {frame["device_id"] for frame in groups["items"][0]["frames"]} == {"camera1", "camera2"}


def test_dispatch_endpoint_returns_dispatch_service_result(client, monkeypatch):
    for device_id, timestamp in [("camera1", 1000), ("camera2", 1010)]:
        response = client.post(
            "/frames/register",
            data={
                "device_id": device_id,
                "timestamp": timestamp,
                "file_path": f"storage/{device_id}/{timestamp}.jpg",
            },
        )
        assert response.status_code == 200

    build_response = client.post("/sync/build", params={"threshold_ms": 20})
    assert build_response.status_code == 200

    captured = {}

    async def fake_dispatch(group, processing_server_url):
        captured["group"] = group
        captured["processing_server_url"] = processing_server_url
        return {
            "success": True,
            "status_code": 200,
            "response_body": {"message": "ok"},
            "payload": {"syncGroupId": group["id"]},
        }

    monkeypatch.setattr(sync_routes, "dispatch_sync_group", fake_dispatch)

    groups_response = client.get("/sync/groups")
    group_id = groups_response.json()["items"][0]["id"]

    dispatch_response = client.post(f"/sync/groups/{group_id}/dispatch")
    group_response = client.get(f"/sync/groups/{group_id}")

    assert dispatch_response.status_code == 200
    assert dispatch_response.json()["success"] is True
    assert dispatch_response.json()["payload"] == {"syncGroupId": group_id}
    assert group_response.status_code == 200
    assert group_response.json()["dispatch_status"] == "success"
    assert group_response.json()["last_dispatch_status_code"] == 200
    assert group_response.json()["last_dispatch_error"] is None
    assert group_response.json()["last_dispatch_at"] is not None
    assert group_response.json()["dispatched_at"] is not None
    assert group_response.json()["retry_count"] == 0
    assert group_response.json()["next_retry_at"] is None
    assert captured["group"]["id"] == group_id
    assert captured["processing_server_url"] == sync_routes.PROCESSING_SERVER_URL


def test_dispatch_endpoint_tracks_failed_dispatch_state(client, monkeypatch):
    for device_id, timestamp in [("camera1", 1000), ("camera2", 1010)]:
        response = client.post(
            "/frames/register",
            data={
                "device_id": device_id,
                "timestamp": timestamp,
                "file_path": f"storage/{device_id}/{timestamp}.jpg",
            },
        )
        assert response.status_code == 200

    build_response = client.post("/sync/build", params={"threshold_ms": 20})
    assert build_response.status_code == 200

    async def fake_dispatch(group, processing_server_url):
        return {
            "success": False,
            "error": "Processing server timeout",
            "payload": {"syncGroupId": group["id"]},
        }

    monkeypatch.setattr(sync_routes, "dispatch_sync_group", fake_dispatch)

    groups_response = client.get("/sync/groups")
    group_id = groups_response.json()["items"][0]["id"]

    dispatch_response = client.post(f"/sync/groups/{group_id}/dispatch")
    group_response = client.get(f"/sync/groups/{group_id}")

    assert dispatch_response.status_code == 200
    assert dispatch_response.json()["success"] is False
    assert group_response.status_code == 200
    assert group_response.json()["dispatch_status"] == "retry_scheduled"
    assert group_response.json()["last_dispatch_status_code"] is None
    assert group_response.json()["last_dispatch_error"] == "Processing server timeout"
    assert group_response.json()["last_dispatch_at"] is not None
    assert group_response.json()["dispatched_at"] is None
    assert group_response.json()["retry_count"] == 1
    assert group_response.json()["next_retry_at"] is not None


def test_sync_group_filters_support_retry_ready_and_exhausted(client, monkeypatch):
    for device_id, timestamp in [("camera1", 1000), ("camera2", 1010), ("camera3", 2000), ("camera4", 2010)]:
        response = client.post(
            "/frames/register",
            data={
                "device_id": device_id,
                "timestamp": timestamp,
                "file_path": f"storage/{device_id}/{timestamp}.jpg",
            },
        )
        assert response.status_code == 200

    build_response = client.post("/sync/build", params={"threshold_ms": 20})
    assert build_response.status_code == 200

    group_ids = [group["id"] for group in client.get("/sync/groups").json()["items"]]
    first_group_id, second_group_id = sorted(group_ids)

    async def fake_failed_dispatch(group, processing_server_url):
        return {
            "success": False,
            "error": "Processing server timeout",
            "payload": {"syncGroupId": group["id"]},
        }

    monkeypatch.setattr(sync_routes, "dispatch_sync_group", fake_failed_dispatch)
    monkeypatch.setattr(sync_service.time, "time", lambda: 100.0)

    first_failure = client.post(f"/sync/groups/{first_group_id}/dispatch")
    assert first_failure.status_code == 200

    monkeypatch.setattr(sync_service.time, "time", lambda: 106.0)
    retry_ready_response = client.get("/sync/groups", params={"retry_ready": "true"})
    assert retry_ready_response.status_code == 200
    assert [group["id"] for group in retry_ready_response.json()["items"]] == [first_group_id]

    for _ in range(3):
        response = client.post(f"/sync/groups/{second_group_id}/dispatch")
        assert response.status_code == 200

    exhausted_response = client.get("/sync/groups", params={"exhausted": "true"})
    assert exhausted_response.status_code == 200
    assert [group["id"] for group in exhausted_response.json()["items"]] == [second_group_id]
    assert exhausted_response.json()["items"][0]["dispatch_status"] == "exhausted"


def test_sync_group_listing_supports_pagination_and_sorting(client):
    for device_id, timestamp in [
        ("camera1", 1000),
        ("camera2", 1010),
        ("camera3", 2000),
        ("camera4", 2010),
        ("camera5", 3000),
        ("camera6", 3010),
    ]:
        response = client.post(
            "/frames/register",
            data={
                "device_id": device_id,
                "timestamp": timestamp,
                "file_path": f"storage/{device_id}/{timestamp}.jpg",
            },
        )
        assert response.status_code == 200

    build_response = client.post("/sync/build", params={"threshold_ms": 20})
    assert build_response.status_code == 200

    response = client.get(
        "/sync/groups",
        params={
            "limit": 1,
            "offset": 1,
            "sort_by": "group_timestamp",
            "sort_order": "asc",
        },
    )

    assert response.status_code == 200
    groups = response.json()
    assert groups["total"] == 3
    assert groups["limit"] == 1
    assert groups["offset"] == 1
    assert len(groups["items"]) == 1
    assert groups["items"][0]["group_timestamp"] == 2000


def test_manual_retry_endpoint_retries_failed_group(client, monkeypatch):
    for device_id, timestamp in [("camera1", 1000), ("camera2", 1010)]:
        response = client.post(
            "/frames/register",
            data={
                "device_id": device_id,
                "timestamp": timestamp,
                "file_path": f"storage/{device_id}/{timestamp}.jpg",
            },
        )
        assert response.status_code == 200

    build_response = client.post("/sync/build", params={"threshold_ms": 20})
    assert build_response.status_code == 200
    group_id = client.get("/sync/groups").json()["items"][0]["id"]

    async def fake_failed_dispatch(group, processing_server_url):
        return {
            "success": False,
            "error": "Processing server timeout",
            "payload": {"syncGroupId": group["id"]},
        }

    monkeypatch.setattr(sync_routes, "dispatch_sync_group", fake_failed_dispatch)
    failed_response = client.post(f"/sync/groups/{group_id}/dispatch")
    assert failed_response.status_code == 200

    async def fake_success_dispatch(group, processing_server_url):
        return {
            "success": True,
            "status_code": 200,
            "response_body": {"message": "retried"},
            "payload": {"syncGroupId": group["id"]},
        }

    monkeypatch.setattr(sync_routes, "dispatch_sync_group", fake_success_dispatch)
    retry_response = client.post(f"/sync/groups/{group_id}/retry")
    group_response = client.get(f"/sync/groups/{group_id}")

    assert retry_response.status_code == 200
    assert retry_response.json()["success"] is True
    assert group_response.json()["dispatch_status"] == "success"
    assert group_response.json()["retry_count"] == 0
    assert group_response.json()["next_retry_at"] is None


def test_manual_retry_endpoint_rejects_success_group(client, monkeypatch):
    for device_id, timestamp in [("camera1", 1000), ("camera2", 1010)]:
        response = client.post(
            "/frames/register",
            data={
                "device_id": device_id,
                "timestamp": timestamp,
                "file_path": f"storage/{device_id}/{timestamp}.jpg",
            },
        )
        assert response.status_code == 200

    build_response = client.post("/sync/build", params={"threshold_ms": 20})
    assert build_response.status_code == 200
    group_id = client.get("/sync/groups").json()["items"][0]["id"]

    async def fake_success_dispatch(group, processing_server_url):
        return {
            "success": True,
            "status_code": 200,
            "response_body": {"message": "ok"},
            "payload": {"syncGroupId": group["id"]},
        }

    monkeypatch.setattr(sync_routes, "dispatch_sync_group", fake_success_dispatch)
    dispatch_response = client.post(f"/sync/groups/{group_id}/dispatch")
    assert dispatch_response.status_code == 200

    retry_response = client.post(f"/sync/groups/{group_id}/retry")

    assert retry_response.status_code == 400
    assert retry_response.json()["detail"] == "Successful sync groups cannot be retried manually"


def test_sync_summary_endpoint_returns_operational_counts(client, monkeypatch):
    for device_id, timestamp in [
        ("camera1", 1000),
        ("camera2", 1010),
        ("camera3", 2000),
        ("camera4", 2010),
        ("camera5", 3000),
        ("camera6", 3010),
    ]:
        response = client.post(
            "/frames/register",
            data={
                "device_id": device_id,
                "timestamp": timestamp,
                "file_path": f"storage/{device_id}/{timestamp}.jpg",
            },
        )
        assert response.status_code == 200

    build_response = client.post("/sync/build", params={"threshold_ms": 20})
    assert build_response.status_code == 200

    group_ids = sorted(group["id"] for group in client.get("/sync/groups").json()["items"])
    retry_group_id, success_group_id, exhausted_group_id = group_ids

    async def fake_timeout_dispatch(group, processing_server_url):
        return {
            "success": False,
            "error": "Processing server timeout",
            "payload": {"syncGroupId": group["id"]},
        }

    monkeypatch.setattr(sync_routes, "dispatch_sync_group", fake_timeout_dispatch)
    monkeypatch.setattr(sync_service.time, "time", lambda: 100.0)
    retry_response = client.post(f"/sync/groups/{retry_group_id}/dispatch")
    assert retry_response.status_code == 200

    async def fake_success_dispatch(group, processing_server_url):
        return {
            "success": True,
            "status_code": 200,
            "response_body": {"message": "ok"},
            "payload": {"syncGroupId": group["id"]},
        }

    monkeypatch.setattr(sync_routes, "dispatch_sync_group", fake_success_dispatch)
    success_response = client.post(f"/sync/groups/{success_group_id}/dispatch")
    assert success_response.status_code == 200

    monkeypatch.setattr(sync_routes, "dispatch_sync_group", fake_timeout_dispatch)
    for _ in range(3):
        response = client.post(f"/sync/groups/{exhausted_group_id}/dispatch")
        assert response.status_code == 200

    monkeypatch.setattr(sync_service.time, "time", lambda: 106.0)
    summary_response = client.get("/sync/summary")

    assert summary_response.status_code == 200
    assert summary_response.json() == {
        "total_groups": 3,
        "pending": 0,
        "retry_scheduled": 1,
        "success": 1,
        "failed": 0,
        "exhausted": 1,
        "retry_ready": 1,
    }

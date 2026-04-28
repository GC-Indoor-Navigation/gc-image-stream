import asyncio

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import SyncFrame, SyncGroup
from app.services.frames.service import create_frame
from app.services.sync.service import (
    build_dispatch_payload,
    build_sync_groups,
    can_manually_retry_group,
    create_sync_group,
    dispatch_sync_group,
    get_groups_ready_for_retry,
    get_sync_groups,
    get_sync_summary,
    get_retry_delay_ms,
    is_retryable_dispatch_result,
    record_sync_group_dispatch_result,
)


def build_test_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return session_factory()


def test_build_dispatch_payload_maps_response_shape():
    group = {
        "id": 12,
        "group_timestamp": 1712321562400,
        "frames": [
            {
                "id": 1,
                "device_id": "camera1",
                "timestamp": 1712321562400,
                "file_path": "storage/camera1/1.jpg",
            },
            {
                "id": 2,
                "device_id": "camera2",
                "timestamp": 1712321562430,
                "file_path": "storage/camera2/2.jpg",
            },
        ],
    }

    payload = build_dispatch_payload(group)

    assert payload == {
        "syncGroupId": 12,
        "groupTimestamp": 1712321562400,
        "frames": [
            {
                "frameId": 1,
                "deviceId": "camera1",
                "timestamp": 1712321562400,
                "filePath": "storage/camera1/1.jpg",
            },
            {
                "frameId": 2,
                "deviceId": "camera2",
                "timestamp": 1712321562430,
                "filePath": "storage/camera2/2.jpg",
            },
        ],
    }


def test_dispatch_sync_group_returns_failure_for_http_error(monkeypatch):
    group = {
        "id": 12,
        "group_timestamp": 1712321562400,
        "frames": [],
    }

    class FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json):
            request = httpx.Request("POST", url, json=json)
            return httpx.Response(
                500,
                request=request,
                json={"detail": "processing failed"},
            )

    monkeypatch.setattr(httpx, "AsyncClient", lambda timeout=5.0: FakeAsyncClient())

    result = asyncio.run(dispatch_sync_group(group, "http://processing.local/process"))

    assert result == {
        "success": False,
        "error": "Processing server returned HTTP 500",
        "status_code": 500,
        "response_body": {"detail": "processing failed"},
        "payload": {
            "syncGroupId": 12,
            "groupTimestamp": 1712321562400,
            "frames": [],
        },
    }


def test_dispatch_sync_group_accepts_non_json_success_body(monkeypatch):
    group = {
        "id": 13,
        "group_timestamp": 1712321562500,
        "frames": [],
    }

    class FakeAsyncClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json):
            request = httpx.Request("POST", url, json=json)
            return httpx.Response(
                200,
                request=request,
                text="accepted",
            )

    monkeypatch.setattr(httpx, "AsyncClient", lambda timeout=5.0: FakeAsyncClient())

    result = asyncio.run(dispatch_sync_group(group, "http://processing.local/process"))

    assert result == {
        "success": True,
        "status_code": 200,
        "response_body": "accepted",
        "payload": {
            "syncGroupId": 13,
            "groupTimestamp": 1712321562500,
            "frames": [],
        },
    }


def test_build_sync_groups_creates_group_only_from_unsynced_frames_within_threshold():
    db = build_test_session()
    try:
        first = create_frame(db, "camera1", 1000, "storage/camera1/1000.jpg")
        second = create_frame(db, "camera2", 1030, "storage/camera2/1030.jpg")
        create_frame(db, "camera3", 1200, "storage/camera3/1200.jpg")

        groups = build_sync_groups(db, threshold_ms=50)

        assert len(groups) == 1
        synced_frame_ids = {item.frame_id for item in db.query(SyncFrame).all()}
        assert synced_frame_ids == {first.id, second.id}
    finally:
        db.close()


def test_build_sync_groups_skips_frames_already_assigned_to_existing_group():
    db = build_test_session()
    try:
        existing = create_frame(db, "camera1", 1000, "storage/camera1/1000.jpg")
        create_sync_group(db, 1000, [existing.id])
        second = create_frame(db, "camera2", 1010, "storage/camera2/1010.jpg")
        third = create_frame(db, "camera3", 1020, "storage/camera3/1020.jpg")

        groups = build_sync_groups(db, threshold_ms=30)

        assert len(groups) == 1
        synced_frame_ids = {item.frame_id for item in db.query(SyncFrame).all()}
        assert synced_frame_ids == {existing.id, second.id, third.id}
    finally:
        db.close()


def test_build_sync_groups_keeps_only_one_frame_per_device_in_group():
    db = build_test_session()
    try:
        first = create_frame(db, "camera1", 1000, "storage/camera1/1000.jpg")
        create_frame(db, "camera1", 1005, "storage/camera1/1005.jpg")
        third = create_frame(db, "camera2", 1010, "storage/camera2/1010.jpg")

        groups = build_sync_groups(db, threshold_ms=20)

        assert len(groups) == 1
        synced_frame_ids = {item.frame_id for item in db.query(SyncFrame).all()}
        assert synced_frame_ids == {first.id, third.id}
    finally:
        db.close()


def test_record_sync_group_dispatch_result_marks_success_and_failure():
    db = build_test_session()
    try:
        group = SyncGroup(group_timestamp=1000, dispatch_status="pending")
        db.add(group)
        db.commit()
        db.refresh(group)

        success_group = record_sync_group_dispatch_result(
            db,
            group.id,
            {
                "success": True,
                "status_code": 200,
                "response_body": {"message": "ok"},
            },
        )

        assert success_group.dispatch_status == "success"
        assert success_group.last_dispatch_status_code == 200
        assert success_group.last_dispatch_error is None
        assert success_group.last_dispatch_at is not None
        assert success_group.dispatched_at is not None
        assert success_group.retry_count == 0
        assert success_group.next_retry_at is None

        failed_group = record_sync_group_dispatch_result(
            db,
            group.id,
            {
                "success": False,
                "error": "Processing server timeout",
            },
        )

        assert failed_group.dispatch_status == "retry_scheduled"
        assert failed_group.last_dispatch_status_code is None
        assert failed_group.last_dispatch_error == "Processing server timeout"
        assert failed_group.last_dispatch_at is not None
        assert failed_group.dispatched_at is not None
        assert failed_group.retry_count == 1
        assert failed_group.next_retry_at == (
            failed_group.last_dispatch_at + get_retry_delay_ms(1)
        )
    finally:
        db.close()


def test_record_sync_group_dispatch_result_does_not_schedule_retry_for_non_retryable_error():
    db = build_test_session()
    try:
        group = SyncGroup(group_timestamp=1000, dispatch_status="pending")
        db.add(group)
        db.commit()
        db.refresh(group)

        failed_group = record_sync_group_dispatch_result(
            db,
            group.id,
            {
                "success": False,
                "status_code": 400,
                "error": "Processing server returned HTTP 400",
            },
        )

        assert failed_group.dispatch_status == "failed"
        assert failed_group.retry_count == 1
        assert failed_group.next_retry_at is None
    finally:
        db.close()


def test_get_groups_ready_for_retry_returns_only_due_retryable_groups():
    db = build_test_session()
    try:
        ready = SyncGroup(
            group_timestamp=1000,
            dispatch_status="retry_scheduled",
            retry_count=1,
            next_retry_at=1_000,
        )
        not_due = SyncGroup(
            group_timestamp=2000,
            dispatch_status="retry_scheduled",
            retry_count=1,
            next_retry_at=5_000,
        )
        success = SyncGroup(
            group_timestamp=3000,
            dispatch_status="success",
            retry_count=0,
            next_retry_at=None,
        )
        db.add_all([ready, not_due, success])
        db.commit()

        groups = get_groups_ready_for_retry(db, now_ms=2_000)

        assert [group["id"] for group in groups] == [ready.id]
    finally:
        db.close()


def test_is_retryable_dispatch_result_matches_retry_policy():
    assert is_retryable_dispatch_result(
        {"success": False, "error": "Processing server timeout"}
    )
    assert is_retryable_dispatch_result(
        {"success": False, "status_code": 503}
    )
    assert not is_retryable_dispatch_result(
        {"success": False, "status_code": 400}
    )
    assert not is_retryable_dispatch_result(
        {"success": True, "status_code": 200}
    )


def test_get_sync_groups_supports_retry_ready_and_exhausted_filters():
    db = build_test_session()
    try:
        ready = SyncGroup(
            group_timestamp=1000,
            dispatch_status="retry_scheduled",
            retry_count=1,
            next_retry_at=1_000,
        )
        exhausted = SyncGroup(
            group_timestamp=2000,
            dispatch_status="exhausted",
            retry_count=3,
            next_retry_at=None,
        )
        success = SyncGroup(
            group_timestamp=3000,
            dispatch_status="success",
            retry_count=0,
            next_retry_at=None,
        )
        db.add_all([ready, exhausted, success])
        db.commit()

        retry_ready_groups = get_sync_groups(db, retry_ready=True, now_ms=2_000)
        exhausted_groups = get_sync_groups(db, exhausted=True, now_ms=2_000)

        assert retry_ready_groups["total"] == 1
        assert exhausted_groups["total"] == 1
        assert [group["id"] for group in retry_ready_groups["items"]] == [ready.id]
        assert [group["id"] for group in exhausted_groups["items"]] == [exhausted.id]
    finally:
        db.close()


def test_get_sync_groups_supports_offset_and_sorting():
    db = build_test_session()
    try:
        low_retry = SyncGroup(
            group_timestamp=1000,
            dispatch_status="pending",
            retry_count=0,
        )
        high_retry = SyncGroup(
            group_timestamp=2000,
            dispatch_status="retry_scheduled",
            retry_count=3,
        )
        middle_retry = SyncGroup(
            group_timestamp=3000,
            dispatch_status="failed",
            retry_count=1,
        )
        db.add_all([low_retry, high_retry, middle_retry])
        db.commit()

        groups = get_sync_groups(
            db,
            limit=1,
            offset=1,
            sort_by="retry_count",
            sort_order="desc",
        )

        assert groups["total"] == 3
        assert groups["limit"] == 1
        assert groups["offset"] == 1
        assert [group["id"] for group in groups["items"]] == [middle_retry.id]
    finally:
        db.close()


def test_can_manually_retry_group_blocks_success_groups():
    assert can_manually_retry_group({"dispatch_status": "failed"})
    assert can_manually_retry_group({"dispatch_status": "retry_scheduled"})
    assert can_manually_retry_group({"dispatch_status": "exhausted"})
    assert can_manually_retry_group({"dispatch_status": "pending"})
    assert not can_manually_retry_group({"dispatch_status": "success"})


def test_record_sync_group_dispatch_result_marks_retryable_failures_as_exhausted_at_limit():
    db = build_test_session()
    try:
        group = SyncGroup(group_timestamp=1000, dispatch_status="pending", retry_count=2)
        db.add(group)
        db.commit()
        db.refresh(group)

        exhausted_group = record_sync_group_dispatch_result(
            db,
            group.id,
            {
                "success": False,
                "error": "Processing server timeout",
            },
        )

        assert exhausted_group.dispatch_status == "exhausted"
        assert exhausted_group.retry_count == 3
        assert exhausted_group.next_retry_at is None
    finally:
        db.close()


def test_get_sync_summary_returns_counts_by_status_and_retry_ready():
    db = build_test_session()
    try:
        db.add_all([
            SyncGroup(group_timestamp=1000, dispatch_status="pending"),
            SyncGroup(group_timestamp=2000, dispatch_status="success"),
            SyncGroup(
                group_timestamp=3000,
                dispatch_status="retry_scheduled",
                retry_count=1,
                next_retry_at=1_000,
            ),
            SyncGroup(
                group_timestamp=4000,
                dispatch_status="retry_scheduled",
                retry_count=1,
                next_retry_at=10_000,
            ),
            SyncGroup(group_timestamp=5000, dispatch_status="failed"),
            SyncGroup(group_timestamp=6000, dispatch_status="exhausted"),
        ])
        db.commit()

        summary = get_sync_summary(db, now_ms=5_000)

        assert summary == {
            "total_groups": 6,
            "pending": 1,
            "retry_scheduled": 2,
            "success": 1,
            "failed": 1,
            "exhausted": 1,
            "retry_ready": 1,
        }
    finally:
        db.close()

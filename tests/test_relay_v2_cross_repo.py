import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from app.infrastructure.grpc.live_relay_v2_client import (
    ProcessingLiveRelayV2Client,
    ReconnectBackoff,
)
from app.models import (
    CaptureRun,
    CaptureSession,
    FrameSetDeliveryProjection,
    FrameSetManifest,
    FrameSetMember,
)
from app.services.identity import canonical_json, sha256_bytes
from app.services.relay_v2 import ProtocolConfig


PROCESSING_SERVER = r"""
import sys
import time

from app.infrastructure.grpc_receiver import create_grpc_server
from app.infrastructure.relay_v2_credit_broker import RelayV2CreditBroker
from app.infrastructure.relay_v2_store import RelayV2WorkStore
from app.infrastructure.relay_v2_transport import (
    RelayV2ShadowPolicy,
    RelayV2ShadowTransport,
)

class SlowTransport(RelayV2ShadowTransport):
    def _receive_frame_set(self, *args, **kwargs):
        time.sleep(float(sys.argv[4]))
        yield from super()._receive_frame_set(*args, **kwargs)

store = RelayV2WorkStore(
    sys.argv[1],
    sys.argv[2],
    maximum_spool_bytes=64 * 1024 * 1024,
)
broker = RelayV2CreditBroker()
transport = SlowTransport(
    work_store=store,
    credit_broker=broker,
    policy=RelayV2ShadowPolicy(
        credit_lease_ms=1000,
        ingress_reserve_ms=20,
        maximum_payload_bytes=4 * 1024 * 1024,
        maximum_frame_age_ms=5000,
    ),
)
server = create_grpc_server(
    frame_handler=lambda frame: None,
    relay_v2_servicer=transport,
)
server.add_insecure_port("127.0.0.1:" + sys.argv[3])
server.start()
print("READY", flush=True)
try:
    while True:
        time.sleep(1)
finally:
    server.stop(0)
"""


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _start_processing_server(tmp_path, *, delay_sec=0.2, port=None):
    processing_repo = Path(__file__).resolve().parents[2] / "gc-image-processing"
    assert processing_repo.is_dir()
    port = port or _free_port()
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(processing_repo)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            PROCESSING_SERVER,
            str(tmp_path / "processing.db"),
            str(tmp_path / "processing-spool"),
            str(port),
            str(delay_sec),
        ],
        cwd=processing_repo,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(
                f"processing shadow exited early\nstdout={stdout}\nstderr={stderr}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return process, port
        except OSError:
            time.sleep(0.05)
    process.terminate()
    stdout, stderr = process.communicate(timeout=5)
    raise AssertionError(
        f"processing shadow did not start\nstdout={stdout}\nstderr={stderr}"
    )


def _persist_latest(
    session_factory,
    archive_dir,
    *,
    frame_set_id,
    capture_timestamp_ms,
):
    uid = f"set-{frame_set_id}"
    payload = f"frame-{frame_set_id}".encode()
    archive_path = archive_dir / f"{uid}.jpg"
    archive_path.write_bytes(payload)
    metadata = canonical_json(
        {
            "width": 640,
            "height": 480,
            "encoding": "image/jpeg",
            "color_space": "sRGB",
        }
    )
    manifest_json = json.dumps(
        {
            "members": [
                {
                    "camera_stream_id": "camera-1",
                    "capture_config_digest": "capture-config-1",
                    "capture_metadata_json": metadata,
                }
            ]
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    with session_factory() as db:
        if db.get(CaptureSession, "capture-session") is None:
            db.add(
                CaptureSession(
                    id="capture-session",
                    state="OPEN",
                    source_sessions_json="[]",
                    started_at_ms=capture_timestamp_ms,
                )
            )
            db.add(
                CaptureRun(
                    id="capture-run",
                    capture_session_id="capture-session",
                    identity_mode="V2",
                    state="OPEN",
                    started_at_ms=capture_timestamp_ms,
                )
            )
        for projection in (
            db.query(FrameSetDeliveryProjection)
            .filter(FrameSetDeliveryProjection.live_state == "ELIGIBLE")
            .all()
        ):
            projection.live_state = "SUPERSEDED_BEFORE_OFFER"
            projection.last_reason = "NEWER_FRAME_SET_AVAILABLE"
            projection.updated_at_ms = capture_timestamp_ms
        db.add(
            FrameSetManifest(
                frame_set_uid=uid,
                capture_session_id="capture-session",
                capture_run_id="capture-run",
                frame_set_id=frame_set_id,
                anchor_timestamp_ms=capture_timestamp_ms,
                freshness_origin_ms=capture_timestamp_ms,
                synchronization_span_ms=0,
                manifest_digest=f"manifest-{frame_set_id}",
                manifest_json=manifest_json,
                created_at_ms=capture_timestamp_ms,
                sync_window_ms=50,
                synchronized_at_ms=capture_timestamp_ms,
                member_count=1,
            )
        )
        db.add(
            FrameSetMember(
                frame_set_uid=uid,
                frame_id=None,
                source_frame_uid=f"source-{frame_set_id}",
                source_session_id="source-session",
                camera_stream_id="camera-1",
                frame_sequence=frame_set_id,
                capture_timestamp_ms=capture_timestamp_ms,
                content_type="image/jpeg",
                image_size=len(payload),
                content_digest=sha256_bytes(payload),
                file_path=str(archive_path),
            )
        )
        db.add(
            FrameSetDeliveryProjection(
                frame_set_uid=uid,
                archive_state="ARCHIVE_DURABLE",
                live_state="ELIGIBLE",
                legacy_relay_state="NOT_ENQUEUED",
                last_reason=None,
                updated_at_ms=capture_timestamp_ms,
            )
        )
        db.commit()
    return uid


def _wait_for_projection(session_factory, uid, *, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with session_factory() as db:
            state = db.get(FrameSetDeliveryProjection, uid).live_state
        if state in {"COMPLETED", "FAILED", "REJECTED", "RECOVERY_REQUIRED"}:
            return state
        time.sleep(0.02)
    raise AssertionError(f"frame set {uid} did not reach a terminal state")


def _wait_for_state(session_factory, uid, expected, *, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with session_factory() as db:
            state = db.get(FrameSetDeliveryProjection, uid).live_state
        if state == expected:
            return
        time.sleep(0.02)
    raise AssertionError(f"frame set {uid} did not reach {expected}")


def test_cross_repo_overload_stays_latest_only_and_bounded(
    session_factory,
    tmp_path,
):
    process, port = _start_processing_server(tmp_path, delay_sec=0.2)
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    client = ProcessingLiveRelayV2Client(
        backoff=ReconnectBackoff(initial_sec=0.02, maximum_sec=0.1),
    )
    try:
        base_ms = int(time.time() * 1000)
        first_uid = _persist_latest(
            session_factory,
            archive_dir,
            frame_set_id=1,
            capture_timestamp_ms=base_ms,
        )
        client.configure(
            target=f"127.0.0.1:{port}",
            enabled=True,
            session_factory=session_factory,
            protocol_config=ProtocolConfig(
                producer_session_id="producer-cross-repo",
                processing_profile_digest="profile-cross-repo",
                producer_freshness_budget_ms=5000,
            ),
        )
        client.start()
        assert _wait_for_projection(session_factory, first_uid) == "FAILED"

        latest_uid = first_uid
        for frame_set_id in range(2, 22):
            latest_uid = _persist_latest(
                session_factory,
                archive_dir,
                frame_set_id=frame_set_id,
                capture_timestamp_ms=base_ms + frame_set_id * 100,
            )
            time.sleep(0.1)

        assert _wait_for_projection(session_factory, latest_uid) == "FAILED"
        with session_factory() as db:
            active_count = (
                db.query(FrameSetDeliveryProjection)
                .filter(
                    FrameSetDeliveryProjection.live_state.in_(
                        ["OFFERED", "UNRESOLVED", "ACCEPTED", "STARTED"]
                    )
                )
                .count()
            )
            eligible_count = (
                db.query(FrameSetDeliveryProjection)
                .filter(FrameSetDeliveryProjection.live_state == "ELIGIBLE")
                .count()
            )
        assert active_count <= 1
        assert eligible_count == 0
        assert client.status()["in_flight"] is False

        with sqlite3.connect(tmp_path / "processing.db") as connection:
            processed_count = connection.execute(
                "SELECT COUNT(*) FROM relay_v2_work"
            ).fetchone()[0]
        assert 1 < processed_count < 21
    finally:
        client.stop()
        process.terminate()
        process.communicate(timeout=5)


def test_cross_repo_disconnect_is_unresolved_then_reconciled(
    session_factory,
    tmp_path,
):
    process, port = _start_processing_server(tmp_path, delay_sec=2.0)
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    uid = _persist_latest(
        session_factory,
        archive_dir,
        frame_set_id=1,
        capture_timestamp_ms=int(time.time() * 1000),
    )
    client = ProcessingLiveRelayV2Client(
        backoff=ReconnectBackoff(initial_sec=0.02, maximum_sec=0.1),
    )
    restarted = None
    try:
        client.configure(
            target=f"127.0.0.1:{port}",
            enabled=True,
            session_factory=session_factory,
            protocol_config=ProtocolConfig(
                producer_session_id="producer-reconnect",
                processing_profile_digest="profile-reconnect",
                producer_freshness_budget_ms=5000,
            ),
        )
        client.start()
        _wait_for_state(session_factory, uid, "OFFERED")

        process.terminate()
        process.communicate(timeout=5)
        _wait_for_state(session_factory, uid, "UNRESOLVED")

        restarted, _ = _start_processing_server(
            tmp_path,
            delay_sec=0.0,
            port=port,
        )

        assert _wait_for_projection(session_factory, uid) == "FAILED"
        status = client.status()
        assert status["connection_count"] >= 2
        assert status["reconnect_count"] >= 1
        assert status["in_flight"] is False
    finally:
        client.stop()
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=5)
        if restarted is not None:
            restarted.terminate()
            restarted.communicate(timeout=5)

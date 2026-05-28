from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from pathlib import Path

from app.infrastructure.grpc.generated import processing_relay_pb2_grpc
from app.infrastructure.grpc.processing_relay_client import (
    RelayAck,
    RelayFrame,
    RelayFrameSet,
    build_relay_frame_set,
)
from app.services.sync import StreamSyncService, SyncInputFrame
from scripts.storage_processing_relay import ROOT_DIR
from scripts.storage_processing_relay.console import RelayProgressBar
from scripts.storage_sync_replay.models import ReplayFrame, ReplayInput


RawRelayStubFactory = Callable[[str], Callable[[Iterable[RelayFrame]], RelayAck]]
FrameSetRelayStubFactory = Callable[[str], Callable[[Iterable[RelayFrameSet]], RelayAck]]


def relative_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT_DIR))
    except ValueError:
        return str(path)


def build_raw_relay_frame(frame: ReplayFrame) -> RelayFrame:
    image_bytes = frame.file_path.read_bytes()
    return RelayFrame(
        device_id=frame.device_id,
        timestamp_ms=frame.timestamp_ms,
        sequence=frame.sequence,
        content_type=frame.content_type,
        image_bytes=image_bytes,
        file_path=relative_path(frame.file_path),
    )


def relay_raw_frames(
    *,
    replay_input: ReplayInput,
    target: str,
    timeout_sec: float | None,
    progress_interval: int,
    stub_factory: RawRelayStubFactory | None = None,
) -> dict:
    channel = None
    sent_count = 0
    sent_image_bytes = 0
    progress = RelayProgressBar(
        label="raw frames",
        total=len(replay_input.frames),
        interval=progress_interval,
    )

    def iter_frames():
        nonlocal sent_count, sent_image_bytes
        progress.start()
        for index, frame in enumerate(replay_input.frames, start=1):
            relay_frame = build_raw_relay_frame(frame)
            sent_count += 1
            sent_image_bytes += len(relay_frame.image_bytes)
            progress.draw(index, sent=sent_count)
            yield relay_frame
        progress.finish(sent=sent_count)

    started_at = time.perf_counter()
    try:
        if stub_factory is not None:
            stub = stub_factory(target)
        else:
            import grpc

            channel = grpc.insecure_channel(target)
            stub = processing_relay_pb2_grpc.FrameRelayServiceStub(channel).StreamFrames
        ack = stub(iter_frames(), timeout=timeout_sec)
    finally:
        if channel is not None:
            channel.close()

    return {
        "sent_count": sent_count,
        "sent_image_bytes": sent_image_bytes,
        "ack_success": ack.success,
        "ack_received_count": ack.received_count,
        "ack_message": ack.message,
        "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
    }


def relay_frame_sets(
    *,
    replay_input: ReplayInput,
    target: str,
    timeout_sec: float | None,
    window_ms: int,
    buffer_size: int,
    recent_limit: int,
    progress_interval: int,
    stub_factory: FrameSetRelayStubFactory | None = None,
) -> dict:
    service = StreamSyncService()
    service.configure(
        enabled=True,
        expected_cameras=replay_input.expected_cameras,
        window_ms=window_ms,
        buffer_size=buffer_size,
        recent_limit=recent_limit,
    )

    channel = None
    sent_count = 0
    sent_image_bytes = 0
    no_set_count = 0
    progress = RelayProgressBar(
        label="frame sets",
        total=len(replay_input.frames),
        interval=progress_interval,
    )

    def iter_frame_sets():
        nonlocal sent_count, sent_image_bytes, no_set_count
        progress.start()
        for index, frame in enumerate(replay_input.frames, start=1):
            image_bytes = frame.file_path.read_bytes()
            frame_set = service.handle_frame(
                SyncInputFrame(
                    frame_id=frame.frame_id,
                    device_id=frame.device_id,
                    timestamp_ms=frame.timestamp_ms,
                    sequence=frame.sequence,
                    content_type=frame.content_type,
                    image_bytes=image_bytes,
                    file_path=relative_path(frame.file_path),
                )
            )
            if frame_set is None:
                no_set_count += 1
            else:
                relay_frame_set = build_relay_frame_set(frame_set)
                sent_count += 1
                sent_image_bytes += sum(
                    len(item.image_bytes)
                    for item in relay_frame_set.frames
                )
                yield relay_frame_set
            progress.draw(index, sent=sent_count)
        progress.finish(sent=sent_count)

    started_at = time.perf_counter()
    try:
        if stub_factory is not None:
            stub = stub_factory(target)
        else:
            import grpc

            channel = grpc.insecure_channel(target)
            stub = processing_relay_pb2_grpc.FrameRelayServiceStub(channel).StreamFrameSets
        ack = stub(iter_frame_sets(), timeout=timeout_sec)
    finally:
        if channel is not None:
            channel.close()

    status = service.status()
    return {
        "sent_count": sent_count,
        "sent_image_bytes": sent_image_bytes,
        "no_set_count": no_set_count,
        "dropped_stale_count": status.get("dropped_stale_count", 0),
        "last_reason": status.get("last_reason"),
        "ack_success": ack.success,
        "ack_received_count": ack.received_count,
        "ack_message": ack.message,
        "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
    }

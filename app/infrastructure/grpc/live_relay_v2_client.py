import logging
import queue
import random
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace

import grpc

from app.infrastructure.grpc.generated import (
    live_frame_relay_v2_pb2 as relay_pb2,
    live_frame_relay_v2_pb2_grpc,
)
from app.infrastructure.grpc.relay_v2_contract import PayloadLimitExceeded
from app.services.relay_v2 import (
    ArchiveIntegrityError,
    CreditRejected,
    FrameSetExpired,
    FrameSetKey,
    LatestLiveStore,
    NegotiatedSession,
    ProtocolConfig,
    accept_hello,
    build_credited_frame_set,
    build_no_data,
    build_producer_hello,
    build_reconciliation_request,
    bind_authorized_claim,
    credit_identity,
)
from app.services.session_identity import (
    ActiveSessionCredentialStore,
    active_session_credentials,
)
from app.services.relay_credentials import (
    ProcessingRelayScope,
    RelayCredentialSessionInactive,
)


LOGGER = logging.getLogger("gc_image_stream.relay_v2")
_STOP = object()


class PermanentRelayError(RuntimeError):
    pass


class CaptureRunChanged(RuntimeError):
    pass


@dataclass(frozen=True)
class ReconnectBackoff:
    initial_sec: float = 0.1
    maximum_sec: float = 5.0
    jitter_ratio: float = 0.2

    def delay(self, attempt: int, *, random_value: float | None = None) -> float:
        base = min(self.maximum_sec, self.initial_sec * (2 ** max(attempt, 0)))
        value = random.random() if random_value is None else random_value
        jitter = (value * 2 - 1) * self.jitter_ratio
        return max(0.0, base * (1 + jitter))


class ProcessingLiveRelayV2Client:
    def __init__(
        self,
        *,
        channel_factory: Callable[[str], object] = grpc.insecure_channel,
        monotonic: Callable[[], float] = time.monotonic,
        utc_now_ms: Callable[[], int] | None = None,
        backoff: ReconnectBackoff | None = None,
        credential_store: ActiveSessionCredentialStore = active_session_credentials,
        relay_credential_provider=None,
    ):
        self.enabled = False
        self.target = ""
        self.last_error: str | None = None
        self._channel_factory = channel_factory
        self._monotonic = monotonic
        self._utc_now_ms = utc_now_ms or (lambda: int(time.time() * 1000))
        self._backoff = backoff or ReconnectBackoff()
        self._store: LatestLiveStore | None = None
        self._protocol_config: ProtocolConfig | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._channel = None
        self._lock = threading.RLock()
        self._connection_count = 0
        self._reconnect_count = 0
        self._offered_count = 0
        self._no_data_count = 0
        self._credential_store = credential_store
        self._relay_credential_provider = relay_credential_provider

    def configure(
        self,
        *,
        target: str = "",
        enabled: bool = False,
        session_factory=None,
        protocol_config: ProtocolConfig | None = None,
        relay_credential_provider=None,
    ) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("cannot configure a running relay v2 client")
            if enabled and (not target or session_factory is None or protocol_config is None):
                raise ValueError(
                    "enabled relay v2 requires target, session factory, and protocol config"
                )
            self.target = target
            self.enabled = enabled
            self._store = (
                LatestLiveStore(session_factory)
                if session_factory is not None
                else None
            )
            self._protocol_config = protocol_config
            self._relay_credential_provider = relay_credential_provider
            self.last_error = None

    def build_stub(self, channel):
        return live_frame_relay_v2_pb2_grpc.LiveFrameRelayServiceStub(
            channel
        ).Relay

    def start(self):
        with self._lock:
            if not self.enabled:
                return None
            if self._thread is not None and self._thread.is_alive():
                return None
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="processing-live-relay-v2",
                daemon=True,
            )
            self._thread.start()
        return None

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            channel = self._channel
        if channel is not None:
            channel.close()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
        store = self._store
        if store is not None:
            current = store.current_in_flight()
            if current is not None:
                store.mark_unresolved(current, reason="SHUTDOWN_INTERRUPTED")
        with self._lock:
            self._channel = None
            self._thread = None

    def status(self) -> dict:
        with self._lock:
            running = self._thread is not None and self._thread.is_alive()
            return {
                "contract_registered": True,
                "enabled": self.enabled,
                "running": running,
                "target": self.target,
                "last_error": self.last_error,
                "connection_count": self._connection_count,
                "reconnect_count": self._reconnect_count,
                "offered_count": self._offered_count,
                "no_data_count": self._no_data_count,
                "in_flight": (
                    self._store.current_in_flight() is not None
                    if self._store is not None
                    else False
                ),
            }

    def _run(self) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            try:
                connected = self._run_connection()
                if connected is None:
                    attempt = 0
                    continue
                if connected:
                    attempt = 0
            except PermanentRelayError as exc:
                self.last_error = str(exc)
                return
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
                LOGGER.warning("relay v2 connection ended: %s", self.last_error)
            if self._stop_event.is_set():
                return
            with self._lock:
                self._reconnect_count += 1
            delay = self._backoff.delay(attempt)
            attempt += 1
            self._stop_event.wait(delay)

    def _run_connection(self) -> bool | None:
        store = self._required_store()
        base_config = self._required_config()
        hello_snapshot = store.snapshot_for_hello()
        if hello_snapshot is None:
            self._stop_event.wait(0.05)
            return None
        config = bind_authorized_claim(base_config, hello_snapshot)
        credential = None
        relay_scope = None
        if hello_snapshot.processing_job_id:
            if self._relay_credential_provider is not None:
                try:
                    credential = self._relay_credential_provider.resolve_for_claim(
                        hello_snapshot
                    )
                except RelayCredentialSessionInactive:
                    store.retire_eligible(
                        hello_snapshot.key,
                        updated_at_ms=self._utc_now_ms(),
                    )
                    return None
                relay_scope = credential.scope
            else:
                credential = self._credential_store.resolve_for_claim(
                    hello_snapshot
                )
        authorized_hello_snapshot = _bind_relay_scope(
            hello_snapshot,
            relay_scope,
        )

        outgoing: queue.Queue = queue.Queue(maxsize=2)
        outgoing.put(
            build_producer_hello(
                config=config,
                claim=authorized_hello_snapshot,
                watermark=store.offered_watermark(),
                unresolved=store.unresolved_keys(),
                proposed_processing_job_id=store.processing_job_for(
                    hello_snapshot.key.capture_run_id
                )
                or hello_snapshot.processing_job_id,
                measured_utc_ms=self._utc_now_ms(),
            )
        )
        channel = self._channel_factory(self.target)
        with self._lock:
            if self._stop_event.is_set():
                channel.close()
                return False
            self._channel = channel
        session: NegotiatedSession | None = None
        reconciled = not bool(store.unresolved_keys())
        connected = False
        try:
            call = self.build_stub(channel)
            responses = (
                call(
                    self._requests(outgoing),
                    metadata=(("authorization", f"Bearer {credential.token}"),),
                )
                if credential is not None
                else call(self._requests(outgoing))
            )
            for envelope in responses:
                if self._stop_event.is_set():
                    break
                body = envelope.WhichOneof("body")
                if session is None:
                    if body == "hello_rejected" and not envelope.hello_rejected.retryable:
                        if (
                            credential is not None
                            and envelope.hello_rejected.detail_code == "SESSION_TOKEN_SCOPE_INVALID"
                            and store.current_in_flight() is None
                            and store.retire_eligible(
                                hello_snapshot.key,
                                state="REJECTED",
                                reason="SESSION_TOKEN_SCOPE_INVALID",
                                updated_at_ms=self._utc_now_ms(),
                            )
                        ):
                            # A stopped session must not permanently disable the
                            # shared worker. Never discard unresolved sent work.
                            self.last_error = "relay candidate rejected: SESSION_TOKEN_SCOPE_INVALID"
                            return None
                        raise PermanentRelayError(
                            f"relay v2 disabled: {envelope.hello_rejected.detail_code}"
                        )
                    session = accept_hello(envelope, config=config)
                    store.bind_processing_job(
                        capture_run_id=hello_snapshot.key.capture_run_id,
                        processing_job_id=session.processing_job_id,
                        updated_at_ms=self._utc_now_ms(),
                    )
                    connected = True
                    with self._lock:
                        self._connection_count += 1
                    unresolved = store.unresolved_keys()
                    if envelope.hello_accepted.reconciliation_required or unresolved:
                        outgoing.put(
                            build_reconciliation_request(
                                session=session,
                                config=config,
                                unresolved=unresolved,
                                resume_token=(
                                    envelope.hello_accepted.reconciliation_resume_token
                                    or None
                                ),
                            )
                        )
                        reconciled = not unresolved
                    continue
                if body == "reconciliation":
                    reconciled = self._apply_reconciliation(
                        envelope.reconciliation,
                        store,
                    )
                elif body == "credit":
                    if not reconciled:
                        raise CreditRejected(
                            "processor issued credit before reconciliation completed"
                        )
                    self._handle_credit(
                        envelope.credit,
                        session,
                        outgoing,
                        expected_capture_run_id=(
                            hello_snapshot.key.capture_run_id
                        ),
                        config=config,
                        relay_scope=relay_scope,
                    )
                elif body == "status":
                    self._apply_status(envelope.status, store)
                elif body in {"hello_accepted", "hello_rejected"}:
                    raise CreditRejected("processor repeated hello response")
        finally:
            try:
                outgoing.put_nowait(_STOP)
            except queue.Full:
                pass
            channel.close()
            with self._lock:
                if self._channel is channel:
                    self._channel = None
            current = store.current_in_flight()
            if current is not None:
                store.mark_unresolved(current)
        return connected

    def _handle_credit(
        self,
        credit: relay_pb2.ProcessorCredit,
        session: NegotiatedSession,
        outgoing: queue.Queue,
        expected_capture_run_id: str | None = None,
        config: ProtocolConfig | None = None,
        relay_scope: ProcessingRelayScope | None = None,
    ) -> None:
        received_monotonic = self._monotonic()
        identity = credit_identity(credit, session)
        store = self._required_store()
        preview = store.snapshot_for_hello()
        if (
            preview is not None
            and expected_capture_run_id is not None
            and preview.key.capture_run_id != expected_capture_run_id
        ):
            raise CaptureRunChanged(
                "capture run changed; a new hello is required"
            )
        claim = store.claim_latest(identity, offered_at_ms=self._utc_now_ms())
        if claim is None:
            outgoing.put(
                build_no_data(
                    credit,
                    newest_known=store.offered_watermark(),
                )
            )
            with self._lock:
                self._no_data_count += 1
            return
        claim = _bind_relay_scope(claim, relay_scope)
        try:
            payload = build_credited_frame_set(
                claim=claim,
                credit=credit,
                session=session,
                config=config or self._required_config(),
                credit_received_monotonic=received_monotonic,
                now_monotonic=self._monotonic(),
                now_utc_ms=self._utc_now_ms(),
            )
        except FrameSetExpired:
            store.release_before_send(
                claim.key,
                state="EXPIRED_BEFORE_OFFER",
                reason="DEADLINE_EXPIRED_BEFORE_SEND",
            )
            outgoing.put(
                build_no_data(
                    credit,
                    reason=relay_pb2.EXPIRED_BEFORE_OFFER,
                    newest_known=claim.key,
                )
            )
            return
        except (ArchiveIntegrityError, OSError) as exc:
            store.release_before_send(
                claim.key,
                state="REJECTED",
                reason=f"INTEGRITY_ERROR:{type(exc).__name__}",
            )
            outgoing.put(
                build_no_data(
                    credit,
                    reason=relay_pb2.INTEGRITY_ERROR,
                    newest_known=claim.key,
                )
            )
            return
        except PayloadLimitExceeded:
            store.release_before_send(
                claim.key,
                state="REJECTED",
                reason="PAYLOAD_TOO_LARGE",
            )
            outgoing.put(
                build_no_data(
                    credit,
                    reason=relay_pb2.PAYLOAD_TOO_LARGE,
                    newest_known=claim.key,
                )
            )
            return
        except CreditRejected:
            store.release_before_send(
                claim.key,
                state="REJECTED",
                reason="CREDIT_EXPIRED",
            )
            raise
        if self._stop_event.is_set():
            store.mark_unresolved(claim.key, reason="SHUTDOWN_INTERRUPTED")
            return
        outgoing.put(payload)
        with self._lock:
            self._offered_count += 1

    def _apply_reconciliation(self, response, store: LatestLiveStore) -> bool:
        seen = set()
        for status in response.canonical_statuses:
            key = self._status_key(status.key)
            seen.add(key.frame_set_uid)
            self._apply_status(status, store)
        if response.complete:
            for key in store.unresolved_keys():
                if key.frame_set_uid not in seen:
                    store.reconcile_not_found(
                        key,
                        retry_allowed=not store.has_newer_eligible(key),
                    )
            return True
        return False

    @staticmethod
    def _apply_status(status, store: LatestLiveStore) -> None:
        state = relay_pb2.ProcessingAttemptState.Name(status.state)
        reason = relay_pb2.Reason.Name(status.reason)
        store.apply_remote_status(
            ProcessingLiveRelayV2Client._status_key(status.key),
            state=state,
            reason=reason,
        )

    @staticmethod
    def _status_key(key) -> FrameSetKey:
        return FrameSetKey(
            capture_run_id=key.capture_run_id,
            frame_set_id=key.frame_set_id,
            frame_set_uid=key.frame_set_uid,
        )

    def _requests(self, outgoing: queue.Queue) -> Iterator:
        while not self._stop_event.is_set():
            item = outgoing.get()
            if item is _STOP:
                return
            yield item

    def _required_store(self) -> LatestLiveStore:
        if self._store is None:
            raise RuntimeError("relay v2 store is not configured")
        return self._store

    def _required_config(self) -> ProtocolConfig:
        if self._protocol_config is None:
            raise RuntimeError("relay v2 protocol is not configured")
        return self._protocol_config


processing_live_relay_v2_client = ProcessingLiveRelayV2Client()


def _bind_relay_scope(claim, scope: ProcessingRelayScope | None):
    if scope is None:
        return claim
    if not scope.matches_claim(claim):
        raise PermanentRelayError(
            "processing relay credential does not match current frame set"
        )
    return replace(
        claim,
        authorized_subject=scope.coordinator_subject,
        session_token_jti=scope.token_jti,
    )

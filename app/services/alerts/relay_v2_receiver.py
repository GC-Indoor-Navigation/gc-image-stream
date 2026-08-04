from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


RECEIVER_CONTRACT_VERSION = 1
VALID_STATES = {"CLEAR", "WARNING", "DANGER", "UNKNOWN"}
EXPECTED_SEVERITY = {
    "CLEAR": "info",
    "WARNING": "warning",
    "DANGER": "danger",
    "UNKNOWN": "warning",
}


@dataclass(frozen=True)
class RelayV2AlertEnvelope:
    contract_version: int
    idempotency_key: str
    payload_digest: str
    processing_job_id: str
    frame_set_uid: str
    transition_event_id: str
    hazard_key: str
    from_state: str
    to_state: str
    from_version: int
    to_version: int
    severity: str
    observation_event_utc_ms: int
    delivery_deadline_utc_ms: int
    alert_payload: dict[str, Any]


@dataclass(frozen=True)
class RelayV2ReceiveResult:
    contract_version: int
    idempotency_key: str
    payload_digest: str
    status: str
    receiver_hazard_version: int
    user_visible_effect_applied: bool
    received_at_utc_ms: int
    delivery_deadline_met: bool | None


class RelayV2AlertReceiver:
    """Durable idempotency and hazard-version boundary for relay v2 alerts."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        current_time_ms: Callable[[], int],
    ):
        self.database_path = str(database_path)
        self._current_time_ms = current_time_ms
        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def receive(self, envelope: RelayV2AlertEnvelope) -> RelayV2ReceiveResult:
        received_at = self._current_time_ms()
        if received_at <= 0:
            raise ValueError("receiver time must be positive")
        payload_digest = _payload_digest(envelope.alert_payload)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            hazard_version = self._hazard_version(connection, envelope.hazard_key)
            previous = connection.execute(
                """
                SELECT payload_digest, status, receiver_hazard_version,
                       delivery_deadline_met
                FROM relay_v2_alert_receipts
                WHERE idempotency_key = ?
                """,
                (envelope.idempotency_key,),
            ).fetchone()
            if previous is not None:
                if previous["payload_digest"] != envelope.payload_digest:
                    self._record_conflict(
                        connection,
                        envelope,
                        received_at,
                        reason="IDEMPOTENCY_DIGEST_CONFLICT",
                    )
                    connection.commit()
                    return self._result(
                        envelope,
                        "CONFLICT",
                        hazard_version,
                        received_at,
                        None,
                    )
                connection.commit()
                deadline_met = previous["delivery_deadline_met"]
                return self._result(
                    envelope,
                    (
                        "DUPLICATE"
                        if previous["status"] == "APPLIED"
                        else previous["status"]
                    ),
                    previous["receiver_hazard_version"],
                    received_at,
                    None if deadline_met is None else bool(deadline_met),
                )

            rejection = self._validate(envelope, payload_digest)
            if rejection is not None:
                self._record_rejection(connection, envelope, received_at, rejection)
                connection.commit()
                return self._result(
                    envelope,
                    rejection,
                    hazard_version,
                    received_at,
                    None,
                )

            hazard = connection.execute(
                """
                SELECT state, version FROM relay_v2_receiver_hazards
                WHERE hazard_key = ?
                """,
                (envelope.hazard_key,),
            ).fetchone()
            current_version = 0 if hazard is None else hazard["version"]
            current_state = envelope.from_state if hazard is None else hazard["state"]
            if (
                current_version != envelope.from_version
                or current_state != envelope.from_state
            ):
                self._record_rejection(
                    connection,
                    envelope,
                    received_at,
                    "VERSION_GAP",
                    receiver_hazard_version=current_version,
                )
                connection.commit()
                return self._result(
                    envelope,
                    "VERSION_GAP",
                    current_version,
                    received_at,
                    None,
                )

            deadline_met = received_at < envelope.delivery_deadline_utc_ms
            connection.execute(
                """
                INSERT INTO relay_v2_receiver_hazards(hazard_key, state, version, updated_at_ms)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(hazard_key) DO UPDATE SET
                    state = excluded.state,
                    version = excluded.version,
                    updated_at_ms = excluded.updated_at_ms
                WHERE relay_v2_receiver_hazards.version = ?
                  AND relay_v2_receiver_hazards.state = ?
                """,
                (
                    envelope.hazard_key,
                    envelope.to_state,
                    envelope.to_version,
                    received_at,
                    envelope.from_version,
                    envelope.from_state,
                ),
            )
            connection.execute(
                """
                INSERT INTO relay_v2_alert_effects(
                    idempotency_key, hazard_key, hazard_version, payload_json,
                    created_at_ms, delivery_deadline_met
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    envelope.idempotency_key,
                    envelope.hazard_key,
                    envelope.to_version,
                    _canonical_payload(envelope.alert_payload),
                    received_at,
                    int(deadline_met),
                ),
            )
            self._record_receipt(
                connection,
                envelope,
                received_at,
                "APPLIED",
                envelope.to_version,
                deadline_met,
            )
            connection.commit()
            return self._result(
                envelope,
                "APPLIED",
                envelope.to_version,
                received_at,
                deadline_met,
                effect_applied=True,
            )

    def _validate(self, envelope: RelayV2AlertEnvelope, actual_digest: str) -> str | None:
        if envelope.contract_version != RECEIVER_CONTRACT_VERSION:
            return "REJECTED"
        required = (
            envelope.idempotency_key,
            envelope.payload_digest,
            envelope.processing_job_id,
            envelope.frame_set_uid,
            envelope.transition_event_id,
            envelope.hazard_key,
        )
        if any(not value or not value.strip() for value in required):
            return "REJECTED"
        if actual_digest != envelope.payload_digest:
            return "CONFLICT"
        if envelope.from_state not in VALID_STATES or envelope.to_state not in VALID_STATES:
            return "REJECTED"
        if EXPECTED_SEVERITY[envelope.to_state] != envelope.severity:
            return "REJECTED"
        if envelope.from_version < 0 or envelope.to_version != envelope.from_version + 1:
            return "REJECTED"
        if envelope.observation_event_utc_ms <= 0 or envelope.delivery_deadline_utc_ms <= 0:
            return "REJECTED"
        return None

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS relay_v2_receiver_hazards (
                    hazard_key TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version >= 0),
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS relay_v2_alert_receipts (
                    idempotency_key TEXT PRIMARY KEY,
                    payload_digest TEXT NOT NULL,
                    hazard_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    receiver_hazard_version INTEGER NOT NULL,
                    received_at_ms INTEGER NOT NULL,
                    delivery_deadline_met INTEGER
                );
                CREATE TABLE IF NOT EXISTS relay_v2_alert_effects (
                    idempotency_key TEXT PRIMARY KEY,
                    hazard_key TEXT NOT NULL,
                    hazard_version INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    delivery_deadline_met INTEGER NOT NULL,
                    FOREIGN KEY(idempotency_key)
                        REFERENCES relay_v2_alert_receipts(idempotency_key)
                        DEFERRABLE INITIALLY DEFERRED
                );
                CREATE TABLE IF NOT EXISTS relay_v2_alert_conflicts (
                    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT NOT NULL,
                    claimed_payload_digest TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    received_at_ms INTEGER NOT NULL
                );
                """
            )
            connection.commit()

    def _record_rejection(
        self,
        connection: sqlite3.Connection,
        envelope: RelayV2AlertEnvelope,
        received_at: int,
        status: str,
        *,
        receiver_hazard_version: int | None = None,
    ) -> None:
        version = (
            self._hazard_version(connection, envelope.hazard_key)
            if receiver_hazard_version is None
            else receiver_hazard_version
        )
        self._record_receipt(
            connection,
            envelope,
            received_at,
            status,
            version,
            None,
        )

    @staticmethod
    def _record_receipt(
        connection: sqlite3.Connection,
        envelope: RelayV2AlertEnvelope,
        received_at: int,
        status: str,
        receiver_hazard_version: int,
        deadline_met: bool | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO relay_v2_alert_receipts(
                idempotency_key, payload_digest, hazard_key, status,
                receiver_hazard_version, received_at_ms, delivery_deadline_met
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                envelope.idempotency_key,
                envelope.payload_digest,
                envelope.hazard_key,
                status,
                receiver_hazard_version,
                received_at,
                None if deadline_met is None else int(deadline_met),
            ),
        )

    @staticmethod
    def _record_conflict(
        connection: sqlite3.Connection,
        envelope: RelayV2AlertEnvelope,
        received_at: int,
        *,
        reason: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO relay_v2_alert_conflicts(
                idempotency_key, claimed_payload_digest, reason, received_at_ms
            ) VALUES (?, ?, ?, ?)
            """,
            (envelope.idempotency_key, envelope.payload_digest, reason, received_at),
        )

    @staticmethod
    def _hazard_version(connection: sqlite3.Connection, hazard_key: str) -> int:
        row = connection.execute(
            "SELECT version FROM relay_v2_receiver_hazards WHERE hazard_key = ?",
            (hazard_key,),
        ).fetchone()
        return 0 if row is None else row["version"]

    @staticmethod
    def _result(
        envelope: RelayV2AlertEnvelope,
        status: str,
        version: int,
        received_at: int,
        deadline_met: bool | None,
        *,
        effect_applied: bool = False,
    ) -> RelayV2ReceiveResult:
        return RelayV2ReceiveResult(
            contract_version=RECEIVER_CONTRACT_VERSION,
            idempotency_key=envelope.idempotency_key,
            payload_digest=envelope.payload_digest,
            status=status,
            receiver_hazard_version=version,
            user_visible_effect_applied=effect_applied,
            received_at_utc_ms=received_at,
            delivery_deadline_met=deadline_met,
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection


def _canonical_payload(payload: dict[str, Any]) -> str:
    try:
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("alert payload must be canonical JSON data") from exc


def _payload_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_payload(payload).encode("utf-8")).hexdigest()

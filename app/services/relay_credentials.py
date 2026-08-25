import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import RLock
from uuid import UUID, uuid4

import httpx
import jwt

from app.services.session_identity import (
    JwksKeyCache,
    PROFILE_DIGEST_PATTERN,
    SessionTokenError,
)


RELAY_CREDENTIAL_REQUIRED_CLAIMS = (
    "iss",
    "aud",
    "sub",
    "credential_kind",
    "tenant_id",
    "site_id",
    "capture_session_id",
    "processing_job_id",
    "profile_digest",
    "coordinator_subject",
    "camera_ids",
    "iat",
    "nbf",
    "exp",
    "jti",
)


@dataclass(frozen=True)
class ProcessingRelayScope:
    tenant_id: str
    site_id: str
    capture_session_id: str
    processing_job_id: str
    profile_digest: str
    coordinator_subject: str
    camera_ids: frozenset[str]
    workload_subject: str
    token_jti: str
    expires_at: int

    def matches_claim(self, claim) -> bool:
        return (
            claim.tenant_id == self.tenant_id
            and claim.site_id == self.site_id
            and claim.capture_session_id == self.capture_session_id
            and claim.processing_job_id == self.processing_job_id
            and claim.profile_digest == self.profile_digest
            and frozenset(claim.authorized_camera_ids) == self.camera_ids
        )


@dataclass(frozen=True)
class ActiveProcessingRelayCredential:
    token: str
    scope: ProcessingRelayScope


class RelayCredentialSessionInactive(SessionTokenError):
    """Main confirmed that the manifest's processing job is no longer active."""


class ProcessingRelayCredentialVerifier:
    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        key_cache: JwksKeyCache,
        leeway_sec: int = 5,
        now: Callable[[], float] = time.time,
    ):
        if not issuer or not audience:
            raise ValueError("issuer and audience are required")
        self.issuer = issuer
        self.audience = audience
        self.key_cache = key_cache
        self.leeway_sec = leeway_sec
        self.now = now

    def verify(self, token: str) -> ProcessingRelayScope:
        if not token:
            raise SessionTokenError("processing relay credential is required")
        try:
            header = jwt.get_unverified_header(token)
            if header.get("alg") != "RS256":
                raise SessionTokenError(
                    "processing relay credential algorithm must be RS256"
                )
            key = self.key_cache.resolve(str(header.get("kid") or ""))
            claims = jwt.decode(
                token,
                key=key.key,
                algorithms=["RS256"],
                issuer=self.issuer,
                audience=self.audience,
                leeway=self.leeway_sec,
                options={"require": list(RELAY_CREDENTIAL_REQUIRED_CLAIMS)},
            )
            scope = _relay_scope_from_claims(claims)
            if scope.expires_at <= int(self.now()):
                raise SessionTokenError("processing relay credential is expired")
            return scope
        except SessionTokenError:
            raise
        except jwt.PyJWTError as exc:
            raise SessionTokenError(
                "processing relay credential validation failed"
            ) from exc


@dataclass(frozen=True)
class WorkloadAccessToken:
    value: str
    expires_at: float


class ClientCredentialsTokenProvider:
    def __init__(
        self,
        *,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str = "",
        timeout_sec: float = 2.0,
        refresh_skew_sec: float = 15.0,
        post: Callable = httpx.post,
        now: Callable[[], float] = time.time,
    ):
        if not token_url or not client_id or not client_secret:
            raise ValueError("workload token URL, client ID, and secret are required")
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.timeout_sec = timeout_sec
        self.refresh_skew_sec = refresh_skew_sec
        self.post = post
        self.now = now
        self._cached: WorkloadAccessToken | None = None
        self._lock = RLock()

    def resolve(self) -> str:
        with self._lock:
            current = self.now()
            if (
                self._cached is not None
                and current + self.refresh_skew_sec < self._cached.expires_at
            ):
                return self._cached.value
            data = {"grant_type": "client_credentials"}
            if self.scope:
                data["scope"] = self.scope
            response = self.post(
                self.token_url,
                data=data,
                auth=(self.client_id, self.client_secret),
                timeout=self.timeout_sec,
            )
            response.raise_for_status()
            payload = response.json()
            token = payload.get("access_token") if isinstance(payload, Mapping) else None
            expires_in = payload.get("expires_in") if isinstance(payload, Mapping) else None
            if not isinstance(token, str) or not token:
                raise SessionTokenError("workload token response is missing access_token")
            if not isinstance(expires_in, (int, float)) or expires_in <= 0:
                raise SessionTokenError("workload token response has invalid expires_in")
            self._cached = WorkloadAccessToken(
                value=token,
                expires_at=current + float(expires_in),
            )
            return token


class MainProcessingRelayCredentialProvider:
    def __init__(
        self,
        *,
        url_template: str,
        access_tokens: ClientCredentialsTokenProvider,
        verifier: ProcessingRelayCredentialVerifier,
        timeout_sec: float = 2.0,
        refresh_skew_sec: int = 15,
        post: Callable = httpx.post,
        now: Callable[[], float] = time.time,
    ):
        if "{tenant_id}" not in url_template or "{processing_job_id}" not in url_template:
            raise ValueError(
                "relay credential URL must contain tenant_id and processing_job_id"
            )
        self.url_template = url_template
        self.access_tokens = access_tokens
        self.verifier = verifier
        self.timeout_sec = timeout_sec
        self.refresh_skew_sec = refresh_skew_sec
        self.post = post
        self.now = now
        self._credentials: dict[str, ActiveProcessingRelayCredential] = {}
        self._lock = RLock()

    def resolve_for_claim(self, claim) -> ActiveProcessingRelayCredential:
        processing_job_id = claim.processing_job_id
        if not processing_job_id:
            raise SessionTokenError("manifest is missing processing_job_id")
        with self._lock:
            existing = self._credentials.get(processing_job_id)
            if existing is not None and (
                existing.scope.expires_at
                > int(self.now()) + self.refresh_skew_sec
                and existing.scope.matches_claim(claim)
            ):
                return existing
            credential = self._issue(claim)
            self._credentials[processing_job_id] = credential
            return credential

    def clear(self) -> None:
        with self._lock:
            self._credentials.clear()

    def _issue(self, claim) -> ActiveProcessingRelayCredential:
        access_token = self.access_tokens.resolve()
        response = self.post(
            self.url_template.format(
                tenant_id=claim.tenant_id,
                processing_job_id=claim.processing_job_id,
            ),
            headers={
                "Authorization": f"Bearer {access_token}",
                "Idempotency-Key": str(uuid4()),
            },
            timeout=self.timeout_sec,
        )
        if response.status_code == 409:
            raise RelayCredentialSessionInactive("processing job is inactive")
        response.raise_for_status()
        relay_token = response.headers.get("X-GC-Processing-Relay-Token")
        if not relay_token:
            raise SessionTokenError(
                "Main relay credential response did not include the one-time token"
            )
        payload = response.json()
        scope = self.verifier.verify(relay_token)
        if not scope.matches_claim(claim):
            raise SessionTokenError(
                "processing relay credential does not match the manifest scope"
            )
        _assert_response_matches_scope(payload, scope)
        return ActiveProcessingRelayCredential(token=relay_token, scope=scope)


def _relay_scope_from_claims(claims: Mapping) -> ProcessingRelayScope:
    if claims.get("credential_kind") != "PROCESSING_RELAY":
        raise SessionTokenError("credential_kind must be PROCESSING_RELAY")
    profile_digest = claims.get("profile_digest")
    if not isinstance(profile_digest, str) or not PROFILE_DIGEST_PATTERN.fullmatch(
        profile_digest
    ):
        raise SessionTokenError("profile_digest must be a lowercase SHA-256 digest")
    camera_ids = claims.get("camera_ids")
    if not isinstance(camera_ids, list) or not camera_ids:
        raise SessionTokenError("camera_ids must be a non-empty list")
    normalized_cameras = frozenset(
        _canonical_uuid(value, "camera_ids") for value in camera_ids
    )
    if len(normalized_cameras) != len(camera_ids):
        raise SessionTokenError("camera_ids must not contain duplicates")
    expires_at = claims.get("exp")
    if not isinstance(expires_at, int):
        raise SessionTokenError("exp must be an integer timestamp")
    return ProcessingRelayScope(
        tenant_id=_canonical_uuid(claims.get("tenant_id"), "tenant_id"),
        site_id=_canonical_uuid(claims.get("site_id"), "site_id"),
        capture_session_id=_canonical_uuid(
            claims.get("capture_session_id"), "capture_session_id"
        ),
        processing_job_id=_canonical_uuid(
            claims.get("processing_job_id"), "processing_job_id"
        ),
        profile_digest=profile_digest,
        coordinator_subject=_required_text(
            claims.get("coordinator_subject"), "coordinator_subject"
        ),
        camera_ids=normalized_cameras,
        workload_subject=_required_text(claims.get("sub"), "sub"),
        token_jti=_canonical_uuid(claims.get("jti"), "jti"),
        expires_at=expires_at,
    )


def _assert_response_matches_scope(payload, scope: ProcessingRelayScope) -> None:
    if not isinstance(payload, Mapping):
        raise SessionTokenError("Main relay credential response must be an object")
    expected = (
        payload.get("credentialId"),
        payload.get("captureSessionId"),
        payload.get("processingJobId"),
        payload.get("credentialKind"),
        frozenset(payload.get("cameraIds") or []),
    )
    actual = (
        scope.token_jti,
        scope.capture_session_id,
        scope.processing_job_id,
        "PROCESSING_RELAY",
        scope.camera_ids,
    )
    if expected != actual:
        raise SessionTokenError(
            "Main relay credential response does not match its signed token"
        )


def _canonical_uuid(value, claim_name: str) -> str:
    if not isinstance(value, str):
        raise SessionTokenError(f"{claim_name} must be a UUID string")
    try:
        normalized = str(UUID(value))
    except ValueError as exc:
        raise SessionTokenError(f"{claim_name} must be a UUID string") from exc
    if normalized != value.lower():
        raise SessionTokenError(f"{claim_name} must use canonical UUID form")
    return normalized


def _required_text(value, claim_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SessionTokenError(f"{claim_name} must be non-empty")
    return value.strip()

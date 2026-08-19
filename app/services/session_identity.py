import re
import time
from dataclasses import dataclass
from threading import RLock
from typing import Callable, Mapping
from uuid import UUID

import httpx
import jwt


PROFILE_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
LEGACY_REQUIRED_CLAIMS = (
    "iss",
    "aud",
    "sub",
    "tenant_id",
    "site_id",
    "capture_session_id",
    "processing_job_id",
    "camera_ids",
    "profile_digest",
    "mode",
    "iat",
    "nbf",
    "exp",
    "jti",
)

CAMERA_INGEST_REQUIRED_CLAIMS = (
    "iss",
    "aud",
    "sub",
    "credential_kind",
    "tenant_id",
    "site_id",
    "capture_session_id",
    "processing_job_id",
    "profile_digest",
    "camera_claim_id",
    "camera_id",
    "device_id",
    "iat",
    "nbf",
    "exp",
    "jti",
)


class SessionTokenError(ValueError):
    pass


@dataclass(frozen=True)
class AuthorizedSessionScope:
    tenant_id: str
    site_id: str
    capture_session_id: str
    processing_job_id: str
    camera_ids: frozenset[str]
    profile_digest: str
    authorized_subject: str
    token_jti: str
    expires_at: int

    def matches_declared_scope(
        self,
        declared,
        *,
        camera_id: str,
        device_id: str | None = None,
    ) -> bool:
        return (
            declared.tenant_id == self.tenant_id
            and declared.site_id == self.site_id
            and declared.capture_session_id == self.capture_session_id
            and declared.processing_job_id == self.processing_job_id
            and declared.profile_digest == self.profile_digest
            and camera_id in self.camera_ids
        )


@dataclass(frozen=True)
class AuthorizedCameraIngestScope:
    tenant_id: str
    site_id: str
    capture_session_id: str
    processing_job_id: str
    profile_digest: str
    camera_claim_id: str
    camera_id: str
    device_id: str
    authorized_subject: str
    token_jti: str
    issued_at: int
    expires_at: int

    @property
    def camera_ids(self) -> frozenset[str]:
        return frozenset({self.camera_id})

    def matches_declared_scope(
        self,
        declared,
        *,
        camera_id: str,
        device_id: str | None = None,
    ) -> bool:
        return (
            declared.tenant_id == self.tenant_id
            and declared.site_id == self.site_id
            and declared.capture_session_id == self.capture_session_id
            and declared.processing_job_id == self.processing_job_id
            and declared.profile_digest == self.profile_digest
            and camera_id == self.camera_id
            and device_id == self.device_id
        )


@dataclass(frozen=True)
class ActiveSessionCredential:
    token: str
    scope: AuthorizedSessionScope


class ActiveSessionCredentialStore:
    def __init__(self, *, now: Callable[[], float] = time.time):
        self.now = now
        self._credentials: dict[str, ActiveSessionCredential] = {}
        self._lock = RLock()

    def register(
        self,
        token: str,
        scope: AuthorizedSessionScope,
    ) -> ActiveSessionCredential:
        if not token:
            raise SessionTokenError("session token is required")
        if scope.expires_at <= int(self.now()):
            raise SessionTokenError("session token is expired")
        credential = ActiveSessionCredential(token=token, scope=scope)
        with self._lock:
            existing = self._credentials.get(scope.processing_job_id)
            if existing is not None and existing.scope != scope:
                raise SessionTokenError(
                    "processing job credential scope cannot be replaced"
                )
            self._credentials[scope.processing_job_id] = credential
        return credential

    def resolve_for_claim(self, claim) -> ActiveSessionCredential:
        processing_job_id = claim.processing_job_id
        if not processing_job_id:
            raise SessionTokenError("authorized manifest is missing processing_job_id")
        with self._lock:
            credential = self._credentials.get(processing_job_id)
            if credential is None:
                raise SessionTokenError("active session credential is unavailable")
            if credential.scope.expires_at <= int(self.now()):
                self._credentials.pop(processing_job_id, None)
                raise SessionTokenError("active session credential is expired")
            expected = (
                claim.tenant_id,
                claim.site_id,
                claim.capture_session_id,
                claim.processing_job_id,
                claim.profile_digest,
                claim.authorized_subject,
                claim.session_token_jti,
                frozenset(claim.authorized_camera_ids),
            )
            actual = (
                credential.scope.tenant_id,
                credential.scope.site_id,
                credential.scope.capture_session_id,
                credential.scope.processing_job_id,
                credential.scope.profile_digest,
                credential.scope.authorized_subject,
                credential.scope.token_jti,
                credential.scope.camera_ids,
            )
            if actual != expected:
                raise SessionTokenError(
                    "active credential does not match authorized manifest"
                )
            return credential

    def revoke(self, processing_job_id: str) -> bool:
        with self._lock:
            return self._credentials.pop(processing_job_id, None) is not None

    def clear(self) -> None:
        with self._lock:
            self._credentials.clear()


class ActiveCameraIngestCredentialStore:
    def __init__(self, *, now: Callable[[], float] = time.time):
        self.now = now
        self._credentials: dict[
            tuple[str, str], ActiveSessionCredential
        ] = {}
        self._lock = RLock()

    def register(
        self,
        token: str,
        scope: AuthorizedCameraIngestScope,
    ) -> ActiveSessionCredential:
        if not token:
            raise SessionTokenError("camera ingest credential is required")
        if scope.expires_at <= int(self.now()):
            raise SessionTokenError("camera ingest credential is expired")
        key = (scope.processing_job_id, scope.camera_id)
        credential = ActiveSessionCredential(token=token, scope=scope)
        with self._lock:
            existing = self._credentials.get(key)
            if existing is not None:
                existing_scope = existing.scope
                if existing_scope.camera_claim_id != scope.camera_claim_id:
                    raise SessionTokenError(
                        "active camera claim cannot be replaced"
                    )
                if existing_scope.issued_at > scope.issued_at:
                    raise SessionTokenError(
                        "older camera ingest credential cannot replace a newer one"
                    )
            self._credentials[key] = credential
        return credential

    def resolve(
        self,
        processing_job_id: str,
        camera_id: str,
    ) -> ActiveSessionCredential:
        key = (processing_job_id, camera_id)
        with self._lock:
            credential = self._credentials.get(key)
            if credential is None:
                raise SessionTokenError("active camera ingest credential is unavailable")
            if credential.scope.expires_at <= int(self.now()):
                self._credentials.pop(key, None)
                raise SessionTokenError("active camera ingest credential is expired")
            return credential

    def assert_current(self, scope: AuthorizedCameraIngestScope) -> None:
        credential = self.resolve(scope.processing_job_id, scope.camera_id)
        if credential.scope != scope:
            raise SessionTokenError(
                "camera ingest credential has been replaced"
            )

    def revoke(self, processing_job_id: str, camera_id: str) -> bool:
        with self._lock:
            return self._credentials.pop(
                (processing_job_id, camera_id), None
            ) is not None

    def clear(self) -> None:
        with self._lock:
            self._credentials.clear()


class SessionStatusCache:
    def __init__(
        self,
        url_template: str,
        *,
        cache_ttl_sec: float = 1.0,
        timeout_sec: float = 1.0,
        fetcher: Callable[[str], Mapping] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        if "{processing_job_id}" not in url_template:
            raise ValueError("session status URL must contain {processing_job_id}")
        if cache_ttl_sec <= 0 or timeout_sec <= 0:
            raise ValueError("session status cache and timeout must be positive")
        self.url_template = url_template
        self.cache_ttl_sec = cache_ttl_sec
        self.timeout_sec = timeout_sec
        self.fetcher = fetcher or self._fetch
        self.monotonic = monotonic
        self._active_until: dict[str, float] = {}
        self._lock = RLock()

    def assert_active(self, processing_job_id: str) -> None:
        with self._lock:
            now = self.monotonic()
            if self._active_until.get(processing_job_id, float("-inf")) > now:
                return
            try:
                payload = self.fetcher(processing_job_id)
            except Exception as exc:
                raise SessionTokenError("could not verify Main session status") from exc
            if (
                not isinstance(payload, Mapping)
                or payload.get("processingJobId") != processing_job_id
                or payload.get("known") is not True
                or payload.get("active") is not True
            ):
                self._active_until.pop(processing_job_id, None)
                raise SessionTokenError("Main session is inactive or unknown")
            self._active_until[processing_job_id] = now + self.cache_ttl_sec

    def _fetch(self, processing_job_id: str) -> Mapping:
        response = httpx.get(
            self.url_template.format(processing_job_id=processing_job_id),
            timeout=self.timeout_sec,
        )
        response.raise_for_status()
        return response.json()


class JwksKeyCache:
    def __init__(
        self,
        jwks_url: str,
        *,
        cache_ttl_sec: float = 300.0,
        timeout_sec: float = 2.0,
        fetcher: Callable[[], Mapping] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        if not jwks_url:
            raise ValueError("jwks_url is required")
        if cache_ttl_sec <= 0:
            raise ValueError("cache_ttl_sec must be positive")
        if timeout_sec <= 0:
            raise ValueError("timeout_sec must be positive")
        self.jwks_url = jwks_url
        self.cache_ttl_sec = cache_ttl_sec
        self.timeout_sec = timeout_sec
        self.fetcher = fetcher or self._fetch
        self.monotonic = monotonic
        self._keys: dict[str, jwt.PyJWK] = {}
        self._loaded_at = float("-inf")
        self._lock = RLock()

    def resolve(self, kid: str) -> jwt.PyJWK:
        if not kid:
            raise SessionTokenError("token header is missing kid")
        with self._lock:
            now = self.monotonic()
            if not self._keys or now - self._loaded_at >= self.cache_ttl_sec:
                self._refresh(now)
            key = self._keys.get(kid)
            if key is None:
                self._refresh(self.monotonic())
                key = self._keys.get(kid)
            if key is None:
                raise SessionTokenError("token kid is not present in Main JWKS")
            return key

    def _refresh(self, loaded_at: float) -> None:
        try:
            payload = self.fetcher()
            raw_keys = payload.get("keys") if isinstance(payload, Mapping) else None
            if not isinstance(raw_keys, list) or not raw_keys:
                raise ValueError("JWKS keys must be a non-empty list")
            parsed: dict[str, jwt.PyJWK] = {}
            for raw_key in raw_keys:
                if not isinstance(raw_key, Mapping):
                    raise ValueError("JWKS key must be an object")
                kid = raw_key.get("kid")
                if not isinstance(kid, str) or not kid or kid in parsed:
                    raise ValueError("JWKS kid must be non-empty and unique")
                if raw_key.get("kty") != "RSA" or raw_key.get("alg") != "RS256":
                    continue
                parsed[kid] = jwt.PyJWK.from_dict(dict(raw_key), algorithm="RS256")
            if not parsed:
                raise ValueError("JWKS contains no RS256 signing keys")
        except SessionTokenError:
            raise
        except Exception as exc:
            raise SessionTokenError("could not refresh Main JWKS") from exc
        self._keys = parsed
        self._loaded_at = loaded_at

    def _fetch(self) -> Mapping:
        response = httpx.get(self.jwks_url, timeout=self.timeout_sec)
        response.raise_for_status()
        return response.json()


class SessionTokenVerifier:
    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        key_cache: JwksKeyCache,
        leeway_sec: int = 5,
        status_cache: SessionStatusCache | None = None,
        now: Callable[[], float] = time.time,
    ):
        if not issuer or not audience:
            raise ValueError("issuer and audience are required")
        if leeway_sec < 0:
            raise ValueError("leeway_sec must be nonnegative")
        self.issuer = issuer
        self.audience = audience
        self.key_cache = key_cache
        self.leeway_sec = leeway_sec
        self.status_cache = status_cache
        self.now = now

    def verify(self, token: str) -> AuthorizedSessionScope:
        if not token:
            raise SessionTokenError("session token is required")
        try:
            header = jwt.get_unverified_header(token)
            if header.get("alg") != "RS256":
                raise SessionTokenError("session token algorithm must be RS256")
            key = self.key_cache.resolve(str(header.get("kid") or ""))
            claims = jwt.decode(
                token,
                key=key.key,
                algorithms=["RS256"],
                issuer=self.issuer,
                audience=self.audience,
                leeway=self.leeway_sec,
                options={"require": list(LEGACY_REQUIRED_CLAIMS)},
            )
            scope = _scope_from_claims(claims)
            self.assert_active(scope)
            return scope
        except SessionTokenError:
            raise
        except jwt.PyJWTError as exc:
            raise SessionTokenError("session token validation failed") from exc

    def assert_active(self, scope: AuthorizedSessionScope) -> None:
        if scope.expires_at <= int(self.now()):
            raise SessionTokenError("session token is expired")
        if self.status_cache is not None:
            self.status_cache.assert_active(scope.processing_job_id)


class CameraIngestCredentialVerifier:
    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        key_cache: JwksKeyCache,
        leeway_sec: int = 5,
        status_cache: SessionStatusCache | None = None,
        now: Callable[[], float] = time.time,
    ):
        if not issuer or not audience:
            raise ValueError("issuer and audience are required")
        if leeway_sec < 0:
            raise ValueError("leeway_sec must be nonnegative")
        self.issuer = issuer
        self.audience = audience
        self.key_cache = key_cache
        self.leeway_sec = leeway_sec
        self.status_cache = status_cache
        self.now = now

    def verify(self, token: str) -> AuthorizedCameraIngestScope:
        if not token:
            raise SessionTokenError("camera ingest credential is required")
        try:
            header = jwt.get_unverified_header(token)
            if header.get("alg") != "RS256":
                raise SessionTokenError(
                    "camera ingest credential algorithm must be RS256"
                )
            key = self.key_cache.resolve(str(header.get("kid") or ""))
            claims = jwt.decode(
                token,
                key=key.key,
                algorithms=["RS256"],
                issuer=self.issuer,
                audience=self.audience,
                leeway=self.leeway_sec,
                options={"require": list(CAMERA_INGEST_REQUIRED_CLAIMS)},
            )
            scope = _camera_ingest_scope_from_claims(claims)
            self.assert_active(scope)
            return scope
        except SessionTokenError:
            raise
        except jwt.PyJWTError as exc:
            raise SessionTokenError(
                "camera ingest credential validation failed"
            ) from exc

    def assert_active(self, scope: AuthorizedCameraIngestScope) -> None:
        if scope.expires_at <= int(self.now()):
            raise SessionTokenError("camera ingest credential is expired")
        if self.status_cache is not None:
            self.status_cache.assert_active(scope.processing_job_id)


class VersionedIngestCredentialVerifier:
    def __init__(
        self,
        *,
        legacy: SessionTokenVerifier | None = None,
        camera_ingest: CameraIngestCredentialVerifier | None = None,
    ):
        if legacy is None and camera_ingest is None:
            raise ValueError("at least one ingest credential verifier is required")
        self.legacy = legacy
        self.camera_ingest = camera_ingest

    def verify(self, token: str):
        try:
            unverified = jwt.decode(
                token,
                options={
                    "verify_signature": False,
                    "verify_aud": False,
                    "verify_exp": False,
                    "verify_iat": False,
                    "verify_nbf": False,
                },
            )
        except jwt.PyJWTError as exc:
            raise SessionTokenError("ingest credential is malformed") from exc
        credential_kind = unverified.get("credential_kind")
        if credential_kind is not None:
            if credential_kind != "CAMERA_INGEST" or self.camera_ingest is None:
                raise SessionTokenError("unsupported ingest credential kind")
            return self.camera_ingest.verify(token)
        if self.legacy is None:
            raise SessionTokenError("legacy session credential is disabled")
        return self.legacy.verify(token)

    def assert_active(self, scope) -> None:
        if isinstance(scope, AuthorizedCameraIngestScope):
            if self.camera_ingest is None:
                raise SessionTokenError("camera ingest credential is disabled")
            self.camera_ingest.assert_active(scope)
            return
        if self.legacy is None:
            raise SessionTokenError("legacy session credential is disabled")
        self.legacy.assert_active(scope)


def extract_bearer_token(context) -> str:
    if context is None or not hasattr(context, "invocation_metadata"):
        raise SessionTokenError("authorization metadata is required")
    values = [
        item.value
        for item in context.invocation_metadata()
        if item.key.lower() == "authorization"
    ]
    if len(values) != 1:
        raise SessionTokenError("exactly one authorization value is required")
    scheme, separator, token = values[0].partition(" ")
    if separator != " " or scheme.lower() != "bearer" or not token.strip():
        raise SessionTokenError("authorization must use Bearer credentials")
    return token.strip()


def _scope_from_claims(claims: Mapping) -> AuthorizedSessionScope:
    camera_ids = claims.get("camera_ids")
    if not isinstance(camera_ids, list) or not camera_ids:
        raise SessionTokenError("camera_ids must be a non-empty list")
    normalized_cameras = frozenset(_canonical_uuid(value, "camera_ids") for value in camera_ids)
    if len(normalized_cameras) != len(camera_ids):
        raise SessionTokenError("camera_ids must not contain duplicates")
    profile_digest = claims.get("profile_digest")
    if not isinstance(profile_digest, str) or not PROFILE_DIGEST_PATTERN.fullmatch(
        profile_digest
    ):
        raise SessionTokenError("profile_digest must be a lowercase SHA-256 digest")
    subject = claims.get("sub")
    if not isinstance(subject, str) or not subject.strip():
        raise SessionTokenError("sub must be non-empty")
    processing_job_id = _canonical_uuid(
        claims.get("processing_job_id"), "processing_job_id"
    )
    token_jti = _canonical_uuid(claims.get("jti"), "jti")
    if token_jti != processing_job_id:
        raise SessionTokenError("jti must equal processing_job_id")
    if claims.get("mode") != "LIVE":
        raise SessionTokenError("session token mode must be LIVE")
    expires_at = claims.get("exp")
    if not isinstance(expires_at, int):
        raise SessionTokenError("exp must be an integer timestamp")
    return AuthorizedSessionScope(
        tenant_id=_canonical_uuid(claims.get("tenant_id"), "tenant_id"),
        site_id=_canonical_uuid(claims.get("site_id"), "site_id"),
        capture_session_id=_canonical_uuid(
            claims.get("capture_session_id"), "capture_session_id"
        ),
        processing_job_id=processing_job_id,
        camera_ids=normalized_cameras,
        profile_digest=profile_digest,
        authorized_subject=subject.strip(),
        token_jti=token_jti,
        expires_at=expires_at,
    )


def _camera_ingest_scope_from_claims(
    claims: Mapping,
) -> AuthorizedCameraIngestScope:
    if claims.get("credential_kind") != "CAMERA_INGEST":
        raise SessionTokenError("credential_kind must be CAMERA_INGEST")
    profile_digest = claims.get("profile_digest")
    if not isinstance(profile_digest, str) or not PROFILE_DIGEST_PATTERN.fullmatch(
        profile_digest
    ):
        raise SessionTokenError("profile_digest must be a lowercase SHA-256 digest")
    subject = _required_text_claim(claims.get("sub"), "sub")
    device_id = _required_text_claim(claims.get("device_id"), "device_id")
    issued_at = claims.get("iat")
    expires_at = claims.get("exp")
    if not isinstance(issued_at, int):
        raise SessionTokenError("iat must be an integer timestamp")
    if not isinstance(expires_at, int):
        raise SessionTokenError("exp must be an integer timestamp")
    return AuthorizedCameraIngestScope(
        tenant_id=_canonical_uuid(claims.get("tenant_id"), "tenant_id"),
        site_id=_canonical_uuid(claims.get("site_id"), "site_id"),
        capture_session_id=_canonical_uuid(
            claims.get("capture_session_id"), "capture_session_id"
        ),
        processing_job_id=_canonical_uuid(
            claims.get("processing_job_id"), "processing_job_id"
        ),
        profile_digest=profile_digest,
        camera_claim_id=_canonical_uuid(
            claims.get("camera_claim_id"), "camera_claim_id"
        ),
        camera_id=_canonical_uuid(claims.get("camera_id"), "camera_id"),
        device_id=device_id,
        authorized_subject=subject,
        token_jti=_canonical_uuid(claims.get("jti"), "jti"),
        issued_at=issued_at,
        expires_at=expires_at,
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


def _required_text_claim(value, claim_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SessionTokenError(f"{claim_name} must be non-empty")
    if value != value.strip():
        raise SessionTokenError(f"{claim_name} must not contain outer whitespace")
    return value


active_session_credentials = ActiveSessionCredentialStore()
active_camera_ingest_credentials = ActiveCameraIngestCredentialStore()

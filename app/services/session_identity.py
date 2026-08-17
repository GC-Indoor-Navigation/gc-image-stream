import re
import time
from dataclasses import dataclass
from threading import RLock
from typing import Callable, Mapping
from uuid import UUID

import httpx
import jwt


PROFILE_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_CLAIMS = (
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

    def matches_declared_scope(self, declared, *, camera_id: str) -> bool:
        return (
            declared.tenant_id == self.tenant_id
            and declared.site_id == self.site_id
            and declared.capture_session_id == self.capture_session_id
            and declared.processing_job_id == self.processing_job_id
            and declared.profile_digest == self.profile_digest
            and camera_id in self.camera_ids
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
    ):
        if not issuer or not audience:
            raise ValueError("issuer and audience are required")
        if leeway_sec < 0:
            raise ValueError("leeway_sec must be nonnegative")
        self.issuer = issuer
        self.audience = audience
        self.key_cache = key_cache
        self.leeway_sec = leeway_sec

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
                options={"require": list(REQUIRED_CLAIMS)},
            )
            return _scope_from_claims(claims)
        except SessionTokenError:
            raise
        except jwt.PyJWTError as exc:
            raise SessionTokenError("session token validation failed") from exc


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


active_session_credentials = ActiveSessionCredentialStore()

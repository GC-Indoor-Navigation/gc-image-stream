import base64
import time
from uuid import uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from app.services.session_identity import (
    ActiveCameraIngestCredentialStore,
    CameraIngestCredentialVerifier,
    JwksKeyCache,
    SessionTokenError,
)


ISSUER = "https://main.example.test"
AUDIENCE = "gc-stream-ingest"
TENANT_ID = str(uuid4())
SITE_ID = str(uuid4())
CAPTURE_SESSION_ID = str(uuid4())
PROCESSING_JOB_ID = str(uuid4())
CAMERA_CLAIM_ID = str(uuid4())
CAMERA_ID = str(uuid4())
CREDENTIAL_ID = str(uuid4())
PROFILE_DIGEST = "a" * 64
DEVICE_ID = "android-installation-01"


@pytest.fixture(scope="module")
def signing_material():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    numbers = private_key.public_key().public_numbers()
    jwk = {
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": "main-key-1",
        "n": _b64uint(numbers.n),
        "e": _b64uint(numbers.e),
    }
    return private_key, jwk


def test_camera_ingest_verifier_accepts_exact_main_contract(signing_material):
    private_key, jwk = signing_material
    verifier = _verifier(jwk)

    scope = verifier.verify(_token(private_key))

    assert scope.tenant_id == TENANT_ID
    assert scope.capture_session_id == CAPTURE_SESSION_ID
    assert scope.processing_job_id == PROCESSING_JOB_ID
    assert scope.camera_claim_id == CAMERA_CLAIM_ID
    assert scope.camera_id == CAMERA_ID
    assert scope.camera_ids == frozenset({CAMERA_ID})
    assert scope.device_id == DEVICE_ID
    assert scope.token_jti == CREDENTIAL_ID


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"aud": ["gc-processing-relay"]}, "validation failed"),
        ({"credential_kind": "PROCESSING_RELAY"}, "credential_kind"),
        ({"camera_id": "not-a-uuid"}, "camera_id"),
        ({"camera_claim_id": "not-a-uuid"}, "camera_claim_id"),
        ({"device_id": "  android-01"}, "outer whitespace"),
        ({"profile_digest": "ABC"}, "profile_digest"),
    ],
)
def test_camera_ingest_verifier_rejects_wrong_boundary_claims(
    signing_material,
    override,
    expected,
):
    private_key, jwk = signing_material

    with pytest.raises(SessionTokenError, match=expected):
        _verifier(jwk).verify(_token(private_key, **override))


def test_camera_scope_binds_declared_scope_camera_and_device(signing_material):
    private_key, jwk = signing_material
    scope = _verifier(jwk).verify(_token(private_key))
    declared = type(
        "DeclaredScope",
        (),
        {
            "tenant_id": TENANT_ID,
            "site_id": SITE_ID,
            "capture_session_id": CAPTURE_SESSION_ID,
            "processing_job_id": PROCESSING_JOB_ID,
            "profile_digest": PROFILE_DIGEST,
        },
    )()

    assert scope.matches_declared_scope(
        declared,
        camera_id=CAMERA_ID,
        device_id=DEVICE_ID,
    )
    assert not scope.matches_declared_scope(
        declared,
        camera_id=str(uuid4()),
        device_id=DEVICE_ID,
    )
    assert not scope.matches_declared_scope(
        declared,
        camera_id=CAMERA_ID,
        device_id="different-device",
    )


def test_camera_store_allows_refresh_but_rejects_claim_replacement(
    signing_material,
):
    private_key, jwk = signing_material
    verifier = _verifier(jwk)
    store = ActiveCameraIngestCredentialStore(now=lambda: int(time.time()))
    first = verifier.verify(_token(private_key))
    refreshed = verifier.verify(
        _token(
            private_key,
            jti=str(uuid4()),
            iat=first.issued_at + 1,
            nbf=first.issued_at,
        )
    )

    store.register("first", first)
    store.register("refreshed", refreshed)
    assert store.resolve(PROCESSING_JOB_ID, CAMERA_ID).token == "refreshed"

    replacement = verifier.verify(
        _token(
            private_key,
            camera_claim_id=str(uuid4()),
            jti=str(uuid4()),
            iat=first.issued_at + 2,
            nbf=first.issued_at,
        )
    )
    with pytest.raises(SessionTokenError, match="claim cannot be replaced"):
        store.register("replacement", replacement)


def _verifier(jwk):
    return CameraIngestCredentialVerifier(
        issuer=ISSUER,
        audience=AUDIENCE,
        key_cache=JwksKeyCache(
            "https://main/jwks",
            fetcher=lambda: {"keys": [jwk]},
        ),
    )


def _token(private_key, **override):
    now = int(time.time())
    claims = {
        "iss": ISSUER,
        "aud": [AUDIENCE],
        "sub": "participant-123",
        "credential_kind": "CAMERA_INGEST",
        "tenant_id": TENANT_ID,
        "site_id": SITE_ID,
        "capture_session_id": CAPTURE_SESSION_ID,
        "processing_job_id": PROCESSING_JOB_ID,
        "profile_digest": PROFILE_DIGEST,
        "camera_claim_id": CAMERA_CLAIM_ID,
        "camera_id": CAMERA_ID,
        "device_id": DEVICE_ID,
        "iat": now,
        "nbf": now,
        "exp": now + 300,
        "jti": CREDENTIAL_ID,
    }
    claims.update(override)
    return jwt.encode(
        claims,
        private_key,
        algorithm="RS256",
        headers={"kid": "main-key-1", "typ": "JWT"},
    )


def _b64uint(value: int) -> str:
    payload = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")

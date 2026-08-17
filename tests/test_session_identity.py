import base64
import time
from collections import namedtuple
from uuid import uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from app.services.session_identity import (
    JwksKeyCache,
    SessionTokenError,
    SessionTokenVerifier,
    extract_bearer_token,
)


PROFILE_DIGEST = "a" * 64
ISSUER = "https://main.example.test"
AUDIENCE = "gc-data-plane"
Metadata = namedtuple("Metadata", "key value")
TENANT_ID = str(uuid4())
SITE_ID = str(uuid4())
CAPTURE_SESSION_ID = str(uuid4())
PROCESSING_JOB_ID = str(uuid4())
CAMERA_ID = str(uuid4())


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


def test_verifier_accepts_main_scope_and_refreshes_unknown_kid(signing_material):
    private_key, jwk = signing_material
    fetch_count = 0

    def fetcher():
        nonlocal fetch_count
        fetch_count += 1
        return {"keys": [jwk]}

    verifier = _verifier(fetcher)
    scope = verifier.verify(_token(private_key))

    assert scope.tenant_id == TENANT_ID
    assert scope.site_id == SITE_ID
    assert scope.capture_session_id == CAPTURE_SESSION_ID
    assert scope.processing_job_id == PROCESSING_JOB_ID
    assert scope.camera_ids == frozenset({CAMERA_ID})
    assert scope.profile_digest == PROFILE_DIGEST
    assert scope.authorized_subject == "user-123"
    assert scope.token_jti == PROCESSING_JOB_ID
    assert fetch_count == 1


def test_unknown_kid_forces_one_refresh_then_fails(signing_material):
    private_key, jwk = signing_material
    fetch_count = 0

    def fetcher():
        nonlocal fetch_count
        fetch_count += 1
        return {"keys": [jwk]}

    verifier = _verifier(fetcher)

    with pytest.raises(SessionTokenError, match="kid"):
        verifier.verify(_token(private_key, kid="rotated-key"))

    assert fetch_count == 2


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"aud": ["wrong-audience"]}, "validation failed"),
        ({"camera_ids": [CAMERA_ID, CAMERA_ID]}, "duplicates"),
        ({"profile_digest": "not-a-digest"}, "profile_digest"),
        ({"mode": "REPLAY"}, "mode"),
        ({"jti": str(uuid4())}, "jti"),
    ],
)
def test_verifier_rejects_invalid_or_unbound_claims(
    signing_material,
    override,
    expected,
):
    private_key, jwk = signing_material
    verifier = _verifier(lambda: {"keys": [jwk]})

    with pytest.raises(SessionTokenError, match=expected):
        verifier.verify(_token(private_key, **override))


def test_verifier_rejects_non_rs256_algorithm(signing_material):
    _, jwk = signing_material
    verifier = _verifier(lambda: {"keys": [jwk]})
    token = jwt.encode(
        _claims(),
        "secret-that-is-long-enough-for-test-only",
        algorithm="HS256",
        headers={"kid": "main-key-1"},
    )

    with pytest.raises(SessionTokenError, match="RS256"):
        verifier.verify(token)


def test_extract_bearer_token_requires_exactly_one_bearer_value():
    class Context:
        def __init__(self, values):
            self.values = values

        def invocation_metadata(self):
            return self.values

    assert extract_bearer_token(
        Context([Metadata("authorization", "Bearer signed-token")])
    ) == "signed-token"

    with pytest.raises(SessionTokenError, match="exactly one"):
        extract_bearer_token(Context([]))
    with pytest.raises(SessionTokenError, match="Bearer"):
        extract_bearer_token(Context([Metadata("authorization", "Basic value")]))


def _verifier(fetcher):
    return SessionTokenVerifier(
        issuer=ISSUER,
        audience=AUDIENCE,
        key_cache=JwksKeyCache("https://main/jwks", fetcher=fetcher),
    )


def _token(private_key, kid="main-key-1", **override):
    claims = _claims()
    claims.update(override)
    return jwt.encode(
        claims,
        private_key,
        algorithm="RS256",
        headers={"kid": kid, "typ": "JWT"},
    )


def _claims():
    now = int(time.time())
    return {
        "iss": ISSUER,
        "aud": [AUDIENCE],
        "sub": "user-123",
        "tenant_id": TENANT_ID,
        "site_id": SITE_ID,
        "capture_session_id": CAPTURE_SESSION_ID,
        "processing_job_id": PROCESSING_JOB_ID,
        "camera_ids": [CAMERA_ID],
        "profile_digest": PROFILE_DIGEST,
        "mode": "LIVE",
        "iat": now,
        "nbf": now,
        "exp": now + 300,
        "jti": PROCESSING_JOB_ID,
    }


def _b64uint(value: int) -> str:
    payload = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")

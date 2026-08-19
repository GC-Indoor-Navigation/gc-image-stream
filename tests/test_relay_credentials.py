import base64
import time
from types import SimpleNamespace
from uuid import uuid4

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from app.services.relay_credentials import (
    ClientCredentialsTokenProvider,
    MainProcessingRelayCredentialProvider,
    ProcessingRelayCredentialVerifier,
)
from app.services.session_identity import JwksKeyCache, SessionTokenError


ISSUER = "https://main.example.test"
AUDIENCE = "gc-processing-relay"
TENANT_ID = str(uuid4())
SITE_ID = str(uuid4())
CAPTURE_SESSION_ID = str(uuid4())
PROCESSING_JOB_ID = str(uuid4())
CREDENTIAL_ID = str(uuid4())
CAMERA_IDS = (str(uuid4()), str(uuid4()))
PROFILE_DIGEST = "a" * 64


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


def test_processing_relay_verifier_accepts_complete_workload_scope(
    signing_material,
):
    private_key, jwk = signing_material

    scope = _verifier(jwk).verify(_relay_token(private_key))

    assert scope.tenant_id == TENANT_ID
    assert scope.processing_job_id == PROCESSING_JOB_ID
    assert scope.camera_ids == frozenset(CAMERA_IDS)
    assert scope.coordinator_subject == "coordinator-123"
    assert scope.workload_subject == "gc-image-stream:service-account"
    assert scope.token_jti == CREDENTIAL_ID


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"aud": ["gc-stream-ingest"]}, "validation failed"),
        ({"credential_kind": "CAMERA_INGEST"}, "credential_kind"),
        ({"camera_ids": [CAMERA_IDS[0], CAMERA_IDS[0]]}, "duplicates"),
        ({"coordinator_subject": ""}, "coordinator_subject"),
    ],
)
def test_processing_relay_verifier_rejects_wrong_contract(
    signing_material,
    override,
    expected,
):
    private_key, jwk = signing_material

    with pytest.raises(SessionTokenError, match=expected):
        _verifier(jwk).verify(_relay_token(private_key, **override))


def test_client_credentials_provider_caches_without_exposing_secret():
    now = [1_000.0]
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return _response(200, {"access_token": "workload-token", "expires_in": 60})

    provider = ClientCredentialsTokenProvider(
        token_url="https://identity/token",
        client_id="gc-image-stream",
        client_secret="private-secret",
        post=post,
        now=lambda: now[0],
    )

    assert provider.resolve() == "workload-token"
    assert provider.resolve() == "workload-token"
    assert len(calls) == 1
    assert calls[0][1]["data"] == {"grant_type": "client_credentials"}
    assert calls[0][1]["auth"] == ("gc-image-stream", "private-secret")

    now[0] = 1_050.0
    provider.resolve()
    assert len(calls) == 2


def test_main_provider_issues_once_and_binds_header_body_and_manifest(
    signing_material,
):
    private_key, jwk = signing_material
    token = _relay_token(private_key)
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return _response(
            201,
            {
                "credentialId": CREDENTIAL_ID,
                "captureSessionId": CAPTURE_SESSION_ID,
                "processingJobId": PROCESSING_JOB_ID,
                "credentialKind": "PROCESSING_RELAY",
                "cameraIds": list(CAMERA_IDS),
                "expiresAt": "2099-01-01T00:00:00Z",
            },
            headers={"X-GC-Processing-Relay-Token": token},
        )

    provider = MainProcessingRelayCredentialProvider(
        url_template=(
            "https://main/internal/v1/tenants/{tenant_id}/processing-jobs/"
            "{processing_job_id}/relay-credentials"
        ),
        access_tokens=_StaticAccessTokens(),
        verifier=_verifier(jwk),
        post=post,
    )

    first = provider.resolve_for_claim(_claim())
    second = provider.resolve_for_claim(_claim())

    assert first is second
    assert first.token == token
    assert len(calls) == 1
    assert calls[0][0].endswith(
        f"/tenants/{TENANT_ID}/processing-jobs/{PROCESSING_JOB_ID}/relay-credentials"
    )
    assert calls[0][1]["headers"]["Authorization"] == "Bearer workload-access"
    assert calls[0][1]["headers"]["Idempotency-Key"]


def test_main_provider_fails_closed_when_one_time_header_is_missing(
    signing_material,
):
    _, jwk = signing_material
    provider = MainProcessingRelayCredentialProvider(
        url_template=(
            "https://main/internal/v1/tenants/{tenant_id}/processing-jobs/"
            "{processing_job_id}/relay-credentials"
        ),
        access_tokens=_StaticAccessTokens(),
        verifier=_verifier(jwk),
        post=lambda *args, **kwargs: _response(201, {}),
    )

    with pytest.raises(SessionTokenError, match="one-time token"):
        provider.resolve_for_claim(_claim())


def _claim():
    return SimpleNamespace(
        tenant_id=TENANT_ID,
        site_id=SITE_ID,
        capture_session_id=CAPTURE_SESSION_ID,
        processing_job_id=PROCESSING_JOB_ID,
        profile_digest=PROFILE_DIGEST,
        authorized_camera_ids=CAMERA_IDS,
    )


def _verifier(jwk):
    return ProcessingRelayCredentialVerifier(
        issuer=ISSUER,
        audience=AUDIENCE,
        key_cache=JwksKeyCache(
            "https://main/jwks",
            fetcher=lambda: {"keys": [jwk]},
        ),
    )


def _relay_token(private_key, **override):
    now = int(time.time())
    claims = {
        "iss": ISSUER,
        "aud": [AUDIENCE],
        "sub": "gc-image-stream:service-account",
        "credential_kind": "PROCESSING_RELAY",
        "tenant_id": TENANT_ID,
        "site_id": SITE_ID,
        "capture_session_id": CAPTURE_SESSION_ID,
        "processing_job_id": PROCESSING_JOB_ID,
        "profile_digest": PROFILE_DIGEST,
        "coordinator_subject": "coordinator-123",
        "camera_ids": list(CAMERA_IDS),
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


def _response(status_code, payload, headers=None):
    return httpx.Response(
        status_code,
        json=payload,
        headers=headers,
        request=httpx.Request("POST", "https://test/request"),
    )


def _b64uint(value: int) -> str:
    payload = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


class _StaticAccessTokens:
    def resolve(self):
        return "workload-access"

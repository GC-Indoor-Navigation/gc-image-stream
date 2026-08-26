import runpy
import time
from dataclasses import replace

import pytest

from app.services.session_identity import (
    AuthorizedCameraIngestScope,
    CameraCredentialStatusCache,
    CameraIngestCredentialVerifier,
    SessionTokenError,
)


def payload(credential_id="credential-1", job_id="job-1", **overrides):
    return {
        "credentialId": credential_id,
        "processingJobId": job_id,
        "credentialKind": "CAMERA_INGEST",
        "known": True,
        "active": True,
        **overrides,
    }


def cache(fetcher, **kwargs):
    return CameraCredentialStatusCache(
        "https://main.test/credential-status/{credential_id}", fetcher=fetcher, **kwargs
    )


def test_revocation_reaches_existing_stream_without_replacement_connection():
    now = [10.0]
    active = [True]
    calls = []

    def fetcher(credential_id):
        calls.append(credential_id)
        return payload(credential_id, active=active[0])

    authority = cache(fetcher, monotonic=lambda: now[0])
    scope = AuthorizedCameraIngestScope(
        tenant_id="tenant", site_id="site", capture_session_id="session",
        processing_job_id="job-1", profile_digest="a" * 64,
        camera_claim_id="claim", camera_id="camera", device_id="device",
        authorized_subject="participant", token_jti="credential-1",
        issued_at=1_000, expires_at=int(time.time()) + 300,
    )
    verifier = CameraIngestCredentialVerifier(
        issuer="main", audience="gc-stream-ingest", key_cache=None,
        credential_status_cache=authority,
    )
    verifier.assert_active(scope)
    active[0] = False
    now[0] = 10.99
    verifier.assert_active(scope)
    assert len(calls) == 1
    now[0] = 11.0
    with pytest.raises(SessionTokenError, match="inactive"):
        verifier.assert_active(scope)
    assert len(calls) == 2

    # New token for the same still-active job is independent of the old cached result.
    active[0] = True
    verifier.assert_active(replace(scope, token_jti="credential-2"))
    assert calls[-1] == "credential-2"


@pytest.mark.parametrize("override", [
    {"credentialId": "wrong"}, {"processingJobId": "wrong"},
    {"credentialKind": "PROCESSING_RELAY"}, {"known": False}, {"active": False},
    {"known": 1}, {"active": "true"},
])
def test_exact_credential_job_kind_and_boolean_binding(override):
    authority = cache(lambda _: payload(**override))
    with pytest.raises(SessionTokenError, match="inactive"):
        authority.assert_active("credential-1", "job-1")


@pytest.mark.parametrize("response", [None, [], "invalid", {}])
def test_malformed_authority_response_fails_closed(response):
    with pytest.raises(SessionTokenError, match="inactive"):
        cache(lambda _: response).assert_active("credential-1", "job-1")


def test_warm_cache_cannot_bypass_job_binding():
    authority = cache(lambda _: payload())
    authority.assert_active("credential-1", "job-1")
    with pytest.raises(SessionTokenError, match="inactive"):
        authority.assert_active("credential-1", "another-job")


def test_unavailable_authority_does_not_extend_cached_authorization():
    now = [10.0]
    offline = [False]

    def fetcher(_):
        if offline[0]:
            raise TimeoutError("authority unavailable")
        return payload()

    authority = cache(fetcher, monotonic=lambda: now[0])
    authority.assert_active("credential-1", "job-1")
    offline[0] = True
    now[0] = 11.0
    for _ in range(2):
        with pytest.raises(SessionTokenError, match="could not verify"):
            authority.assert_active("credential-1", "job-1")


def test_positive_cache_has_bounded_retention():
    authority = cache(lambda credential_id: payload(credential_id))
    for index in range(1030):
        authority.assert_active(f"credential-{index}", "job-1")
    assert len(authority._active_until) == 1024


@pytest.mark.parametrize("kwargs", [{"cache_ttl_sec": 0}, {"timeout_sec": -1}])
def test_cache_rejects_invalid_time_bounds(kwargs):
    with pytest.raises(ValueError, match="positive"):
        cache(lambda _: payload(), **kwargs)


@pytest.mark.parametrize("template", ["", "https://main.test/credential-status"])
def test_camera_auth_requires_explicit_credential_status_template(monkeypatch, template):
    monkeypatch.setenv("STREAM_CAMERA_INGEST_AUTH_ENABLED", "true")
    monkeypatch.setenv("STREAM_CAMERA_CREDENTIAL_STATUS_URL_TEMPLATE", template)
    with pytest.raises(RuntimeError, match="must contain"):
        runpy.run_path("app/core/server.py")

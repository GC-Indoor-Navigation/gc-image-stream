import pytest

from app.core.relay import resolve_stream_relay_mode


def test_resolve_stream_relay_mode_prefers_explicit_mode():
    assert (
        resolve_stream_relay_mode(
            configured_mode="raw",
            raw_relay_enabled=False,
            frame_set_relay_enabled=True,
        )
        == "raw"
    )


def test_resolve_stream_relay_mode_uses_frame_set_legacy_flag_first():
    assert (
        resolve_stream_relay_mode(
            configured_mode="",
            raw_relay_enabled=True,
            frame_set_relay_enabled=True,
        )
        == "frame_set"
    )


def test_resolve_stream_relay_mode_uses_raw_legacy_flag():
    assert (
        resolve_stream_relay_mode(
            configured_mode="",
            raw_relay_enabled=True,
            frame_set_relay_enabled=False,
        )
        == "raw"
    )


def test_resolve_stream_relay_mode_defaults_to_off():
    assert (
        resolve_stream_relay_mode(
            configured_mode="",
            raw_relay_enabled=False,
            frame_set_relay_enabled=False,
        )
        == "off"
    )


def test_resolve_stream_relay_mode_rejects_invalid_mode():
    with pytest.raises(RuntimeError, match="STREAM_RELAY_MODE"):
        resolve_stream_relay_mode(
            configured_mode="both",
            raw_relay_enabled=False,
            frame_set_relay_enabled=False,
        )


def test_server_config_uses_common_relay_target_for_frame_set_mode(monkeypatch):
    from importlib import reload

    import app.core.server as server

    monkeypatch.setenv("DATABASE_URL", "sqlite:///./frames.db")
    monkeypatch.setenv("STORAGE_DIR", "storage")
    monkeypatch.setenv("STREAM_RELAY_MODE", "frame_set")
    monkeypatch.setenv("STREAM_RELAY_TARGET", "127.0.0.1:50051")
    monkeypatch.delenv("STREAM_FRAME_SET_RELAY_TARGET", raising=False)

    reloaded = reload(server)

    assert reloaded.STREAM_RELAY_MODE == "frame_set"
    assert reloaded.STREAM_RELAY_ENABLED is False
    assert reloaded.STREAM_FRAME_SET_RELAY_ENABLED is True
    assert reloaded.STREAM_RELAY_TARGET == "127.0.0.1:50051"


def test_server_config_falls_back_to_legacy_frame_set_target(monkeypatch):
    from importlib import reload

    import app.core.server as server

    monkeypatch.setenv("DATABASE_URL", "sqlite:///./frames.db")
    monkeypatch.setenv("STORAGE_DIR", "storage")
    monkeypatch.setenv("STREAM_RELAY_MODE", "frame_set")
    monkeypatch.delenv("STREAM_RELAY_TARGET", raising=False)
    monkeypatch.setenv("STREAM_FRAME_SET_RELAY_TARGET", "127.0.0.1:50052")

    reloaded = reload(server)

    assert reloaded.STREAM_RELAY_TARGET == "127.0.0.1:50052"


def test_v2_shadow_is_disabled_by_default(monkeypatch):
    from importlib import reload

    import app.core.server as server

    monkeypatch.setenv("DATABASE_URL", "sqlite:///./frames.db")
    monkeypatch.setenv("STORAGE_DIR", "storage")
    monkeypatch.setenv("STREAM_RELAY_MODE", "off")
    monkeypatch.delenv("STREAM_RELAY_V2_SHADOW_ENABLED", raising=False)

    reloaded = reload(server)

    assert reloaded.STREAM_RELAY_V2_SHADOW_ENABLED is False


def test_v2_shadow_requires_target_and_profile(monkeypatch):
    from importlib import reload

    import app.core.server as server

    monkeypatch.setenv("DATABASE_URL", "sqlite:///./frames.db")
    monkeypatch.setenv("STORAGE_DIR", "storage")
    monkeypatch.setenv("STREAM_RELAY_MODE", "off")
    monkeypatch.setenv("STREAM_RELAY_V2_SHADOW_ENABLED", "true")
    monkeypatch.delenv("STREAM_RELAY_V2_TARGET", raising=False)
    monkeypatch.delenv("STREAM_RELAY_TARGET", raising=False)
    monkeypatch.delenv(
        "STREAM_RELAY_V2_PROCESSING_PROFILE_DIGEST",
        raising=False,
    )

    with pytest.raises(RuntimeError, match="V2_TARGET"):
        reload(server)

    monkeypatch.setenv("STREAM_RELAY_V2_TARGET", "127.0.0.1:50051")
    with pytest.raises(RuntimeError, match="PROFILE_DIGEST"):
        reload(server)

    monkeypatch.setenv("STREAM_RELAY_V2_PROCESSING_PROFILE_DIGEST", "profile")
    reloaded = reload(server)
    assert reloaded.STREAM_RELAY_V2_SHADOW_ENABLED is True

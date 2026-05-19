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

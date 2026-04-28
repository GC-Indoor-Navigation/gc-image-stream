import pytest

from app.core.env import get_optional_bool_env


def test_get_optional_bool_env_uses_default_when_missing(monkeypatch):
    monkeypatch.delenv("FEATURE_ENABLED", raising=False)

    assert get_optional_bool_env("FEATURE_ENABLED", False) is False
    assert get_optional_bool_env("FEATURE_ENABLED", True) is True


@pytest.mark.parametrize("value", ["1", "true", "yes", "on", "TRUE"])
def test_get_optional_bool_env_accepts_true_values(monkeypatch, value):
    monkeypatch.setenv("FEATURE_ENABLED", value)

    assert get_optional_bool_env("FEATURE_ENABLED", False) is True


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "FALSE"])
def test_get_optional_bool_env_accepts_false_values(monkeypatch, value):
    monkeypatch.setenv("FEATURE_ENABLED", value)

    assert get_optional_bool_env("FEATURE_ENABLED", True) is False


def test_get_optional_bool_env_rejects_invalid_values(monkeypatch):
    monkeypatch.setenv("FEATURE_ENABLED", "maybe")

    with pytest.raises(RuntimeError, match="Invalid bool value"):
        get_optional_bool_env("FEATURE_ENABLED", False)

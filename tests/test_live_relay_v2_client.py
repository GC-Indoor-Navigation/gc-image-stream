import pytest

from app.infrastructure.grpc.live_relay_v2_client import (
    ProcessingLiveRelayV2Client,
)


def test_live_relay_v2_client_ships_registered_but_disabled():
    client = ProcessingLiveRelayV2Client()

    assert client.start() is None
    assert client.status() == {
        "contract_registered": True,
        "enabled": False,
        "running": False,
        "target": "",
        "last_error": None,
    }


def test_live_relay_v2_client_cannot_be_enabled_before_transport_phase():
    client = ProcessingLiveRelayV2Client()
    client.configure(target="127.0.0.1:50051", enabled=True)

    with pytest.raises(RuntimeError, match="Phase 4"):
        client.start()

    assert client.status()["running"] is False
    assert client.status()["last_error"] is not None

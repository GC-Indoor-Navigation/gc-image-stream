from app.infrastructure.grpc.generated import live_frame_relay_v2_pb2_grpc


class ProcessingLiveRelayV2Client:
    def __init__(self):
        self.enabled = False
        self.target = ""
        self.last_error: str | None = None

    def configure(self, *, target: str = "", enabled: bool = False) -> None:
        self.target = target
        self.enabled = enabled
        self.last_error = None

    def build_stub(self, channel):
        return live_frame_relay_v2_pb2_grpc.LiveFrameRelayServiceStub(
            channel
        ).Relay

    def start(self):
        if not self.enabled:
            return None
        self.last_error = "relay v2 transport is not enabled before Phase 4"
        raise RuntimeError(self.last_error)

    def stop(self) -> None:
        return None

    def status(self) -> dict:
        return {
            "contract_registered": True,
            "enabled": self.enabled,
            "running": False,
            "target": self.target,
            "last_error": self.last_error,
        }


processing_live_relay_v2_client = ProcessingLiveRelayV2Client()

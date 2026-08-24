from app.infrastructure.grpc.grpc_ingest_server import grpc_ingest_service


def test_readiness_reports_database_up_and_disabled_grpc_as_ready(client):
    grpc_ingest_service.configure(bind="", enabled=False)

    response = client.get("/health/readiness")

    assert response.status_code == 200
    assert response.json() == {
        "status": "UP",
        "checks": {
            "database": "UP",
            "grpcIngest": "DISABLED",
        },
    }


def test_readiness_fails_until_configured_grpc_server_is_running(client):
    grpc_ingest_service.configure(bind="127.0.0.1:50052", enabled=True)
    try:
        response = client.get("/health/readiness")

        assert response.status_code == 503
        assert response.json()["status"] == "OUT_OF_SERVICE"
        assert response.json()["checks"]["grpcIngest"] == "DOWN"
    finally:
        grpc_ingest_service.configure(bind="", enabled=False)

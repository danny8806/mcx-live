from fastapi import FastAPI
from fastapi.testclient import TestClient

from dashboard.api_contract import normalize_api_error_status, status_for_error


def test_api_error_status_classification_is_explicit():
    assert status_for_error("Position xyz not found") == 404
    assert status_for_error("instrument outside the exclusive strategy runtime") == 404
    assert status_for_error("Engine not initialized") == 503
    assert status_for_error("Broker audit store unavailable") == 503
    assert status_for_error("database read failed") == 500


def test_api_error_envelope_is_never_reported_as_http_200():
    app = FastAPI()
    app.middleware("http")(normalize_api_error_status)

    @app.get("/api/positions/{position_id}")
    async def missing_position(position_id: str):
        return {"error": f"Position {position_id} not found"}

    response = TestClient(app).get("/api/positions/absent")

    assert response.status_code == 404
    assert response.json() == {
        "error": "Position absent not found",
        "detail": "Position absent not found",
    }


def test_api_unavailable_envelope_returns_503_and_success_payload_stays_200():
    app = FastAPI()
    app.middleware("http")(normalize_api_error_status)

    @app.get("/api/unavailable")
    async def unavailable():
        return {"error": "Dhan funds endpoint unavailable", "cached": True}

    @app.get("/api/healthy")
    async def healthy():
        return {"error": None, "status": "ok"}

    client = TestClient(app)
    unavailable_response = client.get("/api/unavailable")
    healthy_response = client.get("/api/healthy")

    assert unavailable_response.status_code == 503
    assert unavailable_response.json()["detail"] == "Dhan funds endpoint unavailable"
    assert healthy_response.status_code == 200
    assert healthy_response.json() == {"error": None, "status": "ok"}


def test_mounted_live_detail_routes_do_not_mask_not_found_or_unavailable(
        monkeypatch):
    from analytics import routes as analytics_routes
    from dashboard.routes import orders, positions, reversals, strategies, trades
    from live.api import create_live_app

    for module, names in (
            (analytics_routes, ("_trade_ledger", "_event_store")),
            (orders, ("_engine",)),
            (positions, ("_engine",)),
            (reversals, ("_engine", "_persistence")),
            (strategies, ("_engine",)),
            (trades, ("_engine", "_persistence"))):
        for name in names:
            monkeypatch.setattr(module, name, None)

    client = TestClient(create_live_app())
    cases = {
        "/api/analytics/trades/absent": 503,
        "/api/orders/absent": 503,
        "/api/positions/absent": 503,
        "/api/reversals/absent": 503,
        "/api/strategies/absent": 404,
        "/api/trades/absent": 404,
    }
    for path, expected_status in cases.items():
        response = client.get(path)
        assert response.status_code == expected_status, (path, response.text)
        payload = response.json()
        assert payload.get("error")
        assert payload.get("detail") == payload["error"]

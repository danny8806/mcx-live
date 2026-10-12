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


def test_unknown_instrument_panels_return_not_found_instead_of_empty_success(
        monkeypatch):
    from types import SimpleNamespace

    from dashboard.routes import indicators, overview, pnl
    from live.api import create_live_app

    strategy = SimpleNamespace(instrument="GOLDM", strategy_type="supertrend")
    engine = SimpleNamespace(strategies={"gold_01": strategy})
    monkeypatch.setattr(indicators, "_engine", engine)
    monkeypatch.setattr(overview, "_engine", engine)
    monkeypatch.setattr(pnl, "_engine", engine)
    monkeypatch.setattr(pnl, "_persistence", None)

    client = TestClient(create_live_app())
    for path in (
            "/api/indicators/UNKNOWN",
            "/api/overview/UNKNOWN",
            "/api/pnl/UNKNOWN",
            "/api/pnl/UNKNOWN/strategy/no-such-strategy",
            "/api/equity-curve/UNKNOWN"):
        response = client.get(path)
        assert response.status_code == 404, (path, response.text)
        assert "active strategy runtime" in response.json()["detail"]


def test_missing_broker_evidence_ids_do_not_return_json_null(monkeypatch):
    from dashboard.routes import broker_evidence
    from live.api import create_live_app

    class MissingEventStore:
        def get(self, _event_id):
            return None

    monkeypatch.setattr(broker_evidence, "_audit_store", MissingEventStore)
    monkeypatch.setattr(broker_evidence, "_alert_ledger", MissingEventStore)
    client = TestClient(create_live_app())

    for path in ("/api/broker-events/missing", "/api/alert-ledger/missing"):
        response = client.get(path)
        assert response.status_code == 404, (path, response.text)
        assert response.json()["error"] == "event not found"


def test_environment_health_reports_actual_master_gate_state(monkeypatch):
    from types import SimpleNamespace

    from dashboard.routes import env_switch

    live = SimpleNamespace(
        name="live", mode="LIVE", is_live=True, gate_state="OFF",
        gate_enabled=False, broker=None,
        position_manager=SimpleNamespace(open_positions=[]),
    )
    engine = SimpleNamespace(
        _envs={"live": live}, config=SimpleNamespace(get=lambda *_a, **_k: True),
        _running=True,
    )
    monkeypatch.setattr(env_switch, "_engine", engine)

    health = env_switch._health_sync("live")

    assert health["gate_enabled"] is False
    assert health["master_gate"] is False


def test_mutating_route_readiness_and_unknown_target_contracts(monkeypatch):
    from dashboard.routes import settings, strategies
    from live.api import create_live_app

    monkeypatch.setattr(settings, "_engine", None)
    monkeypatch.setattr(settings, "_config_path", None)
    monkeypatch.setattr(settings, "_resolved_config_path", None)
    monkeypatch.setattr(strategies, "_engine", None)
    client = TestClient(create_live_app())
    cases = (
        ("post", "/api/strategies/missing/control", {"action": "resume"}, 404),
        ("post", "/api/live/strategies/missing/control", {"action": "resume"}, 404),
        ("put", "/api/settings/live-gate", {"gate": "OFF"}, 503),
        ("put", "/api/settings/strategies/missing", {"quantity": 1}, 503),
        ("post", "/api/settings/refresh", None, 503),
    )
    for method, path, body, expected_status in cases:
        response = getattr(client, method)(path, json=body) if body is not None else getattr(
            client, method)(path)
        assert response.status_code == expected_status, (path, response.text)
        assert response.json().get("detail")


def test_unknown_environment_pnl_reversals_and_trades_never_fall_back_or_leak(
        monkeypatch):
    from types import SimpleNamespace

    from dashboard.routes import pnl, reversals, trades
    from live.api import create_live_app

    live = SimpleNamespace(name="live", mode="LIVE")
    engine = SimpleNamespace(_envs={"live": live})
    for module in (pnl, reversals, trades):
        monkeypatch.setattr(module, "_engine", engine)
    monkeypatch.setattr(pnl, "_persistence", object())
    monkeypatch.setattr(reversals, "_persistence", object())
    monkeypatch.setattr(trades, "_persistence", object())

    client = TestClient(create_live_app())
    for path in ("/api/no-such-env/pnl",
                 "/api/no-such-env/reversals",
                 "/api/no-such-env/trades"):
        response = client.get(path)
        assert response.status_code == 404, (path, response.text)
        assert response.json()["detail"] == "environment 'no-such-env' not found"

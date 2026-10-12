from dashboard.routes import live_ops
from fastapi import FastAPI
from fastapi.testclient import TestClient


def test_live_ops_orders_expose_local_gate_block_without_fabricating_broker_attempt(
        monkeypatch):
    row = {
        "order_id": "LIVE-local-1",
        "trade_id": "trade-1",
        "pending_order_id": None,
        "signal_id": "signal-1",
        "broker_order_id": None,
        "strategy_id": "gold_02",
        "instrument": "GOLDM",
        "side": "SELL",
        "quantity": 1,
        "order_type": "MARKET",
        "price": 149353.0,
        "planned_entry_price": None,
        "planned_sl": None,
        "planned_order_type": "MARKET",
        "order_intent": None,
        "state": "rejected",
        "filled_quantity": 0,
        "average_fill_price": 0.0,
        "execution_mode": "LIVE",
        "created_at": "2026-10-12T09:45:00+05:30",
        "updated_at": "2026-10-12T09:45:00+05:30",
        "order_role": "ENTRY",
        "trigger_price": None,
        "protected_order_id": None,
        "correlation_id": "MCX-test-1",
        "reason": "LIVE_GATE_CLOSED",
        "submission_outcome": "NOT_SENT",
        "submission_attempt_count": 0,
        "rejection_retry_count": 0,
        "submission_attempts": '[{"attempt":0,"stage":"local_gate"}]',
    }
    captured_sql = []
    monkeypatch.setattr(live_ops, "_live_env", lambda: object())
    monkeypatch.setattr(live_ops, "_q", lambda sql, params=():
                        captured_sql.append(sql) or [row.copy()])
    monkeypatch.setattr(live_ops, "_watcher_records", lambda _env: {})
    monkeypatch.setattr(live_ops, "_product_type", lambda: "MARGIN")
    monkeypatch.setattr(live_ops, "_security_id_for", lambda _instrument: "571445")
    for name in ("_get_funds_sync", "_sync_snapshot", "_candles_sync",
                 "_get_signals_sync", "_get_positions_sync", "_get_pnl_sync",
                 "_get_recon_sync", "_get_telegram_sync", "_global_timeline_sync"):
        if hasattr(live_ops, name):
            monkeypatch.setattr(live_ops, name, lambda *args, **kwargs: {})

    app = FastAPI()
    app.include_router(live_ops.router)
    response = TestClient(app).get("/api/live/orders")

    assert response.status_code == 200
    assert "reason, submission_outcome" in captured_sql[0]
    assert "submission_attempt_count, rejection_retry_count, submission_attempts" in captured_sql[0]
    order = response.json()["orders"][0]
    assert order["reason"] == "LIVE_GATE_CLOSED"
    assert order["submission_outcome"] == "NOT_SENT"
    assert order["submission_attempt_count"] == 0
    assert order["rejection_retry_count"] == 0
    assert order["submission_attempts"] == [{"attempt": 0, "stage": "local_gate"}]
    assert order["broker_order_id"] is None

    dashboard = TestClient(app).get("/api/live/dashboard")
    assert dashboard.status_code == 200
    dashboard_order = dashboard.json()["orders"]["orders"][0]
    assert dashboard_order["reason"] == "LIVE_GATE_CLOSED"
    assert dashboard_order["submission_outcome"] == "NOT_SENT"
    assert dashboard_order["submission_attempt_count"] == 0


def test_live_ops_orders_normalize_corrupt_attempt_history(monkeypatch):
    row = {
        "order_id": "LIVE-local-2",
        "instrument": "SILVERM",
        "submission_attempts": "not-json",
        "submission_attempt_count": None,
        "rejection_retry_count": "bad",
    }
    monkeypatch.setattr(live_ops, "_live_env", lambda: object())
    monkeypatch.setattr(live_ops, "_q", lambda *_args: [row.copy()])
    monkeypatch.setattr(live_ops, "_watcher_records", lambda _env: {})
    monkeypatch.setattr(live_ops, "_product_type", lambda: "MARGIN")
    monkeypatch.setattr(live_ops, "_security_id_for", lambda _instrument: "483080")

    app = FastAPI()
    app.include_router(live_ops.router)
    response = TestClient(app).get("/api/live/orders")

    assert response.status_code == 200
    order = response.json()["orders"][0]
    assert order["submission_attempts"] == []
    assert order["submission_attempt_count"] == 0
    assert order["rejection_retry_count"] == 0

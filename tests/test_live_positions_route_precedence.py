from fastapi import FastAPI
from fastapi.testclient import TestClient

from dashboard.routes import live_ops
from live.api import ROUTE_MODULES


class _PositionManager:
    def snapshot(self):
        return {
            "open_positions": {
                "pos-1": {
                    "position_id": "pos-1",
                    "strategy_id": "silver_01",
                    "instrument": "SILVERM",
                    "side": "SHORT",
                    "quantity": 1,
                    "average_entry_price": 227000.0,
                    "unrealized_pnl": -4250.0,
                    "sl_state": "ARMED",
                    "is_open": True,
                }
            }
        }


class _Environment:
    mode = "LIVE"
    poller = None
    position_manager = _PositionManager()


def test_live_positions_path_returns_broker_comparison_not_local_book(monkeypatch):
    app = FastAPI()
    for module in ROUTE_MODULES:
        app.include_router(module.router)

    monkeypatch.setattr(live_ops, "_live_env", lambda: _Environment())

    response = TestClient(app).get("/api/live/positions")

    assert response.status_code == 200
    payload = response.json()
    assert payload["positions"] == [
        {
            "strategy_id": "silver_01",
            "local_owners": ["silver_01"],
            "instrument": "SILVERM",
            "status": "MISSING_DHAN",
            "delta_qty": -1,
            "local": {
                "side": "SHORT",
                "quantity": 1,
                "average_entry_price": 227000.0,
                "unrealized_pnl": -4250.0,
                "sl_state": "ARMED",
            },
            "dhan": None,
        }
    ]

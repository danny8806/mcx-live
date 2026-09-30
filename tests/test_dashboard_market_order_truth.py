import threading
import time
from types import SimpleNamespace

from dashboard.routes import market_data, orders


def test_market_data_timestamp_is_last_tick_receipt_not_request_time(monkeypatch):
    received_at = time.time() - 240
    adapter = SimpleNamespace(
        connected=True,
        stats={"instrument_ticks": {"GOLDM": 1}, "tick_count": 1},
        _ltp_lock=threading.Lock(),
        _live_ltp={"GOLDM": {
            "ltp": 146099.0,
            "timestamp": received_at + 19800,
            "receive_timestamp": received_at,
        }},
    )
    engine = SimpleNamespace(
        execution_engine=SimpleNamespace(_current_prices={"GOLDM": 146099.0}),
        config=SimpleNamespace(get=lambda key, default=None: {"GOLDM": {}} if key == "instruments" else default),
        data_adapter=adapter,
        market_data_health=SimpleNamespace(is_healthy=lambda _: False),
    )
    monkeypatch.setattr(market_data, "_engine", engine)

    payload = market_data._get_market_data_sync()
    quote = payload["instruments"]["GOLDM"]

    assert quote["receive_timestamp"] == received_at
    assert quote["timestamp"] == received_at
    assert quote["tick_age_seconds"] >= 240
    assert quote["event_timestamp"] > time.time()
    assert quote["feed_healthy"] is False
    assert payload["ws_connected"] is True


def test_order_view_keeps_runtime_status_and_hydrates_durable_price(monkeypatch):
    runtime = SimpleNamespace(
        order_id="LOCAL-1", strategy_id="gold_02", instrument="GOLDM",
        side="BUY", quantity=1, order_type="LIMIT", price=None,
        state=SimpleNamespace(value="filled"), filled_quantity=1,
        average_fill_price=146100.0, created_at=1.0, updated_at=2.0,
        reason=None,
    )
    engine = SimpleNamespace(execution_engine=SimpleNamespace(_orders={"LOCAL-1": runtime}))
    monkeypatch.setattr(orders, "_engine", engine)
    monkeypatch.setattr(orders, "_db_order_rows", lambda: [{
        "order_id": "LOCAL-1", "price": 146101.0,
        "planned_entry_price": 146101.0, "trigger_price": 146099.0,
        "broker_order_id": "DHAN-1", "order_role": "ENTRY", "state": "submitted",
    }])

    payload = orders._list_orders_sync()
    row = payload["orders"][0]

    assert row["state"] == "filled"
    assert row["price"] == 146101.0
    assert row["broker_order_id"] == "DHAN-1"
    assert row["planned_entry_price"] == 146101.0

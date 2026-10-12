from __future__ import annotations

import pytest
from fastapi import HTTPException

from core.environments import validate_environment_config
from dashboard.routes import risk, settings
from execution.live.dhan_transport import DhanRestTransport
from indicators.supertrend import SuperTrend


def test_live_gate_off_is_valid_safe_startup_when_live_can_be_enabled_later():
    validate_environment_config({
        "system": {"environments": ["live"]},
        "live": {"broker": "dhan", "gate": "OFF",
                 "live_trading_enabled": True},
    })


def test_risk_api_does_not_fabricate_unconfigured_loss_caps(monkeypatch):
    class Engine:
        risk_engine = type("Risk", (), {"snapshot": lambda self: {
            "kill_switch_active": False, "daily_pnl": 0, "peak_equity": 300_000}})()
        account_engine = type("Account", (), {"snapshot": lambda self: {
            "equity": 300_000, "used_margin": 0}})()
        position_manager = type("Positions", (), {"snapshot": lambda self: {
            "open_positions": {}}})()
        config = type("Config", (), {"get": lambda self, key, default=None:
            ({"max_positions_total": 2} if key == "risk" else default)})()

    monkeypatch.setattr(risk, "_engine", Engine())
    payload = risk._get_risk_sync()
    assert payload["max_daily_loss"] is None
    assert payload["max_drawdown_pct"] is None
    assert payload["daily_loss_remaining"] is None


def test_supertrend_history_is_bounded_and_incremental_state_is_preserved():
    bounded = SuperTrend(atr_period=3, multiplier=1.2, source="hl2",
                         max_history=8)
    full = SuperTrend(atr_period=3, multiplier=1.2, source="hl2",
                      max_history=100)
    for index in range(60):
        candle = (100 + index, 103 + index, 98 + index, 101 + index)
        bounded.update(*candle, end_ts=index)
        full.update(*candle, end_ts=index)
    assert len(bounded._bars) == bounded.max_history == 8
    assert len(bounded._values) == 8
    assert bounded.snapshot()["history_truncated"] is True
    assert bounded.value == full.value

    corrected = (160, 166, 157, 164)
    bounded.update(*corrected, end_ts=59)
    full.update(*corrected, end_ts=59)
    assert bounded.value == full.value
    assert len(bounded._bars) == 8


def test_supertrend_history_and_atr_period_have_hard_bounds():
    indicator = SuperTrend(atr_period=3, max_history=100_000)
    assert indicator.max_history == 10_000
    with pytest.raises(ValueError, match="between 1 and 500"):
        indicator.configure(atr_period=501, multiplier=2, source="close")


@pytest.mark.parametrize("body", [
    {"quantity": 0}, {"quantity": -1}, {"quantity": 101},
    {"quantity": True}, {"quantity": 1.5},
])
def test_settings_quantity_rejects_invalid_or_over_cap(body):
    with pytest.raises(HTTPException) as exc:
        settings._validate_update(body)
    assert exc.value.status_code == 422


@pytest.mark.parametrize("body", [
    {"supertrend": {"atr_period": 0}},
    {"supertrend": {"atr_period": 501}},
    {"supertrend": {"atr_period": True}},
])
def test_settings_atr_period_rejects_invalid_or_over_cap(body):
    with pytest.raises(HTTPException) as exc:
        settings._validate_update(body)
    assert exc.value.status_code == 422


def test_dhan_wire_rejects_quantity_above_live_hard_cap():
    class NeverCalledHTTP:
        def _post(self, *_args, **_kwargs):
            raise AssertionError("over-cap order reached the broker client")

    transport = DhanRestTransport(
        client_id="test", http=NeverCalledHTTP(), gate_enabled=True,
        instruments={"SILVERM": {"security_id": "1"}},
    )
    with pytest.raises(ValueError, match="safety cap"):
        transport.place_market_order("BUY", 101, "SILVERM")

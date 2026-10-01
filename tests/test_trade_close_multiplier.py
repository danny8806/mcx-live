"""The open contract's multiplier governs every realized close calculation."""

import pytest
from types import SimpleNamespace

from application.fill_flow import FillFlowMixin
from core.trade_close import TradeCloseManager
from execution.fee_model import MCXFeeModel
from execution.models import Fill
from portfolio.pnl import PNLEngine
from portfolio.position_manager import PositionManager


class _Persistence:
    def __init__(self):
        self.trade = None
        self.fill = None

    def save_trade_and_fill(self, trade, fill):
        self.trade = dict(trade)
        self.fill = dict(fill)


@pytest.mark.parametrize(
    "instrument,multiplier,position_multiplier,close_multiplier,entry_side,entry,exit_price,expected_gross",
    [
        ("SILVERM", 5.0, 5.0, 1.0, "BUY", 228647.0, 226623.0, -10120.0),
        ("SILVERM", 5.0, 5.0, 1.0, "SELL", 228595.0, 229227.0, -3160.0),
        ("GOLDM", 10.0, 10.0, 1.0, "BUY", 146900.0, 146800.0, -1000.0),
        ("GOLDM", 10.0, 10.0, 1.0, "SELL", 146800.0, 146900.0, -1000.0),
        # Legacy fallback entries can have the dataclass multiplier default;
        # configured multiplier is supplied only to the close accounting.
        ("SILVERM", 5.0, 1.0, 5.0, "BUY", 228647.0, 226623.0, -10120.0),
        ("GOLDM", 10.0, 1.0, 10.0, "SELL", 146800.0, 146900.0, -1000.0),
    ],
)
def test_close_uses_position_multiplier_when_exit_fill_defaults_to_one(
    instrument, multiplier, position_multiplier, close_multiplier, entry_side,
    entry, exit_price, expected_gross,
):
    strategy_id = "silver_01" if instrument == "SILVERM" else "gold_02"
    persistence = _Persistence()
    position_manager = PositionManager()
    pnl_engine = PNLEngine(MCXFeeModel())
    entry_fill = Fill(
        fill_id="entry-fill", order_id="entry-order", instrument=instrument,
        side=entry_side, quantity=1, price=entry, timestamp=1.0,
        strategy_id=strategy_id, multiplier=multiplier, trade_id="trade-1",
    )
    position = position_manager.open_position(
        entry_fill, multiplier=position_multiplier, trade_id="trade-1")
    exit_fill = Fill(
        fill_id="exit-fill", order_id="fallback-market-order",
        instrument=instrument,
        side="SELL" if entry_side == "BUY" else "BUY",
        quantity=1, price=exit_price, timestamp=2.0,
        strategy_id=strategy_id, multiplier=1.0, trade_id="trade-1",
    )
    manager = TradeCloseManager(
        position_manager=position_manager,
        pnl_engines={strategy_id: pnl_engine},
        account_engines={}, global_account=None, risk_engine=None,
        persistence=persistence, event_store=None,
    )

    result = manager.close_position(
        exit_fill, position, strategy_id, multiplier=close_multiplier,
        exit_reason="STOP_LOSS")

    assert result["gross_pnl"] == expected_gross
    assert result["net_pnl"] == pytest.approx(
        expected_gross - result["charges"])
    assert persistence.trade["multiplier"] == multiplier
    assert persistence.trade["gross_pnl"] == expected_gross
    assert position.realized_pnl == expected_gross
    assert not position_manager.open_positions


def test_partial_close_uses_position_multiplier_when_fill_multiplier_is_default():
    position_manager = PositionManager()
    pnl_engine = PNLEngine(MCXFeeModel())
    entry = Fill(
        fill_id="entry", order_id="entry-order", instrument="SILVERM",
        side="BUY", quantity=2, price=1000.0, timestamp=1.0,
        strategy_id="silver_01", multiplier=5.0, trade_id="partial-trade",
    )
    position = position_manager.open_position(
        entry, multiplier=5.0, trade_id="partial-trade")
    events = []
    runtime = SimpleNamespace(position_manager=position_manager, lifecycle=None)
    env = SimpleNamespace(
        runtimes={"silver_01": runtime}, position_manager=position_manager,
        pnl_engines={"silver_01": pnl_engine}, account_engines={},
        account_engine=None, risk_engine=None, trade_ledger=None, mode="LIVE",
        name="live",
    )

    class _FillFlowHarness(FillFlowMixin):
        def _persist_fill(self, *_args, **_kwargs):
            pass

        def _persist_position(self, *_args, **_kwargs):
            pass

        def _update_fill_reconciliation(self, *_args, **_kwargs):
            pass

        def publish_event(self, _name, payload, **_kwargs):
            events.append(payload)

        def _exit_order_still_working(self, *_args, **_kwargs):
            return False

        def _rearm_sl_after_partial_exit(self, *_args, **_kwargs):
            pass

    fill = Fill(
        fill_id="partial-exit", order_id="fallback-market-order",
        instrument="SILVERM", side="SELL", quantity=1,
        price=1100.0, timestamp=2.0, strategy_id="silver_01",
        multiplier=1.0, trade_id="partial-trade",
    )
    trade = SimpleNamespace(trade_id="partial-trade")
    _FillFlowHarness()._handle_partial_exit(
        env, fill, position, trade, signal_id=None,
        exit_reason="signal_exit", exit_signal_id="")

    assert events[-1]["gross_pnl"] == 500.0
    assert position.quantity == 1
    assert position.is_open


@pytest.mark.parametrize("instrument,multiplier", [("SILVERM", 5.0), ("GOLDM", 10.0)])
def test_close_multiplier_resolver_uses_config_for_legacy_default_position(
    instrument, multiplier,
):
    class _Config:
        def instrument(self, name):
            assert name == instrument
            return {"multiplier": multiplier}

    class _Resolver(FillFlowMixin):
        config = _Config()

    position = SimpleNamespace(instrument=instrument, multiplier=1.0)
    fill = SimpleNamespace(multiplier=1.0)
    assert _Resolver()._position_pnl_multiplier(position, fill) == multiplier

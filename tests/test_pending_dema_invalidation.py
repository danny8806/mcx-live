from types import SimpleNamespace

from core.timeframe_engine import Bar
from strategies.instance import StrategyInstance
from strategies.types import StrategyState


def _bar(ts=2):
    return Bar("GOLDM", "15m", ts, ts + 1, 100.0, 105.0, 95.0, 101.0, 1.0)


def _pending_buy(strategy, *, htf=100.0):
    signal = strategy._create_triggered_entry_signal(
        "LONG", 100.0, 103.0, 99.0, 1, 102.0, 98.0, htf_val=htf)
    strategy._prev_fast_close = 100.0
    strategy._prev_fast_high = 102.0
    strategy._prev_fast_low = 98.0
    strategy._prev_htf_value = htf
    return signal


def _bar_update(strategy, *, htf, long_cross=False, short_cross=False):
    strategy._check_long_cross = lambda *args: long_cross
    strategy._check_short_cross = lambda *args: short_cross
    return strategy.on_bar(_bar(), SimpleNamespace(htf_value=htf), 10.0)


def test_pending_trigger_survives_when_hourly_dema_atr_is_unchanged():
    strategy = StrategyInstance("s1", "GOLDM", "123", "15m")
    old = _pending_buy(strategy)

    emitted = _bar_update(strategy, htf=100.0)

    assert emitted is None
    assert strategy.pending_entry.signal is old
    assert old.metadata["trigger_state"] == "ARMED"
    assert strategy.on_tick(old.trigger_price, timestamp=3) is old


def test_dema_change_cancels_pending_buy_and_emits_cancel_only_without_new_cross():
    strategy = StrategyInstance("s1", "GOLDM", "123", "15m")
    old = _pending_buy(strategy)

    cancellation = _bar_update(strategy, htf=101.0)

    assert cancellation is not None
    assert cancellation.metadata["cancel_pending_only"] is True
    assert cancellation.metadata["old_pending_id"] == old.signal_id
    assert cancellation.metadata["cancel_reason"] == "hourly_dema_atr_changed"
    assert strategy.pending_entry is None
    assert strategy.state == StrategyState.FLAT
    assert old.metadata["trigger_state"] == "CANCELLED"
    assert strategy.on_tick(old.trigger_price, timestamp=3) is None


def test_dema_change_and_fresh_buy_cross_uses_only_new_candle_trigger():
    strategy = StrategyInstance("s1", "GOLDM", "123", "15m")
    old = _pending_buy(strategy)

    fresh = _bar_update(strategy, htf=101.0, long_cross=True)

    assert fresh is not None and fresh is not old
    assert fresh.metadata["signal_htf_dema_atr"] == 101.0
    assert fresh.metadata["signal_candle_high"] == 105.0
    assert fresh.trigger_price == 105.0
    assert fresh.metadata["old_pending_id"] == old.signal_id
    assert fresh.metadata["pending_termination"] == "indicator_change"
    assert fresh.metadata["cancel_inflight"] is True
    assert strategy.pending_entry.signal is fresh
    assert old.metadata["trigger_state"] == "CANCELLED"
    assert strategy.on_tick(old.trigger_price, timestamp=3) is None
    assert strategy.on_tick(fresh.trigger_price - 0.01, timestamp=4) is None
    assert strategy.on_tick(fresh.trigger_price, timestamp=5) is fresh


def test_dema_change_cancels_waiting_reversal_but_preserves_open_position():
    strategy = StrategyInstance("s1", "GOLDM", "123", "15m")
    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.current_position_id = "position-1"
    strategy.current_trade_id = "trade-1"
    strategy.stop_price = 90.0
    strategy.reversal_entry_gap_points = 1
    reversal_exit = strategy._create_reversal_signal(
        "SHORT", 100.0, 105.0, 95.0, 1, 104.0, 96.0, htf_val=100.0)
    reversal_entry = strategy.pending_entry.signal
    strategy._prev_fast_close = 100.0
    strategy._prev_fast_high = 105.0
    strategy._prev_fast_low = 95.0
    strategy._prev_htf_value = 100.0

    cancellation = _bar_update(strategy, htf=101.0)

    assert cancellation is not None
    assert cancellation.metadata["cancel_pending_only"] is True
    assert cancellation.metadata["old_pending_id"] == reversal_entry.signal_id
    assert strategy.pending_entry is None
    assert strategy.pending_exit_trigger is None
    assert strategy.state == StrategyState.LONG_POSITION
    assert strategy.position_side == "LONG"
    assert strategy.current_position_id == "position-1"
    assert reversal_exit.metadata["trigger_state"] == "CANCELLED"
    assert reversal_entry.metadata["trigger_state"] == "CANCELLED"
    assert strategy.on_tick(reversal_exit.trigger_price, timestamp=3) is None

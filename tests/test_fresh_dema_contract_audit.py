"""Independent boundary checks for the hourly-line trigger contract."""
from types import SimpleNamespace

from application.persistence_flow import PersistenceFlowMixin
from core.timeframe_engine import Bar
from strategies.instance import StrategyInstance
from strategies.types import StrategyState
from trading_engine import TradingEngine


def _candle(start, close, high, low):
    return Bar("GOLDM", "15m", start, start + 900, close, high, low, close, 1)


def test_missing_hourly_value_retires_armed_buy_before_an_old_price_can_fire():
    strategy = StrategyInstance("gold_02", "GOLDM", "569003", "15m")
    strategy._prev_fast_close = 99.0
    strategy._prev_htf_value = 100.0
    old = strategy.on_bar(
        _candle(1, 101.0, 103.0, 98.0),
        SimpleNamespace(htf_value=100.0), 100.0)
    assert old is not None and strategy.pending_entry is not None

    control = strategy.on_bar(
        _candle(2, 102.0, 104.0, 99.0),
        SimpleNamespace(htf_value=None), 101.0)

    assert control is not None
    assert control.metadata["cancel_pending_only"] is True
    assert control.metadata["old_pending_id"] == old.signal_id
    assert strategy.pending_entry is None
    assert strategy.on_tick(old.trigger_price, 3) is None


def test_same_hourly_value_allows_new_buy_after_confirmed_stop_exit():
    strategy = StrategyInstance("gold_02", "GOLDM", "569003", "15m")
    strategy._prev_fast_close = 99.0
    strategy._prev_htf_value = 100.0
    first = strategy.on_bar(
        _candle(1, 101.0, 103.0, 98.0),
        SimpleNamespace(htf_value=100.0), 100.0)
    assert first is not None
    assert strategy.on_tick(103.0, 2) is first
    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.current_position_id = "position-1"
    strategy.notify_local_sl_exit()
    resetter = object.__new__(PersistenceFlowMixin)
    resetter._env_for = lambda _name=None: SimpleNamespace(
        strategies={"gold_02": strategy}, runtimes=None,
        pending_triggers=None)
    resetter._reset_strategy_state("gold_02", env_name="live")
    strategy.just_entered = False

    opposite = strategy.on_bar(
        _candle(3, 99.0, 101.0, 97.0),
        SimpleNamespace(htf_value=100.0), 100.0)
    assert opposite is not None and opposite.side is None
    second = strategy.on_bar(
        _candle(4, 102.0, 104.0, 98.0),
        SimpleNamespace(htf_value=100.0), 100.0)
    assert second is not None and second.signal_id != first.signal_id
    assert second.signal_id != opposite.signal_id
    assert opposite.metadata["trigger_state"] == "CANCELLED"
    assert second.metadata["signal_htf_dema_atr"] == 100.0
    assert strategy.on_tick(103.0, 5) is None
    assert strategy.on_tick(104.0, 6) is second


def test_canary_gate_still_executes_dema_cancellation_control():
    strategy = StrategyInstance("gold_02", "GOLDM", "569003", "15m")
    old = strategy._create_triggered_entry_signal(
        "LONG", 101.0, 103.0, 98.0, 1, 102.0, 99.0, htf_val=100.0)
    strategy._prev_fast_close = 101.0
    strategy._prev_htf_value = 100.0
    strategy._check_long_cross = lambda *args: False
    strategy._check_short_cross = lambda *args: False
    control = strategy.on_bar(
        _candle(2, 101.0, 104.0, 99.0),
        SimpleNamespace(htf_value=101.0), 101.0)
    assert control is not None and control.metadata["cancel_pending_only"]
    terminalized = []
    env = SimpleNamespace(
        name="live", mode="LIVE", is_live=True,
        strategies={"gold_02": strategy},
        runtimes=SimpleNamespace(require=lambda _sid: SimpleNamespace(
            lifecycle=None, order_manager=None, position_manager=None)),
        persistence=SimpleNamespace(terminalize_pending_order=lambda *args, **kwargs:
                                    terminalized.append((args, kwargs)) or True),
        pending_triggers=None,
    )
    engine = object.__new__(TradingEngine)
    engine.config = {"live_test_order_cycle": {
        "enabled": True, "strategy_id": "gold_02", "instrument": "GOLDM"}}
    engine._env_for = lambda _name: env
    engine.publish_event = lambda *args, **kwargs: None
    engine._process_signal(control, "live")
    assert terminalized == [((old.signal_id,), {
        "status": "cancelled_by_indicator_change",
        "reason": "hourly_dema_atr_changed",
    })]


def test_broker_submitted_old_pending_row_enters_safe_mode_on_indicator_change():
    strategy = StrategyInstance("gold_02", "GOLDM", "569003", "15m")
    old = strategy._create_triggered_entry_signal(
        "LONG", 101.0, 103.0, 98.0, 1, 102.0, 99.0, htf_val=100.0)
    strategy._prev_fast_close = 101.0
    strategy._prev_htf_value = 100.0
    strategy._check_long_cross = lambda *args: False
    strategy._check_short_cross = lambda *args: False
    control = strategy.on_bar(
        _candle(2, 101.0, 104.0, 99.0),
        SimpleNamespace(htf_value=101.0), 101.0)
    alerts = []
    env = SimpleNamespace(
        name="live", mode="LIVE", is_live=True,
        strategies={"gold_02": strategy},
        runtimes=SimpleNamespace(require=lambda _sid: SimpleNamespace(
            lifecycle=None, order_manager=None, position_manager=None)),
        persistence=SimpleNamespace(
            terminalize_pending_order=lambda *args, **kwargs: False,
            get_pending_order=lambda *args, **kwargs: {
                "signal_id": old.signal_id, "status": "entry_sent"}),
        pending_triggers=None,
        safe_mode=SimpleNamespace(
            enter_safe_mode=lambda *args: alerts.append(args)),
    )
    engine = object.__new__(TradingEngine)
    engine.config = {}
    engine._env_for = lambda _name: env
    engine.publish_event = lambda *args, **kwargs: None

    engine._process_signal(control, "live")

    assert alerts and alerts[0][0] == "pending_trigger_state_mismatch"


def _held_long_with_reversal():
    strategy = StrategyInstance("gold_02", "GOLDM", "569003", "15m")
    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.current_position_id = "held-position"
    strategy.current_trade_id = "held-trade"
    strategy.stop_price = 90.0
    strategy.reversal_entry_gap_points = 2
    old_exit = strategy._create_reversal_signal(
        "SHORT", 101.0, 106.0, 95.0, 1, 105.0, 96.0,
        htf_val=100.0)
    old_entry = strategy.pending_entry.signal
    strategy._prev_fast_close = 101.0
    strategy._prev_htf_value = 100.0
    strategy._prev_fast_high = 106.0
    strategy._prev_fast_low = 95.0
    return strategy, old_exit, old_entry


def test_dema_change_replaces_both_waiting_reversal_legs_on_real_short_cross():
    strategy, old_exit, old_entry = _held_long_with_reversal()
    fresh_exit = strategy.on_bar(
        _candle(2, 100.0, 104.0, 94.0),
        SimpleNamespace(htf_value=101.0), 101.0)

    assert fresh_exit is not None
    assert fresh_exit.signal_id != old_exit.signal_id
    assert fresh_exit.metadata["pending_trigger_kind"] == "REVERSAL_EXIT"
    assert fresh_exit.metadata["superseded_pending_entry_signal_id"] == (
        old_entry.signal_id)
    assert fresh_exit.metadata["superseded_pending_termination"] == (
        "indicator_change")
    assert fresh_exit.metadata["signal_htf_dema_atr"] == 101.0
    assert old_exit.metadata["trigger_state"] == "CANCELLED"
    assert old_entry.metadata["trigger_state"] == "CANCELLED"
    assert strategy.pending_exit_trigger.signal is fresh_exit
    assert strategy.pending_entry.signal.metadata["signal_htf_dema_atr"] == 101.0
    assert strategy.position_side == "LONG"
    assert strategy.stop_price == 90.0
    assert strategy.on_tick(old_exit.trigger_price, 3) is None
    assert strategy.on_tick(fresh_exit.trigger_price, 4) is fresh_exit


def test_dema_change_after_reversal_exit_fired_retires_only_waiting_entry():
    strategy, old_exit, old_entry = _held_long_with_reversal()
    assert strategy.on_tick(old_exit.trigger_price, 2) is old_exit
    assert strategy.state == StrategyState.EXIT_ORDER_SUBMITTED
    assert strategy.pending_entry.signal is old_entry
    strategy._check_long_cross = lambda *args: False
    strategy._check_short_cross = lambda *args: False

    control = strategy.on_bar(
        _candle(3, 100.0, 104.0, 94.0),
        SimpleNamespace(htf_value=101.0), 101.0)

    assert control is not None and control.metadata["cancel_pending_only"]
    assert control.metadata["old_pending_id"] == old_entry.signal_id
    assert strategy.pending_entry is None
    assert strategy.state == StrategyState.EXIT_ORDER_SUBMITTED
    assert strategy.position_side == "LONG"
    assert strategy.current_position_id == "held-position"
    assert strategy.stop_price == 90.0


def test_reversal_replacement_halts_if_old_durable_entry_stays_armed():
    strategy, _old_exit, old_entry = _held_long_with_reversal()
    replacement = strategy.on_bar(
        _candle(2, 100.0, 104.0, 94.0),
        SimpleNamespace(htf_value=101.0), 101.0)
    assert replacement is not None
    alerts = []
    preserved = []
    env = SimpleNamespace(
        name="live", mode="LIVE", is_live=True,
        strategies={"gold_02": strategy},
        runtimes=SimpleNamespace(require=lambda _sid: SimpleNamespace(
            lifecycle=None, order_manager=None, position_manager=None)),
        persistence=SimpleNamespace(
            terminalize_pending_order=lambda *args, **kwargs: False,
            get_pending_order=lambda *args, **kwargs: {
                "signal_id": old_entry.signal_id, "status": "armed"}),
        pending_triggers=None,
        safe_mode=SimpleNamespace(
            enter_safe_mode=lambda *args: alerts.append(args)),
    )
    engine = object.__new__(TradingEngine)
    engine.config = {}
    engine._env_for = lambda _name: env
    engine.publish_event = lambda *args, **kwargs: None
    engine._preserve_position_after_exit_block = lambda *args, **kwargs: (
        preserved.append((args, kwargs)))

    engine._process_signal(replacement, "live")

    assert alerts and alerts[0][0] == "pending_trigger_state_mismatch"
    assert preserved and preserved[0][1]["cancel_reversal"] is True

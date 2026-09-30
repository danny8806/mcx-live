"""Regression coverage for a fresh same-side entry after a confirmed SL exit."""
from types import SimpleNamespace

from application.persistence_flow import PersistenceFlowMixin
from core.timeframe_engine import Bar
from strategies.gold import create_gold_5m


def test_fresh_buy_after_sl_can_arm_and_fire_with_same_dema_context():
    strategy = create_gold_5m()
    dema_atr = 101.25

    first = strategy._entry_signal(
        "LONG", close=103, high=105, low=99, timestamp=1,
        prev_high=104, prev_low=98, htf_val=dema_atr,
        fast_dema_atr=dema_atr)
    first_fired = strategy.on_tick(first.trigger_price, timestamp=2)
    assert first_fired is first

    # Model the SL fire followed by the broker-confirmed flat/reset path.
    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.current_position_id = "position-1"
    strategy.notify_local_sl_exit("stop_loss_hit")
    env = SimpleNamespace(
        strategies={strategy.strategy_id: strategy},
        runtimes=None,
        pending_triggers=None,
    )
    resetter = object.__new__(PersistenceFlowMixin)
    resetter._env_for = lambda _name=None: env
    resetter._reset_strategy_state(strategy.strategy_id, env_name="LIVE")

    # Drive a fresh completed candle through the real on_bar path. Keep the
    # crossover fixture stable to isolate entry-state/reset behavior.
    strategy._detect_signal = lambda *args, **kwargs: strategy._entry_signal(
        "LONG", close=103, high=105, low=99, timestamp=3,
        prev_high=104, prev_low=98, htf_val=dema_atr,
        fast_dema_atr=dema_atr)
    strategy._prev_htf_value = dema_atr
    second = strategy.on_bar(
        Bar("GOLDM", "5m", 3, 4, 102, 105, 99, 103, 1),
        SimpleNamespace(htf_value=dema_atr), dema_atr)

    assert second.signal_id != first.signal_id
    assert second.metadata["trigger_generation"] > first.metadata["trigger_generation"]
    assert second.metadata["signal_htf_dema_atr"] == dema_atr
    assert strategy.on_tick(second.trigger_price - 0.01, timestamp=4) is None
    assert strategy.on_tick(second.trigger_price, timestamp=5) is second
    assert second.metadata["trigger_state"] == "FIRED"


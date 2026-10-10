from threading import RLock
from types import SimpleNamespace

from application.fill_flow import FillFlowMixin
from strategies.types import Signal, SignalType, StrategyState


def _pending_entry():
    signal = Signal(
        signal_type=SignalType.SHORT,
        instrument="SILVERM",
        strategy_id="silver_01",
        timestamp=1.0,
        trigger_price=101.0,
        stop_price=0.0,
        quantity=1,
        side="SHORT",
        metadata={
            "is_reversal": True,
            "is_reversal_entry": True,
            "market_on_signal_close": True,
            "no_separate_stop": True,
        },
    )
    pending = SimpleNamespace(signal=signal, status="pending")
    registered = []
    strategy = SimpleNamespace(
        pending_entry=pending,
        state=None,
        _register_fired_trigger=registered.append,
    )
    return signal, pending, strategy, registered


class _Engine(FillFlowMixin):
    def __init__(self, *, flat=True, price=102.0, reconciled=True):
        self._lock = RLock()
        self._reconciled_envs = {"live"} if reconciled else set()
        self.events = []
        self.processed = []
        self.flat = flat
        self.price = price

    def _broker_flat_for_entry(self, _env, _signal):
        return self.flat, {"reason": "test_not_flat"} if not self.flat else {}

    def _process_signal(self, signal, env_name):
        self.processed.append((signal, env_name))

    def publish_event(self, name, payload, env_name=None):
        self.events.append((name, payload, env_name))


def _env(strategy, *, price=102.0):
    return SimpleNamespace(
        strategies={"silver_01": strategy},
        position_manager=SimpleNamespace(
            get_positions_by_strategy=lambda _sid: []),
        execution_engine=SimpleNamespace(
            _current_prices={"SILVERM": price}),
        pending_triggers=None,
        is_live=True,
        name="live",
        mode="LIVE",
    )


def test_broker_flat_hands_paired_reversal_entry_to_signal_processing():
    signal, pending, strategy, registered = _pending_entry()
    engine = _Engine(flat=True, price=102.0)

    assert engine._activate_reversal_entry_after_flat(
        _env(strategy), "silver_01") is True

    assert engine.processed == [(signal, "live")]
    assert strategy.pending_entry is None
    assert pending.status == "fired"
    assert strategy.state == StrategyState.ENTRY_TRIGGERED
    assert signal.trigger_price == 102.0
    assert signal.metadata["trigger_state"] == "FIRED"
    assert signal.metadata["entry_after_confirmed_reversal_exit"] is True
    assert registered == [signal.signal_id]
    assert any(name == "reversal_entry_triggered_after_flat"
               for name, _payload, _env in engine.events)


def test_broker_not_flat_keeps_reversal_entry_armed_without_submission():
    _signal, pending, strategy, _registered = _pending_entry()
    engine = _Engine(flat=False)

    assert engine._activate_reversal_entry_after_flat(
        _env(strategy), "silver_01") is False

    assert engine.processed == []
    assert strategy.pending_entry is pending
    assert pending.status == "waiting_for_flat"
    assert strategy.state == StrategyState.EXIT_ORDER_SUBMITTED
    assert any(name == "reversal_entry_waiting_for_broker_flat"
               for name, _payload, _env in engine.events)


def test_reconciliation_gate_keeps_paired_entry_for_later_retry():
    _signal, pending, strategy, _registered = _pending_entry()
    engine = _Engine(flat=True, reconciled=False)

    assert engine._activate_reversal_entry_after_flat(
        _env(strategy), "silver_01") is False

    assert engine.processed == []
    assert strategy.pending_entry is pending
    assert pending.status == "waiting_for_flat"
    assert strategy.state == StrategyState.EXIT_ORDER_SUBMITTED
    assert any(name == "reversal_entry_waiting_for_reconciliation"
               for name, _payload, _env in engine.events)


def test_replaced_pending_signal_during_broker_check_is_not_submitted():
    _old_signal, old_pending, strategy, _registered = _pending_entry()
    new_signal, new_pending, _new_strategy, _ = _pending_entry()
    new_signal.signal_id = "new-reversal-entry"
    engine = _Engine(flat=True)

    def replace_then_confirm_flat(_env, _signal):
        strategy.pending_entry = new_pending
        return True, {}

    engine._broker_flat_for_entry = replace_then_confirm_flat

    assert engine._activate_reversal_entry_after_flat(
        _env(strategy), "silver_01") is False

    assert engine.processed == []
    assert strategy.pending_entry is new_pending
    assert old_pending.status == "pending"

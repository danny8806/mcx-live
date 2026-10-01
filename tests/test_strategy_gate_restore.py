from types import SimpleNamespace

from trading_engine import StrategyGate, TradingEngine, restore_strategy_gates
from strategies.types import StrategyState


def test_legacy_saved_gate_does_not_override_current_config():
    configured = {"gold_02": StrategyGate(
        live_gate="ON", entry_enabled=True, exit_enabled=True,
        reversal_enabled=True, sl_enabled=True,
    )}
    legacy_saved = {"gold_02": {
        "live_gate": "CLOSE_ONLY",
        "entry_enabled": False,
        "exit_enabled": True,
        "reversal_enabled": False,
        "sl_enabled": True,
        "close_only": True,
    }}

    restored = restore_strategy_gates(configured, legacy_saved)

    assert restored["gold_02"].entries_allowed


def test_explicit_saved_operator_gate_overrides_current_config():
    configured = {"gold_02": StrategyGate(
        live_gate="ON", entry_enabled=True, exit_enabled=True,
        reversal_enabled=True, sl_enabled=True,
    )}
    operator_saved = {"gold_02": {
        "live_gate": "CLOSE_ONLY",
        "entry_enabled": False,
        "exit_enabled": True,
        "reversal_enabled": False,
        "sl_enabled": True,
        "close_only": True,
        "operator_override": True,
    }}

    restored = restore_strategy_gates(configured, operator_saved)

    assert not restored["gold_02"].entries_allowed
    assert restored["gold_02"].operator_override


def test_runtime_control_is_marked_as_an_operator_override():
    class ObservedLock:
        held = False

        def __enter__(self):
            self.held = True

        def __exit__(self, *_args):
            self.held = False

    engine = object.__new__(TradingEngine)
    engine.strategies = {"gold_02": SimpleNamespace(enabled=True)}
    engine._strategy_gates = {"gold_02": StrategyGate()}
    engine._lock = ObservedLock()
    def publish_while_locked(*_args, **_kwargs):
        assert engine._lock.held
    engine.publish_event = publish_while_locked

    result = engine.control_strategy("gold_02", "close_only")

    assert result["success"]
    assert result["gate"]["operator_override"]
    assert not engine._strategy_gates["gold_02"].entries_allowed


def test_pause_terminalizes_live_trigger_before_clearing_memory():
    calls = []
    signal = SimpleNamespace(signal_id="signal-1")
    strategy = SimpleNamespace(
        enabled=True, pending_entry=SimpleNamespace(signal=signal),
        state=StrategyState.PENDING_LONG, _last_armed_pending_id="signal-1",
        _cancel_trigger=lambda pending: calls.append(("cancel", pending.signal.signal_id)),
    )
    persistence = SimpleNamespace(
        terminalize_pending_order=lambda signal_id, **kwargs:
            calls.append(("db", signal_id, kwargs["reason"])) or True,
    )
    registry = SimpleNamespace(sync_strategy=lambda strat: calls.append(("sync", strat.pending_entry)))
    env = SimpleNamespace(strategies={"gold_02": strategy}, is_live=True,
                          persistence=persistence, pending_triggers=registry)
    engine = object.__new__(TradingEngine)
    engine.strategies = env.strategies
    engine._envs = {"live": env}
    engine.position_manager = SimpleNamespace(get_positions_by_strategy=lambda _: [])
    engine._strategy_gates = {"gold_02": StrategyGate()}
    engine.publish_event = lambda *_args, **_kwargs: None

    result = engine.control_strategy("gold_02", "pause")

    assert result["success"]
    assert calls == [("db", "signal-1", "operator_paused"),
                     ("cancel", "signal-1"), ("sync", None)]
    assert strategy.pending_entry is None
    assert strategy.state == StrategyState.FLAT
    assert strategy._last_armed_pending_id is None
    assert not strategy.enabled


def test_pause_keeps_live_trigger_armed_when_db_cleanup_fails():
    pending = SimpleNamespace(signal=SimpleNamespace(signal_id="signal-1"))
    strategy = SimpleNamespace(enabled=True, pending_entry=pending)
    persistence = SimpleNamespace(
        terminalize_pending_order=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db down")))
    env = SimpleNamespace(strategies={"gold_02": strategy}, is_live=True,
                          persistence=persistence, pending_triggers=None)
    engine = object.__new__(TradingEngine)
    engine.strategies = env.strategies
    engine._envs = {"live": env}
    engine.position_manager = SimpleNamespace(get_positions_by_strategy=lambda _: [])
    engine._strategy_gates = {"gold_02": StrategyGate()}
    engine.publish_event = lambda *_args, **_kwargs: None

    result = engine.control_strategy("gold_02", "pause")

    assert not result["success"]
    assert strategy.pending_entry is pending
    assert strategy.enabled
    assert engine._strategy_gates["gold_02"].entries_allowed

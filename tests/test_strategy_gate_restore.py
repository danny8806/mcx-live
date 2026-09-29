from types import SimpleNamespace

from trading_engine import StrategyGate, TradingEngine, restore_strategy_gates


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
    engine = object.__new__(TradingEngine)
    engine.strategies = {"gold_02": SimpleNamespace(enabled=True)}
    engine._strategy_gates = {"gold_02": StrategyGate()}
    engine.publish_event = lambda *_args, **_kwargs: None

    result = engine.control_strategy("gold_02", "close_only")

    assert result["success"]
    assert result["gate"]["operator_override"]
    assert not engine._strategy_gates["gold_02"].entries_allowed

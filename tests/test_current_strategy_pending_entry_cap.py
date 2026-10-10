from types import SimpleNamespace
from enum import Enum
import json
from pathlib import Path

from execution.order_manager import OrderManager
from trading_engine import TradingEngine


class State(Enum):
    CREATED = "created"
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"


def _engine(order_state):
    order = SimpleNamespace(order_id="broker-order-1", strategy_id="gold_02",
                            state=order_state)
    execution = SimpleNamespace(_orders={order.order_id: order})
    manager = OrderManager(execution)
    runtime = SimpleNamespace(order_manager=manager)
    runtimes = SimpleNamespace(require=lambda _sid: runtime)
    engine = object.__new__(TradingEngine)
    return engine, SimpleNamespace(runtimes=runtimes, order_manager=manager)


def test_unresolved_acknowledged_order_occupies_pending_entry_slot():
    engine, env = _engine(State.ACKNOWLEDGED)

    assert engine._pending_orders_count(env, "gold_02") == 1


def test_new_entry_is_blocked_while_previous_entry_is_broker_acknowledged():
    engine, env = _engine(State.ACKNOWLEDGED)
    engine.config = SimpleNamespace(
        strategy=lambda _sid: {"quantity": 1},
        instrument=lambda _name: {"multiplier": 10.0, "lot_size": 1},
    )
    signal = SimpleNamespace(strategy_id="gold_02", instrument="GOLDM",
                             trigger_price=1000.0, quantity=1)

    gate_env = SimpleNamespace(
        runtimes=env.runtimes, order_manager=env.order_manager,
        pnl_engines={}, account_engines={},
    )
    allowed, reason = engine._validate_strategy_risk_gate(signal, gate_env, None)

    assert allowed is False
    assert reason == "pending_orders_cap"


def test_terminal_fill_does_not_occupy_pending_entry_slot():
    engine, env = _engine(State.FILLED)

    assert engine._pending_orders_count(env, "gold_02") == 0


def test_current_live_profiles_allow_only_one_working_order():
    root = Path(__file__).resolve().parents[1]
    config = json.loads((root / "config" / "live_settings.json").read_text())

    assert config["strategies"]["gold_02"]["max_pending_orders"] == 1
    assert config["strategies"]["silver_01"]["max_pending_orders"] == 1


def test_restore_accepts_position_only_with_current_supertrend_signal_lineage():
    strategy = SimpleNamespace(strategy_type="supertrend")
    env = SimpleNamespace(strategies={"gold_02": strategy})
    signal = {
        "strategy_id": "gold_02", "instrument": "GOLDM",
        "signal_metadata": json.dumps({
            "strategy_type": "supertrend", "strategy_timeframe": "15m",
            "signal_source": "completed_native_15m_candle",
            "market_on_signal_close": True, "no_separate_stop": True,
        }),
    }
    persistence = SimpleNamespace(get_signal=lambda _signal_id: signal)
    position = {"strategy_id": "gold_02", "instrument": "GOLDM",
                "entry_signal_id": "signal-current"}

    assert TradingEngine._is_current_supertrend_position(env, persistence, position)
    assert not TradingEngine._is_current_supertrend_position(
        env, persistence, {**position, "entry_signal_id": ""})


def test_restore_rejects_legacy_strategy_position_even_with_same_profile_id():
    strategy = SimpleNamespace(strategy_type="supertrend")
    env = SimpleNamespace(strategies={"gold_02": strategy})
    signal = {
        "strategy_id": "gold_02", "instrument": "GOLDM",
        "signal_metadata": json.dumps({"strategy_type": "dema_atr"}),
    }
    persistence = SimpleNamespace(get_signal=lambda _signal_id: signal)
    position = {"strategy_id": "gold_02", "instrument": "GOLDM",
                "entry_signal_id": "signal-old"}

    assert not TradingEngine._is_current_supertrend_position(env, persistence, position)


def test_position_restore_filter_quarantines_legacy_row_and_enters_safe_mode():
    strategy = SimpleNamespace(strategy_type="supertrend")
    signal = {"strategy_id": "gold_02", "instrument": "GOLDM",
              "signal_metadata": json.dumps({"strategy_type": "dema_atr"})}
    safe_mode_calls = []
    env = SimpleNamespace(
        strategies={"gold_02": strategy},
        persistence=SimpleNamespace(get_signal=lambda _signal_id: signal),
        safe_mode=SimpleNamespace(enter_safe_mode=lambda *args: safe_mode_calls.append(args)),
    )
    engine = object.__new__(TradingEngine)
    old_row = {"position_id": "old-position", "strategy_id": "gold_02",
               "instrument": "GOLDM", "entry_signal_id": "old-signal"}

    assert engine._current_supertrend_position_rows(env, [old_row]) == []
    assert safe_mode_calls[0][0] == "unverified_strategy_position"


def test_order_restore_accepts_current_market_order_and_quarantines_unknown_order():
    strategy = SimpleNamespace(strategy_type="supertrend", instrument="GOLDM")
    current_signal = {
        "strategy_id": "gold_02", "instrument": "GOLDM",
        "signal_metadata": json.dumps({
            "strategy_type": "supertrend", "strategy_timeframe": "15m",
            "signal_source": "completed_native_15m_candle",
            "market_on_signal_close": True, "no_separate_stop": True,
        }),
    }
    old_signal = {
        "strategy_id": "gold_02", "instrument": "GOLDM",
        "signal_metadata": json.dumps({"strategy_type": "dema_atr"}),
    }
    safe_mode_calls = []
    env = SimpleNamespace(
        strategies={"gold_02": strategy},
        persistence=SimpleNamespace(
            get_signal=lambda signal_id: {
                "CURRENT-SIGNAL": current_signal,
                "OLD-SIGNAL": old_signal,
            }.get(signal_id)),
        safe_mode=SimpleNamespace(enter_safe_mode=lambda *args: safe_mode_calls.append(args)),
    )
    engine = object.__new__(TradingEngine)
    current_order = {
        "order_id": "current-order", "strategy_id": "gold_02",
        "instrument": "GOLDM", "order_type": "MARKET", "order_role": "ENTRY",
        "state": "submitted", "signal_id": "CURRENT-SIGNAL",
    }
    old_order = {
        **current_order, "order_id": "old-order", "signal_id": "OLD-SIGNAL",
    }

    assert engine._current_supertrend_order_rows(env, [current_order, old_order]) == [current_order]
    assert safe_mode_calls[0][0] == "unverified_strategy_order"


def test_reversal_entry_order_can_restore_from_its_current_paired_signal():
    strategy = SimpleNamespace(strategy_type="supertrend", instrument="SILVERM")
    signal = {
        "strategy_id": "silver_01", "instrument": "SILVERM",
        "signal_metadata": json.dumps({
            "strategy_type": "supertrend", "is_reversal_entry": True,
            "strategy_timeframe": "15m",
            "signal_source": "completed_native_15m_candle",
            "market_on_signal_close": True, "no_separate_stop": True,
        }),
    }
    env = SimpleNamespace(
        strategies={"silver_01": strategy},
        persistence=SimpleNamespace(get_signal=lambda _signal_id: signal),
        safe_mode=None,
    )
    order = {
        "order_id": "current-reversal-entry", "strategy_id": "silver_01",
        "instrument": "SILVERM", "order_type": "MARKET",
        "order_role": "REVERSAL_ENTRY", "state": "acknowledged",
        "signal_id": "CURRENT-REVERSAL-ENTRY",
    }

    assert TradingEngine._is_current_supertrend_order(env, env.persistence, order)


def test_durable_broker_mapping_restore_is_filtered_by_current_order_lineage():
    from execution.broker_router import BrokerEventRouter

    mappings = [
        {"broker_order_id": "BROKER-CURRENT", "order_id": "CURRENT",
         "strategy_id": "gold_02", "instrument": "GOLDM"},
        {"broker_order_id": "BROKER-OLD", "order_id": "OLD",
         "strategy_id": "gold_02", "instrument": "GOLDM"},
    ]
    persistence = SimpleNamespace(get_broker_order_mappings=lambda: mappings)
    router = BrokerEventRouter(persistence)
    router.set_mapping_validator(lambda row: row["broker_order_id"] == "BROKER-CURRENT")

    assert router.restore() == 1
    assert router.resolve("BROKER-CURRENT") is not None
    assert router.resolve("BROKER-OLD") is None

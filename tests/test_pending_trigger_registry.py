from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from application.market_flow import MarketEventFlowMixin
from application.persistence_flow import PersistenceFlowMixin
from core.environments import Environment
from core.pending_trigger_registry import PendingTriggerRegistry
from events.types import TickEvent
from strategies.gold import create_gold_15m
from strategies.types import PendingEntry, Signal, SignalType
from persistence.manager import PersistenceManager


class _NoTickDb:
    def get_pending_orders(self, **_kwargs):
        raise AssertionError("SQLite must not be read from the WebSocket tick path")


class _TickHarness(MarketEventFlowMixin):
    def __init__(self, env):
        self.env = env
        self._running = True
        self._lock = threading.RLock()
        self.processed = []

    def _env_for(self, _name):
        return self.env

    def _bind_signal_position(self, *_args):
        pass

    def _process_signal(self, signal, _env_name):
        self.processed.append(signal)


@pytest.mark.parametrize(
    "side,trigger,crossing",
    [("LONG", 100.0, 100.0), ("SHORT", 100.0, 100.0)],
)
def test_websocket_tick_recovers_lost_runtime_reference_and_fires_once(
    side, trigger, crossing
):
    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=100)
    strategy._trigger_generation = 7
    signal = Signal(
        signal_type=SignalType(side), instrument="GOLDM", strategy_id="gold_02",
        timestamp=1.0, trigger_price=trigger, stop_price=90.0 if side == "LONG" else 110.0,
        quantity=100,
        metadata={"pending": True, "triggered": False, "trigger_state": "ARMED",
                  "trigger_generation": 7},
    )
    strategy.pending_entry = PendingEntry(
        signal=signal, trigger_price=trigger, side=side, status="pending")
    env = Environment(name="live", mode="LIVE", is_live=True, persistence=_NoTickDb())
    env.pending_triggers.sync_strategy(strategy)
    # Simulate the bug: strategy RAM reference disappears while registry remains.
    strategy.pending_entry = None
    engine = _TickHarness(env)
    handler = engine._make_tick_handler(strategy, "live")

    handler(TickEvent("GOLDM", crossing, 2.0))
    handler(TickEvent("GOLDM", crossing, 3.0))

    assert [s.signal_id for s in engine.processed] == [signal.signal_id]
    assert strategy.pending_entry is None
    assert env.pending_triggers.entry_for("gold_02") is None


def test_reversal_exit_trigger_recovers_from_registry_without_firing_entry_early():
    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=1)
    strategy._trigger_generation = 9
    strategy.position_side = "LONG"
    entry_signal = Signal(
        signal_type=SignalType.SHORT, instrument="GOLDM", strategy_id="gold_02",
        timestamp=1.0, trigger_price=80.0, stop_price=120.0, quantity=1,
        metadata={"pending": True, "trigger_state": "ARMED",
                  "trigger_generation": 9, "is_reversal_entry": True},
    )
    exit_signal = Signal(
        signal_type=SignalType.SHORT, instrument="GOLDM", strategy_id="gold_02",
        timestamp=1.0, trigger_price=90.0, stop_price=95.0, quantity=1,
        metadata={"pending": True, "exit": True, "trigger_state": "ARMED",
                  "trigger_generation": 9, "position_side": "LONG"},
    )
    strategy.pending_entry = PendingEntry(
        signal=entry_signal, trigger_price=80.0, side="SHORT", status="waiting_for_flat")
    strategy.pending_exit_trigger = PendingEntry(
        signal=exit_signal, trigger_price=90.0, side="SHORT", status="pending")
    env = Environment(name="live", mode="LIVE", is_live=True, persistence=_NoTickDb())
    env.pending_triggers.sync_strategy(strategy)
    strategy.pending_entry = None
    strategy.pending_exit_trigger = None
    engine = _TickHarness(env)
    handler = engine._make_tick_handler(strategy, "live")

    # Before the exit level is crossed, neither trigger can fire.
    handler(TickEvent("GOLDM", 95.0, 2.0))
    assert strategy.position_side == "LONG"
    assert engine.processed == []
    # Crossing the old-position exit trigger submits precisely that exit.
    handler(TickEvent("GOLDM", 89.0, 3.0))
    handler(TickEvent("GOLDM", 88.0, 4.0))
    assert [s.signal_id for s in engine.processed] == [exit_signal.signal_id]
    assert env.pending_triggers.exit_for("gold_02") is None
    assert env.pending_triggers.entry_for("gold_02").status == "waiting_for_flat"


def test_startup_restore_rebuilds_armed_trigger_from_database():
    signal_id = "signal-armed-1"
    row = {
        "pending_order_id": signal_id, "signal_id": signal_id,
        "strategy_id": "gold_02", "instrument": "GOLDM", "direction": "LONG",
        "trigger_price": 147100.0, "quantity": 100, "status": "armed",
        "trigger_state": "ARMED", "trigger_generation": 3,
        "trigger_source": "market_websocket_ltp", "signal_timestamp": 10.0,
    }

    class _Persistence:
        def get_pending_orders(self, **kwargs):
            assert kwargs == {"execution_mode": "LIVE"}
            return [row]

        def get_signal(self, requested_id):
            assert requested_id == signal_id
            return {
                "signal_id": signal_id, "strategy_id": "gold_02",
                "instrument": "GOLDM", "side": "LONG", "trigger_price": 147100.0,
                "stop_price": 146719.0, "quantity": 100, "signal_timestamp": 10.0,
                "candle_timestamp": 10.0, "open": 146800.0, "high": 147100.0,
                "low": 146721.0, "close": 146950.0, "htf_value": 146915.5,
                "mid_value": 146873.6, "fast_dema": 146873.6, "fast_atr": 268.2,
                "signal_metadata": None,
            }

    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=1)
    env = Environment(name="live", mode="LIVE", is_live=True,
                      strategies={"gold_02": strategy}, persistence=_Persistence())
    engine = PersistenceFlowMixin()

    assert engine._restore_live_pending_triggers(env) == 1
    pending = strategy.pending_entry
    assert pending is not None
    assert pending.signal.signal_id == signal_id
    assert pending.trigger_price == 147100.0
    assert pending.signal.stop_price == 146719.0
    assert pending.signal.quantity == 100
    assert strategy._trigger_generation == 3
    assert env.pending_triggers.entry_for("gold_02") is pending
    assert env.pending_triggers.live_row(signal_id)["status"] == "armed"


def test_startup_restore_does_not_resurrect_terminal_snapshot_trigger():
    signal_id = "signal-expired-1"
    signal = Signal(
        signal_type=SignalType.LONG, instrument="GOLDM", strategy_id="gold_02",
        timestamp=10.0, trigger_price=147100.0, stop_price=146719.0, quantity=100)
    signal.signal_id = signal_id
    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=100)
    strategy.pending_entry = PendingEntry(
        signal=signal, trigger_price=147100.0, side="LONG", status="pending")

    class _Persistence:
        def get_pending_orders(self, **_kwargs):
            return [{"pending_order_id": signal_id, "signal_id": signal_id,
                     "strategy_id": "gold_02", "status": "expired"}]

    env = Environment(name="live", mode="LIVE", is_live=True,
                      strategies={"gold_02": strategy}, persistence=_Persistence())

    assert PersistenceFlowMixin()._restore_live_pending_triggers(env) == 0
    assert strategy.pending_entry is None
    assert env.pending_triggers.entry_for("gold_02") is None


def test_live_arming_rebuilds_missing_memory_trigger_and_registry():
    signal = Signal(
        signal_type=SignalType.LONG, instrument="GOLDM", strategy_id="gold_02",
        timestamp=10.0, trigger_price=146985.0, stop_price=146854.0,
        quantity=100,
        metadata={"pending": True, "trigger_state": "ARMED",
                  "trigger_generation": 6, "trigger_source": "market_websocket_ltp"},
    )
    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=100)

    class _Persistence:
        def __init__(self):
            self.rows = []

        def get_pending_orders(self, **_kwargs):
            return list(self.rows)

        def save_pending_order(self, row):
            existing = next((r for r in self.rows
                             if r["pending_order_id"] == row["pending_order_id"]), None)
            if existing:
                existing.update(row)
            else:
                self.rows.append(dict(row))

    class _Harness(PersistenceFlowMixin):
        def publish_event(self, *_args, **_kwargs):
            pass

    env = Environment(name="live", mode="LIVE", is_live=True,
                      strategies={"gold_02": strategy}, persistence=_Persistence())

    _Harness()._arm_live_pending(signal, env)

    assert strategy.pending_entry is not None
    assert strategy.pending_entry.signal.signal_id == signal.signal_id
    assert strategy.pending_entry.trigger_price == 146985.0
    assert strategy._trigger_generation == 6
    assert env.pending_triggers.entry_for("gold_02") is strategy.pending_entry
    assert env.pending_triggers.live_row(signal.signal_id)["status"] == "armed"


def test_startup_restore_rebuilds_both_reversal_triggers():
    entry_id, exit_id = "reversal-entry", "reversal-exit"
    armed = {
        "pending_order_id": entry_id, "signal_id": entry_id,
        "strategy_id": "gold_02", "instrument": "GOLDM", "direction": "SHORT",
        "trigger_price": 80.0, "quantity": 1, "status": "armed",
        "trigger_state": "ARMED", "trigger_generation": 4,
        "trigger_source": "market_websocket_ltp", "signal_timestamp": 10.0,
    }
    signals = {
        entry_id: {
            "signal_id": entry_id, "strategy_id": "gold_02", "instrument": "GOLDM",
            "side": "SHORT", "trigger_price": 80.0, "stop_price": 120.0,
            "quantity": 1, "signal_timestamp": 10.0, "signal_metadata":
                '{"is_reversal_entry": true, "reversal_parent_signal_id": "reversal-exit"}',
        },
        exit_id: {
            "signal_id": exit_id, "strategy_id": "gold_02", "instrument": "GOLDM",
            "side": "SHORT", "trigger_price": 90.0, "stop_price": 95.0,
            "quantity": 1, "signal_timestamp": 10.0, "signal_metadata":
                '{"exit": true, "pending": true, "trigger_state": "ARMED", '
                '"trigger_generation": 4, "position_side": "LONG"}',
        },
    }

    class _Persistence:
        def get_pending_orders(self, **_kwargs):
            return [armed]

        def get_signal(self, requested_id):
            return signals.get(requested_id)

    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=1)
    strategy.position_side = "LONG"
    env = Environment(name="live", mode="LIVE", is_live=True,
                      strategies={"gold_02": strategy}, persistence=_Persistence())

    assert PersistenceFlowMixin()._restore_live_pending_triggers(env) == 1
    assert strategy.pending_entry.status == "waiting_for_flat"
    assert strategy.pending_exit_trigger is not None
    assert strategy.pending_exit_trigger.signal.signal_id == exit_id
    assert strategy.pending_exit_trigger.trigger_price == 90.0
    assert env.pending_triggers.exit_for("gold_02") is strategy.pending_exit_trigger


def test_live_pending_order_lookup_is_memory_only_after_restore():
    signal = Signal(
        signal_type=SignalType.LONG, instrument="GOLDM", strategy_id="gold_02",
        timestamp=1.0, trigger_price=100.0, stop_price=90.0, quantity=1)
    env = SimpleNamespace(
        persistence=_NoTickDb(), pending_triggers=PendingTriggerRegistry())
    env.pending_triggers.cache_live_row({
        "signal_id": signal.signal_id, "pending_order_id": signal.signal_id,
        "status": "armed", "trigger_price": 100.0,
    })

    row = PersistenceFlowMixin()._live_pending_row(env, signal)

    assert row["status"] == "armed"


def test_signal_metadata_schema_roundtrips_reversal_links(tmp_path):
    persistence = PersistenceManager(
        state_path=str(tmp_path / "state.json"),
        db_path=str(tmp_path / "trades.db"),
    )
    persistence.save_signal({
        "signal_id": "sig-meta", "strategy_id": "gold_02", "instrument": "GOLDM",
        "side": "SHORT", "signal_type": "entry", "timestamp": 1.0,
        "trigger_price": 90.0, "stop_price": 110.0, "quantity": 1,
        "signal_metadata": {"is_reversal_entry": True,
                             "reversal_parent_signal_id": "exit-parent"},
    })
    restored = persistence.get_signal("sig-meta")
    assert restored is not None
    assert '"reversal_parent_signal_id": "exit-parent"' in restored["signal_metadata"]
    persistence.close()

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
        "trigger_source": "market_websocket_ltp", "signal_timestamp": 0.0,
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
                "stop_price": 146719.0, "quantity": 100, "signal_timestamp": 0.0,
                "candle_timestamp": 1700000000.0, "open": 146800.0, "high": 147100.0,
                "low": 146721.0, "close": 146950.0, "htf_value": 146915.5,
                "mid_value": 146873.6, "fast_dema": 146873.6, "fast_atr": 268.2,
                # Forensic signal metadata can retain one-shot candle actions.
                # They must not be replayed on the restored tick-trigger path.
                "signal_metadata": ('{"cancel_inflight": true, '
                                     '"cancel_inflight_consumed": true, '
                                     '"old_pending_id": "prior", '
                                     '"pending_termination": "expired", '
                                     '"trigger_state": "FIRED", "triggered": true}'),
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
    assert pending.signal.timestamp == 1700000000.0
    assert pending.signal.metadata["trigger_state"] == "ARMED"
    assert pending.signal.metadata["pending"] is True
    assert pending.signal.metadata["triggered"] is False
    for transient in ("cancel_inflight", "cancel_inflight_consumed",
                      "old_pending_id", "pending_termination", "cancel_only"):
        assert transient not in pending.signal.metadata
    assert strategy._trigger_generation == 3
    assert env.pending_triggers.entry_for("gold_02") is pending
    assert env.pending_triggers.live_row(signal_id)["status"] == "armed"
    assert strategy.on_tick(147100.0, 11.0) is pending.signal
    assert strategy.is_fired_trigger_signal(signal_id)


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


@pytest.mark.parametrize("durable_status", [
    None, "pending", "entry_sent", "expired", "cancelled_by_reversal",
    "resolved", "rejected", "filled",
])
def test_startup_discards_snapshot_entry_without_armed_durable_row(durable_status):
    signal_id = "snapshot-only"
    signal = Signal(
        signal_type=SignalType.LONG, instrument="GOLDM", strategy_id="gold_02",
        timestamp=1.0, trigger_price=100.0, stop_price=90.0, quantity=1,
        metadata={"pending": True, "trigger_state": "ARMED"})
    signal.signal_id = signal_id
    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=1)
    strategy.pending_entry = PendingEntry(
        signal=signal, trigger_price=100.0, side="LONG", status="pending")

    class _Persistence:
        def get_pending_orders(self, **kwargs):
            return ([{"pending_order_id": signal_id, "signal_id": signal_id,
                      "strategy_id": "gold_02", "status": durable_status}]
                    if durable_status == kwargs.get("status") else [])

        def get_pending_order(self, requested_id, **_kwargs):
            return ({"pending_order_id": requested_id, "signal_id": requested_id,
                     "strategy_id": "gold_02", "status": durable_status}
                    if durable_status else None)

    env = Environment(name="live", mode="LIVE", is_live=True,
                      strategies={"gold_02": strategy}, persistence=_Persistence())

    assert PersistenceFlowMixin()._restore_live_pending_triggers(env) == 0
    assert strategy.pending_entry is None
    assert env.pending_triggers.entry_for("gold_02") is None


def test_missing_reversal_entry_row_clears_its_paired_exit_but_keeps_position():
    entry_id, exit_id = "orphan-reversal-entry", "orphan-reversal-exit"
    entry = Signal(
        signal_type=SignalType.SHORT, instrument="GOLDM", strategy_id="gold_02",
        timestamp=1.0, trigger_price=95.0, stop_price=110.0, quantity=1,
        metadata={"pending": True, "is_reversal_entry": True,
                  "reversal_parent_signal_id": exit_id})
    entry.signal_id = entry_id
    exit_signal = Signal(
        signal_type=SignalType.SHORT, instrument="GOLDM", strategy_id="gold_02",
        timestamp=1.0, trigger_price=98.0, stop_price=99.0, quantity=1,
        metadata={"pending": True, "exit": True})
    exit_signal.signal_id = exit_id
    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=1)
    strategy.position_side = "LONG"
    strategy.pending_entry = PendingEntry(
        signal=entry, trigger_price=95.0, side="SHORT", status="waiting_for_flat")
    strategy.pending_exit_trigger = PendingEntry(
        signal=exit_signal, trigger_price=98.0, side="SHORT", status="pending")

    class _Persistence:
        def get_pending_orders(self, **_kwargs):
            return []

        def get_pending_order(self, _requested_id, **_kwargs):
            return None

    env = Environment(name="live", mode="LIVE", is_live=True,
                      strategies={"gold_02": strategy}, persistence=_Persistence())
    env.pending_triggers.sync_strategy(strategy)

    assert PersistenceFlowMixin()._restore_live_pending_triggers(env) == 0
    assert strategy.pending_entry is None
    assert strategy.pending_exit_trigger is None
    assert strategy.position_side == "LONG"
    assert strategy.state.value == "long_position"
    assert env.pending_triggers.entry_for("gold_02") is None
    assert env.pending_triggers.exit_for("gold_02") is None


def test_startup_db_read_failure_discards_snapshot_only_entry():
    signal = Signal(
        signal_type=SignalType.LONG, instrument="GOLDM", strategy_id="gold_02",
        timestamp=1.0, trigger_price=100.0, stop_price=90.0, quantity=1)
    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=1)
    strategy.pending_entry = PendingEntry(
        signal=signal, trigger_price=100.0, side="LONG", status="pending")

    class _Persistence:
        def get_pending_orders(self, **_kwargs):
            raise OSError("SQLite unavailable")

    env = Environment(name="live", mode="LIVE", is_live=True,
                      strategies={"gold_02": strategy}, persistence=_Persistence())

    assert PersistenceFlowMixin()._restore_live_pending_triggers(env) == 0
    assert strategy.pending_entry is None
    assert env.pending_triggers.entry_for("gold_02") is None


def test_live_arm_write_failure_disarms_ram_and_registry_trigger():
    signal = Signal(
        signal_type=SignalType.LONG, instrument="GOLDM", strategy_id="gold_02",
        timestamp=1.0, trigger_price=100.0, stop_price=90.0, quantity=1,
        metadata={"pending": True, "trigger_state": "ARMED"})
    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=1)
    strategy.pending_entry = PendingEntry(
        signal=signal, trigger_price=100.0, side="LONG", status="pending")

    class _Persistence:
        def get_pending_orders(self, **_kwargs):
            return []

        def save_pending_order(self, _row):
            raise OSError("disk full")

    env = Environment(name="live", mode="LIVE", is_live=True,
                      strategies={"gold_02": strategy}, persistence=_Persistence())
    env.pending_triggers.sync_strategy(strategy)

    with pytest.raises(OSError, match="disk full"):
        PersistenceFlowMixin()._arm_live_pending(signal, env)

    assert strategy.pending_entry is None
    assert env.pending_triggers.entry_for("gold_02") is None
    assert env.pending_triggers.live_row(signal.signal_id) is None


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


def test_live_row_cache_drops_entry_sent_and_terminal_history():
    registry = PendingTriggerRegistry()
    registry.cache_live_row({"signal_id": "armed", "status": "armed"})
    assert registry.live_row("armed") is not None
    registry.update_live_row("armed", {"status": "entry_sent"})
    assert registry.live_row("armed") is None

    registry.cache_live_row({"signal_id": "old", "status": "resolved"})
    registry.cache_live_row({"signal_id": "cancelled", "status": "cancelled_by_reversal"})
    assert registry.live_row("old") is None
    assert registry.live_row("cancelled") is None


def test_restore_queries_only_armed_rows_and_checks_snapshot_by_id(tmp_path):
    persistence = PersistenceManager(
        state_path=str(tmp_path / "state.json"),
        db_path=str(tmp_path / "pending.db"), execution_mode="LIVE")
    common = {
        "strategy_id": "gold_02", "instrument": "GOLDM", "direction": "LONG",
        "side": "LONG", "order_type": "LIMIT", "trigger_price": 101.0,
        "quantity": 1, "trigger_state": "ARMED", "signal_timestamp": 1.0,
    }
    for index in range(25):
        sid = f"old-{index}"
        persistence.save_pending_order({
            **common, "signal_id": sid, "pending_order_id": sid,
            "status": "resolved",
        })
    strategy = create_gold_15m(strategy_id="gold_02", instrument="GOLDM", quantity=1)
    stale = Signal(signal_type=SignalType.LONG, instrument="GOLDM",
                   strategy_id="gold_02", timestamp=1, trigger_price=101.0,
                   stop_price=95.0, quantity=1)
    stale.signal_id = "old-0"
    strategy.pending_entry = PendingEntry(
        signal=stale, trigger_price=101.0, side="LONG", status="pending")
    persistence.save_pending_order({
        **common, "signal_id": "active", "pending_order_id": "active",
        "status": "armed",
    })
    persistence.save_signal({
        "signal_id": "active", "strategy_id": "gold_02", "instrument": "GOLDM",
        "side": "LONG", "signal_type": "entry", "timestamp": 1.0,
        "trigger_price": 101.0, "stop_price": 95.0, "quantity": 1,
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
        "signal_metadata": {},
    })
    env = Environment(name="live", mode="LIVE", is_live=True,
                      strategies={"gold_02": strategy}, persistence=persistence)

    assert PersistenceFlowMixin()._restore_live_pending_triggers(env) == 1
    assert strategy.pending_entry is not None
    assert strategy.pending_entry.signal.signal_id == "active"
    assert env.pending_triggers.live_row("active")["status"] == "armed"
    assert env.pending_triggers.live_row("old-0") is None
    persistence.close()


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

"""Restart/orphan exposure: a broker position we cannot attribute must block risk.

The defect this pins down: reconciliation reported "success" even when the broker
held a position the local book had no record of. Because the status was
``reconciled``, the startup/periodic path added the env to ``_reconciled_envs``
and the §35 entry gate opened — so the system was free to open NEW risk while
carrying an existing position that:

  * no local position row owns, therefore
  * no strategy's ``stop_price`` belongs to, therefore
  * carries NO local stop at all, and
  * can never be exited by the position-owned SL monitor.

The stop is system-side, so an orphan position is invisible in the order book and
completely unprotected. It must be surfaced loudly and must keep entries shut.
It must never be auto-opened (inventing a stop) or auto-flattened (trading on a
guess).
"""
from __future__ import annotations

import json
import sys
import os
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_position_owned_sl import (  # noqa: E402
    FakeBroker,
    FakeExecutionEngine,
    FakeEnv,
    FakeStrategy,
    Harness,
    make_pm,
    make_position,
)
from execution.models import OrderState  # noqa: E402
from execution.live.sl_monitor import SLState  # noqa: E402
from portfolio.position_manager import PositionSide  # noqa: E402


def _live_env_with(broker_positions, positions=(), strategies=None):
    h = Harness()
    broker = FakeBroker(broker_positions)
    env = h.add_env(FakeEnv(
        "LIVE", strategies if strategies is not None
        else {"S1": FakeStrategy("S1", "NIFTY")},
        make_pm(list(positions)), broker))
    return h, env


# ── 1. the orphan is detected and flagged ───────────────────────────────────

def test_broker_position_without_local_book_is_flagged_as_orphan_exposure():
    h, env = _live_env_with(
        [{"instrument": "GOLDM", "quantity": 100, "side": "LONG"}],
        positions=[])
    summary = h.sync_sl_from_broker("LIVE")
    assert summary["status"] == "orphan_exposure"
    assert summary["orphan_exposure"] is True
    assert [o["instrument"] for o in summary["orphans"]] == ["GOLDM"]
    assert summary["orphans"][0]["quantity"] == 100


def test_no_orphan_when_the_broker_agrees_with_the_local_book():
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=90.0)
    h, env = _live_env_with(
        [{"instrument": "NIFTY", "quantity": 10, "side": "LONG"}],
        positions=[pos])
    summary = h.sync_sl_from_broker("LIVE")
    assert summary["orphan_exposure"] is False
    assert summary["orphans"] == []


def test_a_flat_broker_is_not_an_orphan():
    h, env = _live_env_with([], positions=[])
    assert h.sync_sl_from_broker("LIVE")["orphan_exposure"] is False


def test_broker_flat_during_owned_filled_exit_does_not_drop_position_before_fill_routing():
    """The order fill must retain the old position identity for fill_flow.

    Dhan positions can turn flat before the REST/WS order fill is routed.  A
    periodic SL sync must leave the owned EXITING position in place for that
    short interval instead of abandoning it as stale.
    """
    pos = make_position(pid="P-EXIT", instrument="GOLDPETAL", stop=90.0)
    pos.exit_started = True
    pos.exit_order_id = "EXIT-1"
    pos.sl_state = "EXITING"
    order = SimpleNamespace(
        order_role="REVERSAL_EXIT", state=OrderState.FILLED)
    h = Harness()
    env = h.add_env(FakeEnv(
        "LIVE", {"S1": FakeStrategy("S1", "GOLDPETAL")},
        make_pm([pos]), FakeBroker([]),
        FakeExecutionEngine({"EXIT-1": order})))

    summary = h.sync_sl_from_broker("LIVE")

    assert summary["dropped_local"] == []
    assert env.position_manager.get_position("P-EXIT").is_open is True
    deferred = next(d for t, d, _ in h.events
                    if t == "sl_broker_flat_exit_fill_pending")
    assert deferred["exit_order_id"] == "EXIT-1"
    assert deferred["trade_id"] == "T1"


# ── 2. it must be LOUD ─────────────────────────────────────────────────────

def test_orphan_publishes_a_dedicated_event():
    h, env = _live_env_with(
        [{"instrument": "GOLDM", "quantity": 100, "side": "LONG"}],
        positions=[])
    h.sync_sl_from_broker("LIVE")
    types = [e[0] for e in h.events]
    assert "broker_orphan_position" in types, \
        "an unprotectable broker position was surfaced only in a log line"


def test_orphan_event_carries_instrument_and_quantity():
    h, env = _live_env_with(
        [{"instrument": "GOLDM", "quantity": 100, "side": "LONG"}],
        positions=[])
    h.sync_sl_from_broker("LIVE")
    ev = next(d for t, d, _ in h.events if t == "broker_orphan_position")
    assert ev["instrument"] == "GOLDM"
    assert ev["quantity"] == 100
    assert ev["reason"] == "broker_position_without_local_book"


def test_unchanged_orphan_is_alerted_once_not_written_on_every_poll():
    h, env = _live_env_with(
        [{"instrument": "GOLDM", "quantity": 1, "side": "LONG"}],
        positions=[])
    h.sync_sl_from_broker("LIVE")
    h.sync_sl_from_broker("LIVE")
    assert len(h.events_of("broker_orphan_position")) == 1
    env.broker._positions[0]["quantity"] = 2
    h.sync_sl_from_broker("LIVE")
    assert len(h.events_of("broker_orphan_position")) == 2


def test_a_missing_telegram_must_not_break_the_sync():
    """The alarm is best-effort; the gate decision is not."""
    h, env = _live_env_with(
        [{"instrument": "GOLDM", "quantity": 5, "side": "LONG"}],
        positions=[])
    # Harness has no .telegram attribute at all -> AttributeError inside the
    # alert must be swallowed, not abort reconciliation.
    assert not hasattr(h, "telegram")
    summary = h.sync_sl_from_broker("LIVE")
    assert summary["orphan_exposure"] is True


# ── 3. it must NEVER be auto-opened or auto-flattened ─────────────────────

def test_orphan_is_never_armed_with_an_invented_stop():
    h, env = _live_env_with(
        [{"instrument": "GOLDM", "quantity": 100, "side": "LONG"}],
        positions=[])
    h.sync_sl_from_broker("LIVE")
    # No position row was conjured up for it.
    assert env.position_manager.open_positions == []
    assert h.processed == [], "an orphan must never be auto-executed on a guess"


def test_orphan_does_not_disarm_a_genuine_local_position():
    """One orphan must not cost a real position its protection."""
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=90.0)
    h, env = _live_env_with(
        [{"instrument": "NIFTY", "quantity": 10, "side": "LONG"},
         {"instrument": "GOLDM", "quantity": 100, "side": "LONG"}],
        positions=[pos])
    summary = h.sync_sl_from_broker("LIVE")
    assert [a["position_id"] for a in summary["armed"]] == ["P1"]
    assert summary["orphan_exposure"] is True


# ── 4. the entry gate must stay shut ──────────────────────────────────────

def test_orphan_keeps_the_env_out_of_reconciled_envs():
    """This is the actual risk control: no new entries while orphaned.

    Reproduces the engine's decision at startup and on each periodic reconcile.
    """
    h, env = _live_env_with(
        [{"instrument": "GOLDM", "quantity": 100, "side": "LONG"}],
        positions=[])
    summary = h.sync_sl_from_broker("LIVE")
    reconciled = set()
    if summary.get("status") == "reconciled" and not summary.get(
            "orphan_exposure"):
        reconciled.add("LIVE")
    assert "LIVE" not in reconciled, \
        "entry gate opened while holding an unprotectable orphan position"


def test_gate_opens_only_once_the_orphan_is_gone():
    h, env = _live_env_with(
        [{"instrument": "GOLDM", "quantity": 100, "side": "LONG"}],
        positions=[])
    assert h.sync_sl_from_broker("LIVE")["orphan_exposure"] is True
    # The position is resolved at the broker.
    h.envs["LIVE"].broker._positions = []
    assert h.sync_sl_from_broker("LIVE")["orphan_exposure"] is False


def test_failed_broker_query_is_never_reported_as_flat():
    """A failed query must stay UNRESOLVED, not masquerade as 'no orphan'."""

    class BrokenBroker(FakeBroker):
        def positions(self):
            raise ConnectionError("dhan down")

    h = Harness()
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([]), BrokenBroker([])))
    summary = h.sync_sl_from_broker("LIVE")
    assert summary["status"] == "failed"
    assert "orphan_exposure" not in summary, \
        "a failed query must not claim the book is clean"


def test_missing_broker_position_never_closes_the_canonical_database():
    pos = make_position(pid="P-DB-CLOSE", instrument="NIFTY", stop=90.0)
    h, env = _live_env_with([], positions=[pos])

    class Persistence:
        def __init__(self):
            self.closed = []

        def close_position_record(self, position):
            self.closed.append(position.position_id)

    env.persistence = Persistence()
    summary = h.sync_sl_from_broker("LIVE")
    assert summary["dropped_local"] == []
    assert summary["status"] == "protection_incomplete"
    assert env.persistence.closed == []
    assert pos.is_open


def test_unique_filled_entry_recovers_position_closed_by_old_false_flat_reconcile():
    """Recover only when Dhan and the original entry identity agree exactly."""
    position = make_position(
        pid="P-STALE", strategy_id="S1", instrument="SILVERM",
        side=PositionSide.SHORT, qty=1, stop=110.0, trade_id="T-STALE")
    position.margin = 157_087.5
    pm = make_pm([position])
    pm.abandon_stale_position(position.position_id)
    trade = SimpleNamespace(
        status="CLOSED", exit_reason="BROKER_FLAT_RECONCILIATION",
        exit_order_id="", exit_fill_id="", exit_price=0,
        exit_timestamp=0, entry_order_id=position.entry_order_id,
        quantity=1, position_id=position.position_id)

    class Lifecycle:
        def get_trade(self, trade_id):
            return trade if trade_id == "T-STALE" else None

        def reopen_trade_from_broker_position(self, trade_id, recovered):
            if trade_id != "T-STALE" or recovered.position_id != "P-STALE":
                return False
            trade.status = "OPEN"
            return True

    order = SimpleNamespace(
        state=OrderState.FILLED, filled_quantity=1,
        average_fill_price=100.0, side="SELL", quantity=1,
        order_role="ENTRY", trade_id="T-STALE", strategy_id="S1",
        instrument="SILVERM", broker_order_id="BRK-ENTRY-1")
    strategy = FakeStrategy("S1", "SILVERM")
    h = Harness()
    env = h.add_env(FakeEnv(
        "LIVE", {"S1": strategy}, pm,
        FakeBroker([{"instrument": "SILVERM", "quantity": 1,
                     "side": "SHORT", "average_entry_price": 100.0,
                     "ltp": 101.0}]),
        FakeExecutionEngine({position.entry_order_id: order})))
    env.broker.instruments = {"SILVERM": {"security_id": "SEC-1"}}
    env.broker.audit = SimpleNamespace(for_broker_order=lambda order_id: [{
        "request_payload": json.dumps({"securityId": "SEC-1"})
    }] if order_id == "BRK-ENTRY-1" else [])
    env.runtimes = {"S1": SimpleNamespace(lifecycle=Lifecycle())}
    strategy_account = SimpleNamespace(used_margin=0.0)
    global_account = SimpleNamespace(used_margin=0.0)
    env.account_engines = {"S1": strategy_account}
    env.account_engine = global_account

    summary = h.sync_sl_from_broker("LIVE")

    assert summary["recovered_from_stale_close"][0]["trade_id"] == "T-STALE"
    assert summary["recovered_from_stale_close"][0]["quantity"] == 1
    restored = env.position_manager.get_position("P-STALE")
    assert restored.is_open is True
    assert restored.quantity == 1
    assert restored.sl_state == SLState.ARMED.value
    assert strategy.position_side == "SHORT"
    assert strategy.current_position_id == "P-STALE"
    assert strategy_account.used_margin == 157_087.5
    assert global_account.used_margin == 157_087.5
    assert summary["orphan_exposure"] is False


def test_stale_close_recovery_refuses_quantity_mismatch():
    position = make_position(
        pid="P-STALE-QTY", strategy_id="S1", instrument="SILVERM",
        side=PositionSide.SHORT, qty=1, stop=110.0, trade_id="T-STALE-QTY")
    pm = make_pm([position])
    pm.abandon_stale_position(position.position_id)
    trade = SimpleNamespace(
        status="CLOSED", exit_reason="BROKER_FLAT_RECONCILIATION",
        exit_order_id="", exit_fill_id="", exit_price=0,
        exit_timestamp=0, entry_order_id=position.entry_order_id,
        quantity=1, position_id=position.position_id)

    class Lifecycle:
        def get_trade(self, trade_id):
            return trade

        def reopen_trade_from_broker_position(self, trade_id, recovered):
            raise AssertionError("unmatched broker exposure must stay blocked")

    h = Harness()
    env = h.add_env(FakeEnv(
        "LIVE", {"S1": FakeStrategy("S1", "SILVERM")}, pm,
        FakeBroker([{"instrument": "SILVERM", "quantity": 2,
                     "side": "SHORT", "average_entry_price": 100.0}]),
        FakeExecutionEngine({position.entry_order_id: SimpleNamespace(
            state=OrderState.FILLED, filled_quantity=1,
            average_fill_price=100.0, side="SELL", quantity=1,
            order_role="ENTRY", trade_id="T-STALE-QTY", strategy_id="S1",
            instrument="SILVERM")})))
    env.runtimes = {"S1": SimpleNamespace(lifecycle=Lifecycle())}

    summary = h.sync_sl_from_broker("LIVE")

    assert summary.get("recovered_from_stale_close", []) == []
    assert summary["orphan_exposure"] is True
    assert env.position_manager.open_positions == []

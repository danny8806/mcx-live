import threading
from collections import OrderedDict
from types import SimpleNamespace

import pytest

from execution.live.poller import (
    LiveBrokerPoller, _has_active_market_fallback_child,
    _has_market_fallback_child,
    _terminal_entry_matches_pending,
)
from execution.models import OrderState
from strategies.types import StrategyState


def test_default_reconciliation_interval_is_fast_for_broker_flat_release():
    poller = LiveBrokerPoller(SimpleNamespace(name="LIVE"), config={})

    assert poller.intervals == {
        "orders": 0.5,
        "positions": 0.5,
        "account": 0.5,
        "reconcile": 0.5,
    }


def test_position_poll_collapses_dhan_instrument_fanout_to_one_net():
    position = SimpleNamespace(
        position_id="P1", strategy_id="silver_01", instrument="SILVERM",
        quantity=1, is_long=True, is_open=True,
    )
    broker = SimpleNamespace(positions=lambda: [
        {"strategy_id": "silver_01", "instrument": "SILVERM",
         "side": "LONG", "quantity": 1},
        {"strategy_id": "silver_02", "instrument": "SILVERM",
         "side": "LONG", "quantity": 1},
    ])
    env = SimpleNamespace(name="LIVE", broker=broker,
                          position_manager=SimpleNamespace(open_positions=[position]))
    poller = LiveBrokerPoller(env, config={})

    assert poller.poll_positions() == []
    assert poller.snapshot()["position_mismatches"] == []


def test_position_poll_reports_instrument_net_mismatch_once():
    position = SimpleNamespace(
        position_id="P1", strategy_id="silver_01", instrument="SILVERM",
        quantity=1, is_long=True, is_open=True,
    )
    broker = SimpleNamespace(positions=lambda: [
        {"strategy_id": "silver_01", "instrument": "SILVERM",
         "side": "LONG", "quantity": 2},
        {"strategy_id": "silver_02", "instrument": "SILVERM",
         "side": "LONG", "quantity": 2},
    ])
    env = SimpleNamespace(name="LIVE", broker=broker,
                          position_manager=SimpleNamespace(open_positions=[position]))
    poller = LiveBrokerPoller(env, config={})

    report = poller.poll_positions()
    assert len(report) == 1
    assert report[0]["instrument"] == "SILVERM"
    assert report[0]["broker_qty"] == 2
    assert report[0]["memory_qty"] == 1
    assert report[0]["reason"] == "INSTRUMENT_NET_MISMATCH"


def test_poller_scheduler_runs_each_live_task_every_half_second(monkeypatch):
    class FakeClock:
        now = 0.0

        def __call__(self):
            return self.now

        def sleep(self, seconds):
            self.now += seconds

    clock = FakeClock()
    poller = LiveBrokerPoller(SimpleNamespace(name="LIVE"), config={},
                              clock=clock)
    poller._running = True
    seen = []

    def run_task(task):
        seen.append((task, clock()))
        if clock() >= 1.0:
            poller._running = False

    poller._run_task = run_task
    monkeypatch.setattr("execution.live.poller.time.sleep", clock.sleep)
    poller._run()

    for task in ("orders", "positions", "account", "reconcile"):
        times = [at for seen_task, at in seen if seen_task == task]
        assert times == pytest.approx([0.0, 0.5, 1.0])


def test_old_terminal_entry_does_not_own_new_pending_trigger():
    strategy = SimpleNamespace(pending_entry=SimpleNamespace(
        signal=SimpleNamespace(signal_id="new-signal")))
    old_order = SimpleNamespace(entry_signal_id="old-signal",
                                parent_signal_id="old-signal")

    assert not _terminal_entry_matches_pending(strategy, old_order)


def test_matching_terminal_entry_can_reset_its_own_pending_trigger():
    strategy = SimpleNamespace(pending_entry=SimpleNamespace(
        signal=SimpleNamespace(signal_id="same-signal")))
    order = SimpleNamespace(entry_signal_id="same-signal",
                            parent_signal_id="same-signal")

    assert _terminal_entry_matches_pending(strategy, order)


def test_missing_order_lineage_cannot_reset_a_pending_trigger():
    strategy = SimpleNamespace(pending_entry=SimpleNamespace(
        signal=SimpleNamespace(signal_id="current-signal")))
    order = SimpleNamespace(entry_signal_id=None, parent_signal_id=None)

    assert not _terminal_entry_matches_pending(strategy, order)


def test_historical_terminal_entry_cannot_reset_just_fired_trigger_between_tick_and_submit():
    """The tick clears pending_entry before SignalFlow submits the fired one.

    The 500 ms broker poller can run in that interval. A historical rejected
    order must not treat the temporary absence of pending_entry as ownership of
    the new fired signal and clear its fired-id before Dhan submission.
    """
    strategy = SimpleNamespace(
        pending_entry=None,
        _last_fired_trigger_signal_id="new-fired-signal",
        current_trade_id=None,
    )
    old_terminal_order = SimpleNamespace(
        entry_signal_id="old-rejected-signal",
        parent_signal_id="old-rejected-signal",
        trade_id="old-trade",
    )
    current_order = SimpleNamespace(
        entry_signal_id="new-fired-signal",
        parent_signal_id="new-fired-signal",
        trade_id="new-trade",
    )

    assert not _terminal_entry_matches_pending(strategy, old_terminal_order)
    assert _terminal_entry_matches_pending(strategy, current_order)


def test_terminal_entry_ownership_uses_trade_id_after_strategy_restore():
    strategy = SimpleNamespace(
        pending_entry=None,
        _last_fired_trigger_signal_id=None,
        current_trade_id="current-trade",
    )
    old_order = SimpleNamespace(
        entry_signal_id="old-signal", parent_signal_id="old-signal",
        trade_id="old-trade",
    )
    restored_current_order = SimpleNamespace(
        entry_signal_id="current-signal", parent_signal_id="current-signal",
        trade_id="current-trade",
    )

    assert not _terminal_entry_matches_pending(strategy, old_order)
    assert _terminal_entry_matches_pending(strategy, restored_current_order)


@pytest.mark.parametrize("role", ["ENTRY", "REVERSAL_ENTRY", "FALLBACK_MARKET"])
def test_terminal_order_roles_cannot_claim_a_newer_fired_signal(role):
    strategy = SimpleNamespace(
        pending_entry=None,
        _last_fired_trigger_signal_id="current-signal",
        current_trade_id=None,
    )
    old_order = SimpleNamespace(
        order_role=role, entry_signal_id="old-signal",
        parent_signal_id="old-signal", trade_id="old-trade",
    )

    assert not _terminal_entry_matches_pending(strategy, old_order)


def test_terminal_reversal_exit_cleanup_waits_for_engine_state_lock():
    """A reversal cleanup cannot clear fired entry ownership mid-submit."""
    state_lock = threading.RLock()
    fired_ids = OrderedDict([("paired-entry", None)])
    strategy = SimpleNamespace(
        pending_entry=None, pending_exit_trigger=None,
        position_side="LONG", state=StrategyState.ENTRY_TRIGGERED,
        _last_fired_trigger_signal_id="paired-entry",
        _fired_trigger_signal_ids=fired_ids,
        stop_exit_submitted=True,
    )
    order = SimpleNamespace(
        order_id="old-reversal-exit", strategy_id="gold_02",
        state=OrderState.REJECTED, order_role="REVERSAL_EXIT",
        parent_position_id=None, lifecycle_id=None, trade_id="old-trade",
    )
    broker = SimpleNamespace(order_statuses=lambda: {"other-order": {}})
    engine = SimpleNamespace(_orders={order.order_id: order},
                             apply_broker_statuses=lambda _statuses: [])
    env = SimpleNamespace(
        name="LIVE", broker=broker, execution_engine=engine,
        broker_router=None, strategies={"gold_02": strategy},
        position_manager=SimpleNamespace(open_positions=[]), persistence=None,
    )
    poller = LiveBrokerPoller(env, config={}, state_lock=state_lock)
    poller._terminalize_pending_from_broker = lambda _statuses: None
    poller._release_terminal_exits = lambda: 0
    poller._persist_order_state = lambda _order: None
    poller._settle_reversal_terminal = lambda *_args: None
    poller._terminalize_pending_entries = lambda *_args: None
    poller._terminalize_pending_from_broker = lambda *_args: None

    completed = threading.Event()
    with state_lock:
        worker = threading.Thread(
            target=lambda: (poller.poll_orders(), completed.set()), daemon=True)
        worker.start()
        # The poll cycle reaches reversal cleanup but cannot mutate strategy
        # state while the signal pipeline owns the shared engine lock.
        assert not completed.wait(0.05)
        assert strategy._last_fired_trigger_signal_id == "paired-entry"
        assert "paired-entry" in strategy._fired_trigger_signal_ids

    worker.join(timeout=1)
    assert completed.is_set()
    assert strategy._last_fired_trigger_signal_id is None
    assert not strategy._fired_trigger_signal_ids


def test_poller_keeps_shared_lock_from_broker_fill_apply_through_fill_route():
    """No trigger callback may observe FILLED order before its position update."""
    state_lock = threading.RLock()
    fill = SimpleNamespace(fill_id="fill-1", entry_signal_id="signal-1")
    fill_applied = []
    route_was_atomic = []
    competing_callback_entered = threading.Event()
    competing_callback_thread = []

    class ExecutionEngine:
        _orders = {}

        def apply_broker_statuses(self, _statuses):
            return [fill]

    class PositionManager:
        open_positions = []

    class Router:
        def route_fill(self, _fill, handler, entry_signal_id=None):
            def concurrent_tick_mutation():
                with state_lock:
                    competing_callback_entered.set()

            contender = threading.Thread(target=concurrent_tick_mutation,
                                         daemon=True)
            competing_callback_thread.append(contender)
            contender.start()
            # The contender must not enter the state transition while the
            # poller has applied the order result but has not routed its fill.
            route_was_atomic.append(not competing_callback_entered.wait(0.05))
            handler(_fill, "gold_02", False)
            assert entry_signal_id == "signal-1"
            return True

    env = SimpleNamespace(
        name="LIVE", broker=SimpleNamespace(
            order_statuses=lambda: {"broker-order": {}}),
        execution_engine=ExecutionEngine(), broker_router=Router(),
        position_manager=PositionManager(), strategies={}, persistence=None,
        order_watcher=None,
    )
    poller = LiveBrokerPoller(
        env, config={}, state_lock=state_lock,
        handle_fill=lambda routed_fill, strategy_id, is_exit:
            fill_applied.append((routed_fill.fill_id, strategy_id, is_exit)),
    )
    poller._terminalize_pending_from_broker = lambda _statuses: None
    poller._release_terminal_exits = lambda: 0
    poller._upgrade_dbs_order_row = lambda _fill: None
    poller._terminalize_pending_entries = lambda *_args: None

    routed = poller.poll_orders()
    competing_callback_thread[0].join(timeout=1)

    assert route_was_atomic == [True]
    assert competing_callback_entered.is_set()
    assert fill_applied == [("fill-1", "gold_02", False)]
    assert routed == [fill]


def test_position_poll_discards_broker_snapshot_spanning_local_exit():
    """A pre-exit broker position row cannot create a false dashboard mismatch."""
    state_lock = threading.RLock()
    query_started = threading.Event()
    release_query = threading.Event()
    position = SimpleNamespace(
        position_id="position-1", trade_id="trade-1", is_open=True,
        instrument="GOLDM", is_long=True, quantity=1,
        position_generation=1, strategy_id="gold_02",
    )
    position_manager = SimpleNamespace(open_positions=[position])

    def delayed_broker_positions():
        query_started.set()
        assert release_query.wait(timeout=3)
        return [{"instrument": "GOLDM", "side": "LONG", "quantity": 1}]

    env = SimpleNamespace(
        name="LIVE", broker=SimpleNamespace(positions=delayed_broker_positions),
        position_manager=position_manager, execution_engine=None,
        strategies={}, persistence=None,
    )
    poller = LiveBrokerPoller(env, config={}, state_lock=state_lock)
    result = []
    worker = threading.Thread(target=lambda: result.append(poller.poll_positions()),
                              daemon=True)
    try:
        worker.start()
        assert query_started.wait(timeout=2)
        # Model the concurrent, lock-protected close transition.
        with state_lock:
            position.is_open = False
            position_manager.open_positions.clear()
        release_query.set()
        worker.join(timeout=4)
        assert not worker.is_alive()
        assert result == [[]]
        assert poller._last_position_report == []
        assert poller._last_broker_positions == []
        assert poller._stats["position_snapshots_discarded"] == 1
    finally:
        release_query.set()


@pytest.mark.parametrize("child_state", [
    "created", "submitted", "acknowledged", "partially_filled",
])
def test_entry_limit_terminal_reset_waits_while_market_fallback_child_is_active(
        child_state):
    limit = SimpleNamespace(order_id="limit", order_type="LIMIT",
                            order_role="ENTRY", state="cancelled")
    child = SimpleNamespace(
        original_order_id="limit", order_type="MARKET",
        order_role="FALLBACK_MARKET", state=child_state,
    )

    assert _has_active_market_fallback_child(
        {"limit": limit, "market": child}, limit)


def test_rejected_entry_fallback_child_is_terminal_for_strategy_cleanup():
    limit = SimpleNamespace(order_id="limit", order_type="LIMIT",
                            order_role="ENTRY", state="cancelled")
    child = SimpleNamespace(
        original_order_id="limit", order_type="MARKET",
        order_role="FALLBACK_MARKET", state="rejected",
    )

    assert not _has_active_market_fallback_child(
        {"limit": limit, "market": child}, limit)


@pytest.mark.parametrize("child_state", ["submitted", "partially_filled", "rejected"])
def test_canceled_reversal_limit_is_not_terminal_while_fallback_child_exists(child_state):
    parent = SimpleNamespace(order_id="reversal-limit", order_type="LIMIT",
                             order_role="REVERSAL_EXIT", state="canceled")
    child = SimpleNamespace(order_id="reversal-market",
                            original_order_id="reversal-limit",
                            order_type="MARKET", order_role="REVERSAL_EXIT",
                            state=child_state)

    assert _has_market_fallback_child(
        {parent.order_id: parent, child.order_id: child}, parent)

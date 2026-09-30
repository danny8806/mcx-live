from types import SimpleNamespace

import pytest

from execution.live.poller import (
    LiveBrokerPoller, _has_market_fallback_child,
    _terminal_entry_matches_pending,
)


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

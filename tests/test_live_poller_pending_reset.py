from types import SimpleNamespace

import pytest

from execution.live.poller import LiveBrokerPoller, _terminal_entry_matches_pending


def test_default_reconciliation_interval_is_fast_for_broker_flat_release():
    poller = LiveBrokerPoller(SimpleNamespace(name="LIVE"), config={})

    assert poller.intervals == {
        "orders": 0.5,
        "positions": 0.5,
        "account": 0.5,
        "reconcile": 0.5,
    }


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

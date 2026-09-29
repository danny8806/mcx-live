from types import SimpleNamespace

from execution.live.poller import _terminal_entry_matches_pending


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

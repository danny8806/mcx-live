"""A reversal parked in AWAITING_LOCAL_SL must be able to converge.

The defect: ``_update_reversal_entry_fill`` writes status ``AWAITING_LOCAL_SL``
when the reversed-into position has no armed stop at fill time — but that status
was never read anywhere in the codebase. Nothing ever promoted it to COMPLETE,
even after the local stop was later recovered from the broker. The reversal
ledger therefore could not distinguish a genuinely protected reversed-into
position from an unprotected one: both looked permanently "awaiting".

This is a diagnostic/audit-trail fix only. It never places an order, never
touches the monitor, and never invents a stop — it only records the fact that
the stop now exists.
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_position_owned_sl import (  # noqa: E402
    FakeBroker,
    FakeEnv,
    FakeStrategy,
    Harness,
    make_pm,
    make_position,
)


class FakePersistence:
    def __init__(self, reversals=None):
        self.reversals = list(reversals or [])
        self.updates = []

    def get_reversals(self, strategy_id=None, limit=50):
        return [dict(r) for r in self.reversals]

    def update_reversal(self, reversal_id, fields):
        self.updates.append((reversal_id, dict(fields)))
        for r in self.reversals:
            if r.get("reversal_id") == reversal_id:
                r.update(fields)


def _harness_with_reversal(reversal):
    h = Harness()
    h.reversal_alerts = []
    h._notify_reversal_complete = lambda env, record, position=None: \
        h.reversal_alerts.append((dict(record), position))
    broker = FakeBroker([{"instrument": "NIFTY", "quantity": 10, "side": "LONG"}])
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([]), broker))
    env.persistence = FakePersistence([reversal])
    return h, env


def _reversal(status="AWAITING_LOCAL_SL", position_id="P1", rid="REV1"):
    return {"reversal_id": rid, "status": status, "new_position_id": position_id,
            "strategy_id": "S1", "instrument": "NIFTY"}


# ── the gap closes once the stop is genuinely armed ────────────────────────

def test_armed_stop_closes_the_reversal_gap():
    h, env = _harness_with_reversal(_reversal())
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=90.0)
    h._arm_position_sl(env, pos)
    assert env.persistence.updates, "AWAITING_LOCAL_SL reversal was never closed"
    rid, fields = env.persistence.updates[-1]
    assert rid == "REV1"
    assert fields["status"] == "COMPLETE"
    assert fields["new_sl_state"] == "ARMED"
    assert len(h.reversal_alerts) == 1
    assert h.reversal_alerts[0][0]["status"] == "COMPLETE"
    assert h.reversal_alerts[0][1] is pos


def test_completed_reversal_alert_waits_for_the_new_stop():
    h, env = _harness_with_reversal(_reversal())
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=None)
    h._close_reversal_sl_gap(env, pos)
    assert h.reversal_alerts == []

    pos.stop_price = 90.0
    h._arm_position_sl(env, pos)
    assert len(h.reversal_alerts) == 1


def test_closing_the_gap_publishes_an_event():
    h, env = _harness_with_reversal(_reversal())
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=90.0)
    h._arm_position_sl(env, pos)
    assert "reversal_sl_gap_closed" in [e[0] for e in h.events]


def test_gap_closes_via_broker_resync_too():
    """Recovery after a restart is the common way the stop appears late."""
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=90.0)
    h, env = _harness_with_reversal(_reversal())
    h.add_env  # noqa: B018 - keep the mixin attribute reference explicit
    env.strategies = {"S1": FakeStrategy("S1", "NIFTY")}
    env.position_manager = make_pm([pos])
    env.broker = FakeBroker([{"instrument": "NIFTY", "quantity": 10,
                              "side": "LONG"}])
    summary = h.sync_sl_from_broker("LIVE")
    assert summary["status"] == "reconciled"
    assert env.persistence.updates, "resync did not close the reversal gap"
    assert env.persistence.updates[-1][1]["status"] == "COMPLETE"


# ── it must ONLY close the matching record, and only when armed ────────────

def test_a_different_reversals_gap_is_left_alone():
    h, env = _harness_with_reversal(_reversal(position_id="P-OTHER"))
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=90.0)
    h._arm_position_sl(env, pos)
    assert env.persistence.updates == [], \
        "closed a reversal belonging to a different position"


def test_an_already_complete_reversal_is_not_rewritten():
    h, env = _harness_with_reversal(_reversal(status="COMPLETE"))
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=90.0)
    h._arm_position_sl(env, pos)
    assert env.persistence.updates == []


def test_no_gap_is_closed_when_the_stop_is_unavailable():
    """A position with no stop must NOT be stamped as protected."""
    h, env = _harness_with_reversal(_reversal())
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=None)
    h._arm_position_sl(env, pos)
    assert env.persistence.updates == [], \
        "stamped COMPLETE for a position that has no stop at all"


def test_missing_persistence_is_not_fatal():
    h, env = _harness_with_reversal(_reversal())
    env.persistence = None
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=90.0)
    h._arm_position_sl(env, pos)
    assert any(e[0] == "sl_armed" for e in h.events), \
        "arming must still succeed without persistence"


def test_a_failing_lookup_does_not_break_arming():
    class BrokenPersistence(FakePersistence):
        def get_reversals(self, strategy_id=None, limit=50):
            raise RuntimeError("db down")

    h, env = _harness_with_reversal(_reversal())
    env.persistence = BrokenPersistence([_reversal()])
    pos = make_position(pid="P1", strategy_id="S1", instrument="NIFTY", stop=90.0)
    state = h._arm_position_sl(env, pos)
    assert state == "ARMED", "a diagnostics failure must not disarm the position"

from types import SimpleNamespace

from application.live_position_flow import LivePositionFlowMixin
from execution.models import OrderState
from execution.live.order_watcher import OrderWatchRecord, OrderWatcher


class _Flow(LivePositionFlowMixin):
    def __init__(self, env):
        self.env = env

    def _env_for(self, _env_name=None):
        return self.env

    @staticmethod
    def _sl_monitor(_env):
        return SimpleNamespace(active_for=lambda _position_id: True)


def _env(order):
    return SimpleNamespace(
        is_live=True,
        position_manager=SimpleNamespace(get_positions_by_strategy=lambda _sid: []),
        execution_engine=SimpleNamespace(_orders={order.order_id: order}),
    )


def test_filled_enum_exit_does_not_lock_new_entry():
    exit_order = SimpleNamespace(
        order_id="old-exit", strategy_id="s1", order_role="EXIT",
        state=OrderState.FILLED,
    )
    flow = _Flow(_env(exit_order))

    assert flow._entry_priority_blocker("s1", rec=SimpleNamespace(
        order_role="ENTRY", internal_order_id="new-entry", extra={})) is None


def test_expired_reversal_exit_does_not_lock_new_entry():
    exit_order = SimpleNamespace(
        order_id="old-exit", strategy_id="s1", order_role="REVERSAL_EXIT",
        # Some broker/recovery adapters represent EXPIRED as a string because
        # it is terminal at the broker but not a canonical engine OrderState.
        state="expired",
    )
    flow = _Flow(_env(exit_order))

    assert flow._entry_priority_blocker("s1", rec=SimpleNamespace(
        order_role="REVERSAL_ENTRY", internal_order_id="new-entry", extra={})) is None


def test_active_exit_still_blocks_entry_until_exit_finishes():
    exit_order = SimpleNamespace(
        order_id="old-exit", strategy_id="s1", order_role="EXIT",
        state=OrderState.SUBMITTED,
    )
    flow = _Flow(_env(exit_order))

    assert flow._entry_priority_blocker("s1", rec=SimpleNamespace(
        order_role="ENTRY", internal_order_id="new-entry", extra={})) == 2


def test_position_lookup_failure_fails_closed_with_diagnostic():
    def fail(_sid):
        raise RuntimeError("store timeout")

    env = _env(SimpleNamespace(order_id="old", strategy_id="s1"))
    env.position_manager = SimpleNamespace(get_positions_by_strategy=fail)
    flow = _Flow(env)
    rec = SimpleNamespace(order_role="ENTRY", internal_order_id="new", extra={})

    assert flow._entry_priority_blocker("s1", rec=rec) == 0
    assert "position_lookup_failed" in rec.extra["priority_blocker_error"]


def test_missing_stop_monitor_fails_closed_with_diagnostic():
    position = SimpleNamespace(
        is_open=True, sl_state="ARMED", position_id="P1")
    env = _env(SimpleNamespace(order_id="old", strategy_id="s1"))
    env.position_manager = SimpleNamespace(
        get_positions_by_strategy=lambda _sid: [position])
    flow = _Flow(env)
    flow._sl_monitor = lambda _env: None
    rec = SimpleNamespace(order_role="ENTRY", internal_order_id="new", extra={})

    assert flow._entry_priority_blocker("s1", rec=rec) == 0
    assert rec.extra["priority_blocker_error"] == "sl_monitor_unavailable"


def test_reversal_entry_unlocks_to_market_fallback_after_exit_fill():
    reversal_exit = SimpleNamespace(
        order_id="old-reversal-exit", strategy_id="s1",
        order_role="REVERSAL_EXIT", state=OrderState.SUBMITTED,
    )
    engine = SimpleNamespace(_orders={reversal_exit.order_id: reversal_exit})
    watcher = OrderWatcher(engine=engine, config={"live": {"order_watcher": {
        "market_fallback_enabled": True,
        "market_fallback_timeout_ms": 0,
    }}})
    rec = OrderWatchRecord(
        internal_order_id="new-reversal-entry", broker_order_id="B-ENTRY",
        strategy_id="s1", instrument="SILVERM", order_role="REVERSAL_ENTRY",
        order_type="LIMIT", status="SUBMITTED", submitted_at=10.0,
        requested_quantity=1, remaining_quantity=1, rest_verified=True,
    )

    # While Dhan still has the old reversal exit active, the new entry waits.
    assert watcher._decide(rec, "A_PENDING_BUT_VALID", now=10.0) == "LOCK"

    # The watcher reevaluates the same still-pending entry when broker truth
    # marks the old exit terminal; it then becomes fallback-eligible.
    reversal_exit.state = OrderState.FILLED
    assert watcher._decide(rec, "A_PENDING_BUT_VALID", now=10.1) == \
        "MARKET_FALLBACK"

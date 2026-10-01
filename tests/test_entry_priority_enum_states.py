from types import SimpleNamespace

from application.live_position_flow import LivePositionFlowMixin
from execution.models import OrderState


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
        position_manager=None,
        execution_engine=SimpleNamespace(_orders={order.order_id: order}),
    )


def test_filled_enum_exit_does_not_lock_new_entry():
    exit_order = SimpleNamespace(
        order_id="old-exit", strategy_id="s1", order_role="EXIT",
        state=OrderState.FILLED,
    )
    flow = _Flow(_env(exit_order))

    assert flow._entry_priority_blocker("s1", rec=SimpleNamespace(
        order_role="ENTRY", internal_order_id="new-entry")) is None


def test_expired_reversal_exit_does_not_lock_new_entry():
    exit_order = SimpleNamespace(
        order_id="old-exit", strategy_id="s1", order_role="REVERSAL_EXIT",
        # Some broker/recovery adapters represent EXPIRED as a string because
        # it is terminal at the broker but not a canonical engine OrderState.
        state="expired",
    )
    flow = _Flow(_env(exit_order))

    assert flow._entry_priority_blocker("s1", rec=SimpleNamespace(
        order_role="REVERSAL_ENTRY", internal_order_id="new-entry")) is None


def test_active_exit_still_blocks_entry_until_exit_finishes():
    exit_order = SimpleNamespace(
        order_id="old-exit", strategy_id="s1", order_role="EXIT",
        state=OrderState.SUBMITTED,
    )
    flow = _Flow(_env(exit_order))

    assert flow._entry_priority_blocker("s1", rec=SimpleNamespace(
        order_role="ENTRY", internal_order_id="new-entry")) == 2

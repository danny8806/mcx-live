from enum import Enum
from types import SimpleNamespace

from dashboard.routes.overview import _active_order_count


class State(Enum):
    CREATED = "created"
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELED = "canceled"


def test_active_order_count_uses_current_states_not_retained_history():
    engine = SimpleNamespace()
    snapshot = {
        "orders_count": 7,
        "orders": [
            {"state": "filled"},
            {"state": "rejected"},
            {"state": "created"},
            {"state": "submitted"},
            {"state": "acknowledged"},
            {"state": "partially_filled"},
            {"state": "canceled"},
        ],
    }

    assert _active_order_count(engine, snapshot) == 4


def test_active_order_count_supports_enum_states_and_engine_fallback():
    engine = SimpleNamespace(_orders={
        "active": SimpleNamespace(state=State.PARTIALLY_FILLED),
        "done": SimpleNamespace(state=State.FILLED),
    })

    assert _active_order_count(engine, {"orders_count": 20}) == 1


def test_active_order_count_accepts_map_snapshot():
    snapshot = {"orders": {"a": {"state": "created"}, "b": {"state": "expired"}}}

    assert _active_order_count(SimpleNamespace(), snapshot) == 1

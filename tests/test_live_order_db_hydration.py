from execution.live.engine import LiveExecutionEngine


def test_missing_snapshot_order_is_hydrated_from_canonical_db_row():
    engine = LiveExecutionEngine.__new__(LiveExecutionEngine)
    engine._orders = {}
    engine._lock = __import__("threading").RLock()
    engine.broker_router = None

    count = engine.restore_orders_from_persistence([{
        "order_id": "LIVE-REV-ENTRY", "execution_mode": "LIVE",
        "strategy_id": "gold_02", "instrument": "GOLDM", "side": "SELL",
        "quantity": 1, "order_type": "LIMIT", "price": 228000.0,
        "trigger_price": 228000.0, "state": "submitted",
        "filled_quantity": 0, "average_fill_price": 0.0,
        "signal_id": "REV-ENTRY-SIGNAL", "trade_id": "TRADE-NEW",
        "broker_order_id": "DHAN-ORDER-1", "order_role": "REVERSAL_ENTRY",
        "reversal_parent_signal_id": "REV-EXIT-SIGNAL",
        "parent_position_id": "POS-NEW", "position_generation": 3,
        "original_order_id": None, "created_at": 1_800_000_000.0,
        "updated_at": 1_800_000_001.0,
    }])

    order = engine.get_order("LIVE-REV-ENTRY")
    assert count == 1
    assert order is not None
    assert order.state.value == "submitted"
    assert order._broker_order_id == "DHAN-ORDER-1"
    assert order.reversal_parent_signal_id == "REV-EXIT-SIGNAL"


def test_existing_snapshot_order_wins_over_database_copy():
    from execution.models import Order, OrderState

    engine = LiveExecutionEngine.__new__(LiveExecutionEngine)
    engine._orders = {"LIVE-1": Order(
        order_id="LIVE-1", strategy_id="gold_02", instrument="GOLDM",
        side="BUY", quantity=1, state=OrderState.FILLED)}
    engine._lock = __import__("threading").RLock()
    engine.broker_router = None

    count = engine.restore_orders_from_persistence([{
        "order_id": "LIVE-1", "execution_mode": "LIVE",
        "strategy_id": "gold_02", "instrument": "GOLDM", "side": "BUY",
        "quantity": 1, "order_type": "LIMIT", "state": "submitted",
    }])

    assert count == 0
    assert engine.get_order("LIVE-1").state == OrderState.FILLED

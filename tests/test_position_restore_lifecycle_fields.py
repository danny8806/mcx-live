from portfolio.position_manager import Position, PositionManager, PositionSide, PositionStatus


def test_restore_round_trips_open_position_lifecycle_and_broker_links():
    source = PositionManager()
    position = Position(
        position_id="P-OPEN",
        strategy_id="silver_01",
        instrument="SILVERM",
        side=PositionSide.LONG,
        quantity=1,
        average_entry=228940.0,
        entry_timestamp=1790745300.0,
        stop_price=228100.0,
        trade_id="T-OPEN",
        status=PositionStatus.OPEN,
        sl_state="ARMED",
        sl_trigger_price=228075.0,
        sl_protected_at=1790745400.0,
        entry_order_id="DHAN-ENTRY",
        exit_order_id="DHAN-EXIT-PENDING",
        position_generation=3,
        exit_started=True,
        lifecycle_id="L-OPEN",
    )
    source.restore_open_position(position)

    restored = PositionManager()
    restored.restore(source.snapshot())
    actual = restored.open_positions[0]

    assert actual.entry_order_id == "DHAN-ENTRY"
    assert actual.exit_order_id == "DHAN-EXIT-PENDING"
    assert actual.sl_state == "ARMED"
    assert actual.stop_price == 228100.0
    assert actual.sl_trigger_price == 228075.0
    assert actual.sl_protected_at == 1790745400.0
    assert actual.position_generation == 3
    assert actual.exit_started is True
    assert actual.lifecycle_id == "L-OPEN"


def test_restore_round_trips_closed_position_lifecycle_and_broker_links():
    source = PositionManager()
    position = Position(
        position_id="P-CLOSED",
        strategy_id="silver_01",
        instrument="SILVERM",
        side=PositionSide.SHORT,
        quantity=1,
        average_entry=228940.0,
        entry_timestamp=1790745300.0,
        stop_price=229100.0,
        trade_id="T-CLOSED",
        status=PositionStatus.CLOSED,
        sl_state="CLOSED",
        sl_trigger_price=229120.0,
        sl_protected_at=1790745400.0,
        entry_order_id="DHAN-ENTRY-CLOSED",
        exit_order_id="DHAN-EXIT-CLOSED",
        position_generation=4,
        lifecycle_id="L-CLOSED",
    )
    source._closed_positions.append(position)

    restored = PositionManager()
    restored.restore(source.snapshot())
    actual = restored.closed_positions[0]

    assert actual.entry_order_id == "DHAN-ENTRY-CLOSED"
    assert actual.exit_order_id == "DHAN-EXIT-CLOSED"
    assert actual.sl_state == "CLOSED"
    assert actual.sl_trigger_price == 229120.0
    assert actual.sl_protected_at == 1790745400.0
    assert actual.position_generation == 4
    assert actual.lifecycle_id == "L-CLOSED"

from __future__ import annotations

from types import SimpleNamespace

from application.position_reconciliation import PositionReconciliationMixin
from portfolio.position_manager import Position, PositionSide, PositionStatus


class _Harness(PositionReconciliationMixin):
    def __init__(self, env):
        self.env = env
        self.events = []

    def _env_for(self, _name=None):
        return self.env

    def publish_event(self, name, data, env_name=None):
        self.events.append((name, data, env_name))


class _Broker:
    def __init__(self, rows):
        self.rows = rows

    def positions(self):
        return self.rows


def _position(*, instrument="SILVERM", side=PositionSide.LONG, quantity=1):
    return Position(
        position_id="P1", strategy_id="silver_01", instrument=instrument,
        side=side, quantity=quantity, average_entry=100.0,
        entry_timestamp=1.0, status=PositionStatus.OPEN, trade_id="T1",
    )


def _engine(broker_rows, local=()):
    env = SimpleNamespace(
        name="live", mode="LIVE", broker=_Broker(broker_rows),
        position_manager=SimpleNamespace(open_positions=list(local)),
    )
    return _Harness(env), env


def test_identical_dhan_strategy_fanout_rows_match_one_local_net_position():
    rows = [
        {"instrument": "SILVERM", "side": "LONG", "quantity": 1,
         "ltp": 105.0, "strategy_id": "silver_01"},
        {"instrument": "SILVERM", "side": "LONG", "quantity": 1,
         "ltp": 105.0, "strategy_id": "silver_02"},
    ]
    position = _position()
    engine, _ = _engine(rows, [position])

    result = engine.sync_positions_from_broker("live")

    assert result["status"] == "reconciled"
    assert result["matches"][0]["position_id"] == "P1"
    assert position.current_mark == 105.0


def test_broker_only_position_is_reported_once_and_blocks_reconciliation():
    rows = [{"instrument": "GOLDM", "side": "SHORT", "quantity": 2}]
    engine, _ = _engine(rows)

    first = engine.sync_positions_from_broker("live")
    second = engine.sync_positions_from_broker("live")

    assert first["status"] == second["status"] == "mismatch"
    assert first["orphan_exposure"] is True
    assert len([event for event in engine.events
                if event[0] == "broker_orphan_position"]) == 1


def test_local_open_but_broker_flat_is_not_silently_closed_or_reconciled():
    position = _position()
    engine, _ = _engine([], [position])

    result = engine.sync_positions_from_broker("live")

    assert result["status"] == "mismatch"
    assert result["local_mismatch"][0]["reason"] == "local_open_but_broker_flat"
    assert position.is_open


def test_quantity_or_side_mismatch_keeps_environment_unreconciled():
    position = _position(quantity=1)
    engine, _ = _engine([{"instrument": "SILVERM", "side": "SHORT",
                          "quantity": 1}], [position])

    result = engine.sync_positions_from_broker("live")

    assert result["status"] == "mismatch"
    assert result["local_mismatch"][0]["reason"] == "local_broker_position_mismatch"


def test_failed_broker_query_is_never_reported_as_flat():
    class BrokenBroker:
        def positions(self):
            raise ConnectionError("broker unavailable")

    env = SimpleNamespace(name="live", mode="LIVE", broker=BrokenBroker(),
                          position_manager=SimpleNamespace(open_positions=[]))
    engine = _Harness(env)

    result = engine.sync_positions_from_broker("live")

    assert result["status"] == "failed"
    assert result["orphan_exposure"] is False

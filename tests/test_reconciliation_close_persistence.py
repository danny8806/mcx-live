from types import SimpleNamespace

from application.persistence_flow import PersistenceFlowMixin
from core.lifecycle import TradeContext, TradeLifecycleManager, TradeStatus


class Persistence:
    def __init__(self):
        self.failures = 1
        self.closed_ids = []

    def close_position_record(self, position):
        if self.failures:
            self.failures -= 1
            raise OSError("sqlite busy")
        self.closed_ids.append(position.position_id)


class Harness(PersistenceFlowMixin):
    def __init__(self):
        self._reconciled_envs = {"LIVE"}
        self.events = []

    def publish_event(self, event_type, data, env_name=None):
        self.events.append((event_type, data, env_name))


def test_failed_closed_position_write_blocks_and_retries_until_durable():
    persistence = Persistence()
    env = SimpleNamespace(name="LIVE", mode="LIVE", persistence=persistence)
    position = SimpleNamespace(
        position_id="P-1", trade_id="T-1", strategy_id="S-1",
        instrument="SILVERM",
    )
    h = Harness()

    h._queue_position_close_persist(env, position, "broker_exit_fill",
                                    OSError("sqlite busy"))
    assert "LIVE" not in h._reconciled_envs
    assert h._retry_position_close_persistence(env) == ["P-1"]
    assert env.pending_position_close_persist["P-1"] is position

    assert h._retry_position_close_persistence(env) == []
    assert env.pending_position_close_persist == {}
    assert persistence.closed_ids == ["P-1"]
    assert any(event[0] == "position_close_persistence_recovered"
               for event in h.events)


def test_broker_flat_trade_close_is_idempotently_persisted_without_fake_pnl():
    class TradePersistence:
        def __init__(self):
            self.failures = 1
            self.saved = []

        def save_trade(self, trade):
            if self.failures:
                self.failures -= 1
                raise OSError("sqlite busy")
            self.saved.append(dict(trade))

    persistence = TradePersistence()
    lifecycle = TradeLifecycleManager(persistence=persistence, strategy_id="S-1")
    trade = TradeContext(trade_id="T-1", strategy_id="S-1",
                         entry_signal_id="SIG-1", status=TradeStatus.OPEN.value)
    lifecycle._trades[trade.trade_id] = trade

    assert lifecycle.close_trade_from_broker_reconciliation("T-1") is False
    assert lifecycle.close_trade_from_broker_reconciliation("T-1") is True
    assert trade.status == TradeStatus.CLOSED.value
    assert trade.exit_reason == "BROKER_FLAT_RECONCILIATION"
    assert trade.net_pnl is None
    assert persistence.saved[-1]["status"] == TradeStatus.CLOSED.value
    assert persistence.saved[-1]["net_pnl"] is None


def test_only_unfilled_broker_flat_close_can_be_reopened_from_verified_position():
    class TradePersistence:
        def __init__(self):
            self.saved = []

        def save_trade(self, trade):
            self.saved.append(dict(trade))

    persistence = TradePersistence()
    lifecycle = TradeLifecycleManager(persistence=persistence, strategy_id="S-1")
    trade = TradeContext(
        trade_id="T-RECOVER", strategy_id="S-1", instrument="SILVERM",
        entry_side="SHORT", entry_order_id="ENTRY-1", position_id="P-1",
        quantity=1, status=TradeStatus.CLOSED.value,
        exit_reason="BROKER_FLAT_RECONCILIATION")
    lifecycle._trades[trade.trade_id] = trade
    position = SimpleNamespace(
        is_open=True, trade_id="T-RECOVER", position_id="P-1", quantity=1)

    assert lifecycle.reopen_trade_from_broker_position("T-RECOVER", position)
    assert trade.status == TradeStatus.OPEN.value
    assert trade.exit_reason == ""
    assert trade.exit_order_id == trade.exit_fill_id == ""
    assert persistence.saved[-1]["status"] == TradeStatus.OPEN.value

    trade.status = TradeStatus.CLOSED.value
    trade.exit_order_id = "EXIT-REAL"
    assert not lifecycle.reopen_trade_from_broker_position("T-RECOVER", position)
    assert trade.status == TradeStatus.CLOSED.value

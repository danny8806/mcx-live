from types import SimpleNamespace

from application.persistence_flow import PersistenceFlowMixin


def test_order_persistence_includes_assigned_broker_order_id():
    saved = []

    class _Persistence:
        def save_order(self, row):
            saved.append(row)

    class _Engine(PersistenceFlowMixin):
        def _env_for(self, env_name=None):
            return SimpleNamespace(persistence=_Persistence())

    order = SimpleNamespace(
        order_id="LIVE-EXIT", strategy_id="gold_01", instrument="GOLDPETAL",
        side="SELL", quantity=1, order_type="MARKET", price=0.0,
        trigger_price=None, planned_entry_price=14897.0, planned_sl=None,
        planned_order_type="LIMIT", order_role="EMERGENCY_EXIT",
        protected_order_id=None, correlation_id="MCX-exit-1",
        state=SimpleNamespace(value="submitted"), filled_quantity=0,
        average_fill_price=0.0, created_at=1.0, updated_at=2.0,
        signal_id="SIG-EXIT", trade_id="TRADE-1", lifecycle_id="TRADE-1",
        parent_signal_id="SIG-EXIT", position_id="POS-1",
        parent_position_id="POS-1", position_generation=1,
        original_order_id=None, trigger_state="FIRED",
        trigger_generation=None, trigger_source="operator_action",
        submission_outcome="BROKER_RESPONSE_RECEIVED", reason=None,
        _broker_order_id="23826092913404",
        reversal_parent_signal_id="REV-EXIT-1",
    )
    signal = SimpleNamespace(signal_id="SIG-EXIT", trigger_price=14897.0)

    _Engine()._persist_order(order, signal, "live")

    assert saved[0]["broker_order_id"] == "23826092913404"
    assert saved[0]["reversal_parent_signal_id"] == "REV-EXIT-1"
    assert saved[0]["submission_outcome"] == "BROKER_RESPONSE_RECEIVED"
    assert saved[0]["reason"] is None


def test_order_database_keeps_submission_evidence_across_later_status_update(tmp_path):
    from persistence.manager import PersistenceManager

    db_path = tmp_path / "orders.db"
    persistence = PersistenceManager(
        state_path=str(tmp_path / "state.json"), db_path=str(db_path),
        execution_mode="LIVE",
    )
    persistence.save_signal({
        "signal_id": "S-AUDIT", "strategy_id": "silver_01",
        "instrument": "SILVERM", "side": "SHORT", "signal_type": "SHORT",
        "timestamp": 1790845200, "quantity": 1,
    })
    persistence.save_trade({
        "trade_id": "T-AUDIT", "strategy_id": "silver_01",
        "instrument": "SILVERM", "side": "SHORT", "status": "pending",
        "entry_signal_id": "S-AUDIT",
    })
    base = {
        "order_id": "LIVE-AUDIT", "strategy_id": "silver_01",
        "instrument": "SILVERM", "side": "SELL", "quantity": 1,
        "order_type": "LIMIT", "price": 100.0, "state": "rejected",
        "filled_quantity": 0, "average_fill_price": 0.0,
        "created_at": "2026-10-01T09:00:00+00:00",
        "updated_at": "2026-10-01T09:00:00+00:00",
        "trade_id": "T-AUDIT", "signal_id": "S-AUDIT",
        "reason": "Dhan RMS rejection",
        "submission_outcome": "BROKER_RESPONSE_RECEIVED",
    }
    persistence.save_order(base)
    # A later poller update may not know the original handoff evidence. It must
    # update status without erasing the fact that Dhan answered the POST.
    persistence.save_order({**base, "state": "rejected",
                            "updated_at": "2026-10-01T09:00:01+00:00"})
    row = persistence.query_one(
        "SELECT reason, submission_outcome FROM orders WHERE order_id=?",
        ("LIVE-AUDIT",),
    )
    assert row == {
        "reason": "Dhan RMS rejection",
        "submission_outcome": "BROKER_RESPONSE_RECEIVED",
    }

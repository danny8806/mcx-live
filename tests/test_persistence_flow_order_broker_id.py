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
        _broker_order_id="23826092913404",
        reversal_parent_signal_id="REV-EXIT-1",
    )
    signal = SimpleNamespace(signal_id="SIG-EXIT", trigger_price=14897.0)

    _Engine()._persist_order(order, signal, "live")

    assert saved[0]["broker_order_id"] == "23826092913404"
    assert saved[0]["reversal_parent_signal_id"] == "REV-EXIT-1"

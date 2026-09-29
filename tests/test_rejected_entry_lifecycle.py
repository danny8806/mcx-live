from core.lifecycle import TradeContext, TradeLifecycleManager, TradeStatus


def test_definitive_unfilled_rejection_settles_pending_trade():
    lifecycle = TradeLifecycleManager(strategy_id="gold_02")
    trade = TradeContext(
        trade_id="T-REJECT", strategy_id="gold_02", entry_signal_id="S-REJECT",
        status=TradeStatus.PENDING.value,
    )
    lifecycle._trades[trade.trade_id] = trade
    lifecycle._signal_to_trade[trade.entry_signal_id] = trade.trade_id

    assert lifecycle.reject_unfilled_entry(
        trade.trade_id, reason="RMS rejected", order_id="O-REJECT")
    assert trade.status == TradeStatus.REJECTED.value
    assert trade.pending_status == "resolved"
    assert trade.entry_order_id == "O-REJECT"
    assert "RMS rejected" in trade.signal_reason


def test_reconciliation_accepts_zero_prices_for_unfilled_terminal_trade():
    from reconciliation.engine import ReconciliationEngine, ReconciliationResult

    engine = ReconciliationEngine.__new__(ReconciliationEngine)
    result = ReconciliationResult()
    engine._check_price_sanity([], [{
        "trade_id": "T-REJECT", "status": "REJECTED",
        "entry_price": 0.0, "exit_price": 0.0,
    }], result)
    assert result.is_consistent
    assert result.errors == []


def test_reconciliation_still_rejects_zero_entry_price_for_open_trade():
    from reconciliation.engine import ReconciliationEngine, ReconciliationResult

    engine = ReconciliationEngine.__new__(ReconciliationEngine)
    result = ReconciliationResult()
    engine._check_price_sanity([], [{
        "trade_id": "T-OPEN", "status": "OPEN",
        "entry_price": 0.0, "exit_price": 0.0,
    }], result)
    assert not result.is_consistent
    assert any("entry_price" in error for error in result.errors)


def test_poller_not_found_heals_order_trade_pending_row_and_strategy():
    from types import SimpleNamespace
    from execution.live.poller import LiveBrokerPoller

    lifecycle = TradeLifecycleManager(strategy_id="gold_02")
    trade = TradeContext(
        trade_id="T-LOST", strategy_id="gold_02", entry_signal_id="S-LOST",
        status=TradeStatus.PENDING.value,
    )
    lifecycle._trades[trade.trade_id] = trade
    lifecycle._signal_to_trade[trade.entry_signal_id] = trade.trade_id

    class Persistence:
        def __init__(self):
            self.terminalized = []
            self.orders = []

        def terminalize_pending_order(self, signal_id, reason=""):
            self.terminalized.append((signal_id, reason))
            return True

        def save_order(self, row):
            self.orders.append(row)

    order = SimpleNamespace(
        correlation_id="CORR-LOST", order_id="O-LOST", state="submitted",
        filled_quantity=0, strategy_id="gold_02", entry_signal_id="S-LOST",
        trade_id=trade.trade_id, updated_at=1.0, instrument="GOLDM",
        side="BUY", quantity=1, created_at=1.0, average_fill_price=0.0,
    )
    persistence = Persistence()
    reset = []
    broker = SimpleNamespace(order_by_correlation_id=lambda _corr: {"status": "not_found"})
    env = SimpleNamespace(
        name="live", persistence=persistence, broker=broker,
        execution_engine=SimpleNamespace(_orders={"O-LOST": order}),
        runtimes={"gold_02": SimpleNamespace(lifecycle=lifecycle)},
        strategies={"gold_02": SimpleNamespace(pending_entry=None)},
    )
    poller = LiveBrokerPoller.__new__(LiveBrokerPoller)
    poller.env = env
    poller._reset_strategy_fn = reset.append
    poller._stats = {"pending_terminalized": 0, "order_persist_errors": 0}
    poller._errors = {}

    assert poller._self_heal_by_correlation(
        {"signal_id": "S-LOST", "pending_order_id": "P-LOST",
         "correlation_id": "CORR-LOST", "strategy_id": "gold_02"},
        ("rejected", "cancelled", "canceled", "expired"),
    )
    assert str(getattr(order.state, "value", order.state)).lower() == "rejected"
    assert trade.status == TradeStatus.REJECTED.value
    assert persistence.terminalized
    assert persistence.orders[-1]["state"] == "rejected"
    assert reset == ["gold_02"]

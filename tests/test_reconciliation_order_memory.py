from reconciliation.engine import ReconciliationEngine, ReconciliationResult


def _check(db_orders, mem_orders):
    engine = object.__new__(ReconciliationEngine)
    result = ReconciliationResult()
    engine._check_db_vs_memory_orders(db_orders, mem_orders, result)
    return result


def test_missing_terminal_database_orders_are_not_a_restart_warning():
    result = _check([
        {"order_id": "filled", "state": "FILLED"},
        {"order_id": "cancelled", "state": "CANCELED"},
        {"order_id": "rejected", "state": "rejected"},
        {"order_id": "expired", "state": "EXPIRED"},
    ], {})

    assert result.warnings == []
    assert result.errors == []


def test_missing_nonterminal_order_still_warns():
    result = _check([
        {"order_id": "working", "state": "PARTIALLY_FILLED"},
    ], {})

    assert len(result.warnings) == 1
    assert "nonterminal order(s)" in result.warnings[0]
    assert "working" in result.warnings[0]


def test_present_order_state_mismatch_still_errors():
    result = _check(
        [{"order_id": "order-1", "state": "FILLED"}],
        {"order-1": {"state": "submitted"}},
    )

    assert result.errors == ["Order order-1 state mismatch: DB='FILLED', memory='submitted'"]

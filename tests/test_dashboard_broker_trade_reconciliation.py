from types import SimpleNamespace

from dashboard.routes import reconciliation


def test_primary_reconciliation_turns_red_for_broker_only_system_fill(monkeypatch):
    report = {
        "status": "MISMATCH", "mismatch_count": 1,
        "checked_at": 123, "broker_order_rows": 7, "broker_trade_rows": 3,
        "mismatches": [{"type": "BROKER_FILL_MISSING_LOCAL",
                        "broker_order_id": "B-1", "correlation_id": "MCX-1"}],
    }
    env = SimpleNamespace(mode="LIVE", sync_service=SimpleNamespace(
        stats=lambda: {"tradebook_reconciliation": report}))
    monkeypatch.setattr(reconciliation, "_engine", SimpleNamespace(live=env))

    result = reconciliation._run_reconciliation_sync()

    assert result["is_consistent"] is False
    check = next(c for c in result["checks"]
                 if c["name"] == "broker_tradebook_vs_local_fills")
    assert check["is_consistent"] is False
    assert result["summary"]["checks_failed"] == 1
    assert result["errors"][0]["type"] == "BROKER_TRADEBOOK_MISMATCH"


def test_primary_reconciliation_accepts_matched_broker_tradebook(monkeypatch):
    env = SimpleNamespace(mode="LIVE", sync_service=SimpleNamespace(stats=lambda: {
        "tradebook_reconciliation": {"status": "MATCHED", "mismatch_count": 0,
                                     "checked_at": 123, "mismatches": []}
    }))
    monkeypatch.setattr(reconciliation, "_engine", SimpleNamespace(live=env))

    result = reconciliation._run_reconciliation_sync()

    assert result["is_consistent"] is True
    assert result["errors"] == []

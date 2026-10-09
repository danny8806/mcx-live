from dashboard.routes.reversals import _chain_payload


def test_reversal_read_model_reports_only_market_execution_steps():
    payload = _chain_payload({
        "status": "COMPLETE",
        "signal_id": "signal-1",
        "reversal_trigger_price": 100.0,
        "old_exit_order_id": "exit-1",
        "old_exit_broker_status": "FILLED",
        "old_position_id": "position-1",
        "exit_verified_at": "2026-09-29T09:00:00+00:00",
        "new_entry_order_id": "entry-1",
        "new_entry_broker_status": "FILLED",
        "new_entry_fill_price": 99.0,
        "new_entry_filled_quantity": 1,
        "entry_fill_confirmed_at": "2026-09-29T09:00:01+00:00",
        "new_position_id": "position-2",
    })

    assert [step["step"] for step in payload["chain"]] == [
        "REVERSAL SIGNAL",
        "TRIGGER",
        "OLD EXIT",
        "OLD POSITION FLAT",
        "NEW ENTRY",
        "NEW FILL",
        "NEW POSITION",
    ]
    assert payload["complete"] is True


def test_reversal_is_complete_after_broker_confirmed_fill_and_position():
    payload = _chain_payload({
        "status": "COMPLETE",
        "signal_id": "signal-1",
        "reversal_trigger_price": 100.0,
        "old_exit_broker_status": "FILLED",
        "exit_verified_at": "2026-09-29T09:00:00+00:00",
        "new_entry_broker_status": "FILLED",
        "entry_fill_confirmed_at": "2026-09-29T09:00:01+00:00",
        "new_position_id": "position-2",
    })

    assert payload["complete"] is True
    assert all("SL" not in step["step"] for step in payload["chain"])

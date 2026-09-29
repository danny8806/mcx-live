from dashboard.routes.reversals import _chain_payload


def test_reversal_read_model_reports_nine_real_lifecycle_steps():
    payload = _chain_payload({
        "status": "COMPLETE",
        "signal_id": "signal-1",
        "reversal_trigger_price": 100.0,
        "old_exit_order_id": "exit-1",
        "old_exit_broker_status": "FILLED",
        "old_position_id": "position-1",
        "old_sl_state": "CLOSED",
        "exit_verified_at": "2026-09-29T09:00:00+00:00",
        "new_entry_order_id": "entry-1",
        "new_entry_broker_status": "FILLED",
        "new_entry_fill_price": 99.0,
        "new_entry_filled_quantity": 1,
        "entry_fill_confirmed_at": "2026-09-29T09:00:01+00:00",
        "new_position_id": "position-2",
        "new_sl_state": "ARMED",
    })

    assert [step["step"] for step in payload["chain"]] == [
        "REVERSAL SIGNAL",
        "TRIGGER",
        "OLD EXIT",
        "OLD POSITION FLAT",
        "OLD LOCAL SL CLEARED",
        "NEW ENTRY",
        "NEW FILL",
        "NEW POSITION",
        "NEW LOCAL SL ARMED",
    ]
    assert payload["complete"] is True


def test_reversal_is_not_complete_until_the_new_local_stop_is_armed():
    payload = _chain_payload({
        "status": "COMPLETE",
        "signal_id": "signal-1",
        "reversal_trigger_price": 100.0,
        "old_exit_broker_status": "FILLED",
        "exit_verified_at": "2026-09-29T09:00:00+00:00",
        "new_entry_broker_status": "FILLED",
        "entry_fill_confirmed_at": "2026-09-29T09:00:01+00:00",
        "new_sl_state": "UNAVAILABLE",
    })

    assert payload["complete"] is False
    assert payload["chain"][-1]["ok"] is False

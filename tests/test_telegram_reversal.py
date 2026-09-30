from notifications.telegram_router import TelegramRouter


class FakeTelegramClient:
    def __init__(self):
        self.messages = []
        self.callback = None

    def set_delivery_callback(self, callback):
        self.callback = callback

    def send(self, text, silent=False, meta=None):
        self.messages.append((text, silent, meta))
        return True


class FakeAlertLedger:
    def __init__(self):
        self.rows = []
        self.delivery = []

    def record(self, **fields):
        self.rows.append(fields)
        return "alert-reversal-1"

    def mark_telegram(self, event_id, status, error=None):
        self.delivery.append((event_id, status, error))


def test_completed_reversal_sends_distinct_telegram_and_ledger_event():
    client = FakeTelegramClient()
    ledger = FakeAlertLedger()
    router = TelegramRouter(client=client, ledger=ledger)
    router.enable()

    router.on_reversal_complete({
        "reversal_id": "RV-1",
        "signal_id": "SIG-1",
        "strategy_id": "silver_01",
        "instrument": "SILVERM",
        "old_side": "LONG",
        "new_side": "SHORT",
        "quantity": 1,
        "old_exit_fill_price": 228670,
        "new_entry_fill_price": 228595,
        "stop_price": 229305,
        "old_exit_order_id": "EXIT-1",
        "new_broker_order_id": "ENTRY-1",
        "status": "COMPLETE",
    })

    assert len(client.messages) == 1
    message, silent, meta = client.messages[0]
    assert "REVERSAL COMPLETE" in message
    assert "LONG → SHORT" in message
    assert "New Local Stop:</b> 229,305.00 (ARMED)" in message
    assert silent is False
    assert meta["event_type"] == "REVERSAL"
    assert len(ledger.rows) == 1
    assert ledger.rows[0]["event_type"] == "REVERSAL"
    assert ledger.rows[0]["signal_id"] == "SIG-1"
    assert ledger.delivery == [("alert-reversal-1", "SENT", None)]


def test_reversal_alert_formatter_handles_missing_optional_prices():
    client = FakeTelegramClient()
    router = TelegramRouter(client=client, ledger=FakeAlertLedger())
    router.enable()

    router.on_reversal_complete({
        "strategy_id": "silver_01",
        "instrument": "SILVERM",
        "old_side": "LONG",
        "new_side": "SHORT",
        "quantity": 1,
        "status": "COMPLETE",
    })

    assert len(client.messages) == 1
    assert "REVERSAL COMPLETE" in client.messages[0][0]

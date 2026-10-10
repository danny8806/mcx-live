from notifications.telegram_router import TelegramRouter


class FakeTelegramClient:
    def set_delivery_callback(self, _callback):
        pass

    def send(self, text, *, silent=False, meta=None):
        self.text = text
        self.meta = meta
        return True


class FakeLedger:
    def __init__(self):
        self.rows = []
        self.delivery = []

    def record(self, **kwargs):
        self.rows.append(kwargs)
        return "event-1"

    def mark_telegram(self, event_id, status, error=None):
        self.delivery.append((event_id, status, error))


def test_configuration_change_is_audited_and_sent_to_telegram():
    client = FakeTelegramClient()
    ledger = FakeLedger()
    router = TelegramRouter(client, ledger)
    router._enabled = True

    router.on_configuration_change({
        "scope": "strategy",
        "strategy_id": "silver_01",
        "changes": {"quantity": {"from": 1, "to": 2}},
    })

    assert ledger.rows[0]["event_type"] == "CONFIGURATION_CHANGED"
    assert ledger.rows[0]["event_source"] == "DASHBOARD"
    assert "silver_01" in client.text
    assert "1 → 2" in client.text
    assert client.meta == {
        "event_id": "event-1", "event_type": "CONFIGURATION_CHANGED"}
    assert ledger.delivery == [("event-1", "SENT", None)]

from types import SimpleNamespace
import threading

from execution.live.order_watcher import OrderWatchRecord, OrderWatcher
from notifications.telegram_router import TelegramRouter
from trading_engine import TradingEngine


def test_router_maps_actionable_engine_events_and_skips_duplicate_or_noise():
    router = TelegramRouter(client=SimpleNamespace())
    sent = []
    router._emit = lambda *args, **kwargs: sent.append((args, kwargs))

    router.on_engine_event("order_submission_outcome", {
        "strategy_id": "silver_01", "instrument": "SILVERM",
        "side": "SELL", "status": "REJECTED", "order_id": "local-1",
        "details": {"error": "broker rejected"},
    })
    router.on_engine_event("position_opened", {"trade_id": "trade-1"})
    router.on_engine_event("settings_refreshed", {})
    router.on_engine_event("market_tick", {"ltp": 123})

    assert len(sent) == 1
    args, kwargs = sent[0]
    assert args[0] == "ORDER"
    assert "ORDER SUBMISSION OUTCOME" in args[2]
    assert "SILVERM" in args[2]
    assert "broker rejected" in args[2]
    assert kwargs["source"] == "ENGINE"
    assert kwargs["async_fast_path"] is True


def test_trading_engine_publish_event_forwards_to_router_without_affecting_callback():
    callback_events = []
    telegram_events = []
    persistence_events = []
    engine = SimpleNamespace(
        _event_callback=lambda kind, data: callback_events.append((kind, data)),
        _persistence=SimpleNamespace(
            save_event=lambda event: persistence_events.append(event)),
        telegram=SimpleNamespace(
            on_engine_event=lambda kind, data: telegram_events.append((kind, data))),
    )
    payload = {"strategy_id": "gold_02", "instrument": "GOLDM"}

    TradingEngine.publish_event(engine, "order_rejected", payload)

    assert callback_events == [("order_rejected", payload)]
    assert persistence_events[0]["event_type"] == "order_rejected"
    assert telegram_events == [("order_rejected", payload)]


def test_order_watcher_emits_trigger_cross_once_and_outside_state_lock():
    watcher = OrderWatcher.__new__(OrderWatcher)
    watcher._lock = threading.RLock()
    watcher._now = lambda: 1234.5
    watcher._tick_cfg = {"trigger_detection": True}
    record = OrderWatchRecord(
        internal_order_id="local-1", strategy_id="silver_01",
        instrument="SILVERM", side="BUY", trigger_price=100.0,
        requested_quantity=1, status="PENDING",
    )
    watcher._records = {"local-1": record}
    observed = []

    def publish(kind, rec, details):
        assert not watcher._lock._is_owned()
        observed.append((kind, rec.internal_order_id, details))

    watcher._publish = publish
    watcher.feed_market("SILVERM", 100.0)
    watcher.feed_market("SILVERM", 101.0)

    assert len(observed) == 1
    kind, order_id, details = observed[0]
    assert (kind, order_id) == ("trigger_crossed", "local-1")
    assert details["trigger_price"] == 100.0
    assert details["price"] == 100.0
    assert details["quantity"] == 1


def test_broker_sync_websocket_status_alerts_only_on_state_changes():
    from execution.live.broker_sync import BrokerSyncService

    service = BrokerSyncService.__new__(BrokerSyncService)
    service.env = SimpleNamespace(name="live")
    service._last_ws_status_state = None
    statuses = []
    service._on_status = statuses.append

    service._on_ws_status("connected")
    service._on_ws_status("connected")
    service._on_ws_status("error: socket reset")
    service._on_ws_status("error: another detail")
    service._on_ws_status("connected")

    assert statuses == ["connected", "error: socket reset", "connected"]


def test_broker_sync_health_watchdog_emits_stale_and_recovered_transitions():
    from execution.live.broker_sync import BrokerSyncService

    stale = {"value": True}
    service = BrokerSyncService.__new__(BrokerSyncService)
    service._lock = threading.RLock()
    service._stats = {"health_checks": 0}
    service.env = SimpleNamespace(name="live")
    service._poller = SimpleNamespace(running=True)
    service._running = True
    service._ws_enabled = True
    service._ws_feed = SimpleNamespace(
        connected=True, is_stale=lambda: stale["value"])
    service._clock = lambda: 1000.0
    service._ws_stale_active = False
    service._ws_stale_warned_at = 0.0
    service._healthy = True
    statuses = []
    service._on_status = statuses.append

    service._health_tick_once()
    service._health_tick_once()
    assert statuses == ["stale: heartbeat timeout"]
    assert service.healthy is False

    stale["value"] = False
    service._health_tick_once()
    assert statuses == ["stale: heartbeat timeout", "connected"]
    assert service.healthy is True

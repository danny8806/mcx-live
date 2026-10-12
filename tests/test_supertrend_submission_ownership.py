from types import SimpleNamespace

from application.environment_factory import EnvironmentFactoryMixin
from core.timeframe_engine import Bar
from execution.live.dhan_transport import DhanRestTransport
from execution.live.engine import LiveExecutionEngine
from execution.broker_router import BrokerEventRouter
from execution.models import OrderState
from execution.price_model import PricePreset
from strategies.supertrend import SuperTrendStrategyInstance


class _SubmissionGuardHarness(EnvironmentFactoryMixin):
    def _gate_for(self, _strategy_id):
        return SimpleNamespace(
            entries_allowed=True,
            reversal_enabled=True,
            exit_enabled=True,
        )


def _strategy():
    return SuperTrendStrategyInstance(
        strategy_id="silver_01",
        instrument="SILVERM",
        security_id="123",
        quantity=1,
        capital=300_000,
        multiplier=5,
        atr_period=2,
        atr_multiplier=1.0,
        source="close",
        first_tr="high_low",
    )


def _bar(index, close):
    return Bar(
        instrument="SILVERM",
        timeframe="15m",
        start_ts=index * 900,
        end_ts=(index + 1) * 900,
        open=close,
        high=close + 1,
        low=close - 1,
        close=close,
    )


class _RecordingDhanHttp:
    def __init__(self):
        self.calls = []

    def _post(self, path, payload, retry_network=True):
        self.calls.append((path, dict(payload), retry_network))
        return {"orderId": "DHAN-ORDER-1", "orderStatus": "TRANSIT"}


def _submit_entry(strategy, signal):
    http = _RecordingDhanHttp()
    broker = DhanRestTransport(
        client_id="TEST-CLIENT",
        http=http,
        instruments={"SILVERM": {
            "security_id": "TEST-SECURITY",
            "exchange_segment": "MCX_COMM",
        }},
        gate_enabled=True,
    )
    engine = LiveExecutionEngine(broker=broker, price_preset=PricePreset())
    env = SimpleNamespace(
        strategies={strategy.strategy_id: strategy},
        gate_enabled=True,
        safe_mode=None,
        position_manager=SimpleNamespace(
            get_positions_by_strategy=lambda _strategy_id: []),
    )
    guard = _SubmissionGuardHarness()
    engine.submission_guard = lambda order: guard._validate_live_order_ownership(
        env, order)
    order = engine.create_order(signal, multiplier=5, trade_id="trade-1")
    submitted = engine.submit_order(order)
    return submitted, http.calls


def test_completed_candle_entry_carries_generation_through_live_guard():
    strategy = _strategy()
    signal = None
    for index, close in enumerate([100, 98, 96, 94]):
        signal = strategy.on_bar(_bar(index, close))

    assert signal is not None
    assert signal.metadata["trigger_generation"] == strategy._trigger_generation
    order, calls = _submit_entry(strategy, signal)
    assert order.state == OrderState.SUBMITTED
    assert order.trigger_generation == strategy._trigger_generation
    assert len(calls) == 1
    path, payload, retry_network = calls[0]
    assert path == "/orders"
    assert retry_network is False
    assert payload["transactionType"] == "SELL"
    assert payload["securityId"] == "TEST-SECURITY"
    assert payload["quantity"] == 1
    assert payload["orderType"] == "MARKET"
    assert payload["price"] == payload["triggerPrice"] == ""
    assert payload["afterMarketOrder"] is False


def test_broker_flat_reversal_entry_carries_generation_through_live_guard():
    strategy = _strategy()
    reversal = None
    for index, close in enumerate([100, 98, 96]):
        strategy.on_bar(_bar(index, close))

    strategy.position_side = "LONG"
    strategy.current_trade_id = "old-trade"
    strategy.current_position_id = "old-position"
    strategy.position_generation = 1
    reversal = strategy.on_bar(_bar(3, 94))

    assert reversal is not None
    entry = reversal.metadata["reversal_entry_signal"]
    assert entry.metadata["trigger_generation"] == strategy._trigger_generation

    # Model the post-flat handoff: the broker has confirmed the old exposure
    # closed and the paired entry is now the fired signal under the same epoch.
    strategy.position_side = None
    strategy._register_fired_trigger(entry.signal_id)
    order, calls = _submit_entry(strategy, entry)
    assert order.state == OrderState.SUBMITTED
    assert order.trigger_generation == strategy._trigger_generation
    assert len(calls) == 1
    assert calls[0][0] == "/orders"
    assert calls[0][1]["orderType"] == "MARKET"
    assert calls[0][1]["quantity"] == 1


def test_live_router_maps_only_the_broker_native_id_after_submission():
    strategy = _strategy()
    signal = None
    for index, close in enumerate([100, 98, 96, 94]):
        signal = strategy.on_bar(_bar(index, close))
    assert signal is not None

    http = _RecordingDhanHttp()
    broker = DhanRestTransport(
        client_id="TEST-CLIENT", http=http,
        instruments={"SILVERM": {
            "security_id": "TEST-SECURITY", "exchange_segment": "MCX_COMM",
        }},
        gate_enabled=True,
    )
    engine = LiveExecutionEngine(broker=broker, price_preset=PricePreset())
    router = BrokerEventRouter()
    engine.broker_router = router

    order = engine.create_order(signal, multiplier=5, trade_id="trade-map")
    assert router.resolve(order.order_id) is None

    submitted = engine.submit_order(order)
    assert submitted.state == OrderState.SUBMITTED
    assert router.resolve(order.order_id) is None
    mapping = router.resolve("DHAN-ORDER-1")
    assert mapping is not None
    assert mapping.order_id == order.order_id


def test_master_gate_rejection_is_not_sent_and_has_no_broker_mapping():
    from execution.broker_router import BrokerEventRouter
    from execution.live.broker_client import StubLiveBroker
    from strategies.types import Signal, SignalType

    signal = Signal(
        signal_type=SignalType.LONG, instrument="SILVERM",
        strategy_id="silver_01", timestamp=1, trigger_price=100,
        stop_price=0, quantity=1,
        metadata={"strategy_type": "supertrend", "market_on_signal_close": True,
                  "trigger_state": "FIRED"},
    )
    router = BrokerEventRouter()
    engine = LiveExecutionEngine(
        broker=StubLiveBroker(gate_enabled=False), price_preset=PricePreset())
    engine.broker_router = router
    order = engine.create_order(signal, trade_id="trade-gate")
    submitted = engine.submit_order(order)

    assert submitted.state == OrderState.REJECTED
    assert submitted.reason == "LIVE_GATE_CLOSED: live trading master gate is OFF"
    assert submitted.submission_outcome == "NOT_SENT"
    assert submitted.submission_attempt_count == 0
    assert router.resolve(order.order_id) is None

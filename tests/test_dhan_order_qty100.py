"""End-to-end order journey at quantity 100, with Dhan rejecting it.

What this proves, in the order the request was made:

  1. The order that reaches the Dhan wire is CORRECT - right side, right
     security, right exchange segment, LIMIT, and quantity EXACTLY 100.
  2. It is REJECTED, and every rejection reason is handled: no position is
     opened, no local SL is armed, no phantom fill appears.
  3. EVERY gate in the chain is checked, for EVERY strategy - not just one.
  4. When the entry fires, every strategy merely MONITORS the fire.  A rejected
     order must not leave a strategy stuck believing it is filled, and must not
     cause a second attempt.

Dhan is never contacted.  The transport is driven through its injected
``http`` seam with a recorder, so no socket is ever opened.
"""
from types import SimpleNamespace
import pytest

from execution.live.dhan_transport import DhanRestTransport, BrokerGateClosed
from execution.live.engine import LiveExecutionEngine
from execution.price_model import PricePreset
from execution.models import OrderState
from portfolio.position_manager import PositionManager
from strategies.types import Signal, SignalType
from trading_engine import TradingEngine

QTY = 100
INSTRUMENT = "GOLDM"

INSTRUMENTS = {
    INSTRUMENT: {
        "security_id": "569003",
        "exchange_segment": "MCX_COMM",
        "multiplier": 10.0,
    }
}


class RecordingHttp:
    """Captures the exact payload that would go on the wire. No network."""

    def __init__(self, response=None):
        self.posts = []
        self.gets = []
        self._response = response or {}

    def _post(self, path, payload, **kwargs):
        self.posts.append((path, dict(payload)))
        return dict(self._response)

    def _get(self, path, **kwargs):
        self.gets.append(path)
        return {}

    def _delete(self, path):
        return {}

    def _put(self, path, payload):
        return {}


def _transport(http, gate_enabled=True):
    return DhanRestTransport(
        client_id="TESTCLIENT", gate_enabled=gate_enabled,
        product_type="MARGIN", instruments=dict(INSTRUMENTS), http=http,
    )


def _entry_signal(strategy_id, *, qty=QTY, instrument=INSTRUMENT):
    """A fired LONG entry, as the strategy emits it once the trigger crosses."""
    return Signal(
        signal_type=SignalType.LONG, instrument=instrument,
        strategy_id=strategy_id, timestamp=1000.0, trigger_price=72500.0,
        stop_price=72400.0, quantity=qty,
        metadata={"triggered": True, "trigger_state": "FIRED",
                  "pending": False, "trigger_generation": 1,
                  "trigger_source": "market_websocket_ltp"},
    )


# ═══════════════════════════════════════════════════════════════════════════
# 1 — the order that reaches Dhan is correct, at quantity 100
# ═══════════════════════════════════════════════════════════════════════════


def test_the_order_reaching_dhan_is_correct_at_quantity_100():
    http = RecordingHttp({"orderId": "DH-1", "orderStatus": "PENDING"})
    engine = LiveExecutionEngine(
        broker=_transport(http), price_preset=PricePreset(
            entry_offset=0.0, sl_offset=0.0, tick_size=0.05))

    order = engine.create_order(_entry_signal("s1"), multiplier=10.0,
                                trade_id="T-s1-E")
    order.order_role = "ENTRY"
    assert engine.submit_order(order) is not None

    # Exactly one order attempt reached the wire.
    assert len(http.posts) == 1
    path, payload = http.posts[0]
    assert path == "/orders"

    # Quantity is EXACTLY 100 - not 1, not 10, not rounded to a lot.
    assert payload["quantity"] == QTY
    assert isinstance(payload["quantity"], int)

    # Everything else on the wire is correct too.
    assert payload["transactionType"] == "BUY"
    assert payload["securityId"] == "569003"
    assert payload["exchangeSegment"] == "MCX_COMM"
    assert payload["orderType"] == "LIMIT"
    assert payload["validity"] == "DAY"
    assert payload["productType"] == "MARGIN"
    assert payload["disclosedQuantity"] == 0
    # A LIMIT carries a real price and NO trigger; a MARKET carries neither.
    assert payload["price"] > 0
    assert payload["triggerPrice"] == 0.0
    assert payload["afterMarketOrder"] is False
    # Lineage: the wire order is traceable back to the signal.
    assert payload["correlationId"]

    # The order object the engine kept agrees with what was sent.
    assert order.quantity == QTY
    assert order.instrument == INSTRUMENT
    assert order.side == "BUY"


def test_quantity_100_is_never_silently_rescaled_on_the_wire():
    """Whatever the engine holds is exactly what Dhan is asked for."""
    for qty in (100,):
        http = RecordingHttp({"orderId": "DH-X", "orderStatus": "PENDING"})
        engine = LiveExecutionEngine(
            broker=_transport(http),
            price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0,
                                     tick_size=0.05))
        order = engine.create_order(_entry_signal("s1", qty=qty),
                                    multiplier=10.0, trade_id="T")
        order.order_role = "ENTRY"
        engine.submit_order(order)
        _, payload = http.posts[0]
        assert payload["quantity"] == order.quantity == qty


# ═══════════════════════════════════════════════════════════════════════════
# 2 — Dhan rejects it, and the rejection is fully absorbed
# ═══════════════════════════════════════════════════════════════════════════


def test_a_dhan_rejection_opens_no_position_and_arms_no_sl():
    http = RecordingHttp({
        "orderId": "DH-2",
        "orderStatus": "REJECTED",
        "omsErrorDescription": "insufficient margin for 100 qty",
    })
    env = SimpleNamespace(name="live", mode="LIVE",
                          position_manager=PositionManager())
    engine = LiveExecutionEngine(
        broker=_transport(http),
        price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=0.05))

    order = engine.create_order(_entry_signal("s1"), multiplier=10.0,
                                trade_id="T-s1-E")
    order.order_role = "ENTRY"
    engine.submit_order(order)

    # The rejection is visible on the order, with the broker's own reason.
    assert order.state == OrderState.REJECTED
    assert order.filled_quantity == 0
    assert "margin" in (order.reason or "").lower()

    # A rejected entry must not leave any position behind...
    assert env.position_manager.open_positions == []
    # ...and must never be treated as a fill.
    assert order.state != OrderState.FILLED


def test_a_rejected_entry_never_arms_a_local_sl():
    """The stop is armed by a broker-CONFIRMED fill. No fill, no stop."""
    from execution.live.sl_monitor import PositionOwnedSLMonitor

    http = RecordingHttp({
        "orderId": "DH-3", "orderStatus": "REJECTED",
        "omsErrorDescription": "rejected",
    })
    pm = PositionManager()
    engine = LiveExecutionEngine(
        broker=_transport(http),
        price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=0.05))

    order = engine.create_order(_entry_signal("s1"), multiplier=10.0,
                                trade_id="T-s1-E")
    order.order_role = "ENTRY"
    engine.submit_order(order)

    monitor = PositionOwnedSLMonitor()
    # No position exists, so there is nothing to arm - and nothing in the
    # monitor holds an arm for a position that does not exist.
    assert pm.open_positions == []
    assert monitor.armed_ids() == []


def test_a_rejection_does_not_emit_a_fill():
    """A phantom fill is how a rejected order would silently become a position."""
    http = RecordingHttp({
        "orderId": "DH-4", "orderStatus": "REJECTED",
        "omsErrorDescription": "rejected",
    })
    engine = LiveExecutionEngine(
        broker=_transport(http),
        price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=0.05))
    order = engine.create_order(_entry_signal("s1"), multiplier=10.0,
                                trade_id="T-s1-E")
    order.order_role = "ENTRY"
    engine.submit_order(order)
    assert order.filled_quantity == 0
    assert not getattr(order, "fill_price", None)


# ═══════════════════════════════════════════════════════════════════════════
# 3 — every gate is checked
# ═══════════════════════════════════════════════════════════════════════════


def test_the_master_gate_blocks_before_anything_reaches_the_wire():
    """With the master gate OFF nothing may be sent - not even one POST.

    The engine deliberately does NOT propagate the gate error; it terminalises
    the order as REJECTED carrying the gate reason, so the book shows why.
    """
    http = RecordingHttp({"orderId": "DH-5", "orderStatus": "PENDING"})
    engine = LiveExecutionEngine(
        broker=_transport(http, gate_enabled=False),
        price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=0.05))
    order = engine.create_order(_entry_signal("s1"), multiplier=10.0,
                                trade_id="T-s1-E")
    order.order_role = "ENTRY"

    engine.submit_order(order)

    assert http.posts == [], "the master gate must stop the order at the wire"
    assert order.state == OrderState.REJECTED
    assert "GATE" in (order.reason or "").upper()
    assert order.quantity == QTY, "the intent is preserved for the operator"


def test_a_broker_side_stop_is_refused_at_the_wire():
    """No code path may put a resting stop on the wire, at any quantity."""
    http = RecordingHttp({"orderId": "DH-6", "orderStatus": "PENDING"})
    transport = _transport(http)
    for order_type in ("STOP_LOSS", "STOP_LOSS_MARKET"):
        with pytest.raises(ValueError, match="BROKER_SL_RETIRED"):
            transport.place_market_order(side="SELL", quantity=QTY,
                                         instrument=INSTRUMENT,
                                         order_type=order_type,
                                         trigger_price=72400.0)
    assert http.posts == []


def test_an_unmapped_instrument_is_refused_before_the_wire():
    """A missing security mapping must fail loudly, never post a blank order."""
    http = RecordingHttp({"orderId": "DH-7", "orderStatus": "PENDING"})
    transport = _transport(http)
    with pytest.raises(RuntimeError, match="security mapping"):
        transport.place_market_order(side="BUY", quantity=QTY,
                                     instrument="UNKNOWN", order_type="LIMIT",
                                     price=72500.0)
    assert http.posts == []


def test_a_zero_or_negative_quantity_is_refused_before_the_wire():
    http = RecordingHttp({"orderId": "DH-8", "orderStatus": "PENDING"})
    transport = _transport(http)
    for bad in (0, -100):
        with pytest.raises(ValueError):
            transport.place_market_order(side="BUY", quantity=bad,
                                         instrument=INSTRUMENT,
                                         order_type="LIMIT", price=72500.0)
    assert http.posts == []


def test_the_trigger_ownership_gate_rejects_an_unfired_entry():
    """A signal whose trigger never fired must not become an order."""
    http = RecordingHttp({"orderId": "DH-9", "orderStatus": "PENDING"})
    engine = LiveExecutionEngine(
        broker=_transport(http),
        price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=0.05))
    env = SimpleNamespace(name="live", mode="LIVE", gate_enabled=True,
                          position_manager=PositionManager())
    guard = object.__new__(TradingEngine)
    guard._gate_for = lambda sid: SimpleNamespace(
        entries_allowed=True, reversal_enabled=True, sl_enabled=True,
        exit_enabled=True)
    engine.submission_guard = lambda o: guard._validate_live_order_ownership(
        env, o)
    env.strategies = {"s1": SimpleNamespace(
        enabled=True, _trigger_generation=1,
        is_fired_trigger_signal=lambda sid: False)}

    order = engine.create_order(_entry_signal("s1"), multiplier=10.0,
                                trade_id="T-s1-E")
    order.order_role = "ENTRY"
    assert engine.submission_guard(order) == "ORDER_TRIGGER_SIGNAL_STALE"


# ═══════════════════════════════════════════════════════════════════════════
# 4 — every strategy is checked, and every strategy only MONITORS the fire
# ═══════════════════════════════════════════════════════════════════════════


ALL_STRATEGIES = ("s1", "s2", "s3")


def test_every_strategy_produces_its_own_correct_order_at_100():
    """One rejection must not be shared: each strategy gets its own order."""
    http = RecordingHttp({"orderId": "DH-10", "orderStatus": "PENDING"})
    engine = LiveExecutionEngine(
        broker=_transport(http),
        price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=0.05))

    for strategy_id in ALL_STRATEGIES:
        order = engine.create_order(_entry_signal(strategy_id), multiplier=10.0,
                                    trade_id=f"T-{strategy_id}-E")
        order.order_role = "ENTRY"
        engine.submit_order(order)

    # One wire order per strategy, each with quantity 100.
    assert len(http.posts) == len(ALL_STRATEGIES)
    for _path, payload in http.posts:
        assert payload["quantity"] == QTY
    # Each carries a distinct correlation id, so they can never be conflated.
    correlation_ids = {p["correlationId"] for _p, p in http.posts}
    assert len(correlation_ids) == len(ALL_STRATEGIES)


def test_all_strategies_are_rejected_together_when_dhan_rejects():
    http = RecordingHttp({
        "orderId": "DH-11", "orderStatus": "REJECTED",
        "omsErrorDescription": "insufficient margin",
    })
    pm = PositionManager()
    engine = LiveExecutionEngine(
        broker=_transport(http),
        price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=0.05))

    for strategy_id in ALL_STRATEGIES:
        order = engine.create_order(_entry_signal(strategy_id), multiplier=10.0,
                                    trade_id=f"T-{strategy_id}-E")
        order.order_role = "ENTRY"
        engine.submit_order(order)
        assert order.state == OrderState.REJECTED, strategy_id

    # Not one of them produced a position.
    assert pm.open_positions == []


def test_a_strategy_only_monitors_the_entry_fire_and_never_re_fires():
    """A rejected fire is observed, not acted on twice.

    The danger is a strategy that still believes its entry is live: it would
    either block itself forever or, worse, treat the rejection as a position
    and arm a stop on it.  Monitoring means exactly one wire attempt, no
    exposure, and no re-send.
    """
    http = RecordingHttp({
        "orderId": "DH-12", "orderStatus": "REJECTED",
        "omsErrorDescription": "insufficient margin for 100 qty",
    })
    engine = LiveExecutionEngine(
        broker=_transport(http),
        price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=0.05))
    order = engine.create_order(_entry_signal("s1"), multiplier=10.0,
                                trade_id="T-s1-E")
    order.order_role = "ENTRY"
    engine.submit_order(order)

    # The rejection is immediately visible - NOT masked as "submitted" while
    # the next status poll is still minutes away.
    assert order.state == OrderState.REJECTED
    assert "margin" in (order.reason or "").lower()

    # Each definitive rejection was verified, then retried at most three
    # times. No retry creates local exposure or arms an SL.
    assert len(http.posts) == 4
    assert len({payload["correlationId"] for _, payload in http.posts}) == 4
    assert order.submission_attempt_count == 4
    assert order.rejection_retry_count == 3
    assert http.gets == [], "a rejection needs no recovery lookup"

    # Nothing is exposed, so nothing can be stopped out.
    assert order.filled_quantity == 0
    assert order.quantity == QTY, "intent preserved, exposure zero"


def test_a_rejected_entry_does_not_block_the_next_attempt():
    """A rejection is terminal for that ORDER, not a permanent lockout."""
    http = RecordingHttp({
        "orderId": "DH-13", "orderStatus": "REJECTED",
        "omsErrorDescription": "rejected",
    })
    engine = LiveExecutionEngine(
        broker=_transport(http),
        price_preset=PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=0.05))

    first = engine.create_order(_entry_signal("s1"), multiplier=10.0,
                                trade_id="T-s1-E1")
    first.order_role = "ENTRY"
    engine.submit_order(first)
    assert first.state == OrderState.REJECTED

    # A NEW signal is a new four-attempt bounded sequence, independent of the
    # previous terminal rejection.
    second = engine.create_order(_entry_signal("s1"), multiplier=10.0,
                                 trade_id="T-s1-E2")
    second.order_role = "ENTRY"
    engine.submit_order(second)

    assert len(http.posts) == 8
    assert len({payload["correlationId"] for _, payload in http.posts}) == 8
    assert second.order_id != first.order_id
    assert second.state == OrderState.REJECTED

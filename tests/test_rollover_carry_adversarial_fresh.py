"""Independent adversarial checks for the live rollover carry boundary.

These tests build fresh engine/service objects and exercise the committed
production methods directly; they do not reuse the older rollover test
fixtures.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from core.rollover import RolloverService
from indicators.supertrend import SuperTrend
from strategies.types import OrderState
from trading_engine import TradingEngine


def _engine_and_env(state_file: Path, *, phase="CARRY_ENTRY_READY",
                    broker_flat=True, orders_clear=True, positions=None,
                    order_rows=None, flat_barrier=None):
    order_rows = {} if order_rows is None else order_rows
    service = RolloverService(
        config={"enabled": True}, state_path=str(state_file))
    service.update_workflow(
        "SILVERM", phase=phase, roll_id="fresh-roll-1",
        carry_positions=[{
            "quantity": 2, "side": "LONG", "old_security_id": "old-1",
            "entry_signal_id": "carry-signal-1",
            "entry_expected_side": "LONG",
            "entry_expected_quantity": 2,
            "entry_strategy_id": "silver_01",
            "entry_security_id": "new-1",
            "entry_submitted_at": datetime.now(timezone.utc).timestamp(),
        }])

    class Broker:
        def instrument_flat(self, _instrument, _security_id):
            if flat_barrier is not None:
                flat_barrier.wait(timeout=3)
            return broker_flat

        def instrument_orders_clear(self, _instrument, _security_id):
            return orders_clear

    indicator = SuperTrend(1, 3.7, "low", "sma_tr", first_tr="high_low")
    indicator.update(100, 102, 99, 101, 900)
    strategy = SimpleNamespace(
        strategy_id="silver_01", strategy_type="supertrend",
        instrument="SILVERM", supertrend=indicator,
    )
    engine = TradingEngine.__new__(TradingEngine)
    engine._rollover_service = service
    engine.config = {"instruments": {"SILVERM": {
        "security_id": "new-1", "lot_size": 1,
    }}}
    engine._rollover_blocked = {("live", "SILVERM")}
    engine._lock = threading.RLock()
    engine.market_data_health = None
    engine.publish_event = lambda *_args, **_kwargs: None
    engine._gate_for = lambda _sid: SimpleNamespace(entries_allowed=True)

    calls = []

    def process(signal, _env_name):
        calls.append(signal)
        order_rows.setdefault(f"order-{len(order_rows) + 1}", SimpleNamespace(
            entry_signal_id=signal.signal_id, state=OrderState.SUBMITTED,
            side="BUY", quantity=2, strategy_id="silver_01", reason=None))

    engine._process_signal = process
    env = SimpleNamespace(
        name="live", mode="LIVE", is_live=True, gate_enabled=True,
        data_adapter=SimpleNamespace(get_live_ltp=lambda _metal: 101.5),
        broker=Broker(),
        strategies={"silver_01": strategy},
        position_manager=SimpleNamespace(open_positions=list(positions or [])),
        execution_engine=SimpleNamespace(_orders=order_rows),
        market_status=SimpleNamespace(is_trading_allowed=True),
        safe_mode=SimpleNamespace(is_active=False),
    )
    return engine, service, env, calls


def test_exact_position_does_not_complete_while_dhan_order_is_still_working(tmp_path):
    exact_position = SimpleNamespace(
        is_open=True, instrument="SILVERM", is_long=True, quantity=2,
        strategy_id="silver_01", entry_signal_id="carry-signal-1",
        position_id="p-1", trade_id="t-1",
    )
    open_order = SimpleNamespace(
        entry_signal_id="carry-signal-1", state=OrderState.SUBMITTED,
        side="BUY", quantity=2, strategy_id="silver_01", reason=None,
    )
    engine, service, env, _ = _engine_and_env(
        tmp_path / "working-order.json", phase="ENTRY_SUBMITTED",
        broker_flat=False, orders_clear=False,
        positions=[exact_position], order_rows={"o-1": open_order})

    engine._try_rollover_carry_entries(env)

    assert service.workflow("SILVERM")["phase"] == "ENTRY_SUBMITTED"
    assert ("live", "SILVERM") in engine._rollover_blocked


def test_simultaneous_ready_callbacks_claim_only_one_carry_submission(tmp_path):
    both_at_broker_check = threading.Barrier(2)
    engine, service, env, submitted = _engine_and_env(
        tmp_path / "concurrent-ready.json", flat_barrier=both_at_broker_check)
    failures = []

    def run_check():
        try:
            engine._try_rollover_carry_entries(env)
        except BaseException as exc:  # surface worker errors to the test thread
            failures.append(exc)

    workers = [threading.Thread(target=run_check, daemon=True) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=5)

    assert all(not worker.is_alive() for worker in workers), "carry checks deadlocked"
    assert failures == []
    assert len(submitted) == 1
    assert service.workflow("SILVERM")["phase"] == "ENTRY_SUBMITTED"


def test_broker_lookup_failure_preserves_ready_workflow_and_blocks_entries(tmp_path):
    engine, service, env, submitted = _engine_and_env(
        tmp_path / "lookup-error.json")

    def unavailable(*_args):
        raise TimeoutError("fresh simulated broker outage")

    env.broker.instrument_flat = unavailable
    engine._try_rollover_carry_entries(env)

    assert submitted == []
    assert service.workflow("SILVERM")["phase"] == "CARRY_ENTRY_READY"
    assert ("live", "SILVERM") in engine._rollover_blocked

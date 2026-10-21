"""Fresh fault-injection checks for the reversal exit -> entry handoff.

These tests exercise current production methods with controlled broker and
reconciliation responses. They intentionally do not submit real Dhan orders.
"""
from __future__ import annotations

import threading
from types import SimpleNamespace

from application.fill_flow import FillFlowMixin
from application.live_position_flow import LivePositionFlowMixin
from strategies.types import Signal, SignalType
from trading_engine import TradingEngine


def _reversal_pair():
    signal = Signal(
        signal_type=SignalType.SHORT,
        instrument="SILVERM",
        strategy_id="silver_01",
        timestamp=1792602000.0,
        trigger_price=101.0,
        stop_price=0.0,
        quantity=1,
        side="SHORT",
        metadata={
            "is_reversal": True,
            "is_reversal_entry": True,
            "trigger_state": "ARMED",
        },
    )
    pending = SimpleNamespace(signal=signal, status="waiting_for_flat")
    fired = []
    strategy = SimpleNamespace(
        pending_entry=pending,
        state=None,
        _register_fired_trigger=fired.append,
    )
    return signal, pending, strategy, fired


class _HandoffEngine(FillFlowMixin, LivePositionFlowMixin):
    def __init__(self, strategy, *, broker_responses, lock=None):
        self._lock = lock or threading.RLock()
        self._reconciled_envs = {"live"}
        self._broker_responses = iter(broker_responses)
        self._broker_calls = 0
        self.processed = []
        self.events = []
        self.env = SimpleNamespace(
            name="live",
            mode="LIVE",
            is_live=True,
            strategies={"silver_01": strategy},
            position_manager=SimpleNamespace(
                get_positions_by_strategy=lambda _sid: []),
            execution_engine=SimpleNamespace(
                _current_prices={"SILVERM": 102.0}),
            broker=SimpleNamespace(positions=self._positions),
            pending_triggers=None,
        )

    def _env_for(self, _name=None):
        return self.env

    def _positions(self):
        self._broker_calls += 1
        response = next(self._broker_responses)
        if isinstance(response, Exception):
            raise response
        return response

    def _process_signal(self, signal, env_name):
        self.processed.append((signal.signal_id, env_name))

    def publish_event(self, name, payload, env_name=None):
        self.events.append((name, payload, env_name))


def test_periodic_reconcile_retries_paired_entry_after_stale_broker_position():
    signal, pending, strategy, fired = _reversal_pair()
    engine = _HandoffEngine(strategy, broker_responses=[
        # First check after exit fill: Dhan's position endpoint is still stale.
        [{"instrument": "SILVERM", "side": "BUY", "quantity": 1}],
        # Next periodic reconciliation: broker is flat.
        [],
    ])
    trading = TradingEngine.__new__(TradingEngine)
    trading._envs = {"live": engine.env}
    trading._lock = threading.RLock()
    trading.config = {"live": {}}
    trading._reconciled_envs = {"live"}
    trading._env_for = lambda name=None: engine.env
    trading.sync_positions_from_broker = lambda **_kw: {"status": "reconciled"}
    trading._activate_reversal_entry_after_flat = engine._activate_reversal_entry_after_flat
    trading.publish_event = lambda *_args, **_kwargs: None

    # First failed flat proof retains the exact paired signal, armed.
    assert engine._activate_reversal_entry_after_flat(engine.env, "silver_01") is False
    assert strategy.pending_entry is pending
    assert pending.status == "waiting_for_flat"
    assert engine.processed == []

    # The production periodic reconciliation path must retry it once later.
    trading._reconcile_strategy_positions("live")
    assert engine._broker_calls == 2
    assert engine.processed == [(signal.signal_id, "live")]
    assert strategy.pending_entry is None
    assert pending.status == "fired"
    assert fired == [signal.signal_id]

    # Further reconciliations cannot submit the same paired entry again.
    trading._reconcile_strategy_positions("live")
    assert engine.processed == [(signal.signal_id, "live")]


def test_broker_position_read_error_keeps_reversal_armed_for_later_retry():
    _signal, pending, strategy, _fired = _reversal_pair()
    engine = _HandoffEngine(strategy, broker_responses=[
        RuntimeError("temporary positions endpoint timeout"), [],
    ])

    assert engine._activate_reversal_entry_after_flat(engine.env, "silver_01") is False
    assert strategy.pending_entry is pending
    assert pending.status == "waiting_for_flat"
    assert engine.processed == []

    assert engine._activate_reversal_entry_after_flat(engine.env, "silver_01") is True
    assert strategy.pending_entry is None
    assert len(engine.processed) == 1


def test_racing_flat_retries_submit_paired_signal_once():
    signal, pending, strategy, fired = _reversal_pair()
    barrier = threading.Barrier(2)
    engine = _HandoffEngine(strategy, broker_responses=[[], []])
    original_positions = engine._positions

    def synchronized_positions():
        rows = original_positions()
        barrier.wait(timeout=2)
        return rows

    engine.env.broker.positions = synchronized_positions
    results = []
    workers = [
        threading.Thread(
            target=lambda: results.append(
                engine._activate_reversal_entry_after_flat(
                    engine.env, "silver_01")))
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=3)

    assert all(not worker.is_alive() for worker in workers)
    assert sorted(results) == [False, True]
    assert engine.processed == [(signal.signal_id, "live")]
    assert fired == [signal.signal_id]
    assert pending.status == "fired"


def test_nonflat_broker_snapshot_never_submits_opposite_entry():
    _signal, pending, strategy, fired = _reversal_pair()
    engine = _HandoffEngine(strategy, broker_responses=[
        [{"instrument": "SILVERM", "side": "BUY", "quantity": 1}],
    ])

    assert engine._activate_reversal_entry_after_flat(engine.env, "silver_01") is False
    assert engine.processed == []
    assert strategy.pending_entry is pending
    assert pending.status == "waiting_for_flat"
    assert fired == []


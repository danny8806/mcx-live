from types import SimpleNamespace

from strategies.instance import StrategyInstance
from strategies.types import StrategyState
from trading_engine import TradingEngine
from application.sl_flow import SLFlowMixin


def _reversal(strategy, *, high, low, timestamp, htf_val=None):
    return strategy._create_reversal_signal(
        "SHORT", close=(high + low) / 2, high=high, low=low,
        timestamp=timestamp, prev_high=high - 1, prev_low=low + 1,
        htf_val=htf_val)


def test_new_reversal_cancels_old_trigger_generation_and_carries_old_pending_id():
    strategy = StrategyInstance("s1", "GOLDM", "123", "15m")
    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.current_position_id = "position-1"
    strategy.stop_price = 80
    strategy.reversal_entry_gap_points = 2

    first_exit = _reversal(strategy, high=105, low=90, timestamp=1)
    first_entry = strategy.pending_entry.signal
    first_generation = strategy._trigger_generation

    second_exit = _reversal(strategy, high=110, low=85, timestamp=2)
    second_entry = strategy.pending_entry.signal

    assert first_entry.metadata["trigger_state"] == "CANCELLED"
    assert first_exit.metadata["trigger_state"] == "CANCELLED"
    assert second_exit.metadata["superseded_pending_entry_signal_id"] == first_entry.signal_id
    assert strategy._trigger_generation == first_generation + 1
    assert strategy.pending_entry.signal is second_entry
    assert strategy.pending_entry.status == "waiting_for_flat"
    assert strategy.pending_exit_trigger.signal is second_exit
    # A tick at the old level must not fire the cancelled exit generation.
    assert strategy.on_tick(first_exit.trigger_price, timestamp=3) is None


def test_replacement_reversal_freezes_its_own_hourly_dema_and_candle_trigger():
    strategy = StrategyInstance("s1", "GOLDM", "123", "15m")
    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.current_position_id = "position-1"
    strategy.stop_price = 80
    strategy.reversal_entry_gap_points = 2

    _reversal(strategy, high=105, low=90, timestamp=1, htf_val=101.25)
    first = strategy.pending_entry.signal
    second_exit = _reversal(strategy, high=110, low=85, timestamp=2,
                            htf_val=102.75)
    second = strategy.pending_entry.signal

    assert first.metadata["signal_htf_dema_atr"] == 101.25
    assert first.context.htf_value == 101.25
    assert second.metadata["signal_htf_dema_atr"] == 102.75
    assert second.context.htf_value == 102.75
    assert second.metadata["signal_candle_high"] == 110
    assert second_exit.metadata["signal_htf_dema_atr"] == 102.75
    assert second_exit.metadata["signal_candle_high"] == 110
    assert second_exit.trigger_price == 85
    assert second.trigger_price == 83


def test_stop_loss_cancels_waiting_reversal_entry_in_memory_registry_and_db():
    strategy = StrategyInstance("s1", "GOLDM", "123", "15m")
    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.current_position_id = "position-1"
    strategy.stop_price = 80
    strategy.reversal_entry_gap_points = 2
    _reversal(strategy, high=105, low=90, timestamp=1, htf_val=101.25)
    pending_id = strategy.pending_entry.signal.signal_id
    terminalized, synced, removed = [], [], []

    class Registry:
        def sync_strategy(self, item):
            synced.append((item.pending_entry, item.pending_exit_trigger))
        def remove_signal(self, signal_id):
            removed.append(signal_id)

    class Flow(SLFlowMixin):
        def publish_event(self, *_a, **_kw):
            pass
        def _persist_position(self, *_a, **_kw):
            pass

    env = SimpleNamespace(
        strategies={"s1": strategy}, pending_triggers=Registry(),
        persistence=SimpleNamespace(terminalize_pending_order=lambda *a, **k:
                                    terminalized.append((a, k))),
    )
    position = SimpleNamespace(
        strategy_id="s1", instrument="GOLDM", is_long=True,
        trade_id="trade-1", position_id="position-1",
        position_generation=1,
    )
    decision = SimpleNamespace(stop_price=80, quantity=1,
                               position_id="position-1")

    signal = Flow()._build_sl_exit_signal(env, position, decision, 79)

    assert signal.metadata["exit_reason"] == "stop_loss_hit"
    assert strategy.pending_entry is None
    assert strategy.pending_exit_trigger is None
    assert synced == [(None, None)]
    assert removed == [pending_id]
    assert terminalized == [((pending_id,), {
        "status": "resolved",
        "reason": "position_closed_by_stop_loss_before_reversal_trigger",
    })]


def test_live_reversal_supersession_terminalizes_old_durable_pending_row():
    strategy = StrategyInstance("s1", "GOLDM", "123", "15m")
    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.current_position_id = "position-1"
    strategy.stop_price = 80
    strategy.reversal_entry_gap_points = 2
    _reversal(strategy, high=105, low=90, timestamp=1)
    old_entry = strategy.pending_entry.signal
    new_exit = _reversal(strategy, high=110, low=85, timestamp=2)
    new_entry = strategy.pending_entry.signal

    rows = {old_entry.signal_id: "armed"}
    terminalized = []
    removed = []
    armed = []

    def terminalize(signal_id, *, status, reason):
        terminalized.append((signal_id, status, reason))
        if signal_id not in rows:
            return False
        rows[signal_id] = status
        return True

    runtime = SimpleNamespace(lifecycle=object(), order_manager=object(),
                              position_manager=object())
    env = SimpleNamespace(
        name="live", mode="LIVE", is_live=True,
        strategies={"s1": strategy},
        runtimes=SimpleNamespace(require=lambda _sid: runtime,
                                 get=lambda _sid: runtime),
        persistence=SimpleNamespace(terminalize_pending_order=terminalize),
        pending_triggers=SimpleNamespace(remove_signal=removed.append),
    )
    engine = object.__new__(TradingEngine)
    engine._env_for = lambda _name: env
    engine._gate_for = lambda _sid: SimpleNamespace(
        reversal_enabled=True, exit_enabled=True, sl_enabled=True)
    engine._reversal_under_cap = lambda _signal: True
    engine._persist_signal = lambda *_a, **_kw: None
    engine.publish_event = lambda *_a, **_kw: None
    engine._arm_live_pending = lambda sig, _env: (armed.append(sig.signal_id),
                                                   rows.__setitem__(sig.signal_id, "armed"))

    engine._process_signal(new_exit, "live")

    assert terminalized == [(
        old_entry.signal_id,
        "cancelled_by_reversal",
        "reversal_signal_replaced_before_exit_trigger",
    )]
    assert rows[old_entry.signal_id] == "cancelled_by_reversal"
    assert removed == [old_entry.signal_id]
    assert armed == [new_entry.signal_id]
    assert rows[new_entry.signal_id] == "armed"
    assert strategy.pending_entry.signal is new_entry


def test_sqlite_armed_reversal_pending_row_becomes_absorbing_cancelled_state(tmp_path):
    from persistence.manager import PersistenceManager

    persistence = PersistenceManager(
        state_path=str(tmp_path / "state.json"),
        db_path=str(tmp_path / "live.db"),
        execution_mode="LIVE",
    )
    persistence.save_pending_order({
        "pending_order_id": "old-reversal-entry",
        "signal_id": "old-reversal-entry",
        "strategy_id": "gold_02",
        "instrument": "GOLDM",
        "direction": "SHORT",
        "trigger_price": 90.0,
        "quantity": 1,
        "status": "armed",
    })

    assert persistence.terminalize_pending_order(
        "old-reversal-entry",
        status="cancelled_by_reversal",
        reason="reversal_signal_replaced_before_exit_trigger",
    )
    rows = persistence.get_pending_orders(execution_mode="LIVE")
    assert len(rows) == 1
    assert rows[0]["status"] == "cancelled_by_reversal"
    assert rows[0]["expired_reason"] == "reversal_signal_replaced_before_exit_trigger"
    # Terminal rows are absorbing and cannot be revived by duplicate cleanup.
    assert not persistence.terminalize_pending_order(
        "old-reversal-entry", status="resolved", reason="duplicate")
    persistence.close()


def test_failed_superseded_row_cleanup_keeps_old_position_and_blocks_replacement():
    strategy = StrategyInstance("s1", "GOLDM", "123", "15m")
    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.current_position_id = "position-1"
    strategy.stop_price = 80
    strategy.reversal_entry_gap_points = 2
    _reversal(strategy, high=105, low=90, timestamp=1)
    old_entry = strategy.pending_entry.signal
    new_exit = _reversal(strategy, high=110, low=85, timestamp=2)
    events = []
    armed = []
    runtime = SimpleNamespace(lifecycle=object(), order_manager=object(),
                              position_manager=object())
    env = SimpleNamespace(
        name="live", mode="LIVE", is_live=True,
        strategies={"s1": strategy},
        runtimes=SimpleNamespace(require=lambda _sid: runtime,
                                 get=lambda _sid: runtime),
        persistence=SimpleNamespace(terminalize_pending_order=lambda *_a, **_kw:
                                    (_ for _ in ()).throw(OSError("db offline"))),
    )
    engine = object.__new__(TradingEngine)
    engine._env_for = lambda _name: env
    engine.publish_event = lambda kind, data, **_kw: events.append((kind, data))
    engine._arm_live_pending = lambda sig, _env: armed.append(sig.signal_id)

    engine._process_signal(new_exit, "live")

    assert not armed
    assert strategy.pending_entry is None
    assert strategy.pending_exit_trigger is None
    assert strategy.state == StrategyState.LONG_POSITION
    assert strategy.position_side == "LONG"
    assert strategy.stop_price == 80
    assert any(kind == "pending_reversal_supersede_cleanup_failed"
               and data["old_signal_id"] == old_entry.signal_id
               for kind, data in events)

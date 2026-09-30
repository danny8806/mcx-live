"""Exercise a fresh DEMA invalidation through the real LIVE runtime graph."""
import json
import time
from pathlib import Path
from types import SimpleNamespace

from core.market_status import EngineStatus, MarketState
from core.timeframe_engine import Bar
from execution.live.broker_client import StubLiveBroker
from persistence.manager import PersistenceManager
from trading_engine import TradingEngine


class _FundedBroker(StubLiveBroker):
    def account_status(self):
        return {"mode": "LIVE", "equity": 10_000_000.0,
                "available_margin": 10_000_000.0, "used_margin": 0.0}


def _runtime(tmp_path):
    project = Path(__file__).resolve().parents[1]
    settings = json.loads((project / "config" / "live_settings.json").read_text())
    for key, name in (
        ("live_db_path", "live.db"), ("live_state_path", "live.json"),
        ("db_path", "paper.db"), ("state_path", "paper.json"),
    ):
        settings["system"][key] = str(tmp_path / name)
    settings["live"]["broker"] = "stub"
    settings["live"]["gate"] = "ON"
    settings["live"]["live_trading_enabled"] = True
    settings["live"]["rollover"]["state_path"] = str(tmp_path / "rollover.json")
    settings["live_test_order_cycle"] = {"enabled": False}
    settings["strategies"] = {
        "gold_02": dict(settings["strategies"]["gold_02"], quantity=1)}
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(settings))

    engine = TradingEngine(config_path=str(config_path), live_only=True)
    env = engine.live
    broker = _FundedBroker(gate_enabled=True, defer_fills=True)
    env.broker = broker
    env.execution_engine.broker = broker
    persistence = PersistenceManager(
        state_path=str(tmp_path / "live.json"),
        db_path=str(tmp_path / "live.db"))
    engine.set_persistence(persistence, env_name="live")
    engine._reconciled_envs.add("live")
    env.market_status._base.force_state(MarketState.LIVE_TRADING)
    env.market_status.set_engine_status(EngineStatus.TRADING)
    env.market_status.mark_rest_data_fresh()
    engine.market_data_health.record_tick("GOLDM", time.time())
    env.account_engines["gold_02"].set_broker_reported(
        equity=10_000_000, available_margin=10_000_000, used_margin=0)
    return engine, env, broker, persistence


def _bar(ts, close, high, low):
    return Bar("GOLDM", "15m", ts, ts + 900, close, high, low, close, 1)


def test_dema_change_removes_ram_and_db_trigger_then_new_crossover_sends_once(tmp_path):
    engine, env, broker, persistence = _runtime(tmp_path)
    strategy = env.strategies["gold_02"]
    try:
        strategy._prev_fast_close = 99.0
        strategy._prev_htf_value = 100.0
        old = strategy.on_bar(
            _bar(1, 101.0, 103.0, 98.0),
            SimpleNamespace(htf_value=100.0), 100.0)
        assert old is not None
        engine._process_signal(old, "live")
        assert persistence.get_pending_order(
            old.signal_id, execution_mode="LIVE")["status"] == "armed"
        assert env.pending_triggers.entry_for("gold_02") is strategy.pending_entry
        assert broker.order_statuses() == {}

        cancel = strategy.on_bar(
            _bar(2, 101.0, 104.0, 99.0),
            SimpleNamespace(htf_value=101.0), 101.0)
        assert cancel.metadata["cancel_pending_only"]
        engine._process_signal(cancel, "live")
        assert persistence.get_pending_order(
            old.signal_id, execution_mode="LIVE")["status"] == (
                "cancelled_by_indicator_change")
        assert env.pending_triggers.entry_for("gold_02") is None
        assert strategy.on_tick(103.0, time.time()) is None
        assert broker.order_statuses() == {}

        newer = strategy.on_bar(
            _bar(3, 102.0, 105.0, 100.0),
            SimpleNamespace(htf_value=101.0), 101.0)
        assert newer is not None and newer.signal_id != old.signal_id
        assert newer.metadata["signal_htf_dema_atr"] == 101.0
        engine._process_signal(newer, "live")
        assert persistence.get_pending_order(
            newer.signal_id, execution_mode="LIVE")["status"] == "armed"
        broker.update_price("GOLDM", 105.0)
        env.execution_engine.update_price("GOLDM", 105.0)
        engine.market_data_health.record_tick("GOLDM", time.time())
        assert strategy.on_tick(104.99, time.time()) is None
        fired = strategy.on_tick(105.0, time.time())
        assert fired is newer
        engine._process_signal(fired, "live")
        assert len(broker.order_statuses()) == 1
        assert len(env.execution_engine._orders) == 1
        assert persistence.get_pending_order(
            newer.signal_id, execution_mode="LIVE")["status"] == "entry_sent"
    finally:
        engine.stop()


def test_startup_hourly_warmup_retires_overnight_trigger_before_any_tick(tmp_path):
    engine, env, broker, persistence = _runtime(tmp_path)
    strategy = env.strategies["gold_02"]
    try:
        strategy._prev_fast_close = 99.0
        strategy._prev_htf_value = 100.0
        old = strategy.on_bar(
            _bar(1, 101.0, 103.0, 98.0),
            SimpleNamespace(htf_value=100.0), 100.0)
        engine._process_signal(old, "live")
        assert persistence.get_pending_order(
            old.signal_id, execution_mode="LIVE")["status"] == "armed"

        strategy.slow_htf_state = SimpleNamespace(last_value=101.0)
        assert engine._invalidate_warmed_pending_triggers(env) == 1

        assert persistence.get_pending_order(
            old.signal_id, execution_mode="LIVE")["status"] == (
                "cancelled_by_indicator_change")
        assert strategy.pending_entry is None
        assert env.pending_triggers.entry_for("gold_02") is None
        assert strategy.on_tick(103.0, time.time()) is None
        assert broker.order_statuses() == {}
    finally:
        engine.stop()


def test_startup_hourly_warmup_keeps_trigger_when_value_is_unchanged(tmp_path):
    engine, env, broker, persistence = _runtime(tmp_path)
    strategy = env.strategies["gold_02"]
    try:
        strategy._prev_fast_close = 99.0
        strategy._prev_htf_value = 100.0
        old = strategy.on_bar(
            _bar(1, 101.0, 103.0, 98.0),
            SimpleNamespace(htf_value=100.0), 100.0)
        engine._process_signal(old, "live")
        strategy.slow_htf_state = SimpleNamespace(last_value=100.0)

        assert engine._invalidate_warmed_pending_triggers(env) == 0
        assert persistence.get_pending_order(
            old.signal_id, execution_mode="LIVE")["status"] == "armed"
        assert strategy.pending_entry.signal is old
        assert env.pending_triggers.entry_for("gold_02") is strategy.pending_entry
        assert broker.order_statuses() == {}
    finally:
        engine.stop()

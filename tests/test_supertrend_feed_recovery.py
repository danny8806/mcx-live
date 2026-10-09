from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from application.environment_factory import EnvironmentFactoryMixin
from application.market_flow import MarketEventFlowMixin
from core.candle_fetcher import CandleFetcher


IST = timezone(timedelta(hours=5, minutes=30))


def test_live_only_factory_skips_unused_shared_dhan_adapter_and_fetcher():
    instance = SimpleNamespace(_live_only=True)
    EnvironmentFactoryMixin._init_data_adapter(instance)
    EnvironmentFactoryMixin._init_candle_fetcher(instance)
    assert instance.data_adapter is None
    assert instance.candle_fetcher is None


def test_live_only_engine_constructs_one_environment_owned_adapter(tmp_path, monkeypatch):
    import json
    from pathlib import Path

    from trading_engine import TradingEngine

    root = Path(__file__).resolve().parents[1]
    config = json.loads((root / "config" / "live_settings.json").read_text())
    for key, filename in (
        ("live_db_path", "live.db"), ("live_state_path", "live-state.json"),
        ("db_path", "paper.db"), ("state_path", "paper-state.json"),
    ):
        config["system"][key] = str(tmp_path / filename)
    config["live"]["broker"] = "stub"
    config["live"]["dhan"]["client_id"] = "test-client"
    config["live"]["rollover"]["state_path"] = str(tmp_path / "rollover.json")
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    constructed = []

    class Adapter:
        def __init__(self, **kwargs):
            constructed.append(self)
            self._instruments = {}

        def register_instruments(self, instruments):
            self._instruments.update(instruments)

    monkeypatch.setattr("application.environment_factory.DhanDataAdapter", Adapter)
    engine = TradingEngine(config_path=str(config_path), live_only=True)

    assert len(constructed) == 1
    assert engine.data_adapter is engine.live.data_adapter is constructed[0]
    assert engine.candle_fetcher is engine.live.candle_fetcher


def test_first_successful_fetch_forwards_only_bars_closed_after_startup():
    start = datetime(2026, 10, 9, 10, 0, tzinfo=IST)
    now = datetime(2026, 10, 9, 10, 16, tzinfo=IST)
    bars = [
        [start.timestamp() - 15 * 60, 100, 105, 95, 102, 10],
        [start.timestamp(), 102, 110, 101, 109, 12],
    ]

    class Adapter:
        def fetch_historical_candles(self, symbol, interval, from_date, to_date):
            assert (symbol, interval) == ("GOLDM", "15")
            return bars

    emitted = []
    fetcher = CandleFetcher(
        data_adapter=Adapter(), instruments={"GOLDM": {}},
        on_candle_closed=emitted.append, timeframes=["15m"],
    )
    fetcher._started_at_epoch = start.timestamp()

    assert fetcher._check_native_timeframe("GOLDM", {}, "15m", now)
    assert len(emitted) == 1
    assert emitted[0].start_ts == start.timestamp()
    assert emitted[0].close == 109


def test_stale_live_rest_candle_warms_indicator_but_does_not_run_signal():
    events = []
    strategy = SimpleNamespace(
        strategy_id="gold_02", signal_timeframe="15m", warmups=[], calls=0,
    )
    strategy.warmup_indicator = lambda bar: strategy.warmups.append(bar)
    strategy.on_candle = lambda event: setattr(strategy, "calls", strategy.calls + 1)
    env = SimpleNamespace(name="live", is_live=True, mode="LIVE",
                          pending_triggers=None)

    class Flow(MarketEventFlowMixin):
        _running = True
        _lock = threading.RLock()
        health = SimpleNamespace(record_bar=lambda: None)
        market_status = SimpleNamespace(mark_rest_data_fresh=lambda: None)
        config = {"live": {"market_data": {"max_signal_age_seconds": 60}}}

        def _env_for(self, name=None):
            return env

        def _maybe_enable_trading(self):
            return None

        def publish_event(self, kind, data, env_name=None):
            events.append((kind, data, env_name))

    flow = Flow()
    handler = flow._make_candle_handler(strategy, "live")
    stale_end = time.time() - 5 * 60
    handler(SimpleNamespace(
        source="rest", instrument="GOLDM", timeframe="15m",
        start_ts=stale_end - 900, end_ts=stale_end,
        open=100, high=110, low=90, close=105, volume=10,
    ))

    assert strategy.calls == 0
    assert len(strategy.warmups) == 1
    assert events[0][0] == "stale_signal_candle_skipped"
    assert events[0][1]["action"] == "indicator_warmed_no_order"

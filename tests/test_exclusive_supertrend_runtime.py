import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from core.candle_fetcher import CandleFetcher, IST
from trading_engine import TradingEngine


def _test_config(tmp_path: Path) -> dict:
    root = Path(__file__).resolve().parents[1]
    config = json.loads((root / "config" / "live_settings.json").read_text())
    for key, filename in (
        ("live_db_path", "live.db"),
        ("live_state_path", "live-state.json"),
        ("db_path", "paper.db"),
        ("state_path", "paper-state.json"),
    ):
        config["system"][key] = str(tmp_path / filename)
    config["live"]["broker"] = "stub"
    config["live"]["rollover"]["state_path"] = str(tmp_path / "rollover.json")
    return config


def test_exclusive_runtime_builds_only_the_two_15m_supertrend_strategies(tmp_path):
    config = _test_config(tmp_path)
    # A mistaken operator/config edit cannot add an unapproved profile.
    config["strategies"]["silver_extra"] = {
        "instrument": "SILVERM", "strategy_type": "supertrend",
        "signal_timeframe": "15m", "enabled": True,
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    engine = TradingEngine(config_path=str(config_path), live_only=True)

    assert set(engine.live.strategies) == {"gold_02", "silver_01"}
    assert engine._rollover_blocked == {("live", "GOLDM"), ("live", "SILVERM")}
    assert engine._configured_candle_timeframes("live") == ["15m"]
    assert engine.live.strategies["gold_02"].strategy_type == "supertrend"
    assert engine.live.strategies["silver_01"].strategy_type == "supertrend"
    assert engine.live.strategies["gold_02"].signal_timeframe == "15m"
    assert engine.live.strategies["silver_01"].signal_timeframe == "15m"
    gold_st = engine.live.strategies["gold_02"].supertrend
    silver_st = engine.live.strategies["silver_01"].supertrend
    assert (gold_st.atr_period, gold_st.multiplier, gold_st.source_name,
            gold_st.atr_method, gold_st.first_tr) == (
                46, 3.9, "ohlc4", "sma_tr", "na")
    assert (silver_st.atr_period, silver_st.multiplier, silver_st.source_name,
            silver_st.atr_method, silver_st.first_tr) == (
                36, 4.9, "low", "sma_tr", "na")
    assert not hasattr(engine.live.strategies["gold_02"], "fast_indicator")
    assert not hasattr(engine.live.strategies["silver_01"], "slow_htf_state")
    assert engine.live.strategies["gold_02"].subscriptions == ["GOLDM:15m"]
    assert engine.live.strategies["silver_01"].subscriptions == ["SILVERM:15m"]
    # The live-only runtime owns its own data graph. With no Dhan credentials
    # resolved in this unit-test environment, both live feed objects are
    # deliberately deferred instead of constructing an unused shared adapter.
    assert engine.data_adapter is engine.live.data_adapter is None
    assert engine.candle_fetcher is engine.live.candle_fetcher is None
    assert engine.live.indicator_engine is None
    assert set(engine.live.indicators) == {"gold_02_supertrend", "silver_01_supertrend"}
    assert set(engine.strategy_gates()) == {"gold_02", "silver_01"}
    assert engine.control_strategy("silver_02", "resume")["success"] is False


def test_persisted_rollover_contract_is_applied_before_runtime_feeds_are_built(tmp_path):
    config = _test_config(tmp_path)
    # Simulate an installation whose base config still points at the prior
    # expiry, while the durable rollover state already selected November.
    config["instruments"]["GOLDM"]["symbol"] = "MCX:GOLDM202610"
    config["instruments"]["GOLDM"]["security_id"] = "569003"
    config["live"]["rollover"]["state_path"] = str(tmp_path / "rollover.json")
    (tmp_path / "rollover.json").write_text(json.dumps({
        "GOLDM": {
            "metal": "GOLDM",
            "active_symbol": "MCX:GOLDM202611",
            "active_security_id": "571445",
            "switched_on": "2026-10-09",
        },
    }))
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    engine = TradingEngine(config_path=str(config_path), live_only=True)

    assert engine.config.instrument("GOLDM")["symbol"] == "MCX:GOLDM202611"
    assert engine.config.instrument("GOLDM")["security_id"] == "571445"
    assert engine.live.strategies["gold_02"].instrument == "GOLDM"
    assert engine._rollover_applied["GOLDM"]["to_security_id"] == "571445"


def test_exclusive_runtime_fails_closed_if_an_allowed_strategy_config_drifts(tmp_path):
    config = _test_config(tmp_path)
    config["strategies"]["gold_02"]["signal_timeframe"] = "5m"
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    try:
        TradingEngine(config_path=str(config_path), live_only=True)
    except RuntimeError as exc:
        assert "STRATEGY_RUNTIME_POLICY_MISMATCH" in str(exc)
    else:
        raise AssertionError("exclusive strategy timeframe mismatch must fail startup")


def test_instrument_factories_create_only_supertrend_profiles(monkeypatch):
    from strategies.gold import create_gold_15m
    from strategies.silver import create_silver_15m
    from config import Config
    from events.types import CandleEvent

    live_config = json.loads(
        (Path(__file__).resolve().parents[1] / "config" / "live_settings.json")
        .read_text())
    monkeypatch.setattr(Config, "_config", live_config)

    gold = create_gold_15m()
    assert gold.strategy_type == "supertrend"
    assert gold.security_id == "571445"
    assert create_silver_15m().strategy_type == "supertrend"
    assert CandleEvent("GOLDM", "15m", 0, 900, 1, 2, 0, 1, 0).security_id == "571445"


def test_checked_in_gold_defaults_point_to_nearest_verified_expiry():
    root = Path(__file__).resolve().parents[1]
    for filename in ("settings.json", "live_settings.json"):
        config = json.loads((root / "config" / filename).read_text())
        gold = config["instruments"]["GOLDM"]
        assert gold["symbol"] == "MCX:GOLDM202611"
        assert gold["security_id"] == "571445"


def test_production_factory_loads_only_supertrend_calculations():
    code = (
        "import sys; from strategies.gold import create_gold_15m; "
        "from strategies.silver import create_silver_15m; "
        "create_gold_15m(); create_silver_15m(); "
        "assert 'indicators.supertrend' in sys.modules; "
        "assert not any('dema' in name.lower() for name in sys.modules)"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_analytics_strategy_list_filters_retired_ids():
    from analytics import routes

    previous = routes._STRATEGY_IDS
    try:
        routes._STRATEGY_IDS = ["gold_extra", "gold_02", "silver_extra", "silver_01"]
        assert routes._strategy_ids() == ["gold_02", "silver_01"]
    finally:
        routes._STRATEGY_IDS = previous


def test_exclusive_runtime_rejects_unconfigured_strategy_added_to_allowlist(tmp_path):
    config = _test_config(tmp_path)
    config["strategy_runtime"]["allowlist"]["silver_extra"] = {
        "strategy_type": "supertrend", "instrument": "SILVERM", "timeframe": "15m"
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    try:
        TradingEngine(config_path=str(config_path), live_only=True)
    except RuntimeError as exc:
        assert "STRATEGY_RUNTIME_ALLOWLIST_MISMATCH" in str(exc)
    else:
        raise AssertionError("exclusive runtime must reject an unconfigured strategy")


def test_live_candle_scope_skips_idle_instruments_but_keeps_open_positions(tmp_path):
    config = _test_config(tmp_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    engine = TradingEngine(config_path=str(config_path), live_only=True)
    env = engine.live
    env.candle_fetcher = SimpleNamespace(instruments={})
    env.position_manager = SimpleNamespace(open_positions=[])

    engine._scope_live_candles_to_runtime(env)
    assert set(env.candle_fetcher.instruments) == {"GOLDM", "SILVERM"}

    env.position_manager.open_positions = [SimpleNamespace(
        instrument="GOLDPETAL", is_open=True)]
    engine._scope_live_candles_to_runtime(env)
    assert set(env.candle_fetcher.instruments) == {"GOLDM", "SILVERM"}


def test_native_candle_retry_uses_bounded_backoff_and_stops_when_bar_arrives():
    fetcher = CandleFetcher(None, {"SILVERM": {}}, lambda bar: None,
                            timeframes=["15m"])
    attempts = []

    def check_native(name, cfg, timeframe, now, expected_start_ts=None):
        attempts.append((name, timeframe, expected_start_ts))
        return len(attempts) >= 2

    fetcher._check_native_timeframe = check_native
    start_ts = 1_800_000_000
    fetcher._schedule_native_retry("SILVERM", "15m", start_ts)
    fetcher._native_retries["SILVERM:15m"]["next_at"] = time.monotonic() - 1

    fetcher._retry_native_closed_bars()
    assert len(attempts) == 1
    assert "SILVERM:15m" in fetcher._native_retries

    fetcher._native_retries["SILVERM:15m"]["next_at"] = time.monotonic() - 1
    fetcher._retry_native_closed_bars()

    assert len(attempts) == 2
    assert attempts[-1] == ("SILVERM", "15m", start_ts)
    assert fetcher._native_retries == {}


def test_native_candle_retry_uses_fast_schedule_and_stays_bounded(monkeypatch):
    import core.candle_fetcher as candle_fetcher_module

    clock = [100.0]
    monkeypatch.setattr(candle_fetcher_module.time, "monotonic",
                        lambda: clock[0])
    fetcher = CandleFetcher(None, {"GOLDM": {}}, lambda bar: None,
                            timeframes=["15m"])
    attempts = []
    fetcher._check_native_timeframe = lambda *args, **kwargs: (
        attempts.append((clock[0], kwargs.get("expected_start_ts"))) or False)
    start_ts = 1_800_000_000
    fetcher._schedule_native_retry("GOLDM", "15m", start_ts)

    state = fetcher._native_retries["GOLDM:15m"]
    assert state["next_at"] == 100.25
    assert state["deadline"] == 116.0

    retry_offsets = (0.25, 0.75, 1.75, 3.75, 15.0)
    for offset in retry_offsets:
        clock[0] = 100.0 + offset
        fetcher._retry_native_closed_bars()

    assert [round(at - 100.0, 2) for at, _ in attempts] == list(retry_offsets)
    assert all(expected == start_ts for _, expected in attempts)
    assert fetcher._native_retries["GOLDM:15m"]["next_at"] == 116.0

    # Once the bounded retry window expires, the task is retired rather than
    # spinning or issuing an unbounded stream of REST requests.
    clock[0] = 116.0
    fetcher._retry_native_closed_bars()
    assert len(attempts) == len(retry_offsets)
    assert fetcher._native_retries == {}


def test_late_native_15m_response_is_emitted_on_short_retry():
    close_at = datetime(2026, 10, 9, 9, 15, tzinfo=IST)
    candle_start = close_at - timedelta(minutes=15)
    candle = [candle_start.timestamp(), 100, 103, 99, 102, 40]

    class DelayedAdapter:
        calls = 0

        def fetch_historical_candles(self, instrument, interval, start, end):
            self.calls += 1
            return [] if self.calls == 1 else [candle]

    emitted = []
    fetcher = CandleFetcher(DelayedAdapter(), {"SILVERM": {}}, emitted.append,
                            timeframes=["15m"])
    fetcher._native_primed.add("SILVERM:15m")
    fetcher._native_offsets["SILVERM:15m"] = 0
    expected_start = int(candle_start.timestamp())

    assert not fetcher._check_native_timeframe(
        "SILVERM", {}, "15m", close_at, expected_start_ts=expected_start)
    fetcher._schedule_native_retry("SILVERM", "15m", expected_start)
    fetcher._native_retries["SILVERM:15m"]["next_at"] = time.monotonic() - 1
    fetcher._retry_native_closed_bars()

    assert len(emitted) == 1
    assert emitted[0].start_ts == candle_start.timestamp()
    assert fetcher._native_retries == {}


def test_close_scheduler_does_not_duplicate_fetches_during_same_close_minute():
    class EmptyAdapter:
        calls = 0

        def fetch_historical_candles(self, instrument, interval, start, end):
            self.calls += 1
            return []

    adapter = EmptyAdapter()
    fetcher = CandleFetcher(adapter, {"GOLDM": {}}, lambda bar: None,
                            timeframes=["15m"])
    fetcher._native_primed.add("GOLDM:15m")
    fetcher._native_offsets["GOLDM:15m"] = 0
    fetcher._check_timeframe("GOLDM", {}, "15m",
                             datetime(2026, 10, 9, 9, 15, 3, tzinfo=IST))
    fetcher._check_timeframe("GOLDM", {}, "15m",
                             datetime(2026, 10, 9, 9, 15, 9, tzinfo=IST))

    assert adapter.calls == 1
    assert "GOLDM:15m" in fetcher._native_retries


def test_15m_fallback_rejects_partial_5m_window_even_when_keep_partial_is_set(monkeypatch):
    candle_time = datetime(2026, 10, 9, 9, 0, tzinfo=IST)
    start = int(candle_time.timestamp())

    class SparseFiveMinuteAdapter:
        def fetch_historical_candles(self, _instrument, interval, _from, _to):
            if interval == "15":
                return []
            return [
                [start, 100, 102, 99, 101, 10],
                [start + 300, 101, 103, 100, 102, 12],
            ]

    emitted = []
    fetcher = CandleFetcher(
        SparseFiveMinuteAdapter(), {"GOLDM": {"keep_partial": True}},
        emitted.append, timeframes=["15m"])
    monkeypatch.setattr("core.candle_fetcher.time.time", lambda: start + 900 + 2)

    fetcher._fetch_candle("GOLDM", {"keep_partial": True}, "15m",
                          candle_time, "GOLDM:15m:test")

    assert emitted == []


def test_15m_fallback_accepts_complete_three_bar_5m_window(monkeypatch):
    candle_time = datetime(2026, 10, 9, 9, 0, tzinfo=IST)
    start = int(candle_time.timestamp())

    class CompleteFiveMinuteAdapter:
        def fetch_historical_candles(self, _instrument, interval, _from, _to):
            if interval == "15":
                return []
            return [
                [start, 100, 102, 99, 101, 10],
                [start + 300, 101, 104, 100, 103, 12],
                [start + 600, 103, 105, 102, 104, 15],
            ]

    emitted = []
    fetcher = CandleFetcher(
        CompleteFiveMinuteAdapter(), {"GOLDM": {"keep_partial": True}},
        emitted.append, timeframes=["15m"])
    monkeypatch.setattr("core.candle_fetcher.time.time", lambda: start + 900 + 2)

    fetcher._fetch_candle("GOLDM", {"keep_partial": True}, "15m",
                          candle_time, "GOLDM:15m:test")

    assert len(emitted) == 1
    assert (emitted[0].open, emitted[0].high, emitted[0].low,
            emitted[0].close, emitted[0].volume) == (100, 105, 99, 104, 37)

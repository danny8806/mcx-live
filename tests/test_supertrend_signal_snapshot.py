import json
from types import SimpleNamespace

from application.persistence_flow import PersistenceFlowMixin
from core.timeframe_engine import Bar
from dashboard.routes import live_ops
from persistence.manager import PersistenceManager
from strategies.supertrend import SuperTrendStrategyInstance


class _PersistHarness(PersistenceFlowMixin):
    def __init__(self, env):
        self.env = env

    def _env_for(self, _env_name=None):
        return self.env


def _strategy():
    return SuperTrendStrategyInstance(
        strategy_id="gold_02", instrument="GOLDM", security_id="571445",
        quantity=1, capital=300_000, multiplier=10,
        atr_period=2, atr_multiplier=1.0, source="ohlc4",
        first_tr="na",
    )


def test_signal_freezes_and_persists_its_completed_candle_and_indicator(tmp_path):
    strategy = _strategy()
    emitted = None
    # Deterministic OHLC path that creates the first SuperTrend flip.
    for index, close in enumerate((100, 98, 96, 94, 92, 90)):
        bar = Bar(
            "GOLDM", "15m", 1_800_000_000 + index * 900,
            1_800_000_900 + index * 900,
            close, close + 1, close - 1, close, volume=100 + index,
        )
        emitted = strategy.on_bar(bar)
        if emitted is not None:
            break

    assert emitted is not None
    assert emitted.context is not None
    assert emitted.context.timestamp == bar.start_ts
    assert (emitted.context.open, emitted.context.high,
            emitted.context.low, emitted.context.close,
            emitted.context.volume) == (
                bar.open, bar.high, bar.low, bar.close, bar.volume)
    assert emitted.context.indicator_values["supertrend_atr"] is not None

    persistence = PersistenceManager(
        state_path=str(tmp_path / "state.json"),
        db_path=str(tmp_path / "live.sqlite3"), execution_mode="LIVE")
    env = SimpleNamespace(
        name="live", persistence=persistence,
        strategies={strategy.strategy_id: strategy},
    )
    _PersistHarness(env)._persist_signal(emitted, "entry", "live")
    row = persistence.get_signal(emitted.signal_id)

    assert row["signal_timestamp"] == bar.start_ts
    assert row["candle_timestamp"] == bar.start_ts
    assert row["security_id"] == "571445"
    assert row["timeframe"] == "15m"
    assert (row["open"], row["high"], row["low"], row["close"],
            row["volume"]) == (bar.open, bar.high, bar.low, bar.close, bar.volume)
    assert json.loads(row["signal_metadata"])["signal_source"] == "completed_native_15m_candle"
    assert json.loads(row["indicator_data"])["supertrend_trend"] == -1
    persistence.close()


def test_live_signal_read_model_recovers_snapshot_from_existing_metadata(monkeypatch):
    candle_start = 1_800_000_000.0
    raw_row = {
        "signal_id": "signal-old-row", "strategy_id": "gold_02",
        "instrument": "GOLDM", "security_id": None, "timeframe": None,
        "side": "SHORT", "signal_type": "ENTRY_SHORT",
        "signal_timestamp": 0.0, "candle_timestamp": None,
        "open": None, "high": None, "low": None, "close": None,
        "volume": None, "trigger_price": 149353.0, "quantity": 1,
        "signal_reason": None,
        "signal_metadata": json.dumps({
            "strategy_type": "supertrend", "strategy_timeframe": "15m",
            "signal_source": "completed_native_15m_candle",
            "signal_candle_start": candle_start,
            "signal_candle_open": 149570.0,
            "signal_candle_high": 149592.0,
            "signal_candle_low": 149336.0,
            "signal_candle_close": 149353.0,
            "signal_candle_volume": 240,
        }),
        "indicator_data": json.dumps({"supertrend_trend": -1}),
        "execution_mode": "LIVE", "created_at": "2026-10-12 04:15:00",
    }
    strategy = SimpleNamespace(security_id="571445", signal_timeframe="15m")
    engine = SimpleNamespace(
        config={}, live=SimpleNamespace(strategies={"gold_02": strategy}))
    monkeypatch.setattr(live_ops, "_engine", engine)
    monkeypatch.setattr(live_ops, "_q", lambda *_args: [dict(raw_row)])

    result = live_ops._get_signals_sync(limit=1)
    signal = result["signals"][0]

    assert signal["signal_timestamp"] == candle_start
    assert signal["candle_timestamp"] == candle_start
    assert signal["security_id"] == "571445"
    assert signal["timeframe"] == "15m"
    assert (signal["open"], signal["high"], signal["low"], signal["close"],
            signal["volume"]) == (149570.0, 149592.0, 149336.0, 149353.0, 240)
    assert signal["signal_source"] == "completed_native_15m_candle"
    assert signal["indicator_values"]["supertrend_trend"] == -1

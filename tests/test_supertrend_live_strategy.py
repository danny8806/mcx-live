from core.timeframe_engine import Bar
from indicators.supertrend import SuperTrend
from strategies.supertrend import SuperTrendStrategyInstance


def test_sma_true_range_warmup_matches_each_supplied_script():
    silver = SuperTrend(atr_period=3, multiplier=1, source="low",
                        atr_method="sma_tr", first_tr="na")
    gold = SuperTrend(atr_period=3, multiplier=1, source="ohlc4",
                      atr_method="sma_tr", first_tr="na")
    bars = [
        (100, 102, 98, 101),
        (101, 104, 100, 103),
        (103, 106, 102, 105),
        (105, 108, 104, 107),
    ]
    silver_values = [silver.update(*bar, end_ts=i * 900) for i, bar in enumerate(bars)]
    gold_values = [gold.update(*bar, end_ts=i * 900) for i, bar in enumerate(bars)]

    assert silver_values[0].atr is None
    assert silver_values[2].atr is None
    assert silver_values[3].atr == 4.0
    assert silver_values[0].source == 98.0
    assert gold_values[2].atr is None
    assert gold_values[3].atr == 4.0
    assert gold_values[0].source == 100.25


def test_source_and_params_rebuild_from_retained_candles():
    indicator = SuperTrend(atr_period=2, multiplier=2, source="low",
                           first_tr="high_low")
    bars = [(100, 102, 98, 101), (101, 104, 100, 103),
            (103, 106, 102, 105)]
    for i, bar in enumerate(bars):
        indicator.update(*bar, end_ts=i * 900)
    old_value = indicator.value

    indicator.configure(2, 1.5, "ohlc4", "sma_tr")

    assert indicator.value.source == 104.0
    assert indicator.value.atr == old_value.atr
    assert indicator.value.upper != old_value.upper


def test_signal_only_emits_after_completed_bar_direction_flip():
    strategy = SuperTrendStrategyInstance(
        strategy_id="silver_01", instrument="SILVERM", security_id="123",
        quantity=1, capital=300_000, multiplier=5,
        atr_period=2, atr_multiplier=1.0, source="close",
        first_tr="high_low")
    strategy.reversal_entry_gap_points = 2
    closes = [100, 98, 96, 94, 92, 90, 92, 94, 96, 98,
              100, 102, 104, 102, 100, 98, 96, 94, 92, 90]
    signals = []
    for i, close in enumerate(closes):
        bar = Bar(
            instrument="SILVERM", timeframe="15m", start_ts=i * 900,
            end_ts=(i + 1) * 900, open=close, high=close + 1,
            low=close - 1, close=close)
        signal = strategy.on_bar(bar)
        if signal is not None:
            signals.append((i, signal.signal_type.value, signal.trigger_price,
                            signal.stop_price))

    assert [item[:2] for item in signals] == [
        (3, "SHORT"), (7, "LONG"), (14, "SHORT")]
    assert all(item[2] > 0 and item[3] == 0 for item in signals)
    assert not hasattr(strategy, "uses_stop_loss")
    assert all(item[2] == closes[item[0]] for item in signals)


def test_duplicate_native_bar_does_not_append_or_recalculate_a_second_signal():
    indicator = SuperTrend(atr_period=2, multiplier=1, source="hl2")
    first = indicator.update(100, 102, 98, 101, end_ts=900)
    second = indicator.update(100, 102, 98, 101, end_ts=900)
    assert second == first
    assert len(indicator._bars) == 1


def test_opposite_supertrend_flip_builds_market_reversal_pair_without_stop():
    strategy = SuperTrendStrategyInstance(
        strategy_id="silver_01", instrument="SILVERM", security_id="123",
        quantity=1, capital=300_000, multiplier=5,
        atr_period=2, atr_multiplier=1.0, source="close",
        first_tr="high_low")
    closes = [100, 98, 96, 94]
    reversal = None
    for i, close in enumerate(closes):
        if i == 3:
            strategy.position_side = "LONG"
            strategy.current_trade_id = "old-trade"
            strategy.current_position_id = "old-position"
            strategy.position_generation = 1
        reversal = strategy.on_bar(Bar(
            instrument="SILVERM", timeframe="15m", start_ts=i * 900,
            end_ts=(i + 1) * 900, open=close, high=close + 1,
            low=close - 1, close=close))

    assert reversal is not None
    assert reversal.signal_type.value == "SHORT"
    assert reversal.trigger_price == closes[-1]
    assert reversal.stop_price == 0
    assert reversal.metadata["market_on_signal_close"] is True
    assert reversal.metadata["no_separate_stop"] is True
    paired = reversal.metadata["reversal_entry_signal"]
    assert paired.signal_type.value == "SHORT"
    assert paired.trigger_price == closes[-1]
    assert paired.stop_price == 0
    assert paired.metadata["strategy_timeframe"] == "15m"
    assert paired.metadata["signal_source"] == "completed_native_15m_candle"
    assert paired.metadata["no_separate_stop"] is True

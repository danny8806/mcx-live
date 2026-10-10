import pandas as pd

from indicators.supertrend import SuperTrend
from tools.backtest_supertrend_15m_csv import supertrend


def test_backtest_uses_the_production_indicator_calculation():
    closes = [100, 98, 96, 94, 92, 90, 92, 94, 96, 98,
              100, 102, 104, 102, 100, 98, 96, 94, 92, 90]
    bars = pd.DataFrame({
        "open": closes,
        "high": [v + 1 for v in closes],
        "low": [v - 1 for v in closes],
        "close": closes,
    }, index=pd.date_range("2026-01-01", periods=len(closes), freq="15min"))

    actual = supertrend(bars, period=3, multiplier=1.5, source="low",
                        atr_method="sma_tr", first_tr="na")
    indicator = SuperTrend(3, 1.5, "low", "sma_tr", first_tr="na")
    expected = []
    for ts, bar in bars.iterrows():
        value = indicator.update(bar.open, bar.high, bar.low, bar.close,
                                 ts.timestamp())
        expected.append((value.atr, value.upper, value.lower, value.trend,
                         value.buy_signal, value.sell_signal))

    assert list(actual.columns) == ["atr", "up", "dn", "trend",
                                    "buy_signal", "sell_signal"]
    for i, row in enumerate(expected):
        for column, expected_value in zip(actual.columns, row):
            actual_value = actual[column].iloc[i]
            if expected_value is None:
                assert pd.isna(actual_value)
            elif isinstance(expected_value, bool):
                assert bool(actual_value) == expected_value
            else:
                assert actual_value == expected_value


def test_backtest_compatibility_switch_maps_to_production_atr_method():
    bars = pd.DataFrame({
        "open": [10.0, 11.0, 12.0],
        "high": [11.0, 13.0, 15.0],
        "low": [9.0, 10.0, 11.0],
        "close": [10.0, 12.0, 14.0],
    }, index=pd.date_range("2026-01-01", periods=3, freq="15min"))

    via_switch = supertrend(bars, 2, 2.0, change_atr=False, source="ohlc4",
                            first_tr="na")
    via_config = supertrend(bars, 2, 2.0, source="ohlc4",
                            atr_method="sma_tr", first_tr="na")
    pd.testing.assert_frame_equal(via_switch, via_config)

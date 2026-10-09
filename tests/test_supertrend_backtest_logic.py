import pandas as pd

from tools.backtest_supertrend_15m_csv import pine_atr, supertrend


def test_pine_atr_uses_sma_seed_then_wilder_rma():
    bars = pd.DataFrame({
        "high": [11.0, 12.0, 14.0, 15.0],
        "low": [9.0, 10.0, 12.0, 13.0],
        "close": [10.0, 11.0, 13.0, 14.0],
    })

    atr = pine_atr(bars, 2, change_atr=True)

    assert pd.isna(atr.iloc[0])
    assert atr.iloc[1] == 2.0
    assert atr.iloc[2] == 2.5
    assert atr.iloc[3] == 2.25


def test_supertrend_emits_signals_only_on_direction_flips():
    close = [100, 98, 96, 94, 92, 90, 92, 94, 96, 98,
             100, 102, 104, 102, 100, 98, 96, 94, 92, 90]
    bars = pd.DataFrame({
        "open": close,
        "high": [v + 1 for v in close],
        "low": [v - 1 for v in close],
        "close": close,
    })

    result = supertrend(bars, period=3, multiplier=1.5, change_atr=True)

    assert result.index[result["sell_signal"]].tolist() == [5, 15]
    assert result.index[result["buy_signal"]].tolist() == [8]
    assert result["trend"].iloc[-1] == -1

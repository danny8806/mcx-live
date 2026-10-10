"""Incremental SuperTrend indicator matching the supplied Pine scripts.

The live strategy feeds completed native candles only.  It retains the candle
history so a flat strategy can rebuild its line after dashboard parameter
changes without restarting the engine.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Optional


_SOURCES = {"open", "high", "low", "close", "hl2", "hlc3", "ohlc4"}
_ATR_METHODS = {"sma_tr", "wilder"}
_FIRST_TR = {"high_low", "na"}
MAX_ATR_PERIOD = 500
MAX_HISTORY = 10_000


@dataclass(frozen=True)
class SuperTrendValue:
    source: float
    true_range: Optional[float]
    atr: Optional[float]
    upper: Optional[float]
    lower: Optional[float]
    trend: int
    buy_signal: bool
    sell_signal: bool


class SuperTrend:
    """Pine-compatible SuperTrend with replaceable parameters."""

    def __init__(self, atr_period: int = 10, multiplier: float = 3.0,
                 source: str = "hl2", atr_method: str = "sma_tr",
                 first_tr: str = "high_low", max_history: int = 10000):
        self._requested_max_history = max(3, int(max_history))
        self.max_history = min(MAX_HISTORY, self._requested_max_history)
        self._history_truncated = False
        self._bars: list[tuple[float, float, float, float, float]] = []
        self.configure(atr_period, multiplier, source, atr_method,
                       first_tr=first_tr, replay=False)

    def configure(self, atr_period: int, multiplier: float, source: str,
                  atr_method: str = "sma_tr", *, first_tr: Optional[str] = None,
                  replay: bool = True) -> None:
        period = int(atr_period)
        mult = float(multiplier)
        src = str(source).lower()
        method = str(atr_method).lower()
        initial_tr = str(first_tr or getattr(self, "first_tr", "high_low")).lower()
        if period < 1 or period > MAX_ATR_PERIOD:
            raise ValueError(f"atr_period must be between 1 and {MAX_ATR_PERIOD}")
        if not isfinite(mult) or mult <= 0:
            raise ValueError("multiplier must be a positive finite number")
        if src not in _SOURCES:
            raise ValueError(f"source must be one of {sorted(_SOURCES)}")
        if method not in _ATR_METHODS:
            raise ValueError(f"atr_method must be one of {sorted(_ATR_METHODS)}")
        if initial_tr not in _FIRST_TR:
            raise ValueError(f"first_tr must be one of {sorted(_FIRST_TR)}")
        self.atr_period = period
        self.multiplier = mult
        self.source_name = src
        self.atr_method = method
        self.first_tr = initial_tr
        self.max_history = min(MAX_HISTORY, max(period + 2,
                                                self._requested_max_history))
        self._values: list[SuperTrendValue] = []
        self._recompute() if replay and self._bars else None

    @property
    def value(self) -> Optional[SuperTrendValue]:
        return self._values[-1] if self._values else None

    @property
    def previous(self) -> Optional[SuperTrendValue]:
        return self._values[-2] if len(self._values) > 1 else None

    def update(self, open_: float, high: float, low: float, close: float,
               end_ts: Optional[float] = None) -> SuperTrendValue:
        values = tuple(float(v) for v in (open_, high, low, close))
        if not all(isfinite(v) for v in values):
            raise ValueError("OHLC values must be finite")
        if values[1] < max(values[0], values[2], values[3]) or values[2] > min(
                values[0], values[1], values[3]):
            raise ValueError("invalid OHLC range")
        ts = float(end_ts if end_ts is not None else len(self._bars))
        if self._bars and ts == self._bars[-1][0]:
            self._bars[-1] = (ts, *values)
            # Recalculate the corrected tail from the retained prior state.
            # Replaying the whole (possibly truncated) buffer here would reset
            # trend state and make a REST correction produce a different line.
            self._values.pop()
            self._append_value(len(self._bars) - 1)
        elif self._bars and ts < self._bars[-1][0]:
            raise ValueError("SuperTrend candles must arrive in timestamp order")
        else:
            self._bars.append((ts, *values))
            self._append_value(len(self._bars) - 1)
            excess = len(self._bars) - self.max_history
            if excess > 0:
                del self._bars[:excess]
                del self._values[:excess]
                self._history_truncated = True
        return self._values[-1]

    def _source(self, o: float, h: float, l: float, c: float) -> float:
        if self.source_name == "open": return o
        if self.source_name == "high": return h
        if self.source_name == "low": return l
        if self.source_name == "close": return c
        if self.source_name == "hl2": return (h + l) / 2.0
        if self.source_name == "hlc3": return (h + l + c) / 3.0
        return (o + h + l + c) / 4.0

    def _recompute(self) -> None:
        self._values = []
        if not self._bars:
            return
        for i in range(len(self._bars)):
            self._append_value(i)

    def _append_value(self, i: int) -> None:
        _, o, h, l, c = self._bars[i]
        src = self._source(o, h, l, c)
        previous_close = self._bars[i - 1][4] if i else None
        tr = (h - l if previous_close is None and self.first_tr == "high_low"
              else None if previous_close is None else max(
                  h - l, abs(h - previous_close), abs(l - previous_close)))
        prior_values = self._values
        if self.atr_method == "sma_tr":
            first = max(0, i - self.atr_period + 1)
            window = [prior_values[j].true_range for j in range(first, i)]
            if tr is not None:
                window.append(tr)
            atr = (sum(window) / self.atr_period
                   if len(window) == self.atr_period and all(v is not None for v in window)
                   else None)
        else:
            previous_atr = prior_values[-1].atr if prior_values else None
            if tr is None:
                atr = None
            elif previous_atr is None:
                window = [prior_values[j].true_range
                          for j in range(max(0, i - self.atr_period + 1), i)] + [tr]
                atr = (sum(window) / self.atr_period
                       if len(window) == self.atr_period and all(v is not None for v in window)
                       else None)
            else:
                atr = (previous_atr * (self.atr_period - 1) + tr) / self.atr_period

        raw_up = src - self.multiplier * atr if atr is not None else None
        raw_dn = src + self.multiplier * atr if atr is not None else None
        prev_up = prior_values[-1].upper if prior_values else None
        prev_dn = prior_values[-1].lower if prior_values else None
        up1 = prev_up if prev_up is not None else raw_up
        dn1 = prev_dn if prev_dn is not None else raw_dn
        up = (max(raw_up, up1) if raw_up is not None and up1 is not None
              and previous_close is not None and previous_close > up1 else raw_up)
        dn = (min(raw_dn, dn1) if raw_dn is not None and dn1 is not None
              and previous_close is not None and previous_close < dn1 else raw_dn)
        prior_trend = prior_values[-1].trend if prior_values else 1
        trend = prior_trend
        if prior_trend == -1 and dn1 is not None and c > dn1:
            trend = 1
        elif prior_trend == 1 and up1 is not None and c < up1:
            trend = -1
        self._values.append(SuperTrendValue(
            source=src, true_range=tr, atr=atr, upper=up, lower=dn,
            trend=trend, buy_signal=(trend == 1 and prior_trend == -1),
            sell_signal=(trend == -1 and prior_trend == 1)))

    def snapshot(self) -> dict:
        current = self.value
        return {
            "type": "supertrend", "atr_period": self.atr_period,
            "multiplier": self.multiplier, "source": self.source_name,
            "atr_method": self.atr_method, "first_tr": self.first_tr,
            "bars": len(self._bars),
            "max_history": self.max_history,
            "history_truncated": self._history_truncated,
            "last_candle_end_ts": self._bars[-1][0] if self._bars else None,
            "value": current,
        }

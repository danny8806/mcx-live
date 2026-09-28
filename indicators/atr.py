"""Average True Range (ATR) indicator with Wilder smoothing."""
from __future__ import annotations

from typing import Optional

import numpy as np


class ATR:
    """Average True Range indicator with Wilder smoothing.
    
    TR = max(
        high - low,
        abs(high - previous_close),
        abs(low - previous_close)
    )
    
    ATR uses Wilder smoothing (exponential with alpha = 1/period).
    First ATR value is the simple average of first 'period' TR values.
    
    Stateful and incremental - maintains internal state for efficient
    updates without full recalculation.
    """

    def __init__(self, period: int):
        self.period = period
        self._tr_values: list[float] = []
        self._atr: Optional[float] = None
        self._prev_close: Optional[float] = None
        self._count = 0
        self._initialized = False

    def reset(self) -> None:
        """Reset indicator state."""
        self._tr_values = []
        self._atr = None
        self._prev_close = None
        self._count = 0
        self._initialized = False

    def update(self, high: float, low: float, close: float) -> Optional[float]:
        """Update with new bar values and return current ATR.
        
        Args:
            high: Bar high
            low: Bar low
            close: Bar close
            
        Returns:
            Current ATR value or None if not enough data
        """
        if any(np.isnan(v) for v in [high, low, close]):
            return self._atr
        
        self._count += 1
        
        # Calculate True Range
        if self._prev_close is None:
            tr = high - low
        else:
            tr = max(
                high - low,
                abs(high - self._prev_close),
                abs(low - self._prev_close),
            )
        
        self._prev_close = close
        self._tr_values.append(tr)
        
        # Keep only needed TR values
        if len(self._tr_values) > self.period + 1:
            self._tr_values = self._tr_values[-(self.period + 1):]
        
        # Calculate ATR
        if len(self._tr_values) < self.period:
            return None
        
        if self._atr is None and len(self._tr_values) >= self.period:
            # First ATR = simple average of first 'period' TRs
            self._atr = sum(self._tr_values[:self.period]) / self.period
            self._initialized = True
        elif self._atr is not None:
            # Wilder smoothing: ATR = (ATR_prev * (period-1) + TR_current) / period
            self._atr = (self._atr * (self.period - 1) + tr) / self.period
        
        return self._atr

    @staticmethod
    def calculate_batch(
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
        period: int,
    ) -> np.ndarray:
        """Calculate ATR for a batch of values (for backtesting/warmup).
        
        Args:
            highs: Array of high prices
            lows: Array of low prices
            closes: Array of close prices
            period: ATR period
            
        Returns:
            Array of ATR values (same length, NaN for insufficient data)
        """
        n = len(highs)
        result = np.full(n, np.nan, dtype=np.float64)
        
        if n < period:
            return result
        
        # Calculate True Range
        tr = np.full(n, np.nan, dtype=np.float64)
        tr[0] = highs[0] - lows[0]
        
        for i in range(1, n):
            tr[i] = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
        
        # First ATR = simple average
        result[period - 1] = np.mean(tr[:period])
        
        # Wilder smoothing
        alpha = 1.0 / period
        for i in range(period, n):
            result[i] = alpha * tr[i] + (1 - alpha) * result[i - 1]
        
        return result

    @property
    def value(self) -> Optional[float]:
        """Current ATR value."""
        return self._atr

    @property
    def initialized(self) -> bool:
        """Whether indicator has enough data for valid values."""
        return self._initialized

    def snapshot(self) -> dict:
        """Get indicator state for persistence."""
        return {
            "atr": self._atr,
            "prev_close": self._prev_close,
            "tr_values": list(self._tr_values),
            "count": self._count,
            "initialized": self._initialized,
        }

    def restore(self, data: dict) -> None:
        """Restore indicator state from persistence."""
        self._atr = data.get("atr")
        self._prev_close = data.get("prev_close")
        self._tr_values = data.get("tr_values", [])
        self._count = data.get("count", 0)
        self._initialized = data.get("initialized", False)

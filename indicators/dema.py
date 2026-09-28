"""Double Exponential Moving Average (DEMA) indicator."""
from __future__ import annotations

from typing import Optional

import numpy as np


class DEMA:
    """Double Exponential Moving Average indicator.
    
    DEMA = 2 * EMA1 - EMA2
    where EMA1 = EMA(close, period)
          EMA2 = EMA(EMA1, period)
    
    Stateful and incremental - maintains internal state for efficient
    updates without full recalculation.
    """

    def __init__(self, period: int):
        self.period = period
        self._ema1: Optional[float] = None
        self._ema2: Optional[float] = None
        self._count = 0

    def reset(self) -> None:
        """Reset indicator state."""
        self._ema1 = None
        self._ema2 = None
        self._initialized = False
        self._count = 0

    def update(self, value: float) -> Optional[float]:
        """Update with new close price and return current DEMA value.
        
        Args:
            value: New close price
            
        Returns:
            Current DEMA value or None if not enough data
        """
        if np.isnan(value):
            return self._ema1 if self._ema1 is not None else None
        
        self._count += 1
        
        if self._ema1 is None:
            # First value - initialize EMA1 and EMA2, return initial DEMA (= value)
            # Matches pandas EWM with min_periods=1 which returns from bar 0
            self._ema1 = value
            self._ema2 = value
            return value
        
        # Calculate alpha for EMA
        alpha = 2.0 / (self.period + 1.0)
        
        # Update EMA1
        self._ema1 = alpha * value + (1 - alpha) * self._ema1
        
        # Update EMA2 (EMA of EMA1)
        self._ema2 = alpha * self._ema1 + (1 - alpha) * self._ema2
        
        # DEMA = 2 * EMA1 - EMA2
        dema = 2 * self._ema1 - self._ema2
        
        return dema

    @staticmethod
    def calculate_batch(values: np.ndarray, period: int) -> np.ndarray:
        """Calculate DEMA for a batch of values (for backtesting/warmup).
        
        Args:
            values: Array of close prices
            period: DEMA period
            
        Returns:
            Array of DEMA values (same length, NaN for insufficient data)
        """
        result = np.full(len(values), np.nan, dtype=np.float64)
        if len(values) < period:
            return result
        
        # Calculate using pandas-style EMA
        alpha = 2.0 / (period + 1.0)
        
        ema1 = np.full(len(values), np.nan, dtype=np.float64)
        ema2 = np.full(len(values), np.nan, dtype=np.float64)
        
        # Initialize with first value (seed EMA1 and EMA2, DEMA = value at bar 0)
        ema1[0] = values[0]
        ema2[0] = values[0]
        result[0] = values[0]  # matches pandas EWM min_periods=1
        
        for i in range(1, len(values)):
            ema1[i] = alpha * values[i] + (1 - alpha) * ema1[i - 1]
            ema2[i] = alpha * ema1[i] + (1 - alpha) * ema2[i - 1]
            result[i] = 2 * ema1[i] - ema2[i]
        
        return result

    @property
    def value(self) -> Optional[float]:
        """Current DEMA value."""
        if self._ema1 is None or self._ema2 is None:
            return None
        return 2 * self._ema1 - self._ema2

    @property
    def initialized(self) -> bool:
        """Whether indicator has enough data for valid values."""
        return self._count >= self.period

    def snapshot(self) -> dict:
        """Get indicator state for persistence."""
        return {
            "ema1": self._ema1,
            "ema2": self._ema2,
            "count": self._count,
        }

    def restore(self, data: dict) -> None:
        """Restore indicator state from persistence."""
        self._ema1 = data.get("ema1")
        self._ema2 = data.get("ema2")
        self._count = data.get("count", 0)

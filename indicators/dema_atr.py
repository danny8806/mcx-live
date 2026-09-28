"""DEMA-ATR indicator with recursive band clamp (existing system faithful)."""
from __future__ import annotations

from typing import Optional

import numpy as np

from .dema import DEMA
from .atr import ATR


class DEMAATR:
    """DEMA-ATR indicator with recursive band clamp.
    
    Computes:
        DEMA = 2 * EMA1 - EMA2
        ATR = Wilder smoothed ATR
        upper = DEMA + ATR * factor
        lower = DEMA - ATR * factor
        
        output = recursive clamp:
            cur = previous output (or DEMA if first)
            if lower > cur: cur = lower
            if upper < cur: cur = upper
            return cur
    
    This implementation matches the existing system's behavior exactly.
    Stateful and incremental for live trading efficiency.
    """

    def __init__(
        self,
        dema_period: int = 3,
        atr_period: int = 6,
        atr_factor: float = 1.0,
    ):
        self.dema_period = dema_period
        self.atr_period = atr_period
        self.atr_factor = atr_factor
        
        self._dema = DEMA(dema_period)
        self._atr = ATR(atr_period)
        
        self._prev_output: Optional[float] = None
        self._count = 0
        self._initialized = False

    def reset(self) -> None:
        """Reset all indicator state."""
        self._dema.reset()
        self._atr.reset()
        self._prev_output = None
        self._count = 0
        self._initialized = False

    def update(
        self,
        open_price: float,
        high: float,
        low: float,
        close: float,
    ) -> Optional[float]:
        """Update with new bar values and return current DEMA-ATR line.
        
        Matches batch dema_atr_batch behavior: returns DEMA value even during
        ATR warmup (when band is NaN, clamp is skipped — same as batch).
        """
        # Update underlying indicators
        dema_val = self._dema.update(close)
        atr_val = self._atr.update(high, low, close)
        
        self._count += 1
        
        if dema_val is None:
            return None
        
        # Match batch behavior: during ATR warmup (atr_val is None),
        # band is NaN so clamp is skipped → output = DEMA value
        band = atr_val * self.atr_factor if atr_val is not None else float("nan")
        upper = dema_val + band
        lower = dema_val - band
        
        # Recursive band clamp (existing system behavior)
        if self._prev_output is None:
            cur = dema_val
        else:
            cur = self._prev_output
        
        if not np.isnan(lower) and lower > cur:
            cur = lower
        if not np.isnan(upper) and upper < cur:
            cur = upper
        
        self._prev_output = cur
        if atr_val is not None:
            self._initialized = True
        
        return cur

    @staticmethod
    def calculate_batch(
        opens: np.ndarray,
        highs: np.ndarray,
        lows: np.ndarray,
        closes: np.ndarray,
        dema_period: int = 3,
        atr_period: int = 6,
        atr_factor: float = 1.0,
    ) -> np.ndarray:
        """Calculate DEMA-ATR for a batch of values (matches dema_atr_batch)."""
        n = len(closes)
        result = np.full(n, np.nan, dtype=np.float64)
        
        if n < 1:
            return result
        
        dema_values = DEMA.calculate_batch(closes, dema_period)
        atr_values = ATR.calculate_batch(highs, lows, closes, atr_period)
        
        prev_output = None
        
        for i in range(n):
            if np.isnan(dema_values[i]):
                continue
            
            dema_val = dema_values[i]
            atr_val = atr_values[i] if not np.isnan(atr_values[i]) else None
            
            band = atr_val * atr_factor if atr_val is not None else float("nan")
            upper = dema_val + band
            lower = dema_val - band
            
            if prev_output is None:
                cur = dema_val
            else:
                cur = prev_output
            
            if not np.isnan(lower) and lower > cur:
                cur = lower
            if not np.isnan(upper) and upper < cur:
                cur = upper
            
            result[i] = cur
            prev_output = cur
        
        return result

    @property
    def value(self) -> Optional[float]:
        """Current DEMA-ATR line value."""
        return self._prev_output

    @property
    def dema_value(self) -> Optional[float]:
        """Current DEMA value."""
        return self._dema.value

    @property
    def atr_value(self) -> Optional[float]:
        """Current ATR value."""
        return self._atr.value

    @property
    def upper_band(self) -> Optional[float]:
        """Current upper band."""
        if self._dema.value is None or self._atr.value is None:
            return None
        return self._dema.value + self._atr.value * self.atr_factor

    @property
    def lower_band(self) -> Optional[float]:
        """Current lower band."""
        if self._dema.value is None or self._atr.value is None:
            return None
        return self._dema.value - self._atr.value * self.atr_factor

    @property
    def initialized(self) -> bool:
        """Whether indicator has enough data for valid values."""
        return self._initialized

    def _dema_snapshot_value(self) -> Optional[float]:
        return self._dema._ema1 if self._dema._ema1 is not None else self._prev_output

    def _atr_snapshot_value(self) -> Optional[float]:
        return self._atr._atr if self._atr._atr is not None else None

    def snapshot(self) -> dict:
        """Get current indicator state for persistence."""
        return {
            "dema": self._dema.snapshot(),
            "atr": self._atr.snapshot(),
            "prev_output": self._prev_output,
            "count": self._count,
            "initialized": self._initialized,
        }

    def restore(self, state: dict) -> None:
        """Restore indicator state from persistence."""
        if state.get("dema"):
            self._dema.restore(state["dema"])
        if state.get("atr"):
            self._atr.restore(state["atr"])
        self._prev_output = state.get("prev_output")
        self._count = state.get("count", 0)
        self._initialized = state.get("initialized", False)

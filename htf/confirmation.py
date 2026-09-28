"""HTF (Higher Timeframe) mapped value."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class HTFMappedValue:
    """HTF value mapped to a lower timeframe bar."""
    htf_value: Optional[float]
    prev_htf_value: Optional[float]
    htf_confirmed: bool
    htf_source_timestamp: Optional[float]

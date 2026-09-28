"""Shared native indicator streams with immutable snapshots (mission §7-§12).

One incremental DEMA/ATR/DEMA-ATR stream per unique (security_id, timeframe,
indicator_config). Calculated once per candle, consumed by all strategies that
subscribe to that timeframe. Consumers read immutable IndicatorSnapshot objects
that are never modified after publication.

Design (minimal-bind, parity-preserving):
  - Stream feed math is identical to the standalone DEMAATR / HTFState it
    replaces. Feeding the same bars in the same order yields byte-identical
    dema / atr / dema_atr values.
  - Live feeds arrive in candle_end_ts order via the event bus. Warmup replay
    is historical. Dedup by candle_end_ts so the planned engine-level double
    feed cleanups are safe.
  - IndicatorSnapshot is immutable. Both strategies subscribed to a stream hold
    the same object (object identity guaranteed).
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass
from typing import Optional

from indicators.dema_atr import DEMAATR
from htf.confirmation import HTFMappedValue


@dataclass(frozen=True)
class IndicatorSnapshot:
    """Immutable per-candle indicator values (mission §10)."""
    security_id: str
    timeframe: str
    candle_start_ts: float
    candle_end_ts: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    dema: Optional[float]
    atr: Optional[float]
    dema_atr: Optional[float]
    previous_dema: Optional[float]
    previous_atr: Optional[float]
    previous_dema_atr: Optional[float]
    is_complete: bool


class IndicatorStream:
    """One shared DEMA-ATR stream for a (security_id, timeframe) pair.

    Feed one candle → advance the single DEMAATR → publish an immutable
    IndicatorSnapshot. Mapping arrays (_end_times, _values) are maintained so
    that ``get_mapped_value(fast_bar)`` reproduces the exact bisect lookup
    that HTFState used.
    """

    def __init__(
        self,
        security_id: str,
        timeframe: str,
        dema_period: int = 3,
        atr_period: int = 6,
        atr_factor: float = 1.0,
    ):
        self.security_id = security_id
        self.timeframe = timeframe
        self.indicator = DEMAATR(dema_period, atr_period, atr_factor)

        # Mapping arrays — same semantics as HTFState._end_times / _values
        self._end_times: list[float] = []
        self._values: list[Optional[float]] = []

        # Dedup key: last candle_end_ts fed into the stream
        self._last_end_ts: Optional[float] = None

        # Latest published snapshot
        self.latest_snapshot: Optional[IndicatorSnapshot] = None

        # Diagnostics
        self._out_of_order_count: int = 0
        self._dedup_count: int = 0

    # ── core properties ──────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return f"{self.security_id}:{self.timeframe}"

    @property
    def value(self) -> Optional[float]:
        return self.indicator.value

    @property
    def dema_value(self) -> Optional[float]:
        return self.indicator.dema_value

    @property
    def atr_value(self) -> Optional[float]:
        return self.indicator.atr_value

    @property
    def initialized(self) -> bool:
        return self.indicator.initialized

    @property
    def _count(self) -> int:
        return self.indicator._count

    def bar_count(self) -> int:
        return len(self._end_times)

    # ── feed ─────────────────────────────────────────────────────────────

    def feed(
        self,
        open_price: float,
        high: float,
        low: float,
        close: float,
        end_ts: Optional[float] = None,
        start_ts: Optional[float] = None,
        volume: float = 0,
    ) -> IndicatorSnapshot:
        """Feed one candle and return the immutable snapshot.

        If *end_ts* matches the last accepted end_ts the feed is deduplicated:
        the DEMAATR is not re-advanced and no new array entry is appended.
        A feed without *end_ts* is always accepted (used during standalone
        construction or direct strategy warmup before binding).
        """
        if end_ts is not None:
            # §11 — one calculation per (security_id, timeframe, candle_end_ts).
            # Dedup is by candle identity, not merely by "same as the previous
            # feed". In both the live router and the warmup replay the same HTF
            # bar is fed once per subscribed strategy; those feeds may arrive
            # non-consecutively (interleaved with other timeframes/strategies).
            # A previous consecutive-only check let a re-feed of an OLD end_ts
            # re-advance the DEMA/ATR, multiplying the bar count and corrupting
            # shared-stream values on restart (warmup != live). Re-feeding an
            # already-accepted candle_id must always be a no-op.
            if self._end_times and self._end_times[-1] == end_ts:
                self._dedup_count += 1
                return self.latest_snapshot
            if end_ts in self._end_times:
                self._dedup_count += 1
                return self.latest_snapshot
            if (self._last_end_ts is not None
                    and end_ts < self._last_end_ts):
                self._out_of_order_count += 1

        # Capture previous values BEFORE advancing the indicator (§10)
        prev_dema = self.indicator.dema_value
        prev_atr = self.indicator.atr_value
        prev_dema_atr = self.indicator.value

        # Advance single DEMAATR
        self.indicator.update(open_price, high, low, close)
        dema = self.indicator.dema_value
        atr = self.indicator.atr_value
        dema_atr = self.indicator.value

        # Append mapping arrays (only when end_ts is provided — fast streams
        # that are never mapped can skip this)
        if end_ts is not None:
            self._end_times.append(end_ts)
            self._values.append(dema_atr)
            self._last_end_ts = end_ts

        # Resolve start_ts
        if start_ts is None and end_ts is not None:
            start_ts = end_ts - self._tf_minutes(self.timeframe) * 60

        snapshot = IndicatorSnapshot(
            security_id=self.security_id,
            timeframe=self.timeframe,
            candle_start_ts=float(start_ts) if start_ts is not None else 0.0,
            candle_end_ts=float(end_ts) if end_ts is not None else 0.0,
            open=float(open_price),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=float(volume),
            dema=dema,
            atr=atr,
            dema_atr=dema_atr,
            previous_dema=prev_dema,
            previous_atr=prev_atr,
            previous_dema_atr=prev_dema_atr,
            is_complete=True,
        )
        self.latest_snapshot = snapshot
        return snapshot

    # ── mapping ──────────────────────────────────────────────────────────

    def get_mapped_value(self, fast_bar) -> HTFMappedValue:
        """Map this stream's latest completed value to a fast bar (bisect logic).

        Identical algorithm to HTFState.get_mapped_value(). Returns an
        HTFMappedValue so the strategy's on_bar() attribute access is unchanged.
        """
        if not self._end_times:
            return HTFMappedValue(None, None, False, None)
        if hasattr(fast_bar, "end_ts"):
            target_close = fast_bar.end_ts
        else:
            target_close = fast_bar[0]
        idx = bisect.bisect_right(self._end_times, target_close) - 1
        if idx < 0:
            return HTFMappedValue(None, None, False, None)
        htf_value = self._values[idx]
        if htf_value is None:
            return HTFMappedValue(None, None, False, None)
        prev_value = self._values[idx - 1] if idx > 0 else None
        return HTFMappedValue(
            htf_value=htf_value,
            prev_htf_value=prev_value,
            htf_confirmed=True,
            htf_source_timestamp=self._end_times[idx],
        )

    def get_latest_value(self) -> Optional[float]:
        return self.value

    def get_prev_value(self) -> Optional[float]:
        if len(self._values) >= 2:
            return self._values[-2]
        return None

    # ── lifecycle ────────────────────────────────────────────────────────

    def reset(self) -> None:
        self.indicator.reset()
        self._end_times.clear()
        self._values.clear()
        self._last_end_ts = None
        self.latest_snapshot = None
        self._out_of_order_count = 0
        self._dedup_count = 0

    def snapshot(self) -> dict:
        return {
            "security_id": self.security_id,
            "timeframe": self.timeframe,
            "bar_count": self.bar_count(),
            "indicator_count": self.indicator._count,
            "indicator_initialized": self.indicator.initialized,
            "out_of_order": self._out_of_order_count,
            "dedup_count": self._dedup_count,
            "latest_snapshot": self.latest_snapshot,
        }

    # ── internal ─────────────────────────────────────────────────────────

    @staticmethod
    def _tf_minutes(tf: str) -> int:
        if not tf:
            return 5
        unit = tf[-1].lower()
        try:
            n = int(tf[:-1])
        except ValueError:
            return 5
        if unit == "h":
            return n * 60
        if unit == "m":
            return n
        return 5


# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY VIEWS — duck-typed wrappers so StrategyInstance code is minimal
# ═══════════════════════════════════════════════════════════════════════════

class StrategyIndicatorView:
    """Fast/mid/slow indicator view over a shared stream.

    Provides the same surface as DEMAATR for the strategy hot-path:
      .update(open, high, low, close, end_ts=None) -> float
      .value  .dema_value  .atr_value  ._count  .initialized
    """

    def __init__(self, stream: IndicatorStream):
        self._stream = stream

    def update(self, open_price, high, low, close, end_ts=None) -> Optional[float]:
        self._stream.feed(open_price, high, low, close, end_ts=end_ts)
        return self._stream.value

    @property
    def value(self) -> Optional[float]:
        return self._stream.value

    @property
    def dema_value(self) -> Optional[float]:
        return self._stream.dema_value

    @property
    def atr_value(self) -> Optional[float]:
        return self._stream.atr_value

    @property
    def initialized(self) -> bool:
        return self._stream.initialized

    @property
    def _count(self) -> int:
        return self._stream._count

    @property
    def _dema(self):
        return self._stream.indicator._dema

    @property
    def _atr(self):
        return self._stream.indicator._atr

    def reset(self) -> None:
        self._stream.reset()


class StreamHTFStateView:
    """HTFState-compatible view over a shared stream.

    The strategy calls .update(bar), .get_mapped_value(bar), .bar_count(),
    .last_value — all forward to the shared stream. Mapping arrays and
    dedup are stream-level, shared across subscribers.
    """

    def __init__(self, stream: IndicatorStream):
        self._stream = stream

    @property
    def instrument(self) -> str:
        return self._stream.security_id

    @property
    def timeframe(self) -> str:
        return self._stream.timeframe

    @property
    def indicator(self) -> DEMAATR:
        return self._stream.indicator

    def update(self, bar) -> None:
        end_ts = getattr(bar, "end_ts", None)
        if isinstance(bar, (tuple, list)):
            open_v, high, low, close = bar[1], bar[2], bar[3], bar[4]
            start_ts = None
            volume = float(bar[5]) if len(bar) > 5 else 0.0
        else:
            open_v, high, low, close = bar.open, bar.high, bar.low, bar.close
            start_ts = getattr(bar, "start_ts", None)
            volume = float(getattr(bar, "volume", 0))
        self._stream.feed(
            open_v, high, low, close,
            end_ts=end_ts,
            start_ts=start_ts,
            volume=volume,
        )

    def get_mapped_value(self, fast_bar) -> HTFMappedValue:
        return self._stream.get_mapped_value(fast_bar)

    @property
    def last_value(self) -> Optional[float]:
        return self._stream.value

    @property
    def prev_value(self) -> Optional[float]:
        return self._stream.get_prev_value()

    @property
    def _end_times(self) -> list:
        return self._stream._end_times

    @property
    def _values(self) -> list:
        return self._stream._values

    def bar_count(self) -> int:
        return self._stream.bar_count()

    def get_latest_value(self) -> Optional[float]:
        return self._stream.value

    def reset(self) -> None:
        self._stream.reset()

    def snapshot(self) -> dict:
        return self._stream.snapshot()


# ═══════════════════════════════════════════════════════════════════════════
# SHARED NATIVE INDICATOR ENGINE (mission §8)
# ═══════════════════════════════════════════════════════════════════════════

class SharedNativeIndicatorEngine:
    """Owns one IndicatorStream per unique (security_id, timeframe).

    Created once in TradingEngine.__init__() and passed to each strategy via
    bind_shared_indicators(). Strategies never call .feed() on the engine
    directly — the stream views handle it.
    """

    def __init__(
        self,
        dema_period: int = 3,
        atr_period: int = 6,
        atr_factor: float = 1.0,
    ):
        self._dema_period = dema_period
        self._atr_period = atr_period
        self._atr_factor = atr_factor
        self._streams: dict[tuple[str, str], IndicatorStream] = {}

    def get_or_create(
        self,
        security_id: str,
        timeframe: str,
    ) -> IndicatorStream:
        key = (security_id, timeframe)
        if key not in self._streams:
            self._streams[key] = IndicatorStream(
                security_id=security_id,
                timeframe=timeframe,
                dema_period=self._dema_period,
                atr_period=self._atr_period,
                atr_factor=self._atr_factor,
            )
        return self._streams[key]

    def get(self, security_id: str, timeframe: str) -> Optional[IndicatorStream]:
        return self._streams.get((security_id, timeframe))

    def snapshot(self, security_id: str, timeframe: str) -> Optional[IndicatorSnapshot]:
        stream = self.get(security_id, timeframe)
        return stream.latest_snapshot if stream else None

    @property
    def stream_count(self) -> int:
        return len(self._streams)

    def reset_all(self) -> None:
        for stream in self._streams.values():
            stream.reset()

    def stats(self) -> dict:
        return {
            "streams": self.stream_count,
            "total_bars": sum(s.bar_count() for s in self._streams.values()),
            "total_dedup": sum(s._dedup_count for s in self._streams.values()),
            "total_out_of_order": sum(s._out_of_order_count for s in self._streams.values()),
            "stream_keys": sorted(
                f"{sid}:{tf}" for sid, tf in self._streams
            ),
        }

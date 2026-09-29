"""REST candle fetcher - fetches completed candles from Dhan REST API when they close.

Architecture:
  WebSocket: ONLY for LTP (live price, order fills, P&L)
  REST API: Fetch actual OHLCV candles when they close
  
Flow:
  1. Timer checks every minute if any candle should have closed
  2. When 5m candle closes at :05, :10, :15, etc. → fetch from REST
  3. Create Bar object from REST data
  4. Feed to indicators and HTF engine
  5. Check for signals

Session-aware:
  - Only fetches candles during MARKET_OPEN / LIVE_TRADING states
  - Skips fetching during PRE_MARKET, MARKET_CLOSE, AFTER_MARKET, OVERNIGHT
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional

IST = timezone(timedelta(hours=5, minutes=30))

# Timeframe minutes
TIMEFRAME_MINUTES = {
    "5m": 5,
    "15m": 15,
    "1h": 60,
}


class CandleFetcher:
    """Fetches completed candles from REST API when they close.
    
    Does NOT form candles from ticks. Uses actual exchange OHLCV data.
    """
    
    def __init__(
        self,
        data_adapter,
        instruments: dict,
        on_candle_closed: Callable,
        session_open: str = "09:00",
        session_close: str = "23:30",
        market_status=None,
    ):
        self.data_adapter = data_adapter
        self.instruments = instruments
        self.on_candle_closed = on_candle_closed
        self.session_open = session_open
        self.session_close = session_close
        self.market_status = market_status
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_fetched: dict[str, float] = {}  # key -> last_fetch_timestamp
        # Learned native HTF offset (candle start minute % timeframe) per
        # timeframe, so close instants are scheduled exactly instead of polled.
        self._native_offsets: dict[str, int] = {}
        
    def start(self):
        """Start the candle fetcher thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="candle-fetcher"
        )
        self._thread.start()
        print("[CandleFetcher] Started", flush=True)
        
    def stop(self):
        """Stop the candle fetcher."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print("[CandleFetcher] Stopped", flush=True)
        
    def _run(self):
        """Main loop: fetch each candle at the moment its window closes.

        5m candles close on the clock boundary (:00/:05/...) and the native
        15m/1h candles close at the exchange's real start offset (learned on
        first sight of the series).  The loop sleeps in short slices toward
        the soonest close deadline, so the fetch happens "as soon as the
        candle closes" (~0.1 s after) — still pure REST, never tick-built
        candles — and the whole trading day costs ~1 REST per candle instead
        of a 30 s polling loop.
        """
        while self._running:
            try:
                # Skip fetching if market is not in active trading state
                if self.market_status and not self.market_status.should_fetch_candles:
                    self._sleep_until_stop(30.0)
                    continue
                self._check_and_fetch()
                # The newly closed 5m bar can appear in Dhan REST a few
                # seconds after its close. Retry that same bar briefly rather
                # than waiting until the next 5m close.
                self._retry_recent_5m()
                self._sleep_until_stop(self._seconds_until_next_close())
            except Exception as e:
                print(f"[CandleFetcher] Error: {e}", flush=True)
                self._sleep_until_stop(30.0)

    def _sleep_until_stop(self, seconds: float) -> None:
        """Sleep in short 100 ms slices toward the exact deadline so stop()
        answers within ~100 ms while the loop wakes precisely at close time."""
        seconds = max(0.0, float(seconds or 0.0))
        deadline = time.time() + seconds
        while self._running and time.time() < deadline:
            time.sleep(0.1)

    def _retry_recent_5m(self) -> None:
        now = datetime.now(IST)
        if now.minute % 5 or now.second >= 30:
            return
        for _ in range(15):
            if not self._running:
                return
            now = datetime.now(IST)
            if now.minute % 5 or now.second >= 30:
                return
            self._sleep_until_stop(2.0)
            now = datetime.now(IST)
            if now.minute % 5 or now.second >= 30:
                return
            missing = False
            for name, cfg in self.instruments.items():
                hour, minute = map(int, self.session_open.split(":"))
                session_start = now.replace(hour=hour, minute=minute,
                                            second=0, microsecond=0)
                if now < session_start:
                    continue
                completed = int((now - session_start).total_seconds() // 300)
                if completed <= 0:
                    continue
                candle_start = session_start + timedelta(minutes=(completed - 1) * 5)
                key = f"{name}:5m:{candle_start.timestamp()}"
                if key not in self._last_fetched:
                    self._check_timeframe(name, cfg, "5m", now)
                    missing = True
            if not missing:
                return

    def _native_close_due(self, name: str, timeframe: str, now: datetime) -> bool:
        """True only when a native HTF window has just closed (or the offset
        is not yet learned and must be probed).

        With a known exchange offset the close instants are exactly the clock
        minutes ≡ offset (mod tf).  Between them a native fetch would be a
        no-op REST call, so it is gated out: the native HTF fetch count drops
        to ~1 per window per instrument per day.
        """
        tf_minutes = TIMEFRAME_MINUTES.get(timeframe, 15)
        offset = self._native_offsets.get(timeframe)
        if offset is None:
            return True  # offset not yet learned -> probe the native series
        residue = (now.hour * 60 + now.minute) % tf_minutes
        return residue == offset

    def _next_minute_with_residue(self, now: datetime, tf_minutes: int,
                                  offset: int) -> datetime:
        """Next minute strictly after now whose minute-of-day ≡ offset (mod
        tf).  ``offset`` is the candle-START clock minute mod tf; since a
        candle closes exactly tf after it starts, the close minutes carry the
        same residue, so this yields the next CLOSE instant."""
        cur = now.hour * 60 + now.minute + 1  # strictly after now (no spin)
        rem = (cur - offset) % tf_minutes
        add = 0 if rem == 0 else (tf_minutes - rem)
        target = cur + add
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_of_day + timedelta(minutes=target)

    def _seconds_until_next_close(self, now: Optional[datetime] = None) -> float:
        """Seconds until the soonest candle close across every watched
        instrument/timeframe (5m clock + native 15m/1h offsets)."""
        now = now or datetime.now(IST)
        deadlines = [
            self._next_minute_with_residue(now, 5, 0),
            self._next_minute_with_residue(now, TIMEFRAME_MINUTES["15m"],
                                           self._native_offsets.get("15m", 0)),
            self._next_minute_with_residue(now, TIMEFRAME_MINUTES["1h"],
                                           self._native_offsets.get("1h", 0)),
        ]
        delay = (min(deadlines) - now).total_seconds()
        return max(0.0, delay)
            
    def _check_and_fetch(self):
        """Check if any candle should have closed and fetch it."""
        now = datetime.now(IST)
        # Prune old entries (>24h) to prevent unbounded growth
        cutoff = time.time() - 86400
        self._last_fetched = {k: v for k, v in self._last_fetched.items() if v > cutoff}
        
        for name, cfg in self.instruments.items():
            # Advance higher-timeframe context FIRST so a fast-bar signal that
            # closes on the same instant is evaluated against the NEW HTF value
            # (matches the backtest's per-bar mapping: the strategy sees the
            # current bar's HTF level, never the previous one).  1H before
            # 15m (an hour boundary is also a 15m/5m boundary).
            self._check_timeframe(name, cfg, "1h", now)
            self._check_timeframe(name, cfg, "15m", now)
            # Check 5m candles LAST.
            self._check_timeframe(name, cfg, "5m", now)
            
    def _check_timeframe(self, name: str, cfg: dict, timeframe: str, now: datetime):
        """Check if a candle of this timeframe just closed and fetch it."""
        tf_minutes = TIMEFRAME_MINUTES.get(timeframe, 5)

        # 5m stays clock-aligned. 15m/1H use NATIVE candles from Dhan, which
        # carry their own real exchange start offset (e.g. MCX 15m bars at
        # :01/:15/:30/:45, anchored to the session open) — they are NOT
        # clock-aligned, so we fetch the native series and emit the most recent
        # candle whose end time has elapsed, deduped by its actual native start.
        if timeframe in ("15m", "1h"):
            # Gate the native REST fetch to the exact close instants (once the
            # exchange offset is learned) so we don't re-fetch the series at
            # every 5m wake as a no-op.
            if self._native_close_due(name, timeframe, now):
                self._check_native_timeframe(name, cfg, timeframe, now)
            return

        # --- 5m clock-aligned scheduling ---
        # Session open guard: no candles before the session starts
        hour, minute = map(int, self.session_open.split(":"))
        session_start = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if now < session_start:
            return  # Market not open yet

        close_h, close_m = map(int, self.session_close.split(":"))
        session_end = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)

        minutes_since_open = (now - session_start).total_seconds() / 60

        # Candle timestamps identify the *start* of a candle.  At 09:05 the
        # completed 5m candle started at 09:00, not 09:05 (which is the start
        # of the currently forming candle).
        completed_buckets = int(minutes_since_open // tf_minutes)
        if completed_buckets <= 0:
            return
        candle_start = session_start + timedelta(
            minutes=(completed_buckets - 1) * tf_minutes
        )
        candle_end = candle_start + timedelta(minutes=tf_minutes)

        keep_partial = bool(cfg.get("keep_partial", False))
        # A window is complete once its close time has passed. A window that
        # extends past session_close (e.g. the 23:00 1H slot after a 23:30 MCX
        # close) is incomplete; keep_partial still forms it from the partial
        # data so the live line matches the backtest KEEP-ALL resample.
        if candle_end > session_end and not keep_partial:
            return  # window not fully elapsed / after session close

        # Create key for dedup
        key = f"{name}:{timeframe}:{candle_start.timestamp()}"
        if key in self._last_fetched:
            return  # Already fetched this candle

        # Don't fetch candles older than 2 candles (avoid re-fetching old data)
        if (now - candle_start).total_seconds() > tf_minutes * 60 * 3:
            return

        # Fetch the candle from REST API
        self._fetch_candle(name, cfg, timeframe, candle_start, key)

    def _check_native_timeframe(self, name: str, cfg: dict, timeframe: str, now: datetime):
        """Emit the most recent CLOSED native HTF candle (offset-aware)."""
        tf_minutes = TIMEFRAME_MINUTES.get(timeframe, 15)

        # Session open guard
        hour, minute = map(int, self.session_open.split(":"))
        session_start = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if now < session_start:
            return

        # Fetch native candles for today (interval "15"/"60").
        native_interval = {"15m": "15", "1h": "60"}[timeframe]
        try:
            candles = self.data_adapter.fetch_historical_candles(name, native_interval, now.date(), now.date())
        except Exception as e:
            print(f"[CandleFetcher] Error fetching native {name} {timeframe}: {e}", flush=True)
            return
        if not candles:
            return

        now_epoch = int(now.timestamp())
        if candles:
            # Learn/refresh the exchange's native start offset from the newest
            # native candle (forming or closed): the close instants are then
            # scheduled precisely instead of probed every 5m.
            newest = max(candles, key=lambda c: c[0])
            st_dt = datetime.fromtimestamp(newest[0], IST)
            self._native_offsets[timeframe] = \
                (st_dt.hour * 60 + st_dt.minute) % tf_minutes
        # Every CLOSED native candle not yet emitted, newest-last: a transient
        # fetch failure between windows self-heals at the next close instead of
        # silently skipping that candle (deduped by the native start ts).
        closed = sorted(
            (c for c in candles if c[0] + tf_minutes * 60 <= now_epoch),
            key=lambda c: c[0],
        )
        if not closed:
            return
        for candle in closed:
            key = f"{name}:{timeframe}:{candle[0]}"
            if key in self._last_fetched:
                continue
            bar = self._create_bar(
                name, timeframe, candle,
                datetime.fromtimestamp(candle[0], IST), tf_minutes)
            if bar:
                self._last_fetched[key] = time.time()
                self.on_candle_closed(bar)
                print(f"[CandleFetcher] {name} {timeframe} native closed: "
                      f"{datetime.fromtimestamp(candle[0], IST).strftime('%H:%M')} "
                      f"O={bar.open:.0f} H={bar.high:.0f} L={bar.low:.0f} C={bar.close:.0f}", flush=True)

    def _fetch_candle(self, name: str, cfg: dict, timeframe: str, candle_time: datetime, key: str):
        """Fetch a single candle from REST API.

        For 5m the native 5m candle is fetched directly.  For 15m/1H the NATIVE
        higher-timeframe candle is fetched directly from Dhan (interval "15"/"60")
        — no 5m aggregation, matching the backtest native method.  Falls back to
        aggregating 5m candles only if the native HTF fetch returns nothing.
        """
        try:
            from_date = candle_time.date()
            to_date = candle_time.date()
            target_ts = int(candle_time.timestamp())
            tf_minutes = TIMEFRAME_MINUTES[timeframe]

            # Fetch native candles. 5m -> "5", 15m -> "15", 1h -> "60".
            native_interval = {"5m": "5", "15m": "15", "1h": "60"}.get(timeframe, "5")
            candles = self.data_adapter.fetch_historical_candles(
                name, native_interval, from_date, to_date,
            )

            if timeframe == "5m":
                for candle in candles:
                    if candle[0] == target_ts:
                        bar = self._create_bar(name, timeframe, candle, candle_time, tf_minutes)
                        if bar:
                            self._last_fetched[key] = time.time()
                            self.on_candle_closed(bar)
                            print(f"[CandleFetcher] {name} {timeframe} closed: {candle_time.strftime('%H:%M')} O={bar.open:.0f} H={bar.high:.0f} L={bar.low:.0f} C={bar.close:.0f}", flush=True)
                        return
            else:
                # Native HTF candle (real exchange bar at its own start offset,
                # e.g. 15m bars at :01/:15/:30/:45 for MCX).  It is NOT clock
                # aligned, so we emit the most recent native candle whose start
                # is >= the target window's start and whose end has elapsed.
                native = [c for c in candles if c[0] >= target_ts]
                if native:
                    # Most recent already-closed native candle (start <= now).
                    native.sort(key=lambda c: c[0])
                    pick = native[0]
                    if pick[0] + tf_minutes * 60 <= int(time.time()):
                        self._last_fetched[key] = time.time()
                        bar = self._create_bar(name, timeframe, pick, candle_time, tf_minutes)
                        if bar:
                            self.on_candle_closed(bar)
                            print(f"[CandleFetcher] {name} {timeframe} native closed: {candle_time.strftime('%H:%M')} O={bar.open:.0f} H={bar.high:.0f} L={bar.low:.0f} C={bar.close:.0f}", flush=True)
                    return

                # Fallback: aggregate the 5m candles in this window (matches the
                # former resample behaviour when native HTF data is unavailable).
                candles5 = self.data_adapter.fetch_historical_candles(name, "5", from_date, to_date)
                window_start = target_ts
                window_end = target_ts + tf_minutes * 60
                window_candles = [c for c in candles5 if window_start <= c[0] < window_end]
                window_candles.sort(key=lambda candle: candle[0])
                keep_partial = bool(cfg.get("keep_partial", False))
                expected_count = tf_minutes // 5
                if len(window_candles) == expected_count or (keep_partial and len(window_candles) > 0):
                    bar = self._aggregate_candles(name, timeframe, window_candles, candle_time, tf_minutes)
                    if bar:
                        self._last_fetched[key] = time.time()
                        self.on_candle_closed(bar)
                        print(f"[CandleFetcher] {name} {timeframe} closed (5m-agg fallback): {candle_time.strftime('%H:%M')} O={bar.open:.0f} H={bar.high:.0f} L={bar.low:.0f} C={bar.close:.0f}", flush=True)

        except Exception as e:
            print(f"[CandleFetcher] Error fetching {name} {timeframe}: {e}", flush=True)
            
    def _create_bar(self, name: str, timeframe: str, candle: list, candle_time: datetime, tf_minutes: int):
        """Create a Bar object from REST candle data."""
        from core.timeframe_engine import Bar, BarState
        
        try:
            timestamp, open_p, high, low, close, volume = candle
            
            # Convert to IST if needed
            bar_dt = candle_time
            if bar_dt.tzinfo is None:
                bar_dt = bar_dt.replace(tzinfo=IST)
            start_ts = bar_dt.timestamp()
            
            return Bar(
                instrument=name,
                timeframe=timeframe,
                start_ts=start_ts,
                end_ts=start_ts + tf_minutes * 60,
                open=float(open_p),
                high=float(high),
                low=float(low),
                close=float(close),
                volume=int(volume),
                state=BarState.CLOSED,
            )
        except Exception as e:
            print(f"[CandleFetcher] Error creating bar: {e}", flush=True)
            return None
            
    def _aggregate_candles(self, name: str, timeframe: str, candles: list, candle_time: datetime, tf_minutes: int):
        """Aggregate multiple 5m candles into a higher timeframe candle."""
        from core.timeframe_engine import Bar, BarState
        
        try:
            if not candles:
                return None
                
            # Aggregate OHLCV
            open_p = float(candles[0][1])  # First candle's open
            high = max(float(c[2]) for c in candles)  # Highest high
            low = min(float(c[3]) for c in candles)  # Lowest low
            close_p = float(candles[-1][4])  # Last candle's close
            volume = sum(int(c[5]) for c in candles)  # Sum of volumes
            
            bar_dt = candle_time
            if bar_dt.tzinfo is None:
                bar_dt = bar_dt.replace(tzinfo=IST)
            start_ts = bar_dt.timestamp()
            
            return Bar(
                instrument=name,
                timeframe=timeframe,
                start_ts=start_ts,
                end_ts=start_ts + tf_minutes * 60,
                open=open_p,
                high=high,
                low=low,
                close=close_p,
                volume=volume,
                state=BarState.CLOSED,
            )
        except Exception as e:
            print(f"[CandleFetcher] Error aggregating: {e}", flush=True)
            return None

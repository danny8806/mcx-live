"""Dhan market data adapter — REST candles + WebSocket LTP only.

Architecture:
  1. REST API  -> fetch completed candles (backfill + periodic)
  2. WebSocket -> live LTP only (display, P&L, triggers)
  3. NO CandleBuilder — candles come from REST exclusively
"""
from __future__ import annotations

import datetime
import time
import threading
from typing import Any, Callable, Optional

from .websocket_client import DhanWebSocketClient
from .rest_client import DhanRESTClient, DhanAuthError
from .instrument_mapper import InstrumentMeta, get_instrument, register_instrument


class DhanDataAdapter:
    """Unified adapter for Dhan market data.

    Combines:
    - REST API for completed candles (source of truth)
    - WebSocket for live LTP only (fast, accurate)

    Does NOT build candles from WebSocket ticks.
    """

    def __init__(
        self,
        client_id: str,
        token_file: str,
        pin: str = "",
        totp_secret: str = "",
        on_tick: Optional[Callable] = None,
        on_status: Optional[Callable] = None,
    ):
        self.client_id = client_id
        self.on_tick = on_tick
        self.on_status = on_status

        # REST client (for candles) — with auto-renewal
        self.rest = DhanRESTClient(
            token_file=token_file,
            client_id=client_id,
            pin=pin,
            totp_secret=totp_secret,
        )

        # Auto-login on startup: ALWAYS mint a freshly auto-generated token via
        # PIN+TOTP (never reuse a possibly stale/expired cached token). This
        # guarantees both REST and the WebSocket connect with a brand-new token
        # after every start/restart, avoiding silent feed stalls caused by a
        # server-side-invalidated but JWT-looking-valid cached token.
        self.rest.renew_token()
        self.rest.start_scheduler()

        # WebSocket client (for live LTP only) — connect with the fresh token.
        # token_loader re-mints if the connection is ever restarted/stale.
        self.ws = DhanWebSocketClient(
            client_id=client_id,
            token=self.rest.load_token(),
            on_tick=self._process_tick,
            on_status=on_status,
            token_loader=self.rest.renew_token,
        )

        # Instrument registry
        self._instruments: dict[str, InstrumentMeta] = {}
        self._security_to_symbol: dict[str, str] = {}

        # State tracking
        self._connected = False
        self._last_tick_time = 0.0
        self._tick_count = 0
        self._error_count = 0
        self._instrument_ticks: dict[str, int] = {}

        # Live LTP cache
        self._live_ltp: dict[str, dict] = {}
        self._ltp_lock = threading.Lock()

    @property
    def connected(self) -> bool:
        return self.ws.connected

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "ws": self.ws.stats,
            "rest": self.rest.stats,
            "tick_count": self._tick_count,
            "error_count": self._error_count,
            "instrument_ticks": dict(self._instrument_ticks),
        }

    def register_instruments(self, instruments: dict[str, dict]) -> None:
        """Register instruments from configuration."""
        for name, config in instruments.items():
            meta = register_instrument(name, config)
            self._instruments[name] = meta
            self._security_to_symbol[meta.security_id] = name

            # Add to WebSocket subscription list (LTP only)
            self.ws.add_instrument(
                meta.symbol,
                meta.security_id,
                meta.exchange_segment,
                meta.instrument,
            )

    def connect(self) -> None:
        """Start data feed connections."""
        self.ws.connect()
        self._connected = True

    def disconnect(self) -> None:
        """Stop data feed connections."""
        self.ws.disconnect()
        self._connected = False

    def _process_tick(self, raw_tick: dict[str, Any]) -> None:
        """Process live tick from WebSocket (LTP only, no candle building)."""
        try:
            tick = self._normalize_tick(raw_tick)
            if tick and self.on_tick:
                self._tick_count += 1
                self._last_tick_time = time.time()
                self._instrument_ticks[tick["instrument"]] = (
                    self._instrument_ticks.get(tick["instrument"], 0) + 1
                )

                # DEBUG: log first 20 ticks per instrument to trace pipeline
                inst = tick["instrument"]
                cnt = self._instrument_ticks[inst]
                if cnt <= 20:
                    print(f"[dhan_adapter] TICK {inst} #{cnt} ltp={tick['ltp']:.1f} sid={tick['security_id']}", flush=True)

                # DEBUG: periodic tick distribution log (every 50 total ticks)
                if self._tick_count % 50 == 0:
                    print(f"[dhan_adapter] TICK_DIST total={self._tick_count} per_instrument={dict(self._instrument_ticks)}", flush=True)

                # Update live LTP cache
                with self._ltp_lock:
                    self._live_ltp[tick["instrument"]] = {
                        "ltp": tick["ltp"],
                        "ltq": tick.get("ltq", 0),
                        "timestamp": tick["event_timestamp"],
                        # Dhan LTT is exchange event time and can differ from
                        # the VPS clock (timezone/clock skew). Keep a separate
                        # local receipt clock for freshness decisions.
                        "receive_timestamp": tick.get("receive_timestamp") or time.time(),
                        "cumvol": tick.get("cumvol"),
                    }

                self.on_tick(tick)
        except Exception as e:
            self._error_count += 1
            print(f"[dhan_adapter] tick processing error: {e}", flush=True)

    def _normalize_tick(self, raw: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Normalize raw tick into canonical format."""
        security_id = raw.get("security_id", "")
        symbol = self._security_to_symbol.get(security_id)
        if not symbol:
            # DEBUG: log dropped tick when security_id not found
            if self._tick_count < 500:
                print(f"[dhan_adapter] DROPPED unknown sid={security_id} known_sids={list(self._security_to_symbol.keys())}", flush=True)
            return None

        meta = self._instruments.get(symbol)
        if not meta:
            # DEBUG: log dropped tick when instrument not found
            if self._tick_count < 500:
                print(f"[dhan_adapter] DROPPED no meta for symbol={symbol}", flush=True)
            return None

        # Dhan v2 defines LTT as Unix epoch seconds. Subtracting the IST
        # offset makes every live tick appear 5.5 hours old to the freshness
        # gate and prevents entry and local-stop triggers.
        ltt_epoch = raw.get("ltt", 0)
        if ltt_epoch:
            event_timestamp = float(ltt_epoch)
        else:
            event_timestamp = time.time()

        return {
            "instrument": symbol,
            "security_id": security_id,
            "exchange_segment": meta.exchange_segment,
            "event_timestamp": event_timestamp,
            "receive_timestamp": time.time(),
            "ltp": raw.get("ltp", 0.0),
            "ltq": raw.get("ltq", 0),
            "cumvol": raw.get("cumvol"),
            "source": "websocket",
        }

    def get_live_ltp(self, symbol: str) -> Optional[float]:
        """Get current live LTP for a symbol (thread-safe)."""
        with self._ltp_lock:
            data = self._live_ltp.get(symbol)
            return data["ltp"] if data else None

    def fetch_historical_candles(
        self,
        symbol: str,
        timeframe: str,
        from_date: datetime.date,
        to_date: datetime.date,
    ) -> list[list]:
        """Fetch historical candles via REST API (source of truth)."""
        meta = self._instruments.get(symbol)
        if not meta:
            raise ValueError(f"Unknown instrument: {symbol}")

        if timeframe == "D":
            return self.rest.fetch_daily(
                meta.security_id, from_date, to_date,
                meta.exchange_segment, meta.instrument,
            )
        else:
            from_dt = datetime.datetime.combine(from_date, datetime.time.min)
            to_dt = datetime.datetime.combine(to_date, datetime.time.max)
            return self.rest.fetch_intraday(
                meta.security_id, timeframe, from_dt, to_dt,
                meta.exchange_segment, meta.instrument,
            )

    def fetch_candle_state(
        self,
        symbol: str,
        timeframe: str = "5",
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> dict:
        """Fetch latest CLOSED + FORMING candle state with retry.

        Dhan /charts/intraday timestamps are already genuine POSIX epochs of
        the IST-aligned bucket start (verified live), so `start`, `end` and
        closed/forming classification are computed directly against now_ts
        (true UTC epoch).

        Returns:
            {"closed": [rows...], "forming": row|None, "cur_bucket": int}
        """
        meta = self._instruments.get(symbol)
        if not meta:
            return {"closed": [], "forming": None, "cur_bucket": 0}

        now = datetime.datetime.now(datetime.timezone.utc)
        now_ts = int(now.timestamp())
        ist_offset = datetime.timedelta(hours=5, minutes=30)
        now_ist = now + ist_offset

        tf_minutes = int(timeframe) if timeframe.isdigit() else 5
        bucket_sec = tf_minutes * 60
        # Lookback window scaled to the timeframe so at least one CLOSED candle
        # (plus the forming bar) is always returned: 5m->12m, 15m->32m, 60m->122m.
        lookback_minutes = tf_minutes * 2 + 2
        frm = now_ist - datetime.timedelta(minutes=lookback_minutes)
        to = now_ist + datetime.timedelta(seconds=10)

        # Grid-agnostic classification: Dhan buckets 60m on IST hours (UTC
        # :30), so start-vs-bucket comparison alone cannot tell closed from
        # forming. A candle is CLOSED iff its end is <= now; FORMING iff now
        # falls inside [start, start+bucket).
        closed = []
        forming = None
        for attempt in range(max_retries):
            try:
                candles = self.rest.fetch_intraday(
                    meta.security_id, timeframe, frm, to,
                    meta.exchange_segment, meta.instrument,
                )
                if not candles:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    break
                for c in candles:
                    cstart = int(c[0])
                    if (cstart + bucket_sec) <= now_ts:
                        closed.append(c)
                    elif cstart <= now_ts:
                        if forming is None or cstart > int(forming[0]):
                            forming = c
                return {"closed": closed, "forming": forming, "cur_bucket": now_ts // bucket_sec * bucket_sec}
            except Exception as e:
                print(f"[adapter] {symbol} fetch err: {e} (attempt {attempt+1}/{max_retries})", flush=True)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        return {"closed": closed, "forming": forming, "cur_bucket": now_ts // bucket_sec * bucket_sec}

    def fetch_closed_candle(
        self,
        symbol: str,
        timeframe: str = "5",
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> list[list]:
        """Fetch latest CLOSED candles (compat wrapper over fetch_candle_state)."""
        return list(self.fetch_candle_state(
            symbol, timeframe, max_retries, retry_delay
        )["closed"])

    def reconcile_candles(
        self,
        symbol: str,
        timeframe: str,
        lookback_minutes: int = 3,
    ) -> list[list]:
        """Reconcile with REST API (verify data accuracy)."""
        meta = self._instruments.get(symbol)
        if not meta:
            return []

        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
        from_dt = now - datetime.timedelta(minutes=lookback_minutes + 2)
        to_dt = now + datetime.timedelta(minutes=1)

        try:
            candles = self.rest.fetch_intraday(
                meta.security_id, timeframe, from_dt, to_dt,
                meta.exchange_segment, meta.instrument,
            )
            return candles
        except DhanAuthError:
            print(f"[dhan_adapter] auth error during reconciliation", flush=True)
            return []
        except Exception as e:
            print(f"[dhan_adapter] reconciliation error: {e}", flush=True)
            return []

    def is_token_valid(self) -> bool:
        """Check if current token is valid."""
        return not self.rest.token_expires_soon()

    def get_token_expiry_info(self) -> Optional[dict]:
        """Get token expiry information."""
        import base64
        import json
        token = self.rest.load_token()
        if not token or "." not in token:
            return None
        try:
            p = token.split(".")[1]
            p += "=" * (-len(p) % 4)
            payload = json.loads(base64.urlsafe_b64decode(p))
            exp = payload.get("exp", 0)
            remaining_hours = (exp - time.time()) / 3600
            return {
                "expires_at": exp,
                "remaining_hours": remaining_hours,
                "expires_soon": remaining_hours < 2.0,
            }
        except Exception:
            return None

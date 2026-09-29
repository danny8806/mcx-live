"""Dhan WebSocket client for real-time market data feed."""
from __future__ import annotations

import json
import logging
import struct
import threading
import time
from typing import Any, Callable, Optional

import websocket

log = logging.getLogger("dhan_ws")

# Packet codes Dhan emits that carry no LTP and are intentionally not parsed.
# 5 = market depth, 8 = OHLC. Only code 2 (LTP) and code 4 (quote) drive the
# trigger and the local stop-loss; candles come from REST.
_IGNORED_PACKET_CODES = frozenset({5, 8})


class DhanWebSocketClient:
    """WebSocket client for Dhan market data feed.

    Handles:
    - Connection management
    - Binary packet parsing
    - Timestamp normalization (LTT to IST)
    - Reconnection logic
    - Heartbeat detection
    - Stale connection detection
    """

    def __init__(
        self,
        client_id: str,
        token: str,
        on_tick: Optional[Callable] = None,
        on_status: Optional[Callable] = None,
        reconnect_delay: float = 10.0,
        ping_interval: float = 10.0,
        token_loader: Optional[Callable] = None,
    ):
        self.client_id = client_id
        self.token = token
        self.on_tick = on_tick
        self.on_status = on_status
        self.reconnect_delay = reconnect_delay
        self.ping_interval = ping_interval
        self.token_loader = token_loader

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._watchdog_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._connected = False
        self._connecting = False
        self._last_tick_time = 0.0
        self._stale_threshold = 60.0  # seconds
        self._watchdog_interval = 15.0  # how often the stale watchdog fires

        self._instruments: dict[str, dict[str, str]] = {}
        self._stats = {"recv": 0, "parse_err": 0, "sub": 0, "tick": 0}

        # Duplicate tick prevention: key = (security_id, ltt, price)
        # LTT is second-granularity; several ticks can share it with different
        # prices. Keying on price suppresses only true redelivered duplicates
        # (same sid + timestamp + price) instead of dropping same-second
        # price updates.
        self._seen_ltt: dict[str, int] = {}
        self._dedup_window = 120  # seconds to keep seen LTTs

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    def add_instrument(self, symbol: str, security_id: str,
                       exchange_segment: str, instrument: str) -> None:
        """Register instrument for subscription."""
        self._instruments[symbol] = {
            "sid": security_id,
            "exch": exchange_segment,
            "inst": instrument,
        }

    def connect(self) -> None:
        """Start WebSocket connection in background thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        # Stale watchdog: force a reconnect when the connection is up but
        # silently stopped delivering ticks (breaks the connected-but-silent
        # stall that the reconnect loop alone cannot detect).
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, daemon=True, name="dhan-ws-watchdog"
        )
        self._watchdog_thread.start()

    def disconnect(self) -> None:
        """Stop WebSocket connection."""
        self._stop.set()
        if self._ws:
            self._ws.close()
        if self._watchdog_thread is not None:
            self._watchdog_thread.join(timeout=5)
            self._watchdog_thread = None

    def _watchdog_loop(self) -> None:
        """Background watchdog: reconnect a stale (silent) connection.

        Detects the case where the socket is still 'connected' but has not
        delivered any tick within _stale_threshold seconds, and force-closes it
        so the reconnect loop starts a fresh connection (with a fresh token via
        token_loader). This prevents indefinite freezes on live LTP.
        """
        while not self._stop.is_set():
            self._stop.wait(self._watchdog_interval)
            if self._stop.is_set():
                break
            self._watchdog_loop_once()

    def _watchdog_loop_once(self) -> None:
        """Single stale-check pass (testable without a live thread)."""
        if not self._connected or self._connecting:
            return
        if self.is_stale():
            print(
                "[dhan_ws] watchdog: stale (no ticks for %.0fs), forcing reconnect"
                % self._stale_threshold,
                flush=True,
            )
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass
            if self.on_status:
                self.on_status("stale_reconnect")

    def _run_loop(self) -> None:
        """Main reconnection loop with bounded backoff and token reload."""
        delay = self.reconnect_delay
        max_delay = self.reconnect_delay * 3  # cap backoff (e.g. 30s max)
        while not self._stop.is_set():
            # Reload/refresh token before each reconnect attempt so the socket
            # always connects with an up-to-date (never stale) access token.
            if self.token_loader:
                try:
                    fresh_token = self.token_loader()
                    if fresh_token:
                        self.token = fresh_token
                except Exception as e:
                    print(f"[dhan_ws] token reload failed: {e}", flush=True)
            try:
                self._connect_once()
                delay = self.reconnect_delay  # reset on success
            except Exception as e:
                print(f"[dhan_ws] connection error: {e}", flush=True)
            if self._stop.is_set():
                break
            self._stop.wait(delay)
            delay = min(delay * 2, max_delay)

    def _connect_once(self) -> None:
        """Single connection attempt."""
        url = (
            f"wss://api-feed.dhan.co?version=2&token={self.token}"
            f"&clientId={self.client_id}&authType=2"
        )

        self._connecting = True
        self._ws = websocket.WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        # Set connected=True only after run_forever returns (clean state)
        # Use a flag that on_open can read
        self._connected = False
        try:
            self._ws.run_forever(ping_interval=self.ping_interval)
        finally:
            self._connecting = False
            self._connected = False
        if self.on_status:
            self.on_status("disconnected")

    def _on_open(self, ws: Any) -> None:
        """Handle WebSocket open - subscribe to instruments."""
        self._connected = True
        # Keep recent tick keys over reconnects: Dhan may replay its last quote
        # after a socket resumes. Clearing here would process that quote twice.
        # The cache remains bounded in _on_message.
        if self.on_status:
            self.on_status("connected")
        subs = [
            {"ExchangeSegment": info["exch"], "SecurityId": info["sid"]}
            for info in self._instruments.values()
        ]
        for i in range(0, len(subs), 100):
            batch = subs[i:i + 100]
            ws.send(json.dumps({
                "RequestCode": 17,
                "InstrumentCount": len(batch),
                "InstrumentList": batch,
            }))
        self._stats["sub"] += 1
        # DEBUG: log which instruments were subscribed
        sub_details = [(info["sid"], sym) for sym, info in self._instruments.items()]
        print(f"[dhan_ws] subscribed {len(subs)} instruments: {sub_details}", flush=True)

    def _on_message(self, ws: Any, message: str | bytes) -> None:
        """Handle incoming WebSocket message with duplicate prevention."""
        if isinstance(message, str):
            return  # Ignore text messages

        self._stats["recv"] += 1
        self._last_tick_time = time.time()

        try:
            tick = self._parse_packet(message)
            if tick and self.on_tick:
                # Duplicate prevention: skip only if same security_id + LTT + price
                sid = tick.get("security_id", "")
                ltt = tick.get("ltt", 0)
                ltp = tick.get("ltp", 0.0)
                dedup_key = f"{sid}|{ltt}|{ltp:.2f}"
                if ltt and dedup_key in self._seen_ltt:
                    # DEBUG: log dedup to identify per-instrument filtering
                    if self._stats["tick"] < 500:
                        print(f"[dhan_ws] DEDUP sid={sid} ltp={ltp:.1f} ltt={ltt}", flush=True)
                    return  # duplicate tick after reconnect
                if ltt:
                    self._seen_ltt[dedup_key] = ltt
                    # Cap dict size while retaining the newest replay keys.
                    while len(self._seen_ltt) > 10000:
                        self._seen_ltt.pop(next(iter(self._seen_ltt)))
                self._stats["tick"] += 1
                self.on_tick(tick)
        except Exception as e:
            self._stats["parse_err"] += 1
            print(f"[dhan_ws] parse error: {e}", flush=True)

    def _on_error(self, ws: Any, error: Exception) -> None:
        """Handle WebSocket error."""
        print(f"[dhan_ws] error: {error}", flush=True)
        if self.on_status:
            self.on_status("error")

    def _on_close(self, ws: Any, close_code: int, close_msg: str) -> None:
        """Handle WebSocket close."""
        print(f"[dhan_ws] closed: {close_code} {close_msg}", flush=True)
        if self.on_status:
            self.on_status("closed")

    def _parse_packet(self, data: bytes) -> Optional[dict[str, Any]]:
        """Parse binary packet from Dhan WebSocket.

        Packet format (code 4 - Quote):
        - Byte 0: packet type (4)
        - Bytes 4-7: security ID (int32 LE)
        - Bytes 8-11: LTP (float32 LE)
        - Bytes 12-13: LTQ (int16 LE)
        - Bytes 14-17: LTT (int32 LE) - epoch in IST
        - Bytes 22-25: cumulative volume (int32 LE)

        Packet format (code 2 - Price):
        - Byte 0: packet type (2)
        - Bytes 4-7: security ID (int32 LE)
        - Bytes 8-11: LTP (float32 LE)
        - Bytes 12-15: LTT (int32 LE)
        """
        if len(data) < 4:
            return None

        code = data[0]

        if code == 4:  # Quote packet
            sid = struct.unpack_from("<i", data, 4)[0]
            ltp = struct.unpack_from("<f", data, 8)[0]
            ltq = struct.unpack_from("<h", data, 12)[0]
            ltt = struct.unpack_from("<i", data, 14)[0]
            cumvol = struct.unpack_from("<i", data, 22)[0]

            return {
                "security_id": str(sid),
                "ltp": ltp,
                "ltq": ltq,
                "ltt": ltt,
                "cumvol": cumvol,
                "timestamp": time.time(),
            }

        elif code == 2:  # Price packet
            sid = struct.unpack_from("<i", data, 4)[0]
            ltp = struct.unpack_from("<f", data, 8)[0]
            ltt = struct.unpack_from("<i", data, 12)[0]

            return {
                "security_id": str(sid),
                "ltp": ltp,
                "ltq": 0,
                "ltt": ltt,
                "cumvol": None,
                "timestamp": time.time(),
            }

        # Dhan also sends market-depth (code 5) and OHLC (code 8) packets. Both
        # are deliberately ignored: the stop-loss and the trigger both need only
        # LTP, and candles come from REST. Counting them separately keeps them
        # from being reported as UNKNOWN packets, which read as data loss.
        if code in _IGNORED_PACKET_CODES:
            self._stats["ignored_packets"] = \
                self._stats.get("ignored_packets", 0) + 1
            self._stats.setdefault("ignored_by_code", {})
            self._stats["ignored_by_code"][str(code)] = \
                self._stats["ignored_by_code"].get(str(code), 0) + 1
            return None

        # A genuinely unrecognised code IS worth flagging: it may mean a feed
        # format change, and silently dropping real data is how a stop goes
        # blind without anyone noticing.
        self._stats["unknown_packets"] = \
            self._stats.get("unknown_packets", 0) + 1
        if self._stats["recv"] < 500:
            log.warning("[dhan_ws] UNKNOWN packet code=%s len=%s hex=%s",
                        code, len(data), data[:16].hex())
        return None

    def is_stale(self) -> bool:
        """Check if connection is stale (no ticks received)."""
        if not self._connected:
            return True
        return (time.time() - self._last_tick_time) > self._stale_threshold

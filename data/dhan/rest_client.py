"""Dhan REST client for historical data and reconciliation."""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests

# Process-global last-renew timestamp.  Multiple DhanDataAdapter instances can
# be constructed in one process (engine restarts, probes).  Each constructor
# mints a fresh token via renew_token(); Dhan however only permits ONE token
# generation per ~2-minute TOTP window *per client id*.  Without a shared
# cooldown, rapidly re-constructed adapters hammer the login endpoint, hit the
# 2-minute rate limit, lose the token, and every REST call fails with
# DH-906 Invalid Token.  Sharing the cooldown across the process makes repeated
# adapter construction reuse an already-minted (JWT-valid) token instead of
# re-issuing.  This is guarded, so a genuinely expired token still renews.
_RENEW_GLOBAL_LOCK = threading.Lock()
_RENEW_LAST_ATTEMPT: float = 0.0


IST = timezone(timedelta(hours=5, minutes=30))


# Genuine Dhan auth-failure markers.  Dhan reports MANY non-auth errors as 400
# with errorCode DH-906 (e.g. "Order Is Cancelled.kindly Refresh Your Orderbook",
# "Rate Not Within Ckt Limit ...").  Renewing the token on those is harmful:
# a token mint INVALIDATES every previously issued token for the client, so a
# spurious renewal on a non-auth DH-906 starves every other client instance of
# a valid token and drives a perpetual every-2-minute renewal storm.  Auto-renew
# must fire ONLY on explicit token-invalid responses (or 401).
_DHAN_AUTH_400_MARKERS = (
    "Invalid Token",
    "Invalid Access Token",
    "Authentication_Failed",
)


def _is_auth_response(status: int, text: str) -> bool:
    """True only for genuine Dhan auth failures (401 / token-invalid 400s)."""
    if status == 401:
        return True
    if status != 400:
        return False
    return any(marker in text for marker in _DHAN_AUTH_400_MARKERS)


class TokenBucket:
    """Rate limiter using token bucket algorithm."""

    __slots__ = ("rate", "capacity", "tokens", "ts", "lock")

    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = rate_per_sec
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.ts = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Wait until a token is available."""
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(
                    self.capacity,
                    self.tokens + (now - self.ts) * self.rate
                )
                self.ts = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                delay = (1.0 - self.tokens) / self.rate
            time.sleep(delay)


class DhanAuthError(Exception):
    """Authentication error from Dhan API."""
    pass


class DhanRateLimit(Exception):
    """Rate limit exceeded."""
    pass


class DhanHTTPError(RuntimeError):
    """Dhan HTTP failure carrying the status and (when parseable) the body.

    ``str(exc)`` keeps the same ``"<status> <text>..."`` shape the previous
    bare RuntimeError produced, so existing catch/repr logic is unaffected;
    the extra attributes let the broker audit and Telegram layers persist the
    structured error without re-parsing a string.
    """

    def __init__(self, status: int, text: str, body: Any = None):
        self.status = int(status)
        self.text = text
        self.body = body
        super().__init__(f"{status} {text[:200]}")
        self.dhan_error_type = None
        self.error_code = None
        if isinstance(body, dict):
            self.dhan_error_type = body.get("errorType")
            self.error_code = body.get("errorCode")
        elif isinstance(body, str):
            try:
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    self.dhan_error_type = parsed.get("errorType")
                    self.error_code = parsed.get("errorCode")
            except Exception:
                pass


class DhanRESTClient:
    """REST client for Dhan API.

    Handles:
    - Historical candle fetching
    - Intraday data
    - Rate limiting
    - Retry logic
    - Token management with auto-renewal via PIN+TOTP
    """

    def __init__(
        self,
        base_url: str = "https://api.dhan.co/v2",
        token_file: Optional[str] = None,
        client_id: str = "",
        pin: str = "",
        totp_secret: str = "",
        rate_per_sec: float = 3.5,
        burst: int = 3,
        max_retries: int = 3,
    ):
        self.base_url = base_url
        self.token_file = token_file
        self.client_id = client_id
        self.pin = pin
        self.totp_secret = totp_secret
        self.max_retries = max_retries
        self.limiter = TokenBucket(rate_per_sec, burst)

        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        self._token_cache: Optional[str] = None
        self._token_ts: float = 0.0
        self._headers_cache: Optional[dict] = None
        self._headers_ts: float = 0.0

        self._stats = {"ok": 0, "empty": 0, "retry": 0}
        self._stats_lock = threading.Lock()

        # Token renewal — single lock prevents concurrent renewals
        self._renew_lock = threading.Lock()
        self._last_renew_attempt: float = 0.0
        self._renew_cooldown: float = 130.0  # Dhan rate limit: 2 min

        # Proactive token renewal scheduler
        self._scheduler_thread: Optional[threading.Thread] = None
        self._scheduler_stop = threading.Event()
        self._renew_hour = 7  # 7 AM daily renewal
        self._renew_minute = 0
        self._safety_check_hours = 6  # Safety check every 6 hours
        self._last_safety_check: float = 0.0

    @property
    def stats(self) -> dict[str, int]:
        with self._stats_lock:
            return dict(self._stats)

    def load_token(self) -> str:
        """Load access token from file with caching."""
        now = time.monotonic()
        if self._token_cache is not None and (now - self._token_ts) < 30:
            return self._token_cache

        if self.token_file and os.path.exists(self.token_file):
            try:
                with open(self.token_file) as f:
                    self._token_cache = json.load(f).get("access_token", "")
                    self._token_ts = now
                    return self._token_cache
            except Exception:
                pass
        return ""

    def token_expires_soon(self, grace_hours: float = 2.0) -> bool:
        """Check if token expires within grace period.

        Returns True only if JWT exp is provably within grace_hours.
        On any decode error, returns False (assume valid) to avoid
        unnecessary renewal storms.
        """
        import base64
        t = self.load_token()
        if not t or "." not in t:
            return True
        try:
            p = t.split(".")[1]
            p += "=" * (-len(p) % 4)
            payload = json.loads(base64.urlsafe_b64decode(p))
            exp = payload.get("exp", 0)
            if exp == 0:
                return False  # no exp claim — assume valid
            return (exp - time.time()) <= grace_hours * 3600
        except Exception:
            return False  # decode error — assume valid, don't trigger renewal

    def renew_token(self) -> str:
        """Auto-renew Dhan token using PIN + TOTP (no browser needed).

        Thread-safe and process-wide rate-limited: only one renewal can happen
        at a time across ALL DhanRESTClient instances, respecting Dhan's
        ~2-minute per-client generation limit.  When rate-limited we fall back
        to the cached/on-disk token if it is still JWT-valid, so a fresh token
        is reused instead of lost.  Returns "" only if no usable token exists.
        """
        global _RENEW_LAST_ATTEMPT

        def _fallback() -> str:
            tok = self._token_cache or self.load_token()
            if tok and not self.token_expires_soon(grace_hours=1.0):
                return tok
            return self._token_cache or ""

        # Unique per-instance cooldown value; the shared cooldown is global.
        cooldown = self._renew_cooldown

        with self._renew_lock:
            # Local instance guard: another thread already minted for us.
            now_local = time.monotonic()
            if (now_local - self._last_renew_attempt) < cooldown:
                print("[auth] rate-limited (local), wait %ds"
                      % int(cooldown - (now_local - self._last_renew_attempt)),
                      flush=True)
                return _fallback()

            # Process-wide guard: some OTHER adapter just minted.  If it minted
            # within the cooldown we must not mint again (Dhan 2-min limit) --
            # reuse the fresh shared token from disk instead.
            with _RENEW_GLOBAL_LOCK:
                now_global = time.monotonic()
                if (now_global - _RENEW_LAST_ATTEMPT) < cooldown:
                    print("[auth] rate-limited (global), wait %ds"
                          % int(cooldown - (now_global - _RENEW_LAST_ATTEMPT)),
                          flush=True)
                    return _fallback()
                _RENEW_LAST_ATTEMPT = now_global
            self._last_renew_attempt = now_global

            if not self.pin or not self.totp_secret:
                print("[auth] no PIN/TOTP configured, cannot auto-renew",
                      flush=True)
                return ""
            try:
                import pyotp
                from dhanhq import DhanLogin
                remaining = 30 - (int(time.time()) % 30)
                if remaining < 7:
                    print("[auth] waiting %ds for fresh TOTP window..." % (remaining + 1), flush=True)
                    time.sleep(remaining + 1)
                totp = pyotp.TOTP(self.totp_secret).now()
                dhan_login = DhanLogin(self.client_id)
                result = dhan_login.generate_token(self.pin, totp)
                new_tok = result.get("accessToken", "")
                if new_tok:
                    if self.token_file:
                        with open(self.token_file, "w") as f:
                            json.dump({"access_token": new_tok}, f, indent=2)
                    self._token_cache = new_tok
                    self._token_ts = time.monotonic()
                    self._headers_cache = None
                    print("[auth] token renewed via TOTP, expires %s" % result.get("expiryTime", "?"), flush=True)
                    return new_tok
                print("[auth] renew failed: %s" % result, flush=True)
            except Exception as e:
                print("[auth] renew error: %s" % e, flush=True)
        return ""

    def ensure_token(self) -> str:
        """Get valid token, auto-renew only if missing or JWT expiring.

        Does NOT validate with an API call — that wastes a request and
        can cause unnecessary renewals on transient network errors.
        JWT expiry is the source of truth.
        """
        t = self.load_token()
        if not t:
            print("[auth] no token found, auto-renewing...", flush=True)
            return self.renew_token()
        if self.token_expires_soon(grace_hours=1.0):
            print("[auth] token expiring within 1h, auto-renewing...", flush=True)
            return self.renew_token()
        # Token exists and JWT says it's valid — trust it
        return t

    # ── Proactive Token Renewal Scheduler ──────────────────────────────────

    def start_scheduler(self) -> None:
        """Start background scheduler for proactive token renewal.

        Schedule:
          - On startup: immediate ensure_token()
          - Every day at 7 AM: proactive renewal (before 24h expiry)
          - Every 6 hours: safety check (renew if expiring within 2h)
        """
        if self._scheduler_thread is not None and self._scheduler_thread.is_alive():
            return
        self._scheduler_stop.clear()
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop, daemon=True, name="token-scheduler"
        )
        self._scheduler_thread.start()
        print("[auth] token scheduler started (daily 7 AM + 6h safety)", flush=True)

    def stop_scheduler(self) -> None:
        """Stop the token scheduler."""
        self._scheduler_stop.set()
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
        print("[auth] token scheduler stopped", flush=True)

    def _scheduler_loop(self) -> None:
        """Background loop: renew at 7 AM daily + safety check every 6 hours.

        Does NOT call ensure_token() on startup — the caller (adapter/engine)
        already does that. This avoids double-renewal.
        """
        self._last_safety_check = time.monotonic()

        while not self._scheduler_stop.is_set():
            try:
                now = datetime.now(IST)
                hour = now.hour
                minute = now.minute

                # Daily 7 AM renewal: renew at 7:00:30 (30s delay for TOTP window)
                if hour == self._renew_hour and minute == self._renew_minute:
                    print("[auth] scheduler: 7 AM daily renewal...", flush=True)
                    self.renew_token()
                    # Sleep until 7:01 to avoid double-fire
                    time.sleep(60)
                    self._last_safety_check = time.monotonic()
                    continue

                # Safety check every 6 hours
                elapsed = time.monotonic() - self._last_safety_check
                if elapsed >= self._safety_check_hours * 3600:
                    self._last_safety_check = time.monotonic()
                    if self.token_expires_soon(grace_hours=2.0):
                        print("[auth] scheduler: safety renewal (token expiring)...", flush=True)
                        self.renew_token()
                    else:
                        token = self.load_token()
                        if token:
                            print("[auth] scheduler: safety check OK (token valid)", flush=True)
                        else:
                            print("[auth] scheduler: no token found, renewing...", flush=True)
                            self.renew_token()

                # Sleep 30 seconds between checks
                self._scheduler_stop.wait(30)

            except Exception as e:
                print(f"[auth] scheduler error: {e}", flush=True)
                self._scheduler_stop.wait(60)

    @property
    def scheduler_running(self) -> bool:
        return self._scheduler_thread is not None and self._scheduler_thread.is_alive()

    # ── End Scheduler ──────────────────────────────────────────────────────

    def _headers(self) -> dict[str, str]:
        """Get request headers with cached token.

        Trust the cached token for the full 30s window.
        Only renew on actual auth failure in _post(), not proactively here.
        This avoids overlapping with the scheduler's renewal.
        """
        now = time.monotonic()
        if self._headers_cache is not None and (now - self._headers_ts) < 30:
            return self._headers_cache

        t = self.load_token()
        if not t:
            # No cached token and none on disk — try to get one
            t = self.renew_token()
        if not t:
            # Boot race: the auth scheduler can hold the global TOTP mint
            # rate-limit (2 per minute) exactly when warmup makes its first
            # historical call, and a rate-limited mint returns None.  The
            # on-disk token is usually still VALID, so re-read it a few times
            # before declaring the session unauthenticated -- otherwise warmup
            # dies at boot with "no access token" while a usable token sits on
            # disk.
            for _ in range(6):
                time.sleep(5)
                t = self.load_token()
                if t:
                    break
        if not t:
            raise DhanAuthError("no access token (auto-renew failed)")

        self._headers_cache = {
            "access-token": t,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._headers_ts = now
        return self._headers_cache

    def _post(self, path: str, payload: dict, retry_network: bool = True,
              extra_headers: Optional[dict] = None) -> dict:
        """Make rate-limited POST request with retry and auto-renew on auth failure.

        ``retry_network=False`` is for ORDER PLACEMENT: a network exception
        leaves the broker outcome UNKNOWN, so this layer must NOT blindly
        re-POST (it could create a duplicate order).  The transport recovers
        such outcomes with a correlation-id lookup one layer up; any error
        that escapes here (HTTP-status exhaustion included) is resolved there.

        ``extra_headers`` merges per-request headers (e.g. the ``client-id``
        header the /marketfeed/* data APIs require on top of ``access-token``).
        """
        self.limiter.acquire()
        last = None
        auth_retried = False
        # Order placement calls use retry_network=False: NEVER blind-re-POST on
        # network outcomes (broker result unknown -> correlation lookup owns it).
        # A status that PROVES the request never reached the broker order engine
        # is safe to retry instead: 401 / token-invalid (rejected before RMS
        # validation) renews once, 429 (gateway rate-limiter) backs off.  Re-POST
        # in those paths cannot create a duplicate order.
        total_allowed = self.max_retries if retry_network else 3

        attempt = 0
        while True:
            attempt += 1
            try:
                headers = self._headers()
                if extra_headers:
                    headers = {**headers, **extra_headers}
                r = self._session.post(
                    f"{self.base_url}{path}",
                    json=payload,
                    headers=headers,
                    timeout=30,
                )
            except requests.RequestException as e:
                last = e
                if not retry_network:
                    break
                if attempt >= total_allowed:
                    break
                time.sleep(1.0)
                continue
            if r.status_code == 200:
                j = r.json()
                if j.get("errorType") == "Authentication_Failed":
                    if auth_retried or attempt >= total_allowed:
                        raise DhanAuthError(f"{path}: {j}")
                    auth_retried = True
                    print("[auth] Authentication_Failed, renewing token...", flush=True)
                    self._headers_cache = None
                    self.renew_token()
                    continue
                with self._stats_lock:
                    self._stats["ok"] += 1
                return j
            if r.status_code == 429:
                if attempt >= total_allowed:
                    last = DhanHTTPError(r.status_code, r.text,
                                         {"errorType": "RateLimit",
                                          "errorCode": "RATE_LIMITED"})
                    break
                with self._stats_lock:
                    self._stats["retry"] += 1
                time.sleep(2.0)
                continue
            # Auto-renew only on GENUINE auth errors (401, token-invalid 400s).
            # Order rejects like 400 DH-906 intentionally fall through: they are
            # final business errors that must surface unchanged to the caller.
            is_auth_error = _is_auth_response(r.status_code, r.text)
            if is_auth_error:
                if auth_retried or attempt >= total_allowed:
                    raise DhanAuthError(f"{path}: {r.text[:200]}")
                auth_retried = True
                print(f"[auth] {r.status_code} auth error, renewing token...", flush=True)
                self._headers_cache = None
                self.renew_token()
                continue

            try:
                dhan_body = r.json()
            except Exception:
                dhan_body = r.text
            last = DhanHTTPError(r.status_code, r.text, dhan_body)
            if not retry_network or attempt >= total_allowed:
                break
            time.sleep(1.0)

        raise last or RuntimeError(f"{path} failed")

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Make a rate-limited GET request with retry + auto-renew on auth failure."""
        self.limiter.acquire()
        last = None
        auth_retried = False

        for attempt in range(self.max_retries):
            try:
                r = self._session.get(
                    f"{self.base_url}{path}",
                    params=params,
                    headers=self._headers(),
                    timeout=30,
                )
            except requests.RequestException as e:
                last = e
                time.sleep(1.0)
                continue

            if r.status_code == 200:
                j = r.json()
                if isinstance(j, list):
                    with self._stats_lock:
                        self._stats["ok"] += 1
                    return j
                if j.get("errorType") == "Authentication_Failed":
                    if not auth_retried:
                        auth_retried = True
                        print("[auth] Authentication_Failed, renewing token...", flush=True)
                        self._headers_cache = None
                        self.renew_token()
                        continue
                    raise DhanAuthError(f"{path}: {j}")
                with self._stats_lock:
                    self._stats["ok"] += 1
                return j

            if r.status_code == 429:
                with self._stats_lock:
                    self._stats["retry"] += 1
                time.sleep(2.0)
                continue

            is_auth_error = _is_auth_response(r.status_code, r.text)
            if is_auth_error:
                if not auth_retried:
                    auth_retried = True
                    print(f"[auth] {r.status_code} auth error, renewing token...", flush=True)
                    self._headers_cache = None
                    self.renew_token()
                    continue
                raise DhanAuthError(f"{path}: {r.text[:200]}")

            try:
                dhan_body = r.json()
            except Exception:
                dhan_body = r.text
            last = DhanHTTPError(r.status_code, r.text, dhan_body)
            time.sleep(1.0)

        raise last or RuntimeError(f"{path} failed")

    def _put(self, path: str, payload: Optional[dict] = None) -> dict:
        """Rate-limited PUT with the same retry + auto-renew housekeeping as _post."""
        return self._mutate("put", path, payload)

    def _delete(self, path: str, payload: Optional[dict] = None) -> dict:
        """Rate-limited DELETE with the same retry + auto-renew housekeeping as _post."""
        return self._mutate("delete", path, payload)

    def _mutate(self, method: str, path: str, payload: Optional[dict] = None) -> dict:
        """Shared implementation for PUT/DELETE (order modify / cancel)."""
        self.limiter.acquire()
        last = None
        auth_retried = False
        resp = getattr(self._session, method)

        for attempt in range(self.max_retries):
            try:
                r = resp(
                    f"{self.base_url}{path}",
                    json=payload if payload is not None else {},
                    headers=self._headers(),
                    timeout=30,
                )
            except requests.RequestException as e:
                last = e
                time.sleep(1.0)
                continue

            if r.status_code == 200:
                j = r.json()
                if j.get("errorType") == "Authentication_Failed":
                    if not auth_retried:
                        auth_retried = True
                        self._headers_cache = None
                        self.renew_token()
                        continue
                    raise DhanAuthError(f"{path}: {j}")
                with self._stats_lock:
                    self._stats["ok"] += 1
                return j

            if r.status_code == 429:
                with self._stats_lock:
                    self._stats["retry"] += 1
                time.sleep(2.0)
                continue

            is_auth_error = _is_auth_response(r.status_code, r.text)
            if is_auth_error:
                if not auth_retried:
                    auth_retried = True
                    self._headers_cache = None
                    self.renew_token()
                    continue
                raise DhanAuthError(f"{path}: {r.text[:200]}")

            try:
                dhan_body = r.json()
            except Exception:
                dhan_body = r.text
            last = DhanHTTPError(r.status_code, r.text, dhan_body)
            time.sleep(1.0)

        raise last or RuntimeError(f"{path} failed")

    def _to_candles(self, j: dict) -> list[list]:
        """Convert API response to candle list format."""
        ts = j.get("timestamp") or []
        if not ts:
            with self._stats_lock:
                self._stats["empty"] += 1
            return []
        return [
            [t, o, h, l, c, v]
            for t, o, h, l, c, v in zip(
                j["timestamp"],
                j["open"],
                j["high"],
                j["low"],
                j["close"],
                j.get("volume") or [0] * len(ts),
            )
        ]

    def fetch_daily(
        self,
        security_id: str,
        from_date: datetime.date,
        to_date: datetime.date,
        exchange_segment: str = "MCX_COMM",
        instrument: str = "FUTCOM",
    ) -> list[list]:
        """Fetch daily OHLC candles."""
        j = self._post("/charts/historical", {
            "securityId": str(security_id),
            "exchangeSegment": exchange_segment,
            "instrument": instrument,
            "expiryCode": 0,
            "oi": False,
            "fromDate": from_date.isoformat(),
            "toDate": to_date.isoformat(),
        })
        return self._to_candles(j)

    def fetch_intraday(
        self,
        security_id: str,
        interval: str,
        from_dt: datetime.datetime,
        to_dt: datetime.datetime,
        exchange_segment: str = "MCX_COMM",
        instrument: str = "FUTCOM",
    ) -> list[list]:
        """Fetch intraday candles (max 90 days per call)."""
        j = self._post("/charts/intraday", {
            "securityId": str(security_id),
            "exchangeSegment": exchange_segment,
            "instrument": instrument,
            "interval": interval,
            "oi": False,
            "fromDate": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "toDate": to_dt.strftime("%Y-%m-%d %H:%M:%S"),
        })
        candles = self._to_candles(j)
        # Dhan /charts/intraday timestamps are genuine POSIX epochs of the IST-
        # aligned bucket start (verified against live data: the API returns the
        # same value as int(time.time()) // bucket * bucket for the forming bar,
        # and IST-hour boundaries for native candles). No offset applied.
        return candles

    def backfill_intraday(
        self,
        security_id: str,
        interval: str,
        exchange_segment: str = "MCX_COMM",
        instrument: str = "FUTCOM",
    ) -> list[list]:
        # Backfill intraday data from contract listing to today (IST).
        today = datetime.now(IST)
        total_candles = []

        probe = today - timedelta(days=90)
        first = None

        # Find first available candle
        for _ in range(60):  # up to ~15 years
            candles = self.fetch_intraday(
                security_id, interval, probe,
                probe + timedelta(days=90),
                exchange_segment, instrument,
            )
            if candles:
                first = candles[0][0]
                break
            if (today - probe).days > 365 * 5:
                break
            probe -= timedelta(days=90)

        if first is None:
            return []

        # Sweep forward from first known candle
        cursor = datetime.fromtimestamp(first, tz=IST)
        while cursor < today:
            to_dt = min(cursor + timedelta(days=90), today)
            candles = self.fetch_intraday(
                security_id, interval, cursor, to_dt,
                exchange_segment, instrument,
            )
            if candles:
                total_candles.extend(candles)
            cursor = to_dt

        return total_candles

    def calculate_margin(
        self,
        security_id: str,
        exchange_segment: str,
        transaction_type: str,
        quantity: int,
        product_type: str,
        price: float,
        security_type: str = "FUTCOM",
    ) -> dict:
        """Calculate actual margin required via Dhan margin calculator API.

        Returns dict with totalMargin, spanMargin, exposureMargin, brokerage, leverage.
        Returns empty dict on failure (caller should fallback to estimated margin).
        """
        payload = {
            "dhanClientId": self.client_id,
            "exchangeSegment": exchange_segment,
            "transactionType": transaction_type,
            "quantity": quantity,
            "productType": product_type,
            "securityId": str(security_id),
            "price": float(price),
        }
        try:
            result = self._post("/margincalculator", payload)
            if "totalMargin" in result:
                return {
                    "totalMargin": float(result.get("totalMargin", 0)),
                    "spanMargin": float(result.get("spanMargin", 0)),
                    "exposureMargin": float(result.get("exposureMargin", 0)),
                    "brokerage": float(result.get("brokerage", 0)),
                    "leverage": result.get("leverage", "1"),
                }
            return {}
        except Exception as e:
            print(f"[Margin] API call failed: {e}", flush=True)
            return {}

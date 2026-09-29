"""Dhan MCX scrip-master resolver for contract rollover.

The Dhan REST APIs address MCX futures by an opaque ``securityId`` that changes
every contract month.  To roll the GOLDM/SILVERM series automatically at expiry
−1 trading day we resolve the next month's trading symbol + security_id +
expiry from Dhan's public scrip master (the CSV published at
``https://images.dhan.co/api-data/api-scrip-master.csv``).

Contract rows we consume (exchange ``MCX``, instrument ``FUTCOM``)::

    MCX,M,571445,FUTCOM,0,GOLDM-05Nov2026-FUT,1.0,GOLDM NOV FUT,
    2026-11-05 23:30:00,0.00000,XX,100.0000,M,FUTCOM,2,GOLDM

Only parse; never orders.  Offline-tolerant: a stale/corrupt cache degrades to
"cannot resolve" (the rollover decider then blocks the metal and alerts instead
of guessing a security id).
"""
from __future__ import annotations

import csv
import io
import logging
import time
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# Header columns of the Dhan api-scrip-master.csv (stable since 2024).
HDR_EXCH = "SEM_EXM_EXCH_ID"
HDR_SEGMENT = "SEM_SEGMENT"
HDR_SECURITY_ID = "SEM_SMST_SECURITY_ID"
HDR_INSTRUMENT_NAME = "SEM_INSTRUMENT_NAME"
HDR_TRADING_SYMBOL = "SEM_TRADING_SYMBOL"
HDR_LOT_UNITS = "SEM_LOT_UNITS"
HDR_EXPIRY_DATE = "SEM_EXPIRY_DATE"
HDR_TICK_SIZE = "SEM_TICK_SIZE"
HDR_EXCH_INSTRUMENT_TYPE = "SEM_EXCH_INSTRUMENT_TYPE"
HDR_SM_SYMBOL = "SM_SYMBOL_NAME"

EXPIRY_FORMAT = "%Y-%m-%d %H:%M:%S"

_KNOWN_ASSETS = ("GOLDM", "SILVERM")


class FutureContract:
    """One MCX futures series extracted from the Dhan scrip master."""

    __slots__ = ("asset", "security_id", "trading_symbol", "expiry",
                 "lot_size", "tick_size")

    def __init__(self, asset: str, security_id: str, trading_symbol: str,
                 expiry: datetime, lot_size: float = 1.0, tick_size: float = 0.0):
        self.asset = asset
        self.security_id = str(security_id)
        self.trading_symbol = trading_symbol
        self.expiry = expiry
        self.lot_size = lot_size
        self.tick_size = tick_size

    @property
    def expiry_date(self) -> date:
        return self.expiry.date()

    def config_symbol(self) -> str:
        """Stable config-style identifier ``MCX:GOLDM202610`` from the expiry."""
        return f"MCX:{self.asset}{self.expiry:%Y%m}"

    def to_dict(self) -> dict:
        return {
            "asset": self.asset,
            "security_id": self.security_id,
            "trading_symbol": self.trading_symbol,
            "expiry": self.expiry.isoformat(),
            "lot_size": self.lot_size,
            "tick_size": self.tick_size,
            "config_symbol": self.config_symbol(),
        }

    def __repr__(self) -> str:
        return (f"FutureContract({self.asset} {self.trading_symbol} "
                f"sid={self.security_id} expiry={self.expiry})")


def parse_scrip_master(text: str) -> list[FutureContract]:
    """Parse the raw scrip-master CSV into MCX FUTCOM contracts.

    Rows without a usable expiry or outside the known assets are skipped.
    """
    contracts: list[FutureContract] = []
    reader = csv.DictReader(io.StringIO(text or ""))
    if not reader.fieldnames:
        return contracts
    for row in reader:
        try:
            exch = (row.get(HDR_EXCH) or "").strip().upper()
            if exch != "MCX":
                continue
            inst_type = (row.get(HDR_EXCH_INSTRUMENT_TYPE) or "").strip().upper()
            inst_name = (row.get(HDR_INSTRUMENT_NAME) or "").strip().upper()
            if inst_type != "FUTCOM" and inst_name != "FUTCOM":
                continue
            asset = (row.get(HDR_SM_SYMBOL) or "").strip().upper()
            if asset not in _KNOWN_ASSETS:
                continue
            security_id = (row.get(HDR_SECURITY_ID) or "").strip()
            expiry_raw = (row.get(HDR_EXPIRY_DATE) or "").strip()
            if not security_id or not expiry_raw:
                continue
            expiry = datetime.strptime(expiry_raw, EXPIRY_FORMAT)
            contracts.append(FutureContract(
                asset=asset,
                security_id=security_id,
                trading_symbol=(row.get(HDR_TRADING_SYMBOL) or "").strip(),
                expiry=expiry,
                lot_size=float(row.get(HDR_LOT_UNITS) or "0" or 0),
                tick_size=float(row.get(HDR_TICK_SIZE) or "0" or 0),
            ))
        except (ValueError, TypeError):
            # A single malformed row must never poison the whole series table.
            continue
    contracts.sort(key=lambda c: (c.asset, c.expiry))
    return contracts


class ScripMasterClient:
    """Cached, offline-tolerant Dhan scrip-master client.

    ``resolve(text=None)`` prefers a caller-supplied payload (tests), then the
    on-disk cache, then the live URL.  Never raises on a failed fetch: it
    degrades to ``futures()`` returning whatever cache we still hold.
    """

    def __init__(
        self,
        url: str = "https://images.dhan.co/api-data/api-scrip-master.csv",
        cache_path: Optional[str] = None,
        refresh_hours: float = 24.0,
        timeout_seconds: float = 60.0,
        fetch: Optional[callable] = None,
    ):
        self.url = url
        self.cache_path = Path(cache_path) if cache_path else None
        self.refresh_hours = float(refresh_hours)
        self.timeout_seconds = float(timeout_seconds)
        self._fetch = fetch  # injected fetch(text_callback) for tests/transport
        self._contracts: Optional[list[FutureContract]] = None
        self._last_error: Optional[str] = None
        self._next_network_attempt: float = 0.0
        self._last_download_epoch: float = 0.0

    # ── loading ────────────────────────────────────────────────────────

    def _cache_stale(self, now: float) -> bool:
        if self.cache_path is None or not self.cache_path.exists():
            return True
        age_h = (time.time() - self.cache_path.stat().st_mtime) / 3600.0
        return age_h > self.refresh_hours

    def _read_cache(self) -> Optional[str]:
        if self.cache_path is None or not self.cache_path.exists():
            return None
        if self._cache_stale(time.time()):
            return None  # stale -> needs refresh, keep file for fallback
        try:
            return self.cache_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None

    def _write_cache(self, text: str) -> None:
        if self.cache_path is None:
            return
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.cache_path.with_suffix(".csv.tmp")
            tmp.write_text(text, encoding="utf-8")
            tmp.replace(self.cache_path)
        except OSError as e:
            log.warning("[ScripMaster] cache write failed: %s", e)

    def resolve(self) -> bool:
        """Populate/refresh the contract table.  False when NO data is known.

        A populated table is used until the cache turns stale; a stale cache
        triggers one download attempt (with a 10-minute backoff after a
        failure), so a transient outage retries at the next tick instead of
        wedging the metal in "cannot resolve" forever.
        """
        now = time.time()
        if self._contracts is not None and not self._cache_stale(now):
            return True
        text = None
        if self._fetch is not None:
            try:
                text = self._fetch()
            except Exception as e:
                self._last_error = f"fetch failed: {e}"
                log.warning("[ScripMaster] %s", self._last_error)
        if not text:
            text = self._read_cache()
            if text:
                log.info("[ScripMaster] using on-disk cache %s", self.cache_path)
        if not text and self.url and now >= self._next_network_attempt:
            try:
                import urllib.request
                req = urllib.request.Request(self.url, headers={
                    "User-Agent": "mcx-trader-scripmaster/1.0"})
                with urllib.request.urlopen(req, timeout=self.timeout_seconds) as r:
                    text = r.read().decode("utf-8", errors="replace")
                self._last_download_epoch = time.time()
                log.info("[ScripMaster] downloaded %d bytes", len(text))
            except Exception as e:
                self._last_error = str(e)
                log.warning("[ScripMaster] download failed: %s", e)
                self._next_network_attempt = now + 600.0
        if text:
            contracts = parse_scrip_master(text)
            if contracts:
                self._contracts = contracts
                self._last_error = None
                self._next_network_attempt = now + self.refresh_hours * 3600.0
                if self._read_cache() is not None:
                    return True
                self._write_cache(text)
                return True
            self._last_error = "parsed 0 MCX FUTCOM contracts"
        return self._contracts is not None

    # ── queries ────────────────────────────────────────────────────────

    def futures(self, asset: str) -> list[FutureContract]:
        """All known series for one asset, sorted by expiry ascending."""
        if not self.resolve():
            return []
        return [c for c in (self._contracts or []) if c.asset == asset]

    def find_symbol(self, asset: str, config_symbol: str) -> Optional[FutureContract]:
        """The contract matching a config-style symbol (``MCX:GOLDM202610``)."""
        wanted = str(config_symbol).strip().upper()
        for c in self.futures(asset):
            if c.config_symbol().upper() == wanted:
                return c
        return None

    def next_contract(self, asset: str, current: FutureContract,
                      as_of: Optional[date] = None) -> Optional[FutureContract]:
        """The next tradable series strictly after ``current``.

        Prefers the soonest series whose expiry is strictly after the current
        contract's expiry; when the current contract is already its expiry the
        next series is the immediately following one.
        """
        today = as_of or datetime.now(IST).date()
        candidates = [
            c for c in self.futures(asset)
            if c.expiry_date > current.expiry_date and c.expiry_date >= today
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda c: c.expiry)

    def last_error(self) -> Optional[str]:
        return self._last_error

    def status(self) -> dict:
        return {
            "contracts_parsed": len(self._contracts or []),
            "cache_path": str(self.cache_path) if self.cache_path else None,
            "last_error": self._last_error,
        }
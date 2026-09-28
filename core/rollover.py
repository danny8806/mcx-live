"""Contract rollover state machine (GOLDM/SILVERM MCX futures).

Role
----
Decide, once per tick and per metal, how the LIVE engine must behave as a
broker contract approaches expiry, and persist the contract OVERRIDE that the
next engine boot applies so trading continues in the next series.

Policy (operator-chosen)
------------------------
* ROLLOVER WINDOW opens at ``last_session - trigger_days`` trading days
  (default 1).  From then on new entries in the EXPIRING series are blocked
  (resting entries are cancelled at window open) and the metal waits to go
  flat — the open trade exits naturally (SL/TP/reversal).
* Once flat inside the window the system resolves the next series from the
  Dhan scrip master (symbol + security_id + expiry) and PERSISTS the override;
  the contract is switched at the next engine boot (full re-warmup in the new
  series happens before the engine goes READY).  The metal stays entry-blocked
  for the rest of the current session.
* If still holding at the START of the last trading session
  (``last_session``), the engine force-closes the expiring series with MARKET
  closes (the fallback the operator chose so nothing rides into expiry).
* If the next series cannot be resolved, entries stay blocked and alerts fire
  — the engine never guesses a security id.

The decider is pure and unit-testable; the engine (``TradingEngine``) is the
only actor that calls :meth:`evaluate` and performs the broker actions.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from data.dhan.scrip_master import FutureContract, ScripMasterClient

log = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# Phase names persisted in the state file.
PHASE_NORMAL = "NORMAL"
PHASE_WINDOW = "WINDOW_BLOCK"
PHASE_SWITCHED = "SWITCHED"


def is_trading_day(d: date, holidays: frozenset[date] = frozenset()) -> bool:
    """Weekday (Mon-Fri) and not a configured holiday."""
    if d.weekday() >= 5:
        return False
    return d not in holidays


def trading_day_before(d: date, holidays: frozenset[date] = frozenset()) -> date:
    """The most recent trading day strictly before ``d``."""
    d = d - timedelta(days=1)
    while not is_trading_day(d, holidays):
        d -= timedelta(days=1)
    return d


def trading_days_before(expiry: date, n: int,
                        holidays: frozenset[date] = frozenset()) -> date:
    """The trading day ``n`` trading days before ``expiry`` (n >= 1)."""
    d = expiry
    for _ in range(max(1, int(n))):
        d = trading_day_before(d, holidays)
    return d


def last_trading_session(expiry: date, holidays: frozenset[date] = frozenset()) -> date:
    """The final trading session of a contract = its expiry date rolled back to
    the previous trading day when the expiry falls on a weekend/holiday."""
    if is_trading_day(expiry, holidays):
        return expiry
    return trading_day_before(expiry, holidays)


def resolve_window(expiry: date,
                   trigger_days: int = 1,
                   holidays: frozenset[date] = frozenset()) -> tuple[date, date]:
    """(window_open, last_session) for a contract expiry.

    ``last_session`` is the contract's final trading session; ``window_open``
    is ``trigger_days`` trading days before it.  The window includes the last
    session (force-close fallback lives there).
    """
    last_session = last_trading_session(expiry, holidays)
    window_open = trading_days_before(last_session, max(1, trigger_days), holidays)
    return window_open, last_session


class MetalRolloverRecord:
    """Per-metal persisted + runtime rollover state."""

    __slots__ = ("metal", "active_symbol", "active_security_id",
                 "prev_symbol", "prev_security_id", "switched_on",
                 "phase", "window_open", "last_session",
                 "expiry_date", "next_symbol", "next_security_id",
                 "force_close_sent", "alerts")

    def __init__(self, metal: str):
        self.metal = metal
        self.active_symbol: Optional[str] = None
        self.active_security_id: Optional[str] = None
        self.prev_symbol: Optional[str] = None
        self.prev_security_id: Optional[str] = None
        self.switched_on: Optional[str] = None  # ISO date when flat-switch happened
        self.phase: str = PHASE_NORMAL
        self.window_open: Optional[str] = None
        self.last_session: Optional[str] = None
        self.expiry_date: Optional[str] = None
        self.next_symbol: Optional[str] = None
        self.next_security_id: Optional[str] = None
        self.force_close_sent: bool = False
        self.alerts: dict[str, str] = {}  # kind -> ISO date last raised

    def to_dict(self) -> dict:
        return {
            "metal": self.metal,
            "active_symbol": self.active_symbol,
            "active_security_id": self.active_security_id,
            "prev_symbol": self.prev_symbol,
            "prev_security_id": self.prev_security_id,
            "switched_on": self.switched_on,
            "phase": self.phase,
            "window_open": self.window_open,
            "last_session": self.last_session,
            "expiry_date": self.expiry_date,
            "next_symbol": self.next_symbol,
            "next_security_id": self.next_security_id,
            "force_close_sent": self.force_close_sent,
            "alerts": dict(self.alerts),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "MetalRolloverRecord":
        rec = cls(str(d.get("metal", "")))
        rec.active_symbol = d.get("active_symbol")
        rec.active_security_id = d.get("active_security_id")
        rec.prev_symbol = d.get("prev_symbol")
        rec.prev_security_id = d.get("prev_security_id")
        rec.switched_on = d.get("switched_on")
        rec.phase = d.get("phase") or PHASE_NORMAL
        rec.window_open = d.get("window_open")
        rec.last_session = d.get("last_session")
        rec.expiry_date = d.get("expiry_date")
        rec.next_symbol = d.get("next_symbol")
        rec.next_security_id = d.get("next_security_id")
        rec.force_close_sent = bool(d.get("force_close_sent"))
        rec.alerts = dict(d.get("alerts") or {})
        return rec


class RolloverService:
    """Per-metal rollover decider + persistence.

    ``emit(kind, data)`` and ``on_alert(dict)`` are engine-bound callbacks
    (event bus + telegram).  All decisions are pure over
    ``today``/``has_positions``/scrip-master data; the engine performs the
    resulting actions exactly once.
    """

    def __init__(
        self,
        config: Optional[dict] = None,
        state_path: Optional[str] = None,
        scrip_master: Optional[ScripMasterClient] = None,
        clock: Optional[callable] = None,
        emit: Optional[Callable[[str, dict], None]] = None,
        on_alert: Optional[Callable[[dict], None]] = None,
    ):
        cfg = config or {}
        self.enabled = bool(cfg.get("enabled", False))
        self.trigger_days = int(cfg.get("trigger_days_before_expiry", 1) or 1)
        self.block_on_window = bool(cfg.get("block_entries_on_window", True))
        self.cancel_on_window = bool(cfg.get("cancel_resting_on_window", True))
        self.force_close_on_last = bool(cfg.get("force_close_on_last_session", True))
        sm_cfg = cfg.get("scrip_master") or {}
        self.holidays: frozenset[date] = frozenset()
        raw_holidays = cfg.get("holidays") or []
        parsed_holidays = set()
        for h in raw_holidays:
            try:
                parsed_holidays.add(date.fromisoformat(str(h)[:10]))
            except ValueError:
                continue
        self.holidays = frozenset(parsed_holidays)
        self.state_path = Path(state_path) if state_path else Path("data/db/rollover_state.json")
        self.scrip = scrip_master or ScripMasterClient(
            url=sm_cfg.get("url", "https://images.dhan.co/api-data/api-scrip-master.csv"),
            cache_path=sm_cfg.get("cache_path") or str(self.state_path.with_suffix(".scrip.csv")),
            refresh_hours=float(sm_cfg.get("refresh_hours", 24.0)),
            timeout_seconds=float(sm_cfg.get("timeout_seconds", 60.0)),
        )
        self._clock = clock or time.time
        self.emit = emit or (lambda kind, data: None)
        self.on_alert = on_alert or (lambda alert: None)
        self._lock = threading.RLock()
        self._records: dict[str, MetalRolloverRecord] = {}
        self._load()

    # ── persistence ────────────────────────────────────────────────────

    def _load(self) -> None:
        try:
            if self.state_path.exists():
                raw = json.loads(self.state_path.read_text(encoding="utf-8"))
                for metal, d in (raw or {}).items():
                    if isinstance(d, dict):
                        self._records[str(metal)] = MetalRolloverRecord.from_dict(d)
        except (OSError, ValueError, TypeError) as e:
            log.warning("[Rollover] state load failed (%s); starting fresh", e)
            self._records = {}

    def _save(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps({
                metal: rec.to_dict() for metal, rec in self._records.items()
            }, indent=2), encoding="utf-8")
            tmp.replace(self.state_path)
        except OSError as e:
            log.warning("[Rollover] state save failed: %s", e)

    def record(self, metal: str) -> MetalRolloverRecord:
        return self._records.setdefault(metal, MetalRolloverRecord(metal))

    def overrides(self) -> dict[str, dict]:
        """Persisted overrides the engine applies at the next boot."""
        out: dict[str, dict] = {}
        for metal, rec in self._records.items():
            if rec.active_symbol and rec.active_security_id:
                out[metal] = {
                    "active_symbol": rec.active_symbol,
                    "active_security_id": rec.active_security_id,
                    "prev_symbol": rec.prev_symbol,
                    "prev_security_id": rec.prev_security_id,
                    "switched_on": rec.switched_on,
                }
        return out

    # ── alert helpers ──────────────────────────────────────────────────

    def _alert_once(self, rec: MetalRolloverRecord, kind: str, today: date,
                    message: str, **detail) -> None:
        iso = today.isoformat()
        if rec.alerts.get(kind) == iso:
            return
        rec.alerts[kind] = iso
        alert = {
            "severity": "WARNING",
            "type": f"contract_rollover_{kind}",
            "message": message,
            "instrument": rec.metal,
            **detail,
        }
        try:
            self.on_alert(alert)
        except Exception as e:  # an alert must never break the tick
            log.warning("[Rollover] alert failed: %s", e)
        self.emit("contract_rollover_alert", alert)

    # ── decider ────────────────────────────────────────────────────────

    def evaluate(self, metal: str, current_symbol: str, current_security_id: str,
                 today: date, has_positions: bool) -> list[str]:
        """Decide the actions the engine must take for one metal.

        Returns a list of action tokens:
          * ``block_entries`` — no new entries in the expiring series today
          * ``cancel_resting``   — cancel resting broker orders (once, window open)
          * ``force_close``      — MARKET-close the expiring series now
          * ``persist_switch``   — next series resolved; override persisted
          * ``unknown``          — scrip-master unavailable (alert; no blocking)
        Never raises: an unresolved series degrades to alerts + blocking.
        """
        if not self.enabled:
            return []
        rec = self.record(metal)
        rec.phase = PHASE_NORMAL

        # A +1-day daily check that nothing is left wedged from a previous
        # session (e.g. a boot without apply).
        contracts = self.scrip.futures(metal)
        if not contracts:
            self._alert_once(
                rec, "cannot_resolve", today,
                f"{metal}: Dhan scrip master unavailable — rollover cannot "
                f"proceed; entries left untouched", security_id=current_security_id)
            return ["unknown"]

        current = self.scrip.find_symbol(metal, current_symbol)
        if current is None:
            # Possible only with a stale/drifted config symbol; do not block
            # blindly — surface loudly so an operator fixes the ladder.
            self._alert_once(
                rec, "current_unknown", today,
                f"{metal}: current contract {current_symbol} not found in the "
                f"scrip master (sid={current_security_id})",
                security_id=current_security_id)
            return []

        expiry = current.expiry_date
        window_open, last_session = resolve_window(
            expiry, self.trigger_days, self.holidays)
        rec.expiry_date = expiry.isoformat()
        rec.window_open = window_open.isoformat()
        rec.last_session = last_session.isoformat()

        if today < window_open:
            rec.phase = PHASE_NORMAL
            rec.force_close_sent = False
            return []

        # ── ROLLOVER WINDOW (window_open <= today <= last_session) ──────
        rec.phase = PHASE_WINDOW

        # Already switched today?  The override was persisted; the current
        # config still points at the expiring series until the next boot, so
        # just keep entries blocked and back off.
        if rec.switched_on == today.isoformat():
            actions = []
            if self.block_on_window:
                actions.append("block_entries")
            return actions

        actions: list[str] = []
        if self.block_on_window:
            actions.append("block_entries")
        if self.cancel_on_window:
            actions.append("cancel_resting")

        if not has_positions:
            # Flat within the window -> attempt the switch to the next series.
            if rec.switched_on is not None and rec.switched_on != today.isoformat():
                # Already switched on a previous day but still evaluating the
                # OLD symbol?  Only possible if boot never applied the override
                # (operator error); keep the metal blocked and alert once.
                self._alert_once(
                    rec, "switch_not_applied", today,
                    f"{metal}: override persisted {rec.switched_on} but the boot "
                    f"never applied it — restart required to resume trading")
                return actions
            nxt = self.scrip.next_contract(metal, current, as_of=today)
            if nxt is None:
                self._alert_once(
                    rec, "no_next_series", today,
                    f"{metal}: flat in {current_symbol} but no NEXT series "
                    f"found — entries stay blocked")
                return actions
            # Persist the override (engine applies at next boot).
            with self._lock:
                rec.prev_symbol = current_symbol
                rec.prev_security_id = current_security_id
                rec.active_symbol = nxt.config_symbol()
                rec.active_security_id = nxt.security_id
                rec.next_symbol = nxt.config_symbol()
                rec.next_security_id = nxt.security_id
                rec.switched_on = today.isoformat()
                rec.phase = PHASE_SWITCHED
                self._save()
            self._alert_once(
                rec, "switched", today,
                f"{metal}: flat — rolling {current_symbol} -> {nxt.config_symbol()} "
                f"(sid {current_security_id} -> {nxt.security_id}); trading resumes "
                f"in the new series at the next engine boot. Entries stay blocked "
                f"until then.",
                next_symbol=nxt.config_symbol(), next_security_id=nxt.security_id)
            return actions + ["persist_switch"]
        else:
            # Still holding.
            if today >= last_session and self.force_close_on_last:
                if not rec.force_close_sent:
                    rec.force_close_sent = True
                    self._save()
                    actions.append("force_close")
                    self._alert_once(
                        rec, "force_close", today,
                        f"{metal}: STILL HOLDING on the final session "
                        f"{last_session} — force-closing the expiring series "
                        f"{current_symbol} at session open")
                else:
                    self._alert_once(
                        rec, "still_holding_last_day", today,
                        f"{metal}: STILL HOLDING on the final session "
                        f"{last_session} — repeated force-close in flight")
            return actions

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "trigger_days": self.trigger_days,
            "state_path": str(self.state_path),
            "records": {m: r.to_dict() for m, r in self._records.items()},
        }
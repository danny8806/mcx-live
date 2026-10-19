"""Contract rollover state machine (GOLDM/SILVERM MCX futures).

Role
----
Decide, once per tick and per metal, how the LIVE engine must behave as a
broker contract approaches expiry, and persist the contract OVERRIDE that the
next engine boot applies so trading continues in the next series.

Policy (operator-chosen)
------------------------
* The rollover window starts ``trigger_days`` trading days before the last
  trading session (default 3). New entries in the expiring series are blocked
  and resting orders are cancelled.
* The engine warms and verifies the next series before closing a local
  position. Once Dhan confirms the old contract is flat and all its orders are
  terminal, the engine promotes the staged series and submits one carry entry
  using the newly warmed SuperTrend direction and the old filled quantity.
* If the final-session close is required before candidate warmup succeeds, the
  old position is still journaled before the close. The contract switch and
  carry entry remain blocked until the candidate is ready and broker state is
  confirmed.
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
                 "force_close_sent", "alerts", "workflow_phase",
                 "roll_id", "candidate", "carry_positions", "last_error",
                 "verified_symbol", "verified_security_id")

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
        self.workflow_phase: str = "IDLE"
        self.roll_id: Optional[str] = None
        self.candidate: Optional[dict] = None
        self.carry_positions: list[dict] = []
        self.last_error: Optional[str] = None
        self.verified_symbol: Optional[str] = None
        self.verified_security_id: Optional[str] = None

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
            "workflow_phase": self.workflow_phase,
            "roll_id": self.roll_id,
            "candidate": dict(self.candidate or {}),
            "carry_positions": [dict(row) for row in self.carry_positions],
            "last_error": self.last_error,
            "verified_symbol": self.verified_symbol,
            "verified_security_id": self.verified_security_id,
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
        rec.workflow_phase = str(d.get("workflow_phase") or "IDLE")
        rec.roll_id = d.get("roll_id")
        rec.candidate = dict(d.get("candidate") or {}) or None
        rec.carry_positions = [dict(row) for row in d.get("carry_positions", [])
                               if isinstance(row, dict)]
        rec.last_error = d.get("last_error")
        rec.verified_symbol = d.get("verified_symbol")
        rec.verified_security_id = d.get("verified_security_id")
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
        self.trigger_days = int(cfg.get("trigger_days_before_expiry", 3) or 3)
        self.block_on_window = bool(cfg.get("block_entries_on_window", True))
        self.cancel_on_window = bool(cfg.get("cancel_resting_on_window", True))
        self.force_close_on_last = bool(cfg.get("force_close_on_last_session", True))
        sm_cfg = cfg.get("scrip_master") or {}
        # Use the same checked-in MCX calendar as MarketStatus. A second,
        # empty rollover-only holiday list silently shifted expiry windows
        # across full exchange closures.
        calendar_path = Path(cfg.get("calendar_path") or
                             (Path(__file__).resolve().parents[1]
                              / "config/mcx_calendar.json"))
        self.calendar_valid_through: Optional[str] = None
        calendar_holidays: set[date] = set()
        try:
            calendar = json.loads(calendar_path.read_text(encoding="utf-8"))
            self.calendar_valid_through = calendar.get("valid_through")
            for day, session in (calendar.get("sessions") or {}).items():
                if isinstance(session, dict) and session.get("closed") is True:
                    try:
                        calendar_holidays.add(date.fromisoformat(str(day)[:10]))
                    except ValueError:
                        continue
        except (OSError, ValueError, TypeError) as e:
            log.warning("[Rollover] MCX holiday calendar unavailable (%s); "
                        "using configured holidays only", e)
        raw_holidays = cfg.get("holidays") or []
        parsed_holidays = set(calendar_holidays)
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

    def _save(self) -> bool:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps({
                metal: rec.to_dict() for metal, rec in self._records.items()
            }, indent=2), encoding="utf-8")
            tmp.replace(self.state_path)
            return True
        except OSError as e:
            log.warning("[Rollover] state save failed: %s", e)
            return False

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

    def next_contract(self, metal: str, current_symbol: str,
                      as_of: date) -> Optional[FutureContract]:
        """Resolve the next contract through the same scrip-master source."""
        current = self.scrip.find_symbol(metal, current_symbol)
        if current is None:
            return None
        return self.scrip.next_contract(metal, current, as_of=as_of)

    def requires_broker_confirmation(self, metal: str, current_symbol: str,
                                     current_security_id: str,
                                     today: date) -> bool:
        """Whether this contract is inside its rollover window.

        Position and order REST checks are only needed once the rollover
        window opens. Unknown metadata is handled by ``evaluate`` as an
        entry-blocking error and must not trigger repeated broker polling.
        """
        if not self.enabled:
            return False
        current = self.scrip.find_symbol(metal, current_symbol)
        if current is not None:
            if str(current.security_id) != str(current_security_id):
                return False
            expiry = current.expiry_date
        else:
            rec = self.record(metal)
            if (rec.verified_symbol != current_symbol
                    or str(rec.verified_security_id or "")
                    != str(current_security_id) or not rec.expiry_date):
                # Unknown current-contract metadata must be reconciled with
                # the broker before the recovery path can switch to a fresh
                # master-listed series.
                return True
            try:
                expiry = date.fromisoformat(rec.expiry_date)
            except ValueError:
                return False
        window_open, _last_session = resolve_window(
            expiry, self.trigger_days, self.holidays)
        return today >= window_open

    def _verified_contract(self, rec: MetalRolloverRecord, metal: str,
                           symbol: str, security_id: str) -> Optional[FutureContract]:
        """Rebuild only a previously master-verified contract identity.

        Dhan can remove an expired contract from the current master. Retaining
        its exact symbol, security ID, and expiry lets the engine reconcile the
        old contract and select a later master-listed series without guessing.
        """
        if (rec.verified_symbol != symbol
                or str(rec.verified_security_id or "") != str(security_id)
                or not rec.expiry_date):
            return None
        try:
            expiry = datetime.fromisoformat(
                f"{date.fromisoformat(rec.expiry_date).isoformat()}T23:30:00+05:30")
        except ValueError:
            return None
        return FutureContract(metal, str(security_id), symbol, expiry)

    def _persist_switch(self, rec: MetalRolloverRecord, metal: str,
                        current_symbol: str, current_security_id: str,
                        nxt: FutureContract, today: date) -> bool:
        """Persist a verified flat rollover before allowing a new boot."""
        with self._lock:
            old = (rec.prev_symbol, rec.prev_security_id, rec.active_symbol,
                   rec.active_security_id, rec.next_symbol, rec.next_security_id,
                   rec.switched_on, rec.phase)
            rec.prev_symbol = current_symbol
            rec.prev_security_id = str(current_security_id)
            rec.active_symbol = nxt.config_symbol()
            rec.active_security_id = nxt.security_id
            rec.next_symbol = nxt.config_symbol()
            rec.next_security_id = nxt.security_id
            rec.switched_on = today.isoformat()
            rec.phase = PHASE_SWITCHED
            if not self._save():
                (rec.prev_symbol, rec.prev_security_id, rec.active_symbol,
                 rec.active_security_id, rec.next_symbol, rec.next_security_id,
                 rec.switched_on, rec.phase) = old
                self._alert_once(
                    rec, "state_persist_failed", today,
                    f"{metal}: rollover state could not be durably saved; "
                    "contract switch withheld")
                return False
        self._alert_once(
            rec, "switched", today,
            f"{metal}: flat — rolling {current_symbol} -> {nxt.config_symbol()} "
            f"(sid {current_security_id} -> {nxt.security_id}); the engine will "
            "promote the warmed contract and keep entries blocked until its "
            "broker state and carry workflow are verified.",
            next_symbol=nxt.config_symbol(), next_security_id=nxt.security_id)
        return True

    def _refresh_contract_master(self) -> None:
        """Request a rate-limited fresh ladder when rollover cannot resolve it."""
        refresh = getattr(self.scrip, "refresh_if_due", None)
        if not callable(refresh):
            return
        try:
            refresh()
        except Exception as e:
            log.warning("[Rollover] scrip-master refresh failed: %s", e)

    def workflow(self, metal: str) -> dict:
        """Return a detached snapshot of the persisted roll workflow."""
        rec = self.record(metal)
        return {
            "phase": rec.workflow_phase,
            "roll_id": rec.roll_id,
            "candidate": dict(rec.candidate or {}),
            "carry_positions": [dict(row) for row in rec.carry_positions],
            "last_error": rec.last_error,
        }

    def update_workflow(self, metal: str, *, phase: Optional[str] = None,
                        roll_id: Optional[str] = None,
                        candidate: Optional[dict] = None,
                        carry_positions: Optional[list[dict]] = None,
                        last_error: Optional[str] = None,
                        expected_phase: Optional[str] = None) -> bool:
        """Persist workflow state before broker actions; never ignore failure."""
        with self._lock:
            rec = self.record(metal)
            if (expected_phase is not None
                    and rec.workflow_phase.upper() != str(expected_phase).upper()):
                return False
            previous = (rec.workflow_phase, rec.roll_id,
                        dict(rec.candidate or {}),
                        [dict(row) for row in rec.carry_positions],
                        rec.last_error)
            if phase is not None:
                rec.workflow_phase = str(phase)
            if roll_id is not None:
                rec.roll_id = str(roll_id)
            if candidate is not None:
                rec.candidate = dict(candidate)
            if carry_positions is not None:
                rec.carry_positions = [dict(row) for row in carry_positions]
            rec.last_error = last_error
            if self._save():
                return True
            (rec.workflow_phase, rec.roll_id, rec.candidate,
             rec.carry_positions, rec.last_error) = previous
            return False

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

    def alert_once(self, metal: str, kind: str, today: date,
                   message: str, **detail) -> None:
        """Expose rate-limited rollover alerts for engine-side safety checks."""
        with self._lock:
            self._alert_once(self.record(metal), kind, today, message, **detail)

    # ── decider ────────────────────────────────────────────────────────

    def evaluate(self, metal: str, current_symbol: str, current_security_id: str,
                 today: date, has_positions: bool,
                 resting_orders_clear: bool = False,
                 candidate_ready: bool = True) -> list[str]:
        """Decide the actions the engine must take for one metal.

        Returns a list of action tokens:
          * ``block_entries`` — no new entries in the expiring series today
          * ``cancel_resting``   — cancel resting broker orders (once, window open)
          * ``force_close``      — MARKET-close the expiring series now
          * ``persist_switch``   — next series resolved; override persisted
          * ``unknown``          — contract metadata unavailable (alert)
        Never raises: uncertain contract metadata blocks new entries. A flat
        contract switch is withheld until resting orders have been verified
        terminal by the broker.
        """
        if not self.enabled:
            return []
        rec = self.record(metal)
        rec.phase = PHASE_NORMAL

        # A +1-day daily check that nothing is left wedged from a previous
        # session (e.g. a boot without apply).
        contracts = self.scrip.futures(metal)
        if not contracts:
            self._refresh_contract_master()
            contracts = self.scrip.futures(metal)
        if not contracts:
            self._alert_once(
                rec, "cannot_resolve", today,
                f"{metal}: Dhan scrip master unavailable — rollover cannot "
                f"proceed; new entries blocked", security_id=current_security_id)
            return ["unknown", "block_entries"]

        current = self.scrip.find_symbol(metal, current_symbol)
        if current is None:
            self._refresh_contract_master()
            current = self.scrip.find_symbol(metal, current_symbol)
        if current is None:
            current = self._verified_contract(
                rec, metal, current_symbol, current_security_id)
        if current is None:
            # Recovery for an expired/stale config whose old row has already
            # disappeared from Dhan's active master. We can safely advance to
            # the nearest listed series only after the exact old SID is flat
            # and its broker order book is clear. Open/unknown old exposure
            # remains blocked for manual reconciliation; never guess its exit.
            candidates = [
                contract for contract in contracts
                if contract.config_symbol().upper()
                != str(current_symbol).strip().upper()
                and contract.expiry_date >= today
            ]
            actions = ["unknown"]
            if self.block_on_window:
                actions.append("block_entries")
            if self.cancel_on_window:
                actions.append("cancel_resting")
            if not candidates:
                self._alert_once(
                    rec, "current_unknown", today,
                    f"{metal}: current contract {current_symbol} is absent "
                    "from Dhan's active master and no later series is listed; "
                    "new entries blocked",
                    security_id=current_security_id)
                return actions
            if has_positions:
                self._alert_once(
                    rec, "unknown_contract_position", today,
                    f"{metal}: the configured contract is absent from Dhan's "
                    "active master while an old-contract position/order may "
                    "exist; rollover is waiting for broker-flat and order-clear "
                    "confirmation",
                    security_id=current_security_id)
                return actions
            if self.cancel_on_window and not resting_orders_clear:
                return actions
            if not candidate_ready:
                self._alert_once(
                    rec, "candidate_not_ready", today,
                    f"{metal}: next contract is not warmed and verified; "
                    "contract switch is withheld")
                return actions
            nxt = min(candidates, key=lambda contract: contract.expiry)
            if self._persist_switch(
                    rec, metal, current_symbol, current_security_id, nxt, today):
                return actions + ["persist_switch"]
            return actions

        if str(current.security_id) != str(current_security_id):
            self._refresh_contract_master()
            current = self.scrip.find_symbol(metal, current_symbol)
        if current is None:
            current = self._verified_contract(
                rec, metal, current_symbol, current_security_id)
        if current is None:
            self._alert_once(
                rec, "current_unknown", today,
                f"{metal}: current contract {current_symbol} not found in the "
                f"scrip master after refresh (sid={current_security_id}); "
                "new entries blocked",
                security_id=current_security_id)
            return ["unknown", "block_entries"]
        if str(current.security_id) != str(current_security_id):
            self._alert_once(
                rec, "security_id_mismatch", today,
                f"{metal}: configured security ID {current_security_id} does not "
                f"match Dhan master ID {current.security_id} for {current_symbol}; "
                "new entries blocked",
                configured_security_id=str(current_security_id),
                master_security_id=str(current.security_id),
            )
            return ["unknown", "block_entries"]

        expiry = current.expiry_date
        window_open, last_session = resolve_window(
            expiry, self.trigger_days, self.holidays)
        metadata_changed = (
            rec.verified_symbol != current_symbol
            or str(rec.verified_security_id or "") != str(current_security_id)
            or rec.expiry_date != expiry.isoformat()
            or rec.window_open != window_open.isoformat()
            or rec.last_session != last_session.isoformat())
        rec.verified_symbol = current_symbol
        rec.verified_security_id = str(current_security_id)
        rec.expiry_date = expiry.isoformat()
        rec.window_open = window_open.isoformat()
        rec.last_session = last_session.isoformat()
        if metadata_changed and not self._save():
            self._alert_once(
                rec, "contract_metadata_persist_failed", today,
                f"{metal}: verified contract expiry could not be saved; "
                "automatic recovery after this contract leaves Dhan's master "
                "may require operator reconciliation",
                security_id=str(current_security_id),
                expiry=expiry.isoformat())

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
            # A successful DELETE acknowledgement is not enough to switch
            # contracts: an order may have filled while cancellation was in
            # flight. The caller must verify all old-contract orders terminal
            # with the broker before asking the decider to persist a switch.
            if self.cancel_on_window and not resting_orders_clear:
                return actions
            if not candidate_ready:
                self._alert_once(
                    rec, "candidate_not_ready", today,
                    f"{metal}: next contract is not warmed and verified; "
                    "contract switch is withheld")
                return actions
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
                self._refresh_contract_master()
                refreshed_current = self.scrip.find_symbol(metal, current_symbol)
                if (refreshed_current is not None
                        and str(refreshed_current.security_id)
                        == str(current_security_id)):
                    current = refreshed_current
                    nxt = self.scrip.next_contract(metal, current, as_of=today)
            if nxt is None:
                self._alert_once(
                    rec, "no_next_series", today,
                    f"{metal}: flat in {current_symbol} but no NEXT series "
                    f"found — entries stay blocked")
                return actions
            if self._persist_switch(
                    rec, metal, current_symbol, current_security_id, nxt, today):
                return actions + ["persist_switch"]
            return actions
        else:
            # Still holding.
            if today >= last_session and self.force_close_on_last:
                if not rec.force_close_sent:
                    rec.force_close_sent = True
                    if not self._save():
                        rec.force_close_sent = False
                        self._alert_once(
                            rec, "state_persist_failed", today,
                            f"{metal}: rollover close intent could not be "
                            "durably saved; automatic close withheld")
                        return actions
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
                # Keep asking the lifecycle exit path to reconcile/retry. That
                # path deduplicates an already-working exit; a prior rejection
                # or unresolved response must not permanently consume rollover.
                actions.append("force_close")
            return actions

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "trigger_days": self.trigger_days,
            "holiday_count": len(self.holidays),
            "calendar_valid_through": self.calendar_valid_through,
            "state_path": str(self.state_path),
            "records": {m: r.to_dict() for m, r in self._records.items()},
        }

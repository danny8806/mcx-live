"""Position-owned local SL lifecycle.

ONE mechanism, one direction:

    OPEN POSITION -> position.stop_price -> SL monitor -> tick crosses
    -> TRIGGERED -> one direct exit order -> broker-confirmed fill
    -> CLOSED -> SL state cleared

There is NO broker-side protective stop in this module or anywhere else.
The exit reuses the existing direct execution path (``_process_signal``), so
no second order type and no second framework is introduced.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from execution.live.sl_monitor import (
    PositionOwnedSLMonitor,
    SLReject,
    SLState,
)
from strategies.types import Signal, SignalType

log = logging.getLogger("trading_engine")

# SL states that mean "this process is actively watching a price".
_ARMING_STATES = (SLState.ARMED, SLState.TRIGGERED, SLState.EXITING)


class SLFlowMixin:
    """SL arming / firing / clearing, driven entirely by the open position."""

    # ── monitor plumbing ────────────────────────────────────────────────

    def _sl_monitor(self, env):
        """Return (creating once) the position-owned SL monitor for ``env``."""
        monitor = getattr(env, "sl_monitor", None)
        if monitor is None:
            monitor = PositionOwnedSLMonitor()
            env.sl_monitor = monitor
        return monitor

    # ── arming ──────────────────────────────────────────────────────────

    def _arm_position_sl(self, env, position, *,
                         source: str = "entry_fill") -> str:
        """Bind ``position.stop_price`` to the local monitor.

        Never invents a stop: a position without one is reported as
        ``SL_UNAVAILABLE`` and left unmonitored (existing risk policy applies).
        """
        if position is None:
            return SLState.NONE.value
        monitor = self._sl_monitor(env)
        state = monitor.arm(position)
        if state == SLState.UNAVAILABLE:
            position.sl_state = SLState.UNAVAILABLE.value
            self._persist_position(position, getattr(env, "name", None))
            self.publish_event("sl_unavailable", {
                "position_id": getattr(position, "position_id", None),
                "strategy_id": getattr(position, "strategy_id", None),
                "instrument": getattr(position, "instrument", None),
                "reason": SLReject.STOP_MISSING,
                "execution_mode": getattr(env, "mode", None),
            }, env_name=getattr(env, "name", None))
            log.error("[SL] %s/%s has NO stop_price — SL_UNAVAILABLE, "
                      "position is UNPROTECTED", position.strategy_id,
                      position.instrument)
            return state.value
        if state == SLState.ARMED:
            position.sl_state = SLState.ARMED.value
            position.sl_protected_at = time.time()
            self._persist_position(position, getattr(env, "name", None))
            self.publish_event("sl_armed", {
                "position_id": position.position_id,
                "trade_id": getattr(position, "trade_id", None),
                "strategy_id": position.strategy_id,
                "instrument": position.instrument,
                "side": "LONG" if position.is_long else "SHORT",
                "quantity": int(getattr(position, "quantity", 0) or 0),
                "stop_price": float(position.stop_price),
                "source": source,
                "execution_mode": getattr(env, "mode", None),
            }, env_name=getattr(env, "name", None))
            # A reversal parked in AWAITING_LOCAL_SL converges now that the
            # stop genuinely exists.
            try:
                self._close_reversal_sl_gap(env, position)
            except Exception as e:
                log.warning("[Engine] reversal gap close skipped: %s", e)
        return state.value

    # ── firing ───────────────────────────────────────────────────────────

    def _evaluate_position_sl(self, env, position, ltp: float,
                              env_name: Optional[str] = None):
        """Evaluate the position's SL against a live tick.

        Returns the minted exit ``Signal`` when the SL fires, else ``None``.
        Every rejection is a silent no-op — no order, no signal, no state
        change beyond the publish of ``sl_evaluation_blocked``.
        """
        env_name = env_name or getattr(env, "name", None)
        if position is None:
            return None
        monitor = self._sl_monitor(env)
        decision = monitor.evaluate(position, ltp)
        if not decision.fire:
            if decision.reason in (SLReject.STOP_MISSING,):
                self._mark_sl_unavailable(env, position)
            return None

        # §9 duplicate protection: latch BEFORE minting the signal.
        if not monitor.mark_triggered(decision.position_id, ltp):
            return None
        position.sl_state = SLState.TRIGGERED.value
        position.sl_trigger_price = float(ltp)
        self._persist_position(position, env_name)

        signal = self._build_sl_exit_signal(env, position, decision, ltp)
        if signal is None:
            monitor.release_exit(decision.position_id)
            return None

        # The exit reuses the existing direct execution path — one order, the
        # ordinary EXIT role, no broker-side protective order.
        self.publish_event("sl_triggered", {
            "position_id": decision.position_id,
            "trade_id": getattr(position, "trade_id", None),
            "strategy_id": decision.strategy_id,
            "instrument": decision.instrument,
            "side": decision.side,
            "quantity": decision.quantity,
            "stop_price": decision.stop_price,
            "market_price": float(ltp),
            "signal_id": signal.signal_id,
            "execution_mode": getattr(env, "mode", None),
        }, env_name=env_name)
        self._process_signal(signal, env_name)
        return signal

    def _build_sl_exit_signal(self, env, position, decision, ltp: float):
        """Mint the single SL exit signal, fully bound to THIS position."""
        strategies = getattr(env, "strategies", {}) or {}
        strategy = strategies.get(position.strategy_id)
        if strategy is None:
            log.error("[SL] %s/%s fired but strategy object is gone — "
                      "no exit minted", position.strategy_id, position.instrument)
            self._mark_sl_unavailable(env, position, SLReject.STRATEGY_MISMATCH)
            return None

        signal = Signal(
            signal_type=(SignalType.SHORT if position.is_long else SignalType.LONG),
            instrument=position.instrument,
            strategy_id=position.strategy_id,
            timestamp=time.time(),
            trigger_price=float(ltp),
            stop_price=decision.stop_price,
            quantity=int(decision.quantity),
        )
        signal.metadata = {
            "exit": True,
            "exit_reason": "stop_loss_hit",
            # Marks this as the LOCAL position-owned stop exit, so it is
            # classified as an ordinary EXIT order, never a broker STOP_LOSS.
            "local_sl_exit": True,
            "exit_price": float(ltp),
            "source": "position_sl_monitor",
            "triggered": True,
            "trigger_state": "FIRED",
            "trigger_source": "market_websocket_ltp",
            "trigger_generation": getattr(strategy, "_trigger_generation", None),
            "position_id": decision.position_id,
            "stop_price": decision.stop_price,
        }
        signal.lifecycle_id = position.trade_id
        signal.parent_position_id = position.position_id
        signal.position_generation = position.position_generation

        # The live ownership validator requires the fired trigger to be the
        # strategy's latest one; register it as such before routing.
        try:
            strategy._last_fired_trigger_signal_id = signal.signal_id
            # A position-owned stop wins over any not-yet-fired reversal. The
            # opposite entry is meaningful only after its reversal exit has
            # fired and the old position has been closed; a stop closes that
            # position independently, so retire the parked entry durably too.
            pending = getattr(strategy, "pending_entry", None)
            pending_signal = getattr(pending, "signal", None)
            pending_signal_id = getattr(pending_signal, "signal_id", None)
            strategy.notify_local_sl_exit("stop_loss_hit")
            registry = getattr(env, "pending_triggers", None)
            if registry is not None:
                registry.sync_strategy(strategy)
                if pending_signal_id:
                    registry.remove_signal(str(pending_signal_id))
            persistence = getattr(env, "persistence", None)
            if pending_signal_id and persistence is not None:
                persistence.terminalize_pending_order(
                    str(pending_signal_id), status="resolved",
                    reason="position_closed_by_stop_loss_before_reversal_trigger",
                )
        except Exception as e:
            log.error("[SL] strategy/pending cleanup failed for %s/%s: %s",
                      position.strategy_id, position.instrument, e)
        return signal

    # ── clearing / latches ───────────────────────────────────────────────

    def _clear_position_sl(self, env, position, *,
                           final_state: str = SLState.CLOSED.value,
                           reason: str = "position_closed") -> None:
        """Terminal SL teardown — INVARIANT 5 (old SL can never fire)."""
        if position is None:
            return
        env_name = getattr(env, "name", None)
        monitor = self._sl_monitor(env)
        position_id = getattr(position, "position_id", None)
        monitor.close(position_id)
        position.sl_state = final_state
        position.sl_trigger_price = None
        try:
            self._persist_position(position, env_name)
        except Exception as e:
            log.debug("[SL] close persist skipped: %s", e)
        self.publish_event("sl_cleared", {
            "position_id": position_id,
            "trade_id": getattr(position, "trade_id", None),
            "strategy_id": getattr(position, "strategy_id", None),
            "instrument": getattr(position, "instrument", None),
            "sl_state": final_state,
            "reason": reason,
            "execution_mode": getattr(env, "mode", None),
        }, env_name=env_name)

    def _mark_sl_exiting(self, env, position, order_id: Optional[str] = None,
                         broker_order_id: Optional[str] = None) -> None:
        """An exit order exists for this position (INVARIANT 4)."""
        if position is None:
            return
        env_name = getattr(env, "name", None)
        monitor = self._sl_monitor(env)
        monitor.mark_exiting(getattr(position, "position_id", None), order_id)
        position.sl_state = SLState.EXITING.value
        if order_id:
            position.exit_order_id = order_id
        try:
            self._persist_position(position, env_name)
        except Exception as e:
            log.debug("[SL] exiting persist skipped: %s", e)
        self.publish_event("sl_exit_submitted", {
            "position_id": getattr(position, "position_id", None),
            "strategy_id": getattr(position, "strategy_id", None),
            "instrument": getattr(position, "instrument", None),
            "order_id": order_id,
            "broker_order_id": broker_order_id,
            "execution_mode": getattr(env, "mode", None),
        }, env_name=env_name)

    def _release_sl_after_failed_exit(self, env, position,
                                      reason: str = "exit_rejected") -> None:
        """The exit attempt was rejected — re-arm so a later tick may retry."""
        if position is None:
            return
        monitor = self._sl_monitor(env)
        monitor.release_exit(getattr(position, "position_id", None))
        position.sl_state = SLState.ARMED.value
        position.exit_order_id = None
        try:
            position.exit_started = False
        except Exception:
            pass
        try:
            self._persist_position(position, getattr(env, "name", None))
        except Exception as e:
            log.debug("[SL] release persist skipped: %s", e)
        self.publish_event("sl_exit_failed", {
            "position_id": getattr(position, "position_id", None),
            "strategy_id": getattr(position, "strategy_id", None),
            "instrument": getattr(position, "instrument", None),
            "reason": reason,
            "execution_mode": getattr(env, "mode", None),
        }, env_name=getattr(env, "name", None))

    def _rearm_sl_after_partial_exit(self, env, position, *,
                                     exit_still_working: bool) -> str:
        """§12 — a partial exit leaves the position OPEN.

        If the exit order still has working quantity, the SL stays EXITING
        (one authoritative exit state).  Once it is done the SL re-arms on the
        position's REMAINING quantity; the position is never marked closed.
        """
        if position is None:
            return SLState.NONE.value
        env_name = getattr(env, "name", None)
        monitor = self._sl_monitor(env)
        pid = getattr(position, "position_id", None)
        if exit_still_working:
            position.sl_state = SLState.EXITING.value
        else:
            monitor.release_exit(pid)
            state = monitor.arm(position)
            position.sl_state = (state.value if state != SLState.UNAVAILABLE
                                 else SLState.UNAVAILABLE.value)
            self.publish_event("sl_rearmed_after_partial", {
                "position_id": pid,
                "strategy_id": getattr(position, "strategy_id", None),
                "instrument": getattr(position, "instrument", None),
                "remaining_quantity": int(getattr(position, "quantity", 0) or 0),
                "stop_price": getattr(position, "stop_price", None),
                "execution_mode": getattr(env, "mode", None),
            }, env_name=env_name)
        try:
            self._persist_position(position, env_name)
        except Exception as e:
            log.debug("[SL] partial rearm persist skipped: %s", e)
        return position.sl_state

    def _mark_sl_unavailable(self, env, position,
                             reason: str = SLReject.STOP_MISSING) -> None:
        """No usable stop on an open position: emit, never invent one (§2)."""
        if position is None:
            return
        env_name = getattr(env, "name", None)
        self._sl_monitor(env).disarm(getattr(position, "position_id", None))
        position.sl_state = SLState.UNAVAILABLE.value
        try:
            self._persist_position(position, env_name)
        except Exception as e:
            log.debug("[SL] unavailable persist skipped: %s", e)
        self.publish_event("sl_unavailable", {
            "position_id": getattr(position, "position_id", None),
            "strategy_id": getattr(position, "strategy_id", None),
            "instrument": getattr(position, "instrument", None),
            "reason": reason,
            "execution_mode": getattr(env, "mode", None),
        }, env_name=env_name)

    def _evaluate_positions_from_candle(self, env, bar) -> int:
        """Evaluate every open position's SL against a COMPLETED candle.

        The tick feed is the primary stop evaluator, but it is not a reliable
        safety net: Dhan's MCX tick feed is explicitly allowed to go silent
        while REST candles keep arriving, and ``has_live_market_data`` is
        satisfied by REST alone.  Without this path a dead tick feed would
        leave every open position UNPROTECTED with nothing in the order book
        to show for it.

        A completed candle is authoritative about the range it covered, so the
        stop is tested against the candle's ADVERSE extreme:

          LONG  (stop below) -> the candle LOW
          SHORT (stop above) -> the candle HIGH

        That is the worst price actually reached, so a stop breached intrabar
        is detected even if no tick ever arrived.  The monitor's own latch and
        ``exit_started`` flag keep this idempotent: a stop already fired by a
        tick is never fired twice by the candle that contained it.
        """
        instrument = getattr(bar, "instrument", None)
        if not instrument:
            return 0
        low = getattr(bar, "low", None)
        high = getattr(bar, "high", None)
        if low is None or high is None:
            return 0
        try:
            low = float(low)
            high = float(high)
        except (TypeError, ValueError):
            return 0
        if low <= 0 or high <= 0 or high < low:
            return 0

        try:
            candle_start = float(getattr(bar, "start_ts"))
            candle_end = float(getattr(bar, "end_ts"))
            candle_close = float(getattr(bar, "close"))
        except (TypeError, ValueError, AttributeError):
            return 0
        if (candle_start >= candle_end or candle_close <= 0
                or not (low <= candle_close <= high)):
            return 0

        fired = 0
        for env_iter in self._envs.values():
            try:
                positions = env_iter.position_manager.get_positions_by_instrument(
                    instrument)
            except Exception:
                continue
            for pos in positions:
                if not getattr(pos, "is_open", False):
                    continue
                if getattr(pos, "exit_started", False):
                    continue
                try:
                    entry_time = float(getattr(pos, "entry_timestamp"))
                except (TypeError, ValueError, AttributeError):
                    # Without a fill time, the candle range cannot be tied to
                    # this position's lifetime. Live ticks remain authoritative.
                    continue
                if entry_time >= candle_end:
                    continue
                # A position opened during this candle was not exposed to its
                # earlier extreme. The close is the only known post-fill price.
                if entry_time > candle_start:
                    reference = candle_close
                else:
                    reference = low if getattr(pos, "is_long", False) else high
                try:
                    signal = self._evaluate_position_sl(
                        env_iter, pos, reference,
                        env_name=getattr(env_iter, "name", None))
                except Exception as e:
                    log.error("[SL] candle evaluation failed for %s/%s: %s",
                              getattr(pos, "strategy_id", "?"),
                              getattr(pos, "position_id", "?"), e)
                    continue
                if signal is not None:
                    fired += 1
        return fired

    # ── reversal gap closure ───────────────────────────────────────────

    def _close_reversal_sl_gap(self, env, position) -> None:
        """Resolve a reversal left in ``AWAITING_LOCAL_SL`` once its stop is armed.

        A reversal's new entry can fill before the local stop is armed (stop not
        yet recovered, position missing it at fill time). The record is then
        parked in ``AWAITING_LOCAL_SL`` — and nothing else ever reads that
        status, so the reversal stays unresolved forever and the audit trail can
        no longer tell a protected reversed-into position from an unprotected
        one.

        Called on every successful arm so the record converges as soon as the
        stop actually exists. Purely a status/diagnostic write: it never places
        an order and never touches the monitor.
        """
        if position is None:
            return
        persistence = getattr(env, "persistence", None)
        if persistence is None:
            return
        getter = getattr(persistence, "get_reversals", None)
        updater = getattr(persistence, "update_reversal", None)
        if not callable(getter) or not callable(updater):
            return
        pid = str(getattr(position, "position_id", "") or "")
        if not pid:
            return
        try:
            records = getter() or []
        except Exception as e:
            log.debug("[SL] reversal gap lookup failed: %s", e)
            return
        for rev in records:
            if str(rev.get("status") or "").upper() != "AWAITING_LOCAL_SL":
                continue
            # Only the reversal whose NEW position this is may be closed here.
            if str(rev.get("new_position_id") or "") != pid:
                continue
            stop_price = getattr(position, "stop_price", None)
            try:
                updater(rev.get("reversal_id"), {
                    "new_sl_state": SLState.ARMED.value,
                    "status": "COMPLETE",
                    "sl_gap_closed_at": datetime.now(
                        timezone.utc).isoformat(),
                })
            except Exception as e:
                log.warning("[Engine] reversal gap close failed for %s: %s",
                            rev.get("reversal_id"), e)
                continue
            self.publish_event("reversal_sl_gap_closed", {
                "reversal_id": rev.get("reversal_id"),
                "position_id": pid,
                "strategy_id": getattr(position, "strategy_id", None),
                "instrument": getattr(position, "instrument", None),
                "stop_price": float(stop_price) if stop_price else None,
                "execution_mode": getattr(env, "mode", None),
            }, env_name=getattr(env, "name", None))
            log.info("[SL] reversal %s SL gap closed: %s is now ARMED",
                     rev.get("reversal_id"), pid)

    # ── startup / crash recovery ────────────────────────────────────────

    def sync_sl_from_broker(self, env_name: Optional[str] = None) -> dict:
        """INVARIANT 9 — arm the SL only from broker-confirmed positions.

        Never arms from the database alone: a local row the broker does not
        confirm is CLOSED in the position book (not just dropped from the
        monitor) so a stale DB row can never arm an SL, hold an entry
        blocker, or be traded against.
        """
        env = self._env_for(env_name)
        if env is None:
            return {"status": "failed", "error": "unknown_environment",
                    "env": env_name, "armed": [], "unavailable": [],
                    "dropped_local": [], "broker_only": []}
        broker = getattr(env, "broker", None)
        pm = getattr(env, "position_manager", None)
        if broker is None or pm is None or not hasattr(broker, "positions"):
            return {"status": "failed", "error": "broker_unavailable",
                    "env": env_name, "armed": [], "unavailable": [],
                    "dropped_local": [], "broker_only": []}
        try:
            broker_positions = broker.positions() or []
        except Exception as e:
            # §35 — an UNKNOWN broker state is not "flat".  Returning {} here
            # used to let startup continue into trading with no reconciliation
            # at all, so a transient API failure silently skipped the whole
            # broker-authoritative check.
            log.error("[SL] broker position query failed: %s", e)
            return {"status": "failed", "error": f"broker_query_failed: {e}",
                    "env": env_name, "armed": [], "unavailable": [],
                    "dropped_local": [], "broker_only": []}

        # Local open positions across every strategy.
        local_positions = []
        for sid in list((getattr(env, "strategies", {}) or {}).keys()):
            for lp in (pm.get_positions_by_strategy(sid) or []):
                if getattr(lp, "is_open", False):
                    local_positions.append(lp)

        # Dhan reports positions PER SECURITY, as a single signed netQty, with
        # no strategy attribution.  The transport fans that one row out to every
        # strategy configured on the instrument, so the rows are duplicates:
        # collapse them to one authoritative net position per instrument.
        # This is the level the broker can actually speak to, and it is the only
        # level at which "the broker says flat" is a real answer.
        broker_net: dict[str, dict] = {}
        for row in broker_positions or []:
            inst = str(row.get("instrument") or "")
            if not inst:
                continue
            try:
                qty = int(row.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            side = str(row.get("side") or "").upper()
            if side not in ("BUY", "SELL", "LONG", "SHORT"):
                continue
            signed = qty if side in ("BUY", "LONG") else -qty
            cur = broker_net.get(inst)
            if cur is None:
                broker_net[inst] = {"instrument": inst, "signed": signed,
                                    "quantity": abs(signed),
                                    "side": "LONG" if signed > 0 else "SHORT"}
            elif cur["signed"] != signed:
                # Two different nets for one instrument cannot both be right;
                # the conservative reading is FLAT, so nothing is armed on it.
                log.error("[SL] conflicting broker nets for %s (%s vs %s) — "
                          "treating as FLAT", inst, cur["signed"], signed)
                broker_net[inst] = {"instrument": inst, "signed": 0,
                                    "quantity": 0, "side": "FLAT"}

        # A filled broker exit makes GET /positions flat before the order
        # poller/order-update path necessarily routes its fill.  Do not let
        # this SL resync race turn that still-owned position into a stale
        # local row: fill_flow needs the original position identity to close
        # its trade and reversal lifecycle.  Keep it in EXITING until the
        # correlated close fill is routed (or its exit order is terminally
        # rejected/cancelled, at which point the normal sync can decide).
        execution = getattr(env, "execution_engine", None)
        deferred_exit_positions = []
        if execution is not None and callable(getattr(execution, "get_order", None)):
            for lp in local_positions:
                net = broker_net.get(str(getattr(lp, "instrument", "")))
                broker_flat_or_conflicting = not net or net["signed"] == 0 or (
                    (net["signed"] > 0) != bool(getattr(lp, "is_long", False)))
                if not broker_flat_or_conflicting:
                    continue
                oid = getattr(lp, "exit_order_id", None)
                order = execution.get_order(oid) if oid else None
                role = str(getattr(order, "order_role", "") or "").upper()
                state_obj = getattr(order, "state", "") if order is not None else ""
                state = str(getattr(state_obj, "value", state_obj)).lower()
                if (order is not None
                        and role in {"EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT"}
                        and state in {"created", "submitted", "acknowledged",
                                      "partially_filled", "filled"}):
                    deferred_exit_positions.append(lp)

        if deferred_exit_positions:
            deferred_ids = {id(p) for p in deferred_exit_positions}
            local_positions = [p for p in local_positions if id(p) not in deferred_ids]
            for lp in deferred_exit_positions:
                self.publish_event("sl_broker_flat_exit_fill_pending", {
                    "position_id": getattr(lp, "position_id", None),
                    "trade_id": getattr(lp, "trade_id", None),
                    "exit_order_id": getattr(lp, "exit_order_id", None),
                    "instrument": getattr(lp, "instrument", None),
                    "reason": "broker_flat_with_owned_exit_order",
                    "execution_mode": getattr(env, "mode", None),
                }, env_name=getattr(env, "name", None))

        # Hand the monitor one clean row per instrument per local position.
        resolved = []
        for lp in local_positions:
            inst = str(lp.instrument)
            net = broker_net.get(inst)
            is_long = bool(getattr(lp, "is_long", False))
            # Confirmed means BOTH: the broker holds a net position here AND
            # its direction agrees with the local book.  A direction conflict
            # means the local row cannot be trusted to exist, and an SL on it
            # could send a real exit for a position the broker does not hold.
            confirmed = bool(net and net["signed"] != 0
                             and ((net["signed"] > 0) == is_long))
            resolved.append({
                "instrument": inst,
                "strategy_id": str(lp.strategy_id),
                "quantity": int(getattr(lp, "quantity", 0) or 0),
                "side": "LONG" if is_long else "SHORT",
                "broker_confirmed": confirmed,
            })
        # A broker position with no local book at all: surfaced, never opened.
        local_instruments = {str(lp.instrument) for lp in local_positions}
        for inst, net in broker_net.items():
            if inst in local_instruments or net["quantity"] <= 0:
                continue
            resolved.append({
                "instrument": inst, "strategy_id": "", "side": net["side"],
                "quantity": net["quantity"], "broker_confirmed": True,
            })

        summary = self._sl_monitor(env).resync_from_broker(
            resolved, local_positions,
            stop_resolver=self._sl_stop_resolver(env))

        # Reflect the recovered state on the position rows and persist them.
        by_pid = {str(getattr(p, "position_id", "")): p for p in local_positions}
        for entry in summary.get("armed", []):
            pos = by_pid.get(str(entry.get("position_id")))
            if pos is None:
                continue
            pos.sl_state = SLState.ARMED.value
            if not pos.sl_protected_at:
                pos.sl_protected_at = time.time()
            try:
                self._persist_position(pos, getattr(env, "name", None))
            except Exception as e:
                log.debug("[SL] arm persist skipped: %s", e)
            self.publish_event("sl_recovered_from_broker", dict(
                entry, execution_mode=getattr(env, "mode", None)),
                env_name=getattr(env, "name", None))
            try:
                self._close_reversal_sl_gap(env, pos)
            except Exception as e:
                log.warning("[Engine] reversal gap close skipped: %s", e)
        for entry in summary.get("unavailable", []):
            pos = by_pid.get(str(entry.get("position_id")))
            if pos is None:
                continue
            self._mark_sl_unavailable(env, pos, entry.get("reason")
                                      or SLReject.STOP_MISSING)

        # Complete crash/restart recovery for positions whose first entry fill
        # arrived before its order chain settled. An active LIMIT remains
        # unprotected until full fill or a confirmed fallback; a fallback fill
        # arms only the broker-confirmed position quantity.
        protect_entry = getattr(self, "_protect_entry_fill_if_ready", None)
        execution_orders = (getattr(execution, "_orders", {}) or {}) if execution else {}
        for entry in summary.get("awaiting_entry_completion", []):
            pos = by_pid.get(str(entry.get("position_id")))
            if pos is None or not callable(protect_entry):
                continue
            entry_order = execution_orders.get(getattr(pos, "entry_order_id", None))
            if entry_order is None:
                continue
            try:
                protect_entry(env, pos, entry_order,
                              source="entry_completion_reconciliation")
            except Exception as e:
                log.error("[SL] deferred entry protection failed for %s: %s",
                          getattr(pos, "position_id", None), e)

        # A local row the broker does not confirm cannot exist: close it.
        for entry in summary.get("dropped_local", []):
            pid = entry.get("position_id")
            if not pid:
                continue
            try:
                stale_position = pm.abandon_stale_position(str(pid))
            except Exception as e2:
                log.error("[SL] could not close stale position %s: %s", pid, e2)
                stale_position = None
            # A broker/manual close ends the pending reversal lifecycle too;
            # a later fresh strategy signal may create a new entry.
            stale_sid = (entry.get("strategy_id")
                         or getattr(stale_position, "strategy_id", None))
            strategy = (getattr(env, "strategies", {}) or {}).get(stale_sid)
            if strategy is not None:
                pending = getattr(strategy, "pending_entry", None)
                pending_signal = getattr(pending, "signal", None)
                pending_signal_id = getattr(pending_signal, "signal_id", None)
                reset_strategy = getattr(self, "_reset_strategy_state", None)
                if callable(reset_strategy):
                    try:
                        reset_strategy(stale_sid, keep_pending=False,
                                       env_name=getattr(env, "name", None))
                    except Exception as e2:
                        log.error("[SL] strategy reset after broker-flat position %s "
                                  "failed: %s", pid, e2)
                if pending_signal_id:
                    persistence = getattr(env, "persistence", None)
                    if persistence is not None:
                        try:
                            persistence.terminalize_pending_order(
                                str(pending_signal_id), status="resolved",
                                reason="position_closed_manually_before_reversal_entry",
                            )
                        except Exception as e2:
                            log.error("[SL] manual-close reversal cleanup failed "
                                      "for %s: %s", pending_signal_id, e2)
                    registry = getattr(env, "pending_triggers", None)
                    if registry is not None:
                        registry.remove_signal(str(pending_signal_id))
                    self.publish_event("pending_reversal_cancelled_after_manual_close", {
                        "signal_id": str(pending_signal_id),
                        "strategy_id": stale_sid,
                        "position_id": pid,
                        "reason": "position_closed_manually_before_reversal_entry",
                        "execution_mode": getattr(env, "mode", None),
                    }, env_name=getattr(env, "name", None))
            self.publish_event("sl_stale_local_position_closed", dict(
                entry, execution_mode=getattr(env, "mode", None)),
                env_name=getattr(env, "name", None))

        # A broker position the local book cannot attribute to any strategy.
        # It is NEVER auto-opened and NEVER auto-flattened: doing either would
        # mean trading on a guess. But it IS real exposure we hold no stop for
        # and can never exit, so it must be surfaced loudly and must keep the
        # entry gate shut rather than let new risk stack on top of it.
        orphans = summary.get("broker_only") or []
        if orphans:
            for entry in orphans:
                log.error("[SL] ORPHAN broker position: %s %s held at broker "
                          "with no local position — UNPROTECTED, unattributable, "
                          "entries stay blocked", entry.get("instrument"),
                          entry.get("quantity"))
                try:
                    self.publish_event("broker_orphan_position", {
                        "instrument": entry.get("instrument"),
                        "quantity": entry.get("quantity"),
                        "reason": "broker_position_without_local_book",
                        "execution_mode": getattr(env, "mode", None),
                    }, env_name=getattr(env, "name", None))
                except Exception as e:
                    log.error("[SL] orphan event publish failed: %s", e)
            try:
                self.telegram.on_risk_alert({
                    "severity": "CRITICAL",
                    "type": "broker_orphan_position",
                    "message": (f"Broker holds {len(orphans)} position(s) with no "
                                f"local record: "
                                f"{[o.get('instrument') for o in orphans]}"),
                })
            except Exception as e:
                log.warning("[SL] orphan risk alert failed: %s", e)
        summary["orphan_exposure"] = bool(orphans)
        summary["orphans"] = orphans

        log.info("[SL] broker-authoritative SL sync: armed=%s unavailable=%s "
                 "dropped_local=%s broker_only=%s",
                 len(summary.get("armed", [])), len(summary.get("unavailable", [])),
                 len(summary.get("dropped_local", [])),
                 len(summary.get("broker_only", [])))
        summary["status"] = "reconciled"
        summary["env"] = getattr(env, "name", None)
        return summary

    def _sl_stop_resolver(self, env):
        """Recover a position's OWN stop only — never invent one (§2).

        Sources, in order: the position row, its entry order's executed
        ``planned_sl``.  An old strategy-wide stop or an old signal's stop is
        deliberately NOT used: it may belong to a previous position.
        """
        engine = getattr(env, "execution_engine", None)

        def _resolve(position):
            stop = getattr(position, "stop_price", None)
            try:
                if stop is not None and float(stop) > 0:
                    return float(stop)
            except (TypeError, ValueError):
                pass
            oid = getattr(position, "entry_order_id", None)
            if oid and engine is not None:
                order = engine.get_order(oid)
                planned = getattr(order, "planned_sl", None) if order else None
                try:
                    if planned is not None and float(planned) > 0:
                        return float(planned)
                except (TypeError, ValueError):
                    pass
            return None

        return _resolve

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
import json
import time
from datetime import datetime, timezone
from typing import Any, Optional

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
                stop_price = float(stop_price)
                if stop_price <= 0:
                    continue
            except (TypeError, ValueError):
                continue
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
            try:
                notifier = getattr(self, "_notify_reversal_complete", None)
                if callable(notifier):
                    notifier(env, {
                        **rev,
                        "new_sl_state": SLState.ARMED.value,
                        "status": "COMPLETE",
                    }, position=position)
            except Exception as e:
                log.warning("[Telegram] reversal completion alert failed for %s: %s",
                            rev.get("reversal_id"), e)
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

    @staticmethod
    def _sl_reconciliation_fingerprint(env) -> tuple:
        """Identify local lifecycle changes that invalidate an in-flight REST snapshot."""
        pm = getattr(env, "position_manager", None)
        positions = []
        for position in list(getattr(pm, "open_positions", []) or []):
            positions.append((
                str(getattr(position, "position_id", "")),
                str(getattr(position, "trade_id", "")),
                bool(getattr(position, "is_open", False)),
                str(getattr(position, "instrument", "")),
                bool(getattr(position, "is_long", False)),
                int(getattr(position, "quantity", 0) or 0),
                str(getattr(position, "position_generation", "")),
                str(getattr(position, "stop_price", "")),
                str(getattr(position, "sl_state", "")),
                str(getattr(position, "exit_order_id", "")),
                bool(getattr(position, "exit_started", False)),
            ))
        execution = getattr(env, "execution_engine", None)
        orders = []
        for order_id, order in list(
                (getattr(execution, "_orders", {}) or {}).items()):
            state = getattr(order, "state", None)
            orders.append((
                str(order_id), str(getattr(state, "value", state)),
                int(getattr(order, "filled_quantity", 0) or 0),
                str(getattr(order, "trade_id", "")),
                str(getattr(order, "parent_position_id", "")),
            ))
        strategies = []
        for strategy_id, strategy in sorted(
                (getattr(env, "strategies", {}) or {}).items()):
            pending = getattr(strategy, "pending_entry", None)
            pending_signal = getattr(pending, "signal", None)
            state = getattr(strategy, "state", None)
            strategies.append((
                str(strategy_id), str(getattr(state, "value", state)),
                str(getattr(pending_signal, "signal_id", "")),
                str(getattr(pending, "status", "")),
                str(getattr(strategy, "current_trade_id", "")),
                str(getattr(strategy, "current_position_id", "")),
                str(getattr(strategy, "position_side", "")),
                bool(getattr(strategy, "stop_exit_submitted", False)),
            ))
        return tuple(sorted(positions)), tuple(sorted(orders)), tuple(strategies)

    def sync_sl_from_broker(self, env_name: Optional[str] = None) -> dict:
        """INVARIANT 9 — arm the SL only from broker-confirmed positions.

        Never arms from the database alone. An unconfirmed local position
        remains owned but unarmed and blocks entries until broker evidence
        agrees again or an attributable exit fill closes it.
        """
        env = self._env_for(env_name)

        def _apply_reconciliation_gate(summary):
            reconciled = getattr(self, "_reconciled_envs", None)
            name = getattr(env, "name", env_name) if env is not None else env_name
            if reconciled is not None and name:
                if (summary.get("status") == "reconciled"
                        and not summary.get("orphan_exposure")):
                    reconciled.add(name)
                else:
                    reconciled.discard(name)
            return summary

        if env is None:
            return _apply_reconciliation_gate({
                "status": "failed", "error": "unknown_environment",
                "env": env_name, "armed": [], "unavailable": [],
                "dropped_local": [], "broker_only": []})
        # A previous broker-confirmed close may have succeeded in memory while
        # SQLite was temporarily unavailable. Retry its canonical row update on
        # every broker reconciliation before allowing the environment to be
        # considered fully reconciled again.
        retry_close = getattr(self, "_retry_position_close_persistence", None)
        if callable(retry_close):
            retry_close(env)
        broker = getattr(env, "broker", None)
        pm = getattr(env, "position_manager", None)
        if broker is None or pm is None or not hasattr(broker, "positions"):
            return _apply_reconciliation_gate({
                "status": "failed", "error": "broker_unavailable",
                "env": env_name, "armed": [], "unavailable": [],
                "dropped_local": [], "broker_only": []})
        state_lock = getattr(self, "_lock", None)
        if state_lock is not None:
            with state_lock:
                before_fingerprint = self._sl_reconciliation_fingerprint(env)
        else:
            before_fingerprint = self._sl_reconciliation_fingerprint(env)
        try:
            broker_positions = broker.positions() or []
        except Exception as e:
            # §35 — an UNKNOWN broker state is not "flat".  Returning {} here
            # used to let startup continue into trading with no reconciliation
            # at all, so a transient API failure silently skipped the whole
            # broker-authoritative check.
            log.error("[SL] broker position query failed: %s", e)
            return _apply_reconciliation_gate({
                "status": "failed", "error": f"broker_query_failed: {e}",
                "env": env_name, "armed": [], "unavailable": [],
                "dropped_local": [], "broker_only": []})

        # Broker I/O stays outside the state lock. Apply this response only
        # after taking the same lock used by tick, fill, and trigger paths so
        # reconciliation cannot close/re-arm a position midway through an SL
        # or reversal transition.
        worker = getattr(self, "_sync_sl_from_broker_snapshot", None)
        if callable(worker):
            if state_lock is not None:
                with state_lock:
                    if self._sl_reconciliation_fingerprint(env) != before_fingerprint:
                        return _apply_reconciliation_gate({
                            "status": "stale_snapshot",
                            "error": "local_lifecycle_changed_during_broker_query",
                            "env": getattr(env, "name", env_name),
                            "armed": [], "unavailable": [],
                            "dropped_local": [], "broker_only": [],
                        })
                    return _apply_reconciliation_gate(
                        worker(env, env_name, broker_positions))
            return _apply_reconciliation_gate(
                worker(env, env_name, broker_positions))
        return _apply_reconciliation_gate(
            SLFlowMixin._sync_sl_from_broker_snapshot(
                self, env, env_name, broker_positions))

    def _sync_sl_from_broker_snapshot(self, env, env_name, broker_positions) -> dict:
        pm = getattr(env, "position_manager", None)
        broker = getattr(env, "broker", None)

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
        broker_conflicts: list[dict] = []
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
                                    "side": "LONG" if signed > 0 else "SHORT",
                                    "average_entry_price": row.get(
                                        "average_entry_price")}
            elif cur["signed"] != signed:
                # Conflicting signed nets are not evidence of FLAT.  Do not
                # mutate local ownership from an internally contradictory
                # broker snapshot; the caller will keep execution blocked.
                log.error("[SL] conflicting broker nets for %s (%s vs %s) — "
                          "snapshot is unresolved", inst, cur["signed"], signed)
                broker_conflicts.append({
                    "instrument": inst, "first_signed": cur["signed"],
                    "conflicting_signed": signed,
                })

        if broker_conflicts:
            # In particular, never turn conflicting broker rows into FLAT and
            # abandon a real local position.  Keep the current book untouched
            # and fail closed until a consistent broker snapshot arrives.
            try:
                self.publish_event("broker_position_snapshot_conflict", {
                    "conflicts": broker_conflicts,
                    "severity": "CRITICAL",
                    "execution_mode": getattr(env, "mode", None),
                }, env_name=getattr(env, "name", None))
            except Exception:
                pass
            return {
                "status": "failed", "error": "conflicting_broker_position_rows",
                "broker_conflicts": broker_conflicts,
                "orphan_exposure": True, "orphans": broker_conflicts,
                "armed": [], "unavailable": [], "dropped_local": [],
                "broker_only": [], "env": env_name,
            }

        # Recover only the false-close shape created by older startup logic:
        # closed for broker-flat reconciliation, no exit order/fill, and a
        # unique original FILLED entry whose identity, side, size and average
        # still match Dhan's current net. This avoids leaving a real broker
        # position permanently orphaned after deploying the no-false-close fix.
        recovered_positions = []
        closed_positions = list(getattr(pm, "closed_positions", []) or [])
        execution = getattr(env, "execution_engine", None)
        runtimes = getattr(env, "runtimes", None)
        persistence = getattr(env, "persistence", None)
        for inst, net in broker_net.items():
            if net.get("quantity", 0) <= 0:
                continue
            open_for_instrument = [p for p in local_positions
                                   if str(getattr(p, "instrument", "")) == inst]
            if open_for_instrument:
                continue
            possible = []
            for candidate in closed_positions:
                if (str(getattr(candidate, "instrument", "")) != inst
                        or ("LONG" if getattr(candidate, "is_long", False)
                            else "SHORT") != net.get("side")
                        or str(getattr(candidate, "exit_reason", "") or "").upper()
                            not in {"STARTUP_BROKER_FLAT",
                                    "BROKER_FLAT_RECONCILIATION"}
                        or getattr(candidate, "exit_fills", None)
                        or getattr(candidate, "exit_order_id", None)
                        or getattr(candidate, "exit_started", False)):
                    continue
                strategy_id = str(getattr(candidate, "strategy_id", "") or "")
                runtime = runtimes.get(strategy_id) if runtimes is not None else None
                lifecycle = getattr(runtime, "lifecycle", None)
                trade_id = str(getattr(candidate, "trade_id", "") or "")
                trade = lifecycle.get_trade(trade_id) if lifecycle and trade_id else None
                if (trade is None
                        or str(getattr(trade, "status", "")).upper() != "CLOSED"
                        or str(getattr(trade, "exit_reason", "") or "").upper()
                            not in {"STARTUP_BROKER_FLAT",
                                    "BROKER_FLAT_RECONCILIATION"}
                        or getattr(trade, "exit_order_id", "")
                        or getattr(trade, "exit_fill_id", "")
                        or float(getattr(trade, "exit_price", 0) or 0) > 0
                        or float(getattr(trade, "exit_timestamp", 0) or 0) > 0
                        or not getattr(candidate, "entry_order_id", None)
                        or str(getattr(trade, "entry_order_id", "") or "")
                            != str(candidate.entry_order_id)):
                    continue
                order = (execution.get_order(candidate.entry_order_id)
                         if execution is not None else None)
                persisted_order = None
                if persistence is not None:
                    rows = persistence.get_orders(candidate.entry_order_id)
                    persisted_order = rows[0] if rows else None
                if order is None:
                    order = persisted_order
                def _field(obj, key, default=None):
                    return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)
                def _order_field(key, default=None):
                    value = _field(order, key, default)
                    if value in (None, "") and persisted_order is not None:
                        value = _field(persisted_order, key, default)
                    return value
                state_obj = _order_field("state", "")
                state = str(getattr(state_obj, "value", state_obj)).lower()
                fill_qty = int(_order_field("filled_quantity", 0) or 0)
                fill_price = float(_order_field("average_fill_price", 0) or 0)
                order_side = str(_order_field("side", "") or "").upper()
                order_role = str(_order_field("order_role", "") or "").upper()
                order_trade_id = str(_order_field("trade_id", "") or "")
                order_strategy = str(_order_field("strategy_id", "") or "")
                order_instrument = str(_order_field("instrument", "") or "")
                order_quantity = int(_order_field("quantity", 0) or 0)
                order_security_id = str(_order_field("security_id", "") or "")
                instrument_config = (getattr(broker, "instruments", {}) or {}).get(inst, {})
                current_security_id = str(
                    (instrument_config or {}).get("security_id") or "")
                if current_security_id and not order_security_id:
                    broker_order_id = str(
                        _order_field("broker_order_id", "")
                        or getattr(order, "_broker_order_id", "") or "")
                    audit = getattr(broker, "audit", None)
                    audit_reader = getattr(audit, "for_broker_order", None)
                    if broker_order_id and callable(audit_reader):
                        try:
                            audit_rows = audit_reader(broker_order_id) or []
                        except Exception:
                            audit_rows = []
                        for evidence in audit_rows:
                            order_security_id = str(
                                evidence.get("security_id") or "")
                            if not order_security_id:
                                payload = evidence.get("request_payload")
                                if isinstance(payload, str):
                                    try:
                                        payload = json.loads(payload)
                                    except (TypeError, ValueError):
                                        payload = None
                                if isinstance(payload, dict):
                                    order_security_id = str(
                                        payload.get("securityId")
                                        or payload.get("security_id") or "")
                            if order_security_id:
                                break
                expected_order_side = "BUY" if net["side"] == "LONG" else "SELL"
                broker_avg = float(net.get("average_entry_price") or 0)
                if (state != "filled" or fill_qty != int(net["quantity"])
                        or order_quantity != fill_qty
                        or int(getattr(trade, "quantity", 0) or 0) != fill_qty
                        or order_side != expected_order_side
                        or order_role != "ENTRY"
                        or order_trade_id != trade_id
                        or order_strategy != strategy_id
                        or order_instrument != inst
                        or (current_security_id and
                            order_security_id != current_security_id)
                        or not fill_price or not broker_avg
                        or abs(fill_price - broker_avg) > 1.0
                        or not getattr(candidate, "stop_price", None)
                        or float(candidate.stop_price) <= 0):
                    continue
                possible.append((candidate, trade, lifecycle, runtime, fill_qty,
                                 fill_price))
            if len(possible) != 1:
                continue
            candidate, trade, lifecycle, runtime, fill_qty, fill_price = possible[0]
            from portfolio.position_manager import PositionStatus
            candidate.quantity = fill_qty
            candidate.average_entry = fill_price
            candidate.status = PositionStatus.OPEN
            candidate.exit_reason = None
            candidate.exit_started = False
            candidate.exit_order_id = None
            candidate.exit_signal_id = None
            candidate.sl_state = SLState.UNAVAILABLE.value
            candidate.sl_trigger_price = None
            mark = next((float(row.get("ltp")) for row in broker_positions or []
                         if str(row.get("instrument") or "") == inst
                         and row.get("ltp") not in (None, "")), None)
            if mark and mark > 0:
                candidate.update_mark(mark)
            if not lifecycle.reopen_trade_from_broker_position(
                    candidate.trade_id, candidate):
                candidate.status = PositionStatus.CLOSED
                candidate.quantity = 0
                candidate.exit_reason = "startup_broker_flat"
                continue
            try:
                pm.restore_open_position(candidate)
            except Exception:
                log.exception("[SL] broker-verified stale-close restore failed for %s",
                              candidate.position_id)
                continue
            try:
                self._persist_position(candidate, getattr(env, "name", None))
            except Exception:
                log.exception("[SL] restored position persistence failed for %s",
                              candidate.position_id)
            strategy = (getattr(env, "strategies", {}) or {}).get(
                candidate.strategy_id)
            if strategy is not None:
                from strategies.types import StrategyState
                strategy.position_side = net["side"]
                strategy.current_position_id = candidate.position_id
                strategy.position_generation = candidate.position_generation
                strategy.position_quantity = candidate.quantity
                strategy.current_trade_id = candidate.trade_id
                strategy.stop_price = candidate.stop_price
                strategy.stop_exit_submitted = False
                strategy.state = (StrategyState.LONG_POSITION
                                  if net["side"] == "LONG"
                                  else StrategyState.SHORT_POSITION)
            local_positions.append(candidate)
            recovered_positions.append({
                "position_id": candidate.position_id,
                "trade_id": candidate.trade_id,
                "strategy_id": candidate.strategy_id,
                "instrument": inst, "side": net["side"],
                "quantity": fill_qty, "average_entry_price": fill_price,
                "stop_price": candidate.stop_price,
            })
            self.publish_event("broker_position_recovered_from_stale_close", {
                **recovered_positions[-1],
                "reason": "unique_filled_entry_matches_broker_net",
                "execution_mode": getattr(env, "mode", None),
            }, env_name=getattr(env, "name", None))

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
                if not oid:
                    continue
                # A fallback MARKET is a child of the original LIMIT.  The
                # position keeps the root exit id, so checking only that row
                # misses the exact cancel-confirm -> market-fill race: the
                # root is CANCELLED while its owned child is already FILLED
                # but its fill has not yet been routed.
                candidates = []
                root = execution.get_order(oid)
                if root is not None:
                    candidates.append(root)
                for child in (getattr(execution, "_orders", {}) or {}).values():
                    if getattr(child, "original_order_id", None) != oid:
                        continue
                    if (getattr(child, "parent_position_id", None)
                            != getattr(lp, "position_id", None)
                            or (getattr(child, "lifecycle_id", None)
                                or getattr(child, "trade_id", None))
                            != getattr(lp, "trade_id", None)
                            or getattr(child, "position_generation", None)
                            != getattr(lp, "position_generation", None)):
                        continue
                    candidates.append(child)
                for order in candidates:
                    role = str(getattr(order, "order_role", "") or "").upper()
                    state_obj = getattr(order, "state", "")
                    state = str(getattr(state_obj, "value", state_obj)).lower()
                    is_owned_fallback = (
                        order is not root
                        and getattr(order, "order_type", "") == "MARKET"
                        and bool(getattr(order, "fallback_cancel_confirmed", False))
                        and role in {"EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT"}
                    )
                    if (role in {"EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT"}
                            and state in {"created", "submitted", "acknowledged",
                                          "partially_filled", "filled"}
                            and (order is root or is_owned_fallback)):
                        deferred_exit_positions.append(lp)
                        break

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
        local_by_instrument: dict[str, list[Any]] = {}
        for lp in local_positions:
            local_by_instrument.setdefault(str(lp.instrument), []).append(lp)

        # Dhan exposes a net position per instrument, not a strategy allocation.
        # Multiple local owners cannot each be assigned the full broker quantity.
        # If there is more than one same-side local owner, fail closed for that
        # instrument and require reconciliation instead of arming duplicate SLs.
        ambiguous_instruments: dict[str, list[Any]] = {}
        for inst, locals_for_inst in local_by_instrument.items():
            net = broker_net.get(inst)
            if not net or net["signed"] == 0:
                continue
            matching = [lp for lp in locals_for_inst
                        if bool(getattr(lp, "is_long", False)) == (net["signed"] > 0)]
            if len(matching) > 1:
                ambiguous_instruments[inst] = matching

        resolved = []
        ambiguous_ids = {
            id(lp) for group in ambiguous_instruments.values() for lp in group
        }
        sync_local_positions = []
        for lp in local_positions:
            inst = str(lp.instrument)
            if id(lp) in ambiguous_ids:
                continue
            sync_local_positions.append(lp)
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
        # Any broker exposure without one unambiguous local owner is surfaced,
        # including the case where a stale local row exists for the same symbol
        # but on the opposite side.
        locally_owned_instruments = {
            str(lp.instrument) for lp in sync_local_positions
            if (net := broker_net.get(str(lp.instrument)))
            and net["signed"] != 0
            and ((net["signed"] > 0) == bool(getattr(lp, "is_long", False)))
        }
        for inst, net in broker_net.items():
            if inst in locally_owned_instruments or net["quantity"] <= 0:
                continue
            resolved.append({
                "instrument": inst, "strategy_id": "", "side": net["side"],
                "quantity": net["quantity"], "broker_confirmed": True,
            })

        summary = self._sl_monitor(env).resync_from_broker(
            resolved, sync_local_positions,
            stop_resolver=self._sl_stop_resolver(env))
        if recovered_positions:
            summary["recovered_from_stale_close"] = recovered_positions

        # Preserve ambiguous same-instrument owners for operator resolution,
        # but disarm their individually-owned stops: the broker net cannot tell
        # us which local lifecycle owns which contracts.  The corresponding
        # broker-only record ensures the environment remains blocked.
        for inst, group in ambiguous_instruments.items():
            net = broker_net[inst]
            summary.setdefault("broker_only", []).append({
                "strategy_id": "", "instrument": inst,
                "quantity": net["quantity"], "side": net["side"],
                "reason": "multiple_local_strategy_owners_for_broker_net",
            })
            summary.setdefault("ownership_ambiguous", []).append({
                "instrument": inst,
                "position_ids": [getattr(lp, "position_id", None) for lp in group],
                "reason": "broker_net_has_no_strategy_attribution",
            })
            for lp in group:
                self._sl_monitor(env).disarm(str(getattr(lp, "position_id", "")))
                lp.sl_state = SLState.UNAVAILABLE.value
                self._persist_position(lp, getattr(env, "name", None))
                summary.setdefault("unavailable", []).append({
                    "position_id": getattr(lp, "position_id", None),
                    "strategy_id": getattr(lp, "strategy_id", None),
                    "instrument": inst,
                    "reason": "broker_net_ownership_ambiguous",
                })

        # Reflect the recovered state on the position rows and persist them.
        by_pid = {str(getattr(p, "position_id", "")): p for p in local_positions}
        for entry in summary.get("armed", []):
            pos = by_pid.get(str(entry.get("position_id")))
            if pos is None:
                continue
            pos.sl_state = SLState.ARMED.value
            if not pos.sl_protected_at:
                pos.sl_protected_at = time.time()
            if entry.get("changed", True):
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

        # Missing/opposite broker rows stay OPEN with SL_UNAVAILABLE. Only
        # an attributable exit fill (or an explicit, verified recovery) may
        # retire ownership. This also preserves pending reversals and margin
        # across empty responses, reconnects and process restarts.

        # A broker position the local book cannot attribute to any strategy.
        # It is NEVER auto-opened and NEVER auto-flattened: doing either would
        # mean trading on a guess. But it IS real exposure we hold no stop for
        # and can never exit, so it must be surfaced loudly and must keep the
        # entry gate shut rather than let new risk stack on top of it.
        orphans = summary.get("broker_only") or []
        orphan_fingerprint = tuple(sorted(
            (str(entry.get("instrument") or ""),
             str(entry.get("side") or ""), int(entry.get("quantity") or 0))
            for entry in orphans
        ))
        orphan_changed = orphan_fingerprint != getattr(
            env, "_sl_orphan_fingerprint", None)
        env._sl_orphan_fingerprint = orphan_fingerprint
        if orphans:
            if orphan_changed:
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
        if (summary.get("unavailable") or summary.get("ownership_ambiguous")
                or summary.get("trade_close_unresolved")
                or getattr(env, "pending_position_close_persist", None)
                or getattr(env, "pending_trade_close_persist", None)):
            summary["status"] = "protection_incomplete"
        elif summary.get("orphan_exposure"):
            summary["status"] = "orphan_exposure"
        else:
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

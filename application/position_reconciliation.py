"""Broker-authoritative reconciliation for the two SuperTrend strategies.

This module checks position ownership and quantities only. It does not infer
trades, create stops, close local records, or place orders from a positions
snapshot. Dhan may fan one instrument-level net row out across strategy ids,
so identical rows are collapsed before comparison.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Optional

log = logging.getLogger("trading_engine")


class PositionReconciliationMixin:
    def sync_positions_from_broker(self, env_name: Optional[str] = None) -> dict:
        env = self._env_for(env_name)
        if env is None:
            return {"status": "failed", "error": "unknown_environment",
                    "env": env_name, "orphan_exposure": False}
        broker = getattr(env, "broker", None)
        manager = getattr(env, "position_manager", None)
        if broker is None or manager is None or not callable(
                getattr(broker, "positions", None)):
            return {"status": "failed", "error": "broker_or_book_unavailable",
                    "env": env.name, "orphan_exposure": False}

        # Network I/O is deliberately outside the state lock.
        try:
            broker_rows = broker.positions() or []
        except Exception as exc:
            log.error("[Reconcile:%s] broker position query failed: %s",
                      env.name, exc)
            return {"status": "failed", "error": f"broker_query_failed: {exc}",
                    "env": env.name, "orphan_exposure": False}

        lock = getattr(self, "_lock", None)
        if lock is not None:
            with lock:
                return self._compare_broker_positions(env, broker_rows)
        return self._compare_broker_positions(env, broker_rows)

    def _compare_broker_positions(self, env, broker_rows) -> dict:
        manager = env.position_manager
        broker_by_instrument: dict[str, dict] = {}
        inconsistent_broker_rows = []
        for row in broker_rows:
            if not isinstance(row, dict):
                inconsistent_broker_rows.append({"reason": "invalid_row"})
                continue
            instrument = str(row.get("instrument") or "")
            if not instrument:
                inconsistent_broker_rows.append({"reason": "missing_instrument"})
                continue
            side = str(row.get("side") or "").upper()
            try:
                quantity = abs(int(row.get("quantity") or 0))
            except (TypeError, ValueError):
                inconsistent_broker_rows.append({
                    "instrument": instrument, "reason": "invalid_quantity"})
                continue
            if quantity == 0:
                continue
            candidate = {"instrument": instrument, "side": side,
                         "quantity": quantity, "ltp": row.get("ltp")}
            current = broker_by_instrument.get(instrument)
            if current is None:
                broker_by_instrument[instrument] = candidate
            elif (current["side"], current["quantity"]) != (side, quantity):
                inconsistent_broker_rows.append({
                    "instrument": instrument,
                    "reason": "conflicting_strategy_fanout_rows"})

        local_by_instrument: dict[str, list] = defaultdict(list)
        for position in list(getattr(manager, "open_positions", []) or []):
            if getattr(position, "is_open", False):
                local_by_instrument[str(position.instrument)].append(position)

        local_mismatch = []
        broker_only = []
        matches = []
        for instrument in sorted(set(local_by_instrument) | set(broker_by_instrument)):
            locals_for_instrument = local_by_instrument.get(instrument, [])
            broker_pos = broker_by_instrument.get(instrument)
            if len(locals_for_instrument) > 1:
                local_mismatch.append({
                    "instrument": instrument,
                    "reason": "multiple_local_position_owners",
                    "position_ids": [p.position_id for p in locals_for_instrument],
                })
                continue
            local = locals_for_instrument[0] if locals_for_instrument else None
            if local is None and broker_pos is not None:
                broker_only.append({**broker_pos,
                                   "reason": "broker_position_without_local_book"})
                continue
            if local is not None and broker_pos is None:
                local_mismatch.append({
                    "instrument": instrument,
                    "position_id": local.position_id,
                    "trade_id": local.trade_id,
                    "reason": "local_open_but_broker_flat",
                })
                continue
            if local is None or broker_pos is None:
                continue
            local_side = "LONG" if bool(local.is_long) else "SHORT"
            local_qty = int(getattr(local, "quantity", 0) or 0)
            if local_side != broker_pos["side"] or local_qty != broker_pos["quantity"]:
                local_mismatch.append({
                    "instrument": instrument,
                    "position_id": local.position_id,
                    "trade_id": local.trade_id,
                    "local_side": local_side,
                    "broker_side": broker_pos["side"],
                    "local_quantity": local_qty,
                    "broker_quantity": broker_pos["quantity"],
                    "reason": "local_broker_position_mismatch",
                })
                continue
            if broker_pos.get("ltp") not in (None, ""):
                try:
                    local.update_mark(float(broker_pos["ltp"]))
                except (TypeError, ValueError):
                    pass
            matches.append({
                "instrument": instrument,
                "strategy_id": str(local.strategy_id),
                "position_id": local.position_id,
                "trade_id": local.trade_id,
                "side": local_side,
                "quantity": local_qty,
                "broker_confirmed": True,
            })

        orphan_exposure = bool(broker_only or inconsistent_broker_rows)
        status = "reconciled" if not (local_mismatch or broker_only
                                        or inconsistent_broker_rows) else "mismatch"
        summary = {
            "status": status,
            "env": env.name,
            "matches": matches,
            "local_mismatch": local_mismatch,
            "broker_only": broker_only,
            "invalid_broker_rows": inconsistent_broker_rows,
            "orphan_exposure": orphan_exposure,
        }
        if broker_only:
            fingerprint = tuple(sorted((r["instrument"], r["side"], r["quantity"])
                                       for r in broker_only))
            prior = getattr(env, "_reported_broker_orphans", None)
            if prior != fingerprint:
                env._reported_broker_orphans = fingerprint
                for row in broker_only:
                    self.publish_event("broker_orphan_position", {
                        "instrument": row["instrument"],
                        "side": row["side"], "quantity": row["quantity"],
                        "reason": row["reason"], "execution_mode": env.mode,
                    }, env_name=env.name)
        elif hasattr(env, "_reported_broker_orphans"):
            env._reported_broker_orphans = ()
        if local_mismatch or inconsistent_broker_rows:
            self.publish_event("broker_position_reconciliation_error", {
                **summary, "execution_mode": env.mode,
            }, env_name=env.name)
        return summary

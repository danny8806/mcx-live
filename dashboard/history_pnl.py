"""P&L aggregates derived from the same deduplicated trade history API rows."""
from __future__ import annotations

from typing import Any, Optional


_CLOSED_STATES = {"CLOSED", "EXITED", "COMPLETED"}


def trade_history_pnl(env: Any, *, instrument: Optional[str] = None,
                      strategy: Optional[str] = None) -> dict:
    """Aggregate closed trades after fill-based history reconciliation.

    The trade route reconstructs legacy rows from attributable persisted fills
    without mutating the database. Reuse those exact rows here so dashboard
    P&L cards do not continue reporting older in-memory totals.
    """
    from dashboard.routes.trades import _list_trades_sync

    response = _list_trades_sync(strategy=strategy, instrument=instrument, env=env)
    rows = response.get("trades", []) if isinstance(response, dict) else []
    closed = [row for row in rows
              if str(row.get("status") or "").upper() in _CLOSED_STATES]

    by_instrument: dict[str, dict] = {}
    by_strategy: dict[str, dict] = {}
    total = _empty_bucket()
    for row in closed:
        inst = str(row.get("instrument") or "UNKNOWN").upper()
        strategy_id = str(row.get("strategy_id") or "UNKNOWN")
        gross = _number(row.get("gross_pnl"))
        charges = _number(row.get("charges"))
        net_value = row.get("net_pnl")
        net = _number(net_value) if net_value is not None else gross - charges
        reconciled = bool(row.get("pnl_reconciled_from_fills"))
        for bucket in (total,
                       by_instrument.setdefault(inst, _empty_bucket()),
                       by_strategy.setdefault(strategy_id, _empty_bucket())):
            bucket["trade_count"] += 1
            bucket["reconciled_trade_count"] += int(reconciled)
            bucket["unreconciled_trade_count"] += int(not reconciled)
            if reconciled:
                bucket["realized_gross"] += gross
                bucket["realized_charges"] += charges
                bucket["realized_net"] += net
                bucket["pnl_trade_count"] += 1
                bucket["wins"] += int(net > 0)
                bucket["losses"] += int(net < 0)

    for bucket in [total, *by_instrument.values(), *by_strategy.values()]:
        count = bucket["pnl_trade_count"]
        bucket["win_rate"] = bucket["wins"] / count if count else 0.0
        for key in ("realized_gross", "realized_charges", "realized_net"):
            bucket[key] = round(bucket[key], 2)

    return {
        "source": "fill_reconciled_trade_history",
        "history_source": response.get("source"),
        "total": total,
        "by_instrument": by_instrument,
        "by_strategy": by_strategy,
        "rows": closed,
    }


def _empty_bucket() -> dict:
    return {
        "realized_gross": 0.0,
        "realized_charges": 0.0,
        "realized_net": 0.0,
        "trade_count": 0,
        "pnl_trade_count": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "reconciled_trade_count": 0,
        "unreconciled_trade_count": 0,
    }


def _number(value: Any) -> float:
    try:
        number = float(value or 0)
        return number if number == number and abs(number) != float("inf") else 0.0
    except (TypeError, ValueError):
        return 0.0

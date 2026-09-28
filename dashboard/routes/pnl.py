"""P&L routes - per-instrument, per-strategy, portfolio-level."""
from __future__ import annotations
import asyncio
import time
from typing import Optional
from fastapi import APIRouter
router = APIRouter()
_engine = None
_bus = None
_persistence = None

def init(engine, event_bus, persistence=None):
    global _engine, _bus, _persistence
    _engine = engine
    _bus = event_bus
    _persistence = persistence

def _env_for(env=None):
    from dashboard.envs import resolve as _resolve_env
    return _resolve_env(_engine, env)


def _get_portfolio_pnl_sync(env=None):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        env = _env_for(env)
        account = env.account_engine.snapshot()
        strategies_cfg = _engine.config.get("strategies", {})
        by_instrument = {}
        for strat_name, eng in env.pnl_engines.items():
            cfg = strategies_cfg.get(strat_name, {})
            inst = cfg.get("instrument", strat_name)
            if inst not in by_instrument:
                by_instrument[inst] = {"realized_gross": 0, "realized_charges": 0, "realized_net": 0, "unrealized": 0, "trade_count": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "_count": 0}
            snap = eng.snapshot()
            by_instrument[inst]["realized_gross"] += snap.get("realized_gross", 0)
            by_instrument[inst]["realized_charges"] += snap.get("realized_charges", 0)
            by_instrument[inst]["realized_net"] += snap.get("realized_net", 0)
            by_instrument[inst]["trade_count"] += snap.get("trade_count", 0)
            by_instrument[inst]["wins"] += snap.get("wins", 0)
            by_instrument[inst]["losses"] += snap.get("losses", 0)
            by_instrument[inst]["_count"] += 1
        for inst, data in by_instrument.items():
            tc = data["trade_count"]
            data["win_rate"] = data["wins"] / tc if tc > 0 else 0.0
            data["unrealized"] = sum(
                p.unrealized_pnl
                for p in env.position_manager.get_positions_by_instrument(inst)
                if p.is_open
            )
            del data["_count"]
        realized = sum(v.get("realized_net", 0) for v in by_instrument.values())
        return {
            "execution_mode": getattr(env, "mode", "PAPER"),
            "portfolio": {
                "realized_pnl": realized,
                "unrealized_pnl": account.get("unrealized_pnl", 0),
                # Attribution net P&L (= realized + unrealized), as the
                # AccountEngine reports. Never equity - starting_capital: when
                # the broker reports equity (LIVE), that subtraction reveals
                # the configured capital base, not trading P&L.
                "net_pnl": account.get(
                    "net_pnl",
                    account.get("equity", 0) - account.get("starting_capital", 0),
                ),
                "charges": account.get("charges", 0),
                "equity": account.get("equity", 0),
                "starting_capital": account.get("starting_capital", 0),
            },
            "by_instrument": by_instrument,
            "timestamp": time.time(),
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/pnl")
async def get_portfolio_pnl():
    return await asyncio.to_thread(_get_portfolio_pnl_sync)


@router.get("/api/{env}/pnl")
async def get_portfolio_pnl_env(env: str):
    return await asyncio.to_thread(_get_portfolio_pnl_sync, env)

def _get_instrument_pnl_sync(instrument: str):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        inst = instrument.upper()
        strategies_cfg = _engine.config.get("strategies", {})
        aggregated = {"realized_gross": 0, "realized_charges": 0, "realized_net": 0, "trade_count": 0, "wins": 0, "losses": 0, "win_rate": 0.0}
        by_strategy = {}
        for strat_name, eng in _engine.pnl_engines.items():
            cfg = strategies_cfg.get(strat_name, {})
            if cfg.get("instrument", "") != inst:
                continue
            snap = eng.snapshot()
            aggregated["realized_gross"] += snap.get("realized_gross", 0)
            aggregated["realized_charges"] += snap.get("realized_charges", 0)
            aggregated["realized_net"] += snap.get("realized_net", 0)
            aggregated["trade_count"] += snap.get("trade_count", 0)
            aggregated["wins"] += snap.get("wins", 0)
            aggregated["losses"] += snap.get("losses", 0)
            by_strategy[strat_name] = {
                "realized": {
                    "realized_gross": snap.get("realized_gross", 0),
                    "realized_charges": snap.get("realized_charges", 0),
                    "realized_net": snap.get("realized_net", 0),
                    "trade_count": snap.get("trade_count", 0),
                    "wins": snap.get("wins", 0),
                    "losses": snap.get("losses", 0),
                    "win_rate": snap.get("win_rate", 0),
                },
            }
        tc = aggregated["trade_count"]
        aggregated["win_rate"] = aggregated["wins"] / tc if tc > 0 else 0.0
        account = _engine.account_engine.snapshot()
        positions = _engine.position_manager.get_positions_by_instrument(inst)
        unrealized = sum(p.unrealized_pnl for p in positions if p.is_open)
        return {
            "instrument": inst,
            "realized": aggregated,
            "strategies": by_strategy,
            "unrealized": unrealized,
            "position_count": len([p for p in positions if p.is_open]),
            "timestamp": time.time(),
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/pnl/{instrument}")
async def get_instrument_pnl(instrument: str):
    return await asyncio.to_thread(_get_instrument_pnl_sync, instrument)

def _get_strategy_pnl_sync(instrument: str, strategy_id: str):
    if not _engine:
        return {"error": "Engine not initialized"}
    try:
        inst = instrument.upper()
        eng = _engine.pnl_engines.get(strategy_id)
        positions = _engine.position_manager.get_positions_by_strategy(strategy_id)
        unrealized = sum(p.unrealized_pnl for p in positions if p.is_open)
        return {
            "strategy_id": strategy_id,
            "instrument": inst,
            "realized": eng.snapshot() if eng else {},
            "unrealized": unrealized,
            "position_count": len([p for p in positions if p.is_open]),
            "timestamp": time.time(),
        }
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/pnl/{instrument}/strategy/{strategy_id}")
async def get_strategy_pnl(instrument: str, strategy_id: str):
    return await asyncio.to_thread(_get_strategy_pnl_sync, instrument, strategy_id)

def _get_equity_curve_sync():
    try:
        if _persistence:
            snapshots = _persistence.get_account_snapshots(limit=500)
            return {"equity_curve": snapshots}
        if not _engine:
            return {"error": "Engine not initialized"}
        account = _engine.account_engine.snapshot()
        return {"equity_curve": [{"equity": account.get("equity", 0), "timestamp": time.time()}]}
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/equity-curve")
async def get_equity_curve():
    return await asyncio.to_thread(_get_equity_curve_sync)

def _get_instrument_equity_curve_sync(instrument: str):
    """Per-instrument equity curve derived from the canonical trades ledger.

    Account snapshots are portfolio-level only, so the per-instrument curve is
    built from each closed trade's net PnL for that instrument, ordered by exit
    time, as a cumulative realized-PnL journey.  If no trades exist yet the
    route falls back to the portfolio account snapshot so the endpoint always
    returns a shape the equity chart can render.
    """
    try:
        inst = instrument.upper()
        points = []
        if _persistence:
            trades = _persistence.get_trades()
            inst_trades = [
                t for t in trades
                if (t.get("instrument") or "").upper() == inst
                and t.get("net_pnl") is not None
            ]
            for t in sorted(
                inst_trades,
                key=lambda x: x.get("exit_timestamp") or x.get("entry_timestamp") or "",
            ):
                ts = t.get("exit_timestamp") or t.get("entry_timestamp") or ""
                ts_num = 0
                if isinstance(ts, (int, float)):
                    ts_num = float(ts)
                elif isinstance(ts, str) and ts:
                    try:
                        parsed = time.mktime(time.strptime(ts.replace("Z", ""), "%Y-%m-%dT%H:%M:%S"))
                        ts_num = parsed
                    except Exception:
                        try:
                            ts_num = time.mktime(time.strptime(ts.split(".")[0], "%Y-%m-%dT%H:%M:%S"))
                        except Exception:
                            ts_num = 0
                points.append({
                    "timestamp": ts_num,
                    "equity": float(t.get("net_pnl", 0) or 0),
                    "trade_id": t.get("trade_id"),
                })
        if not points:
            if not _engine:
                return {"error": "Engine not initialized"}
            account = _engine.account_engine.snapshot()
            points = [{"timestamp": time.time(), "equity": account.get("equity", 0)}]
        # Running cumulative total (per-instrument realized journey)
        acc = 0.0
        for pt in sorted(points, key=lambda x: x["timestamp"]):
            acc += pt["equity"]
            pt["equity"] = acc
        return {"instrument": inst, "equity_curve": points, "count": len(points)}
    except Exception as e:
        return {"error": str(e)}

@router.get("/api/equity-curve/{instrument}")
async def get_instrument_equity_curve(instrument: str):
    return await asyncio.to_thread(_get_instrument_equity_curve_sync, instrument)

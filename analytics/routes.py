"""Analytics API Routes - Strategy Intelligence and Performance Analytics."""
from __future__ import annotations
import json
import time
from typing import Optional
from fastapi import APIRouter, Query
from pathlib import Path

from persistence.database import Database

router = APIRouter()

# Module-level state (initialized by init())
_event_store = None
_trade_ledger = None
_performance_engine = None
_db_path = None
_default_starting_equity = 1_000_000
_STRATEGY_IDS = None

_STRATEGY_FALLBACK = ["gold_01", "gold_02", "silver_01", "silver_02"]


def _strategy_ids() -> list[str]:
    """Live strategy ids: explicit engine list first, then config file, then
    distinct ids present in the analytics DB, then defaults."""
    if _STRATEGY_IDS:
        return list(_STRATEGY_IDS)
    try:
        cfg_path = Path(__file__).resolve().parent.parent / "config" / "settings.json"
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        strat = cfg.get("strategies") or {}
        ids = [k for k in strat if strat.get(k)]
        if ids:
            return ids
    except Exception:
        pass
    try:
        if _db_path:
            db = Database(_db_path)
            ids = [r["strategy_id"] for r in db.query(
                "SELECT DISTINCT strategy_id FROM trades_analytics "
                "ORDER BY strategy_id")]
            if ids:
                return ids
    except Exception:
        pass
    return list(_STRATEGY_FALLBACK)


def set_default_starting_equity(value: Optional[float] = None):
    """Set the baseline used for equity/drawdown curves when the API callers do
    not pass an explicit starting_equity.  Must match the capital the frontend
    subtracts (account starting_capital) or the reported net P&L is wrong."""
    global _default_starting_equity
    if value is not None:
        _default_starting_equity = float(value)


def init(db_path: str = "trading.db", strategy_ids: Optional[list[str]] = None,
         starting_equity: Optional[float] = None):
    """Initialize analytics routes with database path and optional strategy list."""
    global _event_store, _trade_ledger, _performance_engine, _db_path, _STRATEGY_IDS, _default_starting_equity
    _db_path = db_path
    _STRATEGY_IDS = list(strategy_ids) if strategy_ids else None
    if starting_equity is not None:
        _default_starting_equity = float(starting_equity)
    try:
        from .event_store import EventStore
        _event_store = EventStore(db_path)
    except Exception:
        pass
    try:
        from .trade_ledger import TradeLedger
        _trade_ledger = TradeLedger(db_path)
    except Exception:
        pass
    try:
        from .performance import PerformanceEngine
        _performance_engine = PerformanceEngine(db_path)
    except Exception:
        pass


# =====================================================================
# STRATEGY OVERVIEW
# =====================================================================

@router.get("/api/analytics/strategies")
async def get_strategies_overview():
    """Get performance overview for all strategies."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    strategy_ids = _strategy_ids()
    
    strategies = []
    for sid in strategy_ids:
        try:
            perf = _performance_engine.calculate_strategy_performance(sid)
            strategies.append({
                "strategy_id": sid,
                "instrument": perf.instrument,
                "trade_count": perf.trade_count,
                "win_rate": round(perf.win_rate, 2),
                "profit_factor": round(perf.profit_factor, 2) if perf.profit_factor else None,
                "net_pnl": round(perf.net_pnl, 2),
                "max_drawdown": round(perf.max_drawdown, 2),
                "expectancy": round(perf.expectancy, 2),
                "sharpe": round(perf.sharpe, 2) if perf.sharpe else None,
                "sortino": round(perf.sortino, 2) if perf.sortino else None,
                "sample_warning": perf.sample_size_warning,
            })
        except Exception as e:
            strategies.append({"strategy_id": sid, "error": str(e)})
    
    return {"strategies": strategies, "timestamp": time.time()}


# =====================================================================
# INDIVIDUAL STRATEGY PERFORMANCE
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}")
async def get_strategy_performance(strategy_id: str):
    """Get comprehensive performance for a single strategy."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    perf = _performance_engine.calculate_strategy_performance(strategy_id)
    return {
        "strategy_id": perf.strategy_id,
        "instrument": perf.instrument,
        "trade_count": perf.trade_count,
        "winning_trades": perf.winning_trades,
        "losing_trades": perf.losing_trades,
        "win_rate": round(perf.win_rate, 2),
        "gross_profit": round(perf.gross_profit, 2),
        "gross_loss": round(perf.gross_loss, 2),
        "net_pnl": round(perf.net_pnl, 2),
        "profit_factor": round(perf.profit_factor, 2) if perf.profit_factor else None,
        "average_trade": round(perf.average_trade, 2),
        "average_win": round(perf.average_win, 2),
        "average_loss": round(perf.average_loss, 2),
        "median_trade": round(perf.median_trade, 2),
        "expectancy": round(perf.expectancy, 2),
        "payoff_ratio": round(perf.payoff_ratio, 2),
        "largest_win": round(perf.largest_win, 2),
        "largest_loss": round(perf.largest_loss, 2),
        "max_consecutive_wins": perf.max_consecutive_wins,
        "max_consecutive_losses": perf.max_consecutive_losses,
        "max_drawdown": round(perf.max_drawdown, 2),
        "sharpe": round(perf.sharpe, 2) if perf.sharpe else None,
        "sortino": round(perf.sortino, 2) if perf.sortino else None,
        "calmar": round(perf.calmar, 2) if perf.calmar else None,
        "avg_mfe": round(perf.avg_mfe, 2),
        "avg_mae": round(perf.avg_mae, 2),
        "avg_duration_minutes": round(perf.avg_duration_minutes, 2),
        "sample_warning": perf.sample_size_warning,
        "timestamp": time.time(),
    }


# =====================================================================
# TRADE HISTORY
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/trades")
async def get_strategy_trades(
    strategy_id: str,
    status: Optional[str] = Query(None, description="OPEN, CLOSED, PARTIALLY_CLOSED"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get trade history for a strategy."""
    if not _trade_ledger:
        return {"error": "Analytics not initialized"}
    
    if status == "OPEN":
        trades = _trade_ledger.get_open_trades(strategy_id=strategy_id)
    elif status == "CLOSED":
        trades = _trade_ledger.get_closed_trades(strategy_id=strategy_id, limit=limit)
    else:
        trades = _trade_ledger.get_trades_for_strategy(strategy_id)
    
    return {
        "strategy_id": strategy_id,
        "trades": [
            {
                "trade_id": t.trade_id,
                "instrument": t.instrument,
                "side": t.side,
                "status": t.status,
                "entry_price": t.average_entry_price,
                "exit_price": t.average_exit_price,
                "quantity": t.filled_quantity,
                "multiplier": t.multiplier,
                "net_pnl": t.net_pnl,
                "gross_pnl": t.gross_pnl,
                "fees": t.fees,
                "r_multiple": t.r_multiple,
                "mfe": t.mfe,
                "mae": t.mae,
                "duration_minutes": t.duration_minutes,
                "exit_reason": t.exit_reason,
                "signal_time": t.signal_time,
                "closed_at": t.closed_at,
            }
            for t in trades[:limit]
        ],
        "count": len(trades[:limit]),
    }


# =====================================================================
# EQUITY CURVE
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/equity")
async def get_strategy_equity(strategy_id: str, starting_equity: Optional[float] = None):
    """Get equity curve for a strategy."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}

    baseline = starting_equity if starting_equity is not None else _default_starting_equity
    curve = _performance_engine.calculate_equity_curve(strategy_id, baseline)
    return {"strategy_id": strategy_id, "equity_curve": curve, "count": len(curve)}


# =====================================================================
# DRAWDOWN
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/drawdown")
async def get_strategy_drawdown(strategy_id: str, starting_equity: Optional[float] = None):
    """Get drawdown curve for a strategy."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}

    baseline = starting_equity if starting_equity is not None else _default_starting_equity
    curve = _performance_engine.calculate_drawdown_curve(strategy_id, baseline)
    return {"strategy_id": strategy_id, "drawdown_curve": curve, "count": len(curve)}


# =====================================================================
# DAILY PERFORMANCE
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/daily")
async def get_strategy_daily(strategy_id: str):
    """Get daily performance aggregation."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    daily = _performance_engine.calculate_daily_performance(strategy_id)
    return {"strategy_id": strategy_id, "daily": daily, "count": len(daily)}


# =====================================================================
# MONTHLY PERFORMANCE
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/monthly")
async def get_strategy_monthly(strategy_id: str):
    """Get monthly performance aggregation."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    monthly = _performance_engine.calculate_monthly_performance(strategy_id)
    return {"strategy_id": strategy_id, "monthly": monthly, "count": len(monthly)}


# =====================================================================
# TIME OF DAY
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/time-of-day")
async def get_strategy_time_of_day(strategy_id: str):
    """Get time-of-day analysis."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    tod = _performance_engine.calculate_time_of_day_analysis(strategy_id)
    return {"strategy_id": strategy_id, "time_of_day": tod}


# =====================================================================
# DAY OF WEEK
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/day-of-week")
async def get_strategy_day_of_week(strategy_id: str):
    """Get day-of-week analysis."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    dow = _performance_engine.calculate_day_of_week_analysis(strategy_id)
    return {"strategy_id": strategy_id, "day_of_week": dow}


# =====================================================================
# MAE/MFE
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/mae-mfe")
async def get_strategy_mae_mfe(strategy_id: str):
    """Get MAE/MFE analysis from closed trades."""
    if not _trade_ledger:
        return {"error": "Analytics not initialized"}
    
    trades = _trade_ledger.get_closed_trades(strategy_id=strategy_id)
    mae_mfe = []
    for t in trades:
        entry = t.average_entry_price or 0
        mae_mfe.append({
            "trade_id": t.trade_id,
            "mfe": t.mfe or 0,
            "mae": t.mae or 0,
            "mfe_pct": ((t.mfe or 0) / entry * 100) if entry > 0 else 0,
            "mae_pct": ((t.mae or 0) / entry * 100) if entry > 0 else 0,
            "net_pnl": t.net_pnl or 0,
            "side": t.side,
        })
    
    return {"strategy_id": strategy_id, "mae_mfe": mae_mfe, "count": len(mae_mfe)}


# =====================================================================
# ROLLING PERFORMANCE
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/rolling")
async def get_strategy_rolling(strategy_id: str, window: int = Query(20, ge=5, le=100)):
    """Get rolling performance metrics."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    rolling = _performance_engine.calculate_rolling_performance(strategy_id, window)
    return {"strategy_id": strategy_id, "window": window, "rolling": rolling, "count": len(rolling)}


# =====================================================================
# EXECUTION QUALITY
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/execution")
async def get_strategy_execution(strategy_id: str):
    """Get execution quality metrics."""
    if not _trade_ledger:
        return {"error": "Analytics not initialized"}
    
    trades = _trade_ledger.get_closed_trades(strategy_id=strategy_id)
    executions = []
    for t in trades:
        legs = _trade_ledger.get_legs_for_trade(t.trade_id)
        executions.append({
            "trade_id": t.trade_id,
            "entry_slippage": t.entry_slippage or 0,
            "total_slippage": t.slippage_cost or 0,
            "fees": t.fees or 0,
            "fill_count": len(legs),
            "duration_seconds": t.duration_seconds or 0,
        })
    
    avg_slippage = sum(e["total_slippage"] for e in executions) / len(executions) if executions else 0
    avg_fees = sum(e["fees"] for e in executions) / len(executions) if executions else 0
    
    return {
        "strategy_id": strategy_id,
        "execution": executions,
        "avg_slippage": round(avg_slippage, 2),
        "avg_fees": round(avg_fees, 2),
        "count": len(executions),
    }


# =====================================================================
# PARAMETERS
# =====================================================================

@router.get("/api/analytics/strategies/{strategy_id}/parameters")
async def get_strategy_parameters(strategy_id: str):
    """Get parameter analysis for a strategy."""
    if not _trade_ledger:
        return {"error": "Analytics not initialized"}
    
    trades = _trade_ledger.get_trades_for_strategy(strategy_id)
    versions = {}
    for t in trades:
        ver = t.strategy_version or "v1"
        if ver not in versions:
            versions[ver] = {"version": ver, "trade_count": 0, "net_pnl": 0}
        versions[ver]["trade_count"] += 1
        versions[ver]["net_pnl"] += t.net_pnl or 0
    
    return {
        "strategy_id": strategy_id,
        "versions": list(versions.values()),
    }


# =====================================================================
# CORRELATION
# =====================================================================

@router.get("/api/analytics/correlation")
async def get_correlation():
    """Get strategy return correlation matrix."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    strategy_ids = _strategy_ids()
    
    corr = _performance_engine.calculate_strategy_correlation(strategy_ids)
    return corr


# =====================================================================
# PORTFOLIO
# =====================================================================

@router.get("/api/analytics/portfolio")
async def get_portfolio_analytics():
    """Get portfolio-level analytics."""
    if not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    strategy_ids = _strategy_ids()
    
    contributions = _performance_engine.calculate_portfolio_contribution(strategy_ids)
    total_pnl = sum(c["net_pnl"] for c in contributions)
    total_trades = sum(c["trade_count"] for c in contributions)
    
    # Monte Carlo on portfolio
    mc = _performance_engine.calculate_monte_carlo("gold_01", simulations=500)
    
    return {
        "portfolio": {
            "total_net_pnl": round(total_pnl, 2),
            "total_trades": total_trades,
            "strategy_count": len(strategy_ids),
        },
        "contributions": contributions,
        "monte_carlo": mc,
        "timestamp": time.time(),
    }


# =====================================================================
# TRADE DETAIL / FORENSICS
# =====================================================================

@router.get("/api/analytics/trades/{trade_id}")
async def get_trade_detail(trade_id: str):
    """Get detailed trade forensics."""
    if not _trade_ledger or not _event_store:
        return {"error": "Analytics not initialized"}
    
    trade = _trade_ledger.get_trade(trade_id)
    if not trade:
        return {"error": "Trade not found"}
    
    legs = _trade_ledger.get_legs_for_trade(trade_id)
    events = _event_store.get_events_for_trade(trade_id)
    
    return {
        "trade": {
            "trade_id": trade.trade_id,
            "strategy_id": trade.strategy_id,
            "instrument": trade.instrument,
            "side": trade.side,
            "status": trade.status,
            "entry_price": trade.average_entry_price,
            "exit_price": trade.average_exit_price,
            "quantity": trade.filled_quantity,
            "net_pnl": trade.net_pnl,
            "gross_pnl": trade.gross_pnl,
            "fees": trade.fees,
            "r_multiple": trade.r_multiple,
            "mfe": trade.mfe,
            "mae": trade.mae,
            "duration_minutes": trade.duration_minutes,
            "exit_reason": trade.exit_reason,
            "signal_time": trade.signal_time,
            "first_fill_time": trade.first_fill_time,
            "closed_at": trade.closed_at,
            "initial_stop": trade.initial_stop,
            "initial_risk": trade.initial_risk,
        },
        "legs": [
            {
                "leg_id": l.leg_id,
                "fill_id": l.fill_id,
                "side": l.side,
                "quantity": l.quantity,
                "price": l.price,
                "timestamp": l.timestamp,
                "is_entry": l.is_entry,
            }
            for l in legs
        ],
        "events": events,
    }


# =====================================================================
# OPEN TRADES
# =====================================================================

@router.get("/api/analytics/open-trades")
async def get_open_trades():
    """Get all currently open trades."""
    if not _trade_ledger:
        return {"error": "Analytics not initialized"}
    
    trades = _trade_ledger.get_open_trades()
    return {
        "open_trades": [
            {
                "trade_id": t.trade_id,
                "strategy_id": t.strategy_id,
                "instrument": t.instrument,
                "side": t.side,
                "quantity": t.filled_quantity,
                "average_entry": t.average_entry_price,
                "initial_stop": t.initial_stop,
                "mfe": t.mfe,
                "mae": t.mae,
                "signal_time": t.signal_time,
                "status": t.status,
            }
            for t in trades
        ],
        "count": len(trades),
    }


# =====================================================================
# EVENT JOURNAL
# =====================================================================

def _shape_events(events: list) -> list:
    """Reshape raw event-store rows into the UI contract {id, type, data,
    timestamp}, while retaining the raw event_type/payload columns."""
    import json as _json
    shaped = []
    for e in events:
        payload = e.get("payload")
        if isinstance(payload, str):
            try:
                payload = _json.loads(payload)
            except Exception:
                payload = None
        shaped.append({
            "id": e.get("event_id") or e.get("id") or e.get("sequence_number"),
            "type": e.get("event_type"),
            "timestamp": e.get("timestamp"),
            "data": payload if isinstance(payload, dict) else ({"message": payload} if payload else {}),
            "event_type": e.get("event_type"),
            "payload": e.get("payload"),
            "trade_id": e.get("trade_id"),
            "strategy_id": e.get("strategy_id"),
            "instrument": e.get("instrument"),
        })
    return shaped


@router.get("/api/analytics/events")
async def get_events(
    strategy_id: Optional[str] = None,
    trade_id: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = Query(100, ge=1, le=5000),
):
    """Get trading events."""
    if not _event_store:
        return {"error": "Analytics not initialized"}
    
    if trade_id:
        events = _event_store.get_events_for_trade(trade_id)
    elif strategy_id:
        events = _event_store.get_events_for_strategy(strategy_id, event_type, limit)
    else:
        events = []
    
    return {"events": _shape_events(events[:limit]), "count": len(events[:limit])}


# =====================================================================
# RECONCILIATION
# =====================================================================

@router.get("/api/analytics/reconciliation")
async def get_reconciliation():
    """Run reconciliation checks."""
    if not _trade_ledger or not _performance_engine:
        return {"error": "Analytics not initialized"}
    
    issues = []
    strategy_ids = _strategy_ids()
    
    for sid in strategy_ids:
        # Check: no trade with strategy_id = NULL
        trades = _trade_ledger.get_trades_for_strategy(sid)
        for t in trades:
            if not t.strategy_id:
                issues.append({"type": "MISSING_STRATEGY", "trade_id": t.trade_id})
        
        # Check: closed trade must have exit info
        closed = _trade_ledger.get_closed_trades(strategy_id=sid)
        for t in closed:
            if not t.average_exit_price:
                issues.append({"type": "CLOSED_NO_EXIT", "trade_id": t.trade_id})
            if not t.closed_at:
                issues.append({"type": "CLOSED_NO_TIMESTAMP", "trade_id": t.trade_id})
        
        # Check: no negative quantity
        all_trades = _trade_ledger.get_trades_for_strategy(sid)
        for t in all_trades:
            if t.filled_quantity < 0:
                issues.append({"type": "NEGATIVE_QUANTITY", "trade_id": t.trade_id})
    
    return {
        "status": "PASS" if not issues else "FAIL",
        "issues": issues,
        "issue_count": len(issues),
        "timestamp": time.time(),
    }


# =====================================================================
# ANALYTICS INIT STATUS
# =====================================================================

@router.get("/api/analytics/status")
async def get_analytics_status():
    """Get analytics system status."""
    return {
        "initialized": _performance_engine is not None,
        "event_store": _event_store is not None,
        "trade_ledger": _trade_ledger is not None,
        "performance_engine": _performance_engine is not None,
        "db_path": _db_path,
        "timestamp": time.time(),
    }

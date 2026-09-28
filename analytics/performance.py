"""Strategy Performance Engine - derives all analytics from canonical trades."""
from __future__ import annotations

from persistence.database import Database
import math
import statistics
import time
from collections import defaultdict
from typing import Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
import random

IST = timezone(timedelta(hours=5, minutes=30))


def _ist_format(fmt: str, ts: float) -> str:
    """Format an epoch timestamp in IST (the MCX trading session calendar).

    time.localtime() uses the container OS timezone (commonly UTC), which
    bucketed session trades onto the wrong hour/local date.
    """
    if not ts:
        ts = 0.0
    return datetime.fromtimestamp(ts, IST).strftime(fmt)


@dataclass
class TradeMetrics:
    """Metrics for a single closed trade."""
    trade_id: str
    strategy_id: str
    instrument: str
    side: str
    gross_pnl: float = 0.0
    net_pnl: float = 0.0
    return_pct: float = 0.0
    initial_risk: float = 0.0
    r_multiple: float = 0.0
    r_status: str = "UNDEFINED"
    mfe: float = 0.0
    mae: float = 0.0
    mfe_pct: float = 0.0
    mae_pct: float = 0.0
    duration_seconds: float = 0.0
    duration_minutes: float = 0.0
    entry_slippage: float = 0.0
    exit_slippage: float = 0.0
    total_slippage: float = 0.0
    fees: float = 0.0
    exit_efficiency_pct: Optional[float] = None
    entry_time: float = 0.0
    exit_time: float = 0.0
    entry_price: float = 0.0
    exit_price: float = 0.0
    quantity: int = 0
    multiplier: float = 1.0


@dataclass
class StrategyPerformance:
    """Aggregated performance metrics for a strategy."""
    strategy_id: str
    instrument: str = ""
    trade_count: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    breakeven_trades: int = 0
    win_rate: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    net_pnl: float = 0.0
    profit_factor: Optional[float] = None
    average_trade: float = 0.0
    average_win: float = 0.0
    average_loss: float = 0.0
    median_trade: float = 0.0
    expectancy: float = 0.0
    payoff_ratio: float = 0.0
    largest_win: float = 0.0
    largest_loss: float = 0.0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    max_drawdown: float = 0.0
    max_drawdown_duration: float = 0.0
    sharpe: Optional[float] = None
    sortino: Optional[float] = None
    calmar: Optional[float] = None
    recovery_factor: Optional[float] = None
    avg_mfe: float = 0.0
    avg_mae: float = 0.0
    avg_duration_minutes: float = 0.0
    sample_size_warning: Optional[str] = None


class PerformanceEngine:
    """Calculates all strategy performance metrics from canonical trades."""

    LOW_SAMPLE_THRESHOLD = 20
    LIMITED_SAMPLE_THRESHOLD = 50
    RISK_FREE_RATE = 0.0
    ANNUALIZATION_FACTOR = 252

    def __init__(self, db_path: str = "trading.db"):
        self._db = Database(db_path)

    def get_closed_trades(self, strategy_id: Optional[str] = None,
                          instrument: Optional[str] = None,
                          date_from: Optional[float] = None,
                          date_to: Optional[float] = None) -> list[dict]:
        """Get closed trades from database."""
        query = "SELECT * FROM trades_analytics WHERE status = 'CLOSED'"
        params: list[Any] = []
        if strategy_id:
            query += " AND strategy_id = ?"
            params.append(strategy_id)
        if instrument:
            query += " AND instrument = ?"
            params.append(instrument)
        if date_from:
            query += " AND closed_at >= ?"
            params.append(date_from)
        if date_to:
            query += " AND closed_at <= ?"
            params.append(date_to)
        query += " ORDER BY closed_at ASC"
        return self._db.query(query, params)

    def calculate_trade_metrics(self, trade: dict) -> TradeMetrics:
        """Calculate metrics for a single closed trade."""
        net_pnl = trade.get("net_pnl") or 0
        gross_pnl = trade.get("gross_pnl") or 0
        fees = trade.get("fees") or 0
        entry_price = trade.get("average_entry_price") or 0
        exit_price = trade.get("average_exit_price") or 0
        quantity = trade.get("exit_quantity") or trade.get("filled_quantity") or 0
        initial_risk = trade.get("initial_risk") or 0
        mfe = trade.get("mfe") or 0
        mae = trade.get("mae") or 0
        duration = trade.get("duration_seconds") or 0

        r_multiple = 0.0
        r_status = "UNDEFINED"
        if initial_risk > 0:
            r_multiple = net_pnl / (initial_risk * quantity)
            r_status = "DEFINED"

        return_pct = 0.0
        if entry_price > 0 and quantity > 0:
            return_pct = (net_pnl / (entry_price * quantity)) * 100

        mfe_pct = (mfe / entry_price * 100) if entry_price > 0 else 0
        mae_pct = (mae / entry_price * 100) if entry_price > 0 else 0

        exit_efficiency = None
        if mfe > 0:
            actual_profit = max(0, net_pnl)
            exit_efficiency = (actual_profit / mfe) * 100

        return TradeMetrics(
            trade_id=trade.get("trade_id", ""),
            strategy_id=trade.get("strategy_id", ""),
            instrument=trade.get("instrument", ""),
            side=trade.get("side", ""),
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            return_pct=return_pct,
            initial_risk=initial_risk,
            r_multiple=r_multiple,
            r_status=r_status,
            mfe=mfe,
            mae=mae,
            mfe_pct=mfe_pct,
            mae_pct=mae_pct,
            duration_seconds=duration,
            duration_minutes=duration / 60.0,
            entry_slippage=trade.get("entry_slippage") or 0,
            exit_slippage=0,
            total_slippage=trade.get("slippage_cost") or 0,
            fees=fees,
            exit_efficiency_pct=exit_efficiency,
            entry_time=trade.get("first_fill_time") or 0,
            exit_time=trade.get("last_exit_fill_time") or 0,
            entry_price=entry_price,
            exit_price=exit_price,
            quantity=quantity,
            multiplier=trade.get("multiplier") or 1.0,
        )

    def calculate_strategy_performance(self, strategy_id: str,
                                       date_from: Optional[float] = None,
                                       date_to: Optional[float] = None) -> StrategyPerformance:
        """Calculate comprehensive performance metrics for a strategy."""
        trades = self.get_closed_trades(strategy_id=strategy_id,
                                         date_from=date_from, date_to=date_to)

        perf = StrategyPerformance(strategy_id=strategy_id)

        if not trades:
            perf.sample_size_warning = "NO_TRADES"
            return perf

        perf.trade_count = len(trades)
        perf.instrument = trades[0].get("instrument", "")

        if perf.trade_count < self.LOW_SAMPLE_THRESHOLD:
            perf.sample_size_warning = f"LOW_SAMPLE ({perf.trade_count} trades)"
        elif perf.trade_count < self.LIMITED_SAMPLE_THRESHOLD:
            perf.sample_size_warning = f"LIMITED_SAMPLE ({perf.trade_count} trades)"

        net_pnls = [t.get("net_pnl") or 0 for t in trades]
        gross_pnls = [t.get("gross_pnl") or 0 for t in trades]

        perf.net_pnl = sum(net_pnls)
        perf.gross_profit = sum(p for p in gross_pnls if p > 0)
        perf.gross_loss = sum(p for p in gross_pnls if p < 0)

        perf.winning_trades = sum(1 for p in net_pnls if p > 0)
        perf.losing_trades = sum(1 for p in net_pnls if p < 0)
        perf.breakeven_trades = perf.trade_count - perf.winning_trades - perf.losing_trades
        perf.win_rate = (perf.winning_trades / perf.trade_count * 100) if perf.trade_count > 0 else 0

        if perf.gross_loss != 0:
            perf.profit_factor = perf.gross_profit / abs(perf.gross_loss)
        else:
            perf.profit_factor = None

        perf.average_trade = perf.net_pnl / perf.trade_count if perf.trade_count > 0 else 0
        # Averages share the SAME basis as the win/loss counts (net P&L).
        # Using gross_profit/gross_loss here mixed gross money with net counts,
        # inflating average_win/loss whenever a trade had fees.
        net_win_pnls = [p for p in net_pnls if p > 0]
        net_loss_pnls = [p for p in net_pnls if p < 0]
        perf.average_win = (sum(net_win_pnls) / len(net_win_pnls)) if net_win_pnls else 0
        perf.average_loss = (sum(net_loss_pnls) / len(net_loss_pnls)) if net_loss_pnls else 0
        perf.median_trade = statistics.median(net_pnls) if net_pnls else 0

        if perf.trade_count > 0:
            perf.expectancy = perf.net_pnl / perf.trade_count

        if perf.average_loss != 0:
            perf.payoff_ratio = abs(perf.average_win / perf.average_loss)

        perf.largest_win = max(net_pnls) if net_pnls else 0
        perf.largest_loss = min(net_pnls) if net_pnls else 0

        perf.max_consecutive_wins = self._max_consecutive(net_pnls, positive=True)
        perf.max_consecutive_losses = self._max_consecutive(net_pnls, positive=False)

        # Build a cumulative equity curve from per-trade net P&L before
        # measuring drawdown — _calculate_drawdown expects a running equity
        # series, not raw per-trade returns (feeding it raw P&L grossly
        # overstates drawdown when a big win precedes a loss).
        cumulative_equity: list[float] = []
        running = 0.0
        for p in net_pnls:
            running += p
            cumulative_equity.append(running)
        dd, dd_duration = self._calculate_drawdown(cumulative_equity)
        perf.max_drawdown = dd
        perf.max_drawdown_duration = dd_duration

        if len(net_pnls) >= 2:
            perf.sharpe = self._calculate_sharpe(net_pnls)
            perf.sortino = self._calculate_sortino(net_pnls)

        if perf.max_drawdown > 0:
            perf.recovery_factor = perf.net_pnl / perf.max_drawdown
            # Calmar = annualized net profit per unit of max drawdown.
            # Recovery factor is the non-annualized total; the two separate
            # once the trade history spans more/less than one year.
            times = [t.get("closed_at") or 0 for t in trades]
            times = [ts for ts in times if ts]
            years = 1.0
            if len(times) >= 2:
                span = max(times) - min(times)
                years = span / (365.25 * 86400.0) if span > 0 else 1.0
            if years <= 0:
                years = 1.0
            perf.calmar = (perf.net_pnl / years) / perf.max_drawdown

        mfe_values = [t.get("mfe") or 0 for t in trades]
        mae_values = [t.get("mae") or 0 for t in trades]
        perf.avg_mfe = statistics.mean(mfe_values) if mfe_values else 0
        perf.avg_mae = statistics.mean(mae_values) if mae_values else 0

        durations = [t.get("duration_minutes") or 0 for t in trades]
        perf.avg_duration_minutes = statistics.mean(durations) if durations else 0

        return perf

    def calculate_equity_curve(self, strategy_id: str,
                                starting_equity: float = 1_000_000) -> list[dict]:
        """Calculate equity curve from closed trades."""
        trades = self.get_closed_trades(strategy_id=strategy_id)
        equity = starting_equity
        curve = [{"timestamp": 0, "equity": equity, "trade_id": None}]

        for trade in trades:
            net_pnl = trade.get("net_pnl") or 0
            equity += net_pnl
            curve.append({
                "timestamp": trade.get("closed_at") or 0,
                "equity": equity,
                "trade_id": trade.get("trade_id"),
                "pnl": net_pnl,
            })

        return curve

    def calculate_drawdown_curve(self, strategy_id: str,
                                  starting_equity: float = 1_000_000) -> list[dict]:
        """Calculate drawdown over time."""
        curve = self.calculate_equity_curve(strategy_id, starting_equity)
        peak = starting_equity
        dd_curve = []

        for point in curve:
            eq = point["equity"]
            if eq > peak:
                peak = eq
            dd = peak - eq
            dd_pct = (dd / peak * 100) if peak > 0 else 0
            dd_curve.append({
                "timestamp": point["timestamp"],
                "equity": eq,
                "peak": peak,
                "drawdown": dd,
                "drawdown_pct": dd_pct,
            })

        return dd_curve

    def calculate_rolling_performance(self, strategy_id: str,
                                       window: int = 20) -> list[dict]:
        """Calculate rolling metrics over trade windows."""
        trades = self.get_closed_trades(strategy_id=strategy_id)
        if len(trades) < window:
            return []

        rolling = []
        for i in range(window, len(trades) + 1):
            window_trades = trades[i - window:i]
            net_pnls = [t.get("net_pnl") or 0 for t in window_trades]
            wins = sum(1 for p in net_pnls if p > 0)

            gross_profit = sum(p for p in net_pnls if p > 0)
            gross_loss = sum(p for p in net_pnls if p < 0)
            pf = gross_profit / abs(gross_loss) if gross_loss != 0 else None

            rolling.append({
                "trade_start": i - window + 1,
                "trade_end": i,
                "net_pnl": sum(net_pnls),
                "win_rate": (wins / window * 100) if window > 0 else 0,
                "profit_factor": pf,
                "expectancy": sum(net_pnls) / window if window > 0 else 0,
            })

        return rolling

    def calculate_daily_performance(self, strategy_id: str) -> list[dict]:
        """Calculate daily performance aggregation."""
        trades = self.get_closed_trades(strategy_id=strategy_id)
        daily: dict[str, dict] = {}

        for trade in trades:
            closed_at = trade.get("closed_at") or 0
            if closed_at:
                date_str = _ist_format("%Y-%m-%d", closed_at)
            else:
                date_str = "unknown"

            if date_str not in daily:
                daily[date_str] = {
                    "date": date_str,
                    "strategy_id": strategy_id,
                    "trade_count": 0,
                    "winning_trades": 0,
                    "gross_profit": 0.0,
                    "gross_loss": 0.0,
                    "net_pnl": 0.0,
                    "fees": 0.0,
                }

            d = daily[date_str]
            d["trade_count"] += 1
            net_pnl = trade.get("net_pnl") or 0
            gross = trade.get("gross_pnl") or 0
            d["net_pnl"] += net_pnl
            d["gross_profit"] += gross if gross > 0 else 0
            d["gross_loss"] += gross if gross < 0 else 0
            d["fees"] += trade.get("fees") or 0
            if net_pnl > 0:
                d["winning_trades"] += 1

        result = list(daily.values())
        for d in result:
            d["win_rate"] = (d["winning_trades"] / d["trade_count"] * 100) if d["trade_count"] > 0 else 0
            d["profit_factor"] = (d["gross_profit"] / abs(d["gross_loss"])) if d["gross_loss"] != 0 else None

        return sorted(result, key=lambda x: x["date"])

    def calculate_monthly_performance(self, strategy_id: str) -> list[dict]:
        """Calculate monthly performance aggregation."""
        trades = self.get_closed_trades(strategy_id=strategy_id)
        monthly: dict[str, dict] = {}

        for trade in trades:
            closed_at = trade.get("closed_at") or 0
            if closed_at:
                month_str = _ist_format("%Y-%m", closed_at)
            else:
                month_str = "unknown"

            if month_str not in monthly:
                monthly[month_str] = {
                    "month": month_str,
                    "strategy_id": strategy_id,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "gross_profit": 0.0,
                    "gross_loss": 0.0,
                    "gross_pnl": 0.0,
                    "fees": 0.0,
                    "net_pnl": 0.0,
                }

            m = monthly[month_str]
            m["trades"] += 1
            net_pnl = trade.get("net_pnl") or 0
            gross_pnl = trade.get("gross_pnl") or 0
            m["net_pnl"] += net_pnl
            m["gross_pnl"] += gross_pnl
            m["gross_profit"] += gross_pnl if gross_pnl > 0 else 0
            m["gross_loss"] += gross_pnl if gross_pnl < 0 else 0
            m["fees"] += trade.get("fees") or 0
            if net_pnl > 0:
                m["wins"] += 1
            elif net_pnl < 0:
                m["losses"] += 1

        result = list(monthly.values())
        for m in result:
            m["win_rate"] = (m["wins"] / m["trades"] * 100) if m["trades"] > 0 else 0
            m["profit_factor"] = (
                (m["gross_profit"] / abs(m["gross_loss"]))
                if m["gross_loss"] != 0 else None
            )

        return sorted(result, key=lambda x: x["month"])

    def calculate_time_of_day_analysis(self, strategy_id: str) -> list[dict]:
        """Analyze performance by entry time bucket."""
        trades = self.get_closed_trades(strategy_id=strategy_id)
        buckets: dict[str, dict] = {}

        for trade in trades:
            entry_time = trade.get("first_fill_time") or 0
            if entry_time:
                hour = int(_ist_format("%H", entry_time))
                bucket = f"{hour:02d}:00-{hour + 1:02d}:00"
            else:
                bucket = "unknown"

            if bucket not in buckets:
                buckets[bucket] = {
                    "time_bucket": bucket,
                    "strategy_id": strategy_id,
                    "trades": 0,
                    "wins": 0,
                    "net_pnl": 0.0,
                    "gross_profit": 0.0,
                    "gross_loss": 0.0,
                }

            b = buckets[bucket]
            b["trades"] += 1
            net_pnl = trade.get("net_pnl") or 0
            gross = trade.get("gross_pnl") or 0
            b["net_pnl"] += net_pnl
            b["gross_profit"] += gross if gross > 0 else 0
            b["gross_loss"] += gross if gross < 0 else 0
            if net_pnl > 0:
                b["wins"] += 1

        result = list(buckets.values())
        for b in result:
            b["win_rate"] = (b["wins"] / b["trades"] * 100) if b["trades"] > 0 else 0
            b["profit_factor"] = (b["gross_profit"] / abs(b["gross_loss"])) if b["gross_loss"] != 0 else None
            b["expectancy"] = b["net_pnl"] / b["trades"] if b["trades"] > 0 else 0

        return sorted(result, key=lambda x: x["time_bucket"])

    def calculate_day_of_week_analysis(self, strategy_id: str) -> list[dict]:
        """Analyze performance by day of week."""
        trades = self.get_closed_trades(strategy_id=strategy_id)
        days: dict[str, dict] = {}

        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        for trade in trades:
            entry_time = trade.get("first_fill_time") or 0
            if entry_time:
                dow = int(_ist_format("%w", entry_time))
                day_name = day_names[dow]
            else:
                day_name = "unknown"

            if day_name not in days:
                days[day_name] = {
                    "day": day_name,
                    "strategy_id": strategy_id,
                    "trades": 0,
                    "wins": 0,
                    "net_pnl": 0.0,
                }

            d = days[day_name]
            d["trades"] += 1
            net_pnl = trade.get("net_pnl") or 0
            d["net_pnl"] += net_pnl
            if net_pnl > 0:
                d["wins"] += 1

        result = list(days.values())
        for d in result:
            d["win_rate"] = (d["wins"] / d["trades"] * 100) if d["trades"] > 0 else 0

        return result

    def calculate_monte_carlo(self, strategy_id: str, simulations: int = 1000,
                               confidence_levels: list[float] | None = None) -> dict:
        """Monte Carlo simulation using closed trade returns."""
        if confidence_levels is None:
            confidence_levels = [5, 25, 50, 75, 95]

        trades = self.get_closed_trades(strategy_id=strategy_id)
        returns = [t.get("net_pnl") or 0 for t in trades]

        if len(returns) < 10:
            return {"error": "Insufficient trades for Monte Carlo", "trade_count": len(returns)}

        simulated_final_equity = []
        simulated_max_dd = []

        for _ in range(simulations):
            shuffled = returns.copy()
            random.shuffle(shuffled)

            equity = 0.0
            peak = 0.0
            max_dd = 0.0

            for r in shuffled:
                equity += r
                if equity > peak:
                    peak = equity
                dd = peak - equity
                if dd > max_dd:
                    max_dd = dd

            simulated_final_equity.append(equity)
            simulated_max_dd.append(max_dd)

        simulated_final_equity.sort()
        simulated_max_dd.sort()

        percentiles = {}
        for level in confidence_levels:
            idx = int(len(simulated_final_equity) * level / 100)
            idx = min(idx, len(simulated_final_equity) - 1)
            percentiles[f"p{level}"] = {
                "final_equity": simulated_final_equity[idx],
                "max_drawdown": simulated_max_dd[idx],
            }

        return {
            "simulations": simulations,
            "trade_count": len(returns),
            "percentiles": percentiles,
            "median_final_equity": simulated_final_equity[len(simulated_final_equity) // 2],
            "median_max_drawdown": simulated_max_dd[len(simulated_max_dd) // 2],
            "label": "SIMULATION",
        }

    def calculate_strategy_correlation(self, strategy_ids: list[str]) -> dict:
        """Calculate correlation matrix between strategies using daily returns."""
        daily_returns: dict[str, dict[str, float]] = defaultdict(dict)

        for sid in strategy_ids:
            trades = self.get_closed_trades(strategy_id=sid)
            for trade in trades:
                closed_at = trade.get("closed_at") or 0
                if closed_at:
                    date_str = _ist_format("%Y-%m-%d", closed_at)
                    daily_returns[sid][date_str] = daily_returns[sid].get(date_str, 0) + (trade.get("net_pnl") or 0)

        all_dates = set()
        for sid_returns in daily_returns.values():
            all_dates.update(sid_returns.keys())

        correlation = {}
        for i, s1 in enumerate(strategy_ids):
            for j, s2 in enumerate(strategy_ids):
                if i == j:
                    correlation[f"{s1}_{s2}"] = 1.0
                elif j < i:
                    continue
                else:
                    r1 = [daily_returns[s1].get(d, 0) for d in sorted(all_dates)]
                    r2 = [daily_returns[s2].get(d, 0) for d in sorted(all_dates)]
                    corr = self._pearson_correlation(r1, r2)
                    correlation[f"{s1}_{s2}"] = corr
                    correlation[f"{s2}_{s1}"] = corr

        return {
            "strategies": strategy_ids,
            "correlation_matrix": correlation,
            "sample_dates": len(all_dates),
        }

    def calculate_portfolio_contribution(self, strategy_ids: list[str]) -> list[dict]:
        """Calculate each strategy's contribution to portfolio P&L."""
        contributions = []
        total_pnl = 0

        for sid in strategy_ids:
            perf = self.calculate_strategy_performance(sid)
            total_pnl += perf.net_pnl
            contributions.append({
                "strategy_id": sid,
                "instrument": perf.instrument,
                "net_pnl": perf.net_pnl,
                "trade_count": perf.trade_count,
            })

        for c in contributions:
            c["pnl_contribution_pct"] = (c["net_pnl"] / total_pnl * 100) if total_pnl != 0 else 0

        return sorted(contributions, key=lambda x: abs(x["net_pnl"]), reverse=True)

    def _max_consecutive(self, values: list[float], positive: bool = True) -> int:
        """Calculate max consecutive wins or losses."""
        max_count = 0
        current = 0
        for v in values:
            if (positive and v > 0) or (not positive and v < 0):
                current += 1
                max_count = max(max_count, current)
            else:
                current = 0
        return max_count

    def _calculate_drawdown(self, equity_curve: list[float]) -> tuple[float, float]:
        """Calculate max drawdown and its duration.

        Duration = longest stretch of trades the equity stayed below its
        running peak, including an ongoing (unrecovered) drawdown through the
        end of the series. Measures underwater time instead of only elapsed
        trades between recovered peaks.
        """
        if not equity_curve:
            return 0.0, 0.0

        peak = equity_curve[0]
        peak_idx = 0
        max_dd = 0.0
        max_dd_duration = 0.0

        for i, val in enumerate(equity_curve):
            if val > peak:
                peak = val
                peak_idx = i
            dd = peak - val
            if dd > max_dd:
                max_dd = dd
            current_dd_duration = i - peak_idx
            if current_dd_duration > max_dd_duration:
                max_dd_duration = current_dd_duration

        return max_dd, max_dd_duration

    def _calculate_sharpe(self, returns: list[float]) -> Optional[float]:
        """Calculate annualized Sharpe ratio."""
        if len(returns) < 2:
            return None

        avg = statistics.mean(returns)
        std = statistics.stdev(returns)

        if std == 0:
            return None

        excess_return = avg - self.RISK_FREE_RATE / self.ANNUALIZATION_FACTOR
        return (excess_return / std) * math.sqrt(self.ANNUALIZATION_FACTOR)

    def _calculate_sortino(self, returns: list[float]) -> Optional[float]:
        """Calculate annualized Sortino ratio."""
        if len(returns) < 2:
            return None

        avg = statistics.mean(returns)
        downside = [r for r in returns if r < 0]

        if len(downside) < 2:
            return None

        downside_std = statistics.stdev(downside)
        if downside_std == 0:
            return None

        excess_return = avg - self.RISK_FREE_RATE / self.ANNUALIZATION_FACTOR
        return (excess_return / downside_std) * math.sqrt(self.ANNUALIZATION_FACTOR)

    def _pearson_correlation(self, x: list[float], y: list[float]) -> float:
        """Calculate Pearson correlation coefficient."""
        n = len(x)
        if n < 2:
            return 0.0

        mean_x = statistics.mean(x)
        mean_y = statistics.mean(y)

        numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
        denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

        if denom_x == 0 or denom_y == 0:
            return 0.0

        return numerator / (denom_x * denom_y)

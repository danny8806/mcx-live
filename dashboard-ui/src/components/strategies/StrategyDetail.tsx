import { useEffect, useState, useCallback } from "react";
import { api } from "../../lib/api";
import { formatINR } from "../../lib/utils";
import StrategyEquityChart from "./StrategyEquityChart";

interface StrategyDetailData {
  strategy_id: string;
  configuration: {
    instrument: string;
    fast_timeframe: string;
    htf_timeframe: string;
    quantity: number;
    enabled: boolean;
    dema_period: number;
    atr_period: number;
    atr_factor: number;
    starting_capital: number;
  };
  current_state: {
    state: string;
    position_side: string | null;
    stop_price: number | null;
    pending_entry: number | null;
    bars_processed: number;
  };
  indicators: {
    dema_value: number | null;
    atr_value: number | null;
    prev_output: any;
    count: number;
    initialized: boolean;
  };
  htf: {
    instrument: string;
    timeframe: string;
    last_confirmed_value: number | null;
    prev_confirmed_value: number | null;
    source_timestamp: number | null;
    indicator: {
      dema_value: number | null;
      atr_value: number | null;
      prev_output: any;
      count: number;
      initialized: boolean;
    };
  };
  performance: {
    realized_gross: number;
    realized_charges: number;
    realized_net: number;
    trade_count: number;
    wins: number;
    losses: number;
    win_rate: number;
  };
  positions: any[];
  snapshot: any;
}

interface AnalyticsPerf {
  strategy_id: string;
  instrument: string;
  trade_count: number;
  winning_trades: number;
  losing_trades: number;
  win_rate: number;
  gross_profit: number;
  gross_loss: number;
  net_pnl: number;
  profit_factor: number | null;
  average_trade: number;
  average_win: number;
  average_loss: number;
  median_trade: number;
  expectancy: number;
  payoff_ratio: number;
  largest_win: number;
  largest_loss: number;
  max_consecutive_wins: number;
  max_consecutive_losses: number;
  max_drawdown: number;
  sharpe: number | null;
  sortino: number | null;
  calmar: number | null;
  avg_mfe: number;
  avg_mae: number;
  avg_duration_minutes: number;
  sample_warning: string;
}

interface Trade {
  trade_id: string;
  instrument: string;
  side: string;
  status: string;
  entry_price: number;
  exit_price: number | null;
  quantity: number;
  net_pnl: number;
  gross_pnl: number;
  fees: number;
  r_multiple: number | null;
  mfe: number | null;
  mae: number | null;
  duration_minutes: number | null;
  exit_reason: string | null;
  signal_time: string | null;
  closed_at: string | null;
}

interface Event {
  id: number;
  type: string;
  timestamp: string;
  data: any;
}

interface EquityPoint {
  timestamp: number;
  equity: number;
  trade_id?: string | null;
}

interface DrawdownPoint {
  timestamp: number;
  equity: number;
  peak: number;
  drawdown: number;
  drawdown_pct: number;
}

interface Props {
  strategyId: string;
  marketData: any;
}

const PERIODS = ["TODAY", "1W", "1M", "3M", "ALL"];

const s = {
  panel: {
    background: "var(--bg-panel)",
    border: "1px solid var(--border)",
    borderRadius: 6,
    overflow: "hidden" as const,
  },
  section: {
    padding: "8px 12px",
    borderBottom: "1px solid var(--border-subtle)",
  },
  label: {
    fontSize: 9,
    fontWeight: 600,
    color: "var(--text-disabled)",
    textTransform: "uppercase" as const,
    letterSpacing: "0.5px",
    marginBottom: 6,
  },
  grid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fill, minmax(100px, 1fr))",
    gap: "6px 12px",
  },
  field: {
    display: "flex",
    flexDirection: "column" as const,
    gap: 2,
  },
  fieldLabel: {
    fontSize: 8,
    color: "var(--text-disabled)",
    textTransform: "uppercase" as const,
    letterSpacing: "0.3px",
  },
  fieldValue: {
    fontSize: 11,
    color: "var(--text-primary)",
    fontWeight: 500,
    fontVariantNumeric: "tabular-nums" as const,
  },
};

function MetricBox({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color?: string;
}) {
  return (
    <div style={s.field}>
      <div style={s.fieldLabel}>{label}</div>
      <div
        style={{
          ...s.fieldValue,
          color: color || "var(--text-primary)",
          fontSize: 13,
        }}
      >
        {value}
      </div>
    </div>
  );
}

export default function StrategyDetail({ strategyId, marketData }: Props) {
  const [detail, setDetail] = useState<StrategyDetailData | null>(null);
  const [analytics, setAnalytics] = useState<AnalyticsPerf | null>(null);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [equityCurve, setEquityCurve] = useState<EquityPoint[]>([]);
  const [drawdownCurve, setDrawdownCurve] = useState<DrawdownPoint[]>([]);
  const [events, setEvents] = useState<Event[]>([]);
  const [equityPeriod, setEquityPeriod] = useState("ALL");
  const [params, setParams] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const fetchData = useCallback(async () => {
    try {
      const [d, a, t, eq, dd, ev, pr] = await Promise.all([
        api.strategy(strategyId),
        api.analyticsStrategy(strategyId).catch(() => null),
        api.analyticsStrategyTrades(strategyId, 5).catch(() => null),
        api.analyticsStrategyEquity(strategyId).catch(() => null),
        api.analyticsStrategyDrawdown(strategyId).catch(() => null),
        api.analyticsEvents({ strategy_id: strategyId, limit: "20" }).catch(() => null),
        api.strategyParams(strategyId),
      ]);
      setDetail(d);
      setAnalytics(a);
      setTrades(t?.trades || []);
      setEquityCurve(eq?.equity_curve || []);
      setDrawdownCurve(dd?.drawdown_curve || []);
      setEvents(ev?.events || []);
      setParams(pr);
      setLoading(false);
      setError(false);
      // ### OBSERVATION (equity curve baseline)
      // The analytics curve is built on the default starting equity unless
      // `starting_equity` is passed.  StrategyDetail previously fetched it with
      // no baseline (1.2M default) but drew it against cfg.starting_capital
      // (300k), so the chart's "Current / P&L" readout was off by the
      // difference.  Re-fetch with the strategy's own capital so the curve and
      // the labels agree.
      const startCap = d?.configuration?.starting_capital;
      if (startCap) {
        api
          .analyticsStrategyEquity(strategyId, startCap)
          .then((eq2) => {
            if (eq2?.equity_curve) setEquityCurve(eq2.equity_curve);
          })
          .catch(() => {});
      }
    } catch (e) {
      console.error("StrategyDetail fetch error:", e);
      setError(true);
      setLoading(false);
    }
  }, [strategyId]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  if (loading) {
    return (
      <div
        style={{
          padding: "20px 12px",
          textAlign: "center",
          color: "var(--text-muted)",
          fontSize: 10,
        }}
      >
        Loading strategy details...
      </div>
    );
  }

  if (error || !detail) {
    return (
      <div
        style={{
          padding: "12px",
          color: "var(--red)",
          fontSize: 10,
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}
      >
        <span>Performance data unavailable</span>
        <button
          onClick={fetchData}
          style={{
            background: "var(--bg-input)",
            border: "1px solid var(--border)",
            borderRadius: 3,
            color: "var(--text-secondary)",
            padding: "2px 8px",
            fontSize: 9,
            cursor: "pointer",
          }}
        >
          Retry
        </button>
      </div>
    );
  }

  const cfg = detail.configuration;
  const st = detail.current_state;
  const ind = detail.indicators;
  const htf = detail.htf;
  const perf = detail.performance;
  const ann = analytics;

  const instData = marketData?.instruments?.[cfg.instrument] || marketData?.[cfg.instrument] || {};
  const ltp = instData.ltp || instData.last_price || 0;
  // ### OBSERVATION (P&L + multiplier source)
  // Unrealized P&L and the contract multiplier are reported by the backend
  // (/api/positions carries unrealized_pnl and the engine's multiplier).
  // No client-side guessing of the contract factor is applied here; when the
  // backend has no open position the fields render as em-dash rather than a
  // fabricated value.
  const pos = detail.positions?.[0];
  const posMultiplier = pos?.multiplier ?? null;
  const unrealized =
    pos?.unrealized_pnl != null ? pos.unrealized_pnl : null;

  const entryPrice = pos?.entry_price ?? 0;
  const posQty = pos?.quantity ?? 0;

  return (
    <div
      style={{
        padding: "8px 12px 12px",
        display: "flex",
        flexDirection: "column",
        gap: 8,
      }}
    >
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 8 }}>
        <div style={s.panel}>
          <div style={{ ...s.section, borderBottom: "1px solid var(--border-subtle)" }}>
            <div style={s.label}>Current State</div>
            <div style={s.grid}>
              <MetricBox label="Status" value={st.state?.toUpperCase() || "—"} />
              <MetricBox
                label="Signal"
                value={st.position_side || "FLAT"}
                color={
                  st.position_side === "LONG"
                    ? "var(--green)"
                    : st.position_side === "SHORT"
                    ? "var(--red)"
                    : "var(--text-muted)"
                }
              />
              <MetricBox
                label="Position"
                value={
                  posQty > 0
                    ? `${posQty} ${st.position_side || ""}`
                    : "0"
                }
              />
              <MetricBox label="Entry" value={entryPrice ? `₹${entryPrice.toLocaleString("en-IN")}` : "—"} />
              <MetricBox label="LTP" value={ltp ? `₹${ltp.toLocaleString("en-IN")}` : "—"} />
              <MetricBox
                label="Unrealized P&L"
                value={unrealized != null ? formatINR(unrealized) : "—"}
                color={unrealized != null ? (unrealized >= 0 ? "var(--green)" : "var(--red)") : "var(--text-muted)"}
              />
            </div>
          </div>
        </div>

        <div style={s.panel}>
          <div style={{ ...s.section, borderBottom: "1px solid var(--border-subtle)" }}>
            <div style={s.label}>Indicators</div>
            <div style={s.grid}>
              <MetricBox label="TF" value={cfg.fast_timeframe} />
              <MetricBox label="HTF" value={cfg.htf_timeframe} />
              <MetricBox label="DEMA Period" value={String(cfg.dema_period)} />
              <MetricBox
                label="DEMA Value"
                value={
                  ind.dema_value != null
                    ? `₹${ind.dema_value.toLocaleString("en-IN")}`
                    : "N/A"
                }
              />
              <MetricBox label="ATR Period" value={String(cfg.atr_period)} />
              <MetricBox
                label="ATR Value"
                value={
                  ind.atr_value != null
                    ? `₹${ind.atr_value.toLocaleString("en-IN")}`
                    : "N/A"
                }
              />
              <MetricBox
                label="HTF DEMA"
                value={
                  htf?.indicator?.dema_value != null
                    ? `₹${htf.indicator.dema_value.toLocaleString("en-IN")}`
                    : "N/A"
                }
              />
              <MetricBox
                label="HTF Trend"
                value={
                  htf?.last_confirmed_value != null
                    ? htf.last_confirmed_value > (htf?.prev_confirmed_value || 0)
                      ? "BULLISH"
                      : "BEARISH"
                    : "N/A"
                }
                color={
                  htf?.last_confirmed_value != null
                    ? htf.last_confirmed_value > (htf?.prev_confirmed_value || 0)
                      ? "var(--green)"
                      : "var(--red)"
                    : "var(--text-muted)"
                }
              />
            </div>
          </div>
        </div>

        <div style={s.panel}>
          <div style={{ ...s.section, borderBottom: "1px solid var(--border-subtle)" }}>
            <div style={s.label}>Strategy Parameters</div>
            <div style={s.grid}>
              <MetricBox label="Timeframe" value={cfg.fast_timeframe} />
              <MetricBox label="HTF" value={cfg.htf_timeframe} />
              <MetricBox label="DEMA Period" value={String(cfg.dema_period)} />
              <MetricBox label="ATR Period" value={String(cfg.atr_period)} />
              <MetricBox label="ATR Factor" value={String(cfg.atr_factor)} />
              <MetricBox label="Quantity" value={String(cfg.quantity)} />
              <MetricBox label="Instrument" value={cfg.instrument} />
              <MetricBox label="Multiplier" value={posMultiplier != null ? String(posMultiplier) : "—"} />
              {params && (
                <>
                  <MetricBox
                    label="Daily Loss Limit"
                    value={params.risk?.max_daily_loss != null ? `₹${params.risk.max_daily_loss.toLocaleString("en-IN")}` : "—"}
                  />
                  <MetricBox
                    label="Max Positions"
                    value={params.risk?.max_open_positions_total != null ? String(params.risk.max_open_positions_total) : "—"}
                  />
                  <MetricBox
                    label="Per-Strat Positions"
                    value={params.risk?.max_open_positions_per_strategy != null ? String(params.risk.max_open_positions_per_strategy) : "—"}
                  />
                  <MetricBox
                    label="Margin/Trade"
                    value={params.risk?.margin_per_trade_pct != null ? `${params.risk.margin_per_trade_pct}%` : "—"}
                  />
                  <MetricBox
                    label="Slippage (ticks)"
                    value={params.execution?.slippage_ticks != null ? String(params.execution.slippage_ticks) : "—"}
                  />
                  <MetricBox
                    label="Brokerage/Order"
                    value={
                      params.charges?.brokerage_per_side != null
                        ? `₹${params.charges.brokerage_per_side.toLocaleString("en-IN")}`
                        : "—"
                    }
                  />
                  <MetricBox
                    label="STT"
                    value={params.charges?.stt_sell_pct != null ? `${params.charges.stt_sell_pct * 100}%` : "—"}
                  />
                  <MetricBox
                    label="Exchange"
                    value={params.charges?.exchange_pct != null ? `${params.charges.exchange_pct * 100}%` : "—"}
                  />
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      <div style={s.panel}>
        <div style={{ ...s.section, borderBottom: "1px solid var(--border-subtle)" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
            <div style={s.label}>Strategy Equity Curve</div>
            <div style={{ display: "flex", gap: 4 }}>
              {PERIODS.map((p) => (
                <button
                  key={p}
                  onClick={() => setEquityPeriod(p)}
                  style={{
                    background: equityPeriod === p ? "var(--bg-panel-active)" : "transparent",
                    border: `1px solid ${equityPeriod === p ? "var(--border-active)" : "var(--border-subtle)"}`,
                    borderRadius: 3,
                    color: equityPeriod === p ? "var(--text-primary)" : "var(--text-muted)",
                    padding: "1px 6px",
                    fontSize: 8,
                    fontWeight: equityPeriod === p ? 600 : 400,
                    cursor: "pointer",
                    textTransform: "uppercase" as const,
                  }}
                >
                  {p}
                </button>
              ))}
            </div>
          </div>
          <StrategyEquityChart
            equityCurve={equityCurve}
            drawdownCurve={drawdownCurve}
            startingEquity={cfg.starting_capital ?? null}
            period={equityPeriod}
            height={130}
          />
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
        <div style={s.panel}>
          <div style={{ ...s.section, borderBottom: "1px solid var(--border-subtle)" }}>
            <div style={s.label}>Performance</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "6px 12px" }}>
              <MetricBox
                label="Net P&L"
                value={formatINR(perf.realized_net)}
                color={perf.realized_net >= 0 ? "var(--green)" : "var(--red)"}
              />
              <MetricBox
                label="Gross Profit"
                value={formatINR(ann?.gross_profit || 0)}
                color="var(--green)"
              />
              <MetricBox
                label="Gross Loss"
                value={formatINR(ann?.gross_loss || 0)}
                color="var(--red)"
              />
              <MetricBox label="Trades" value={String(perf.trade_count)} />
              <MetricBox label="Wins" value={String(perf.wins)} />
              <MetricBox label="Losses" value={String(perf.losses)} />
              <MetricBox
                label="Win Rate"
                value={`${perf.win_rate.toFixed(0)}%`}
              />
              <MetricBox
                label="Profit Factor"
                value={
                  ann?.profit_factor != null
                    ? ann.profit_factor.toFixed(2)
                    : "N/A"
                }
              />
              <MetricBox
                label="Max Drawdown"
                value={
                  ann?.max_drawdown
                    ? `₹${ann.max_drawdown.toLocaleString("en-IN")}`
                    : "₹0"
                }
                color={ann?.max_drawdown ? "var(--red)" : "var(--text-muted)"}
              />
              <MetricBox
                label="Avg Win"
                value={ann?.average_win ? formatINR(ann.average_win) : "N/A"}
              />
              <MetricBox
                label="Avg Loss"
                value={ann?.average_loss ? formatINR(ann.average_loss) : "N/A"}
              />
              <MetricBox
                label="Expectancy"
                value={ann?.expectancy ? formatINR(ann.expectancy) : "N/A"}
              />
              <MetricBox
                label="Sharpe"
                value={ann?.sharpe != null ? ann.sharpe.toFixed(2) : "N/A"}
              />
              <MetricBox
                label="Sortino"
                value={ann?.sortino != null ? ann.sortino.toFixed(2) : "N/A"}
              />
              <MetricBox
                label="Avg Duration"
                value={
                  ann?.avg_duration_minutes
                    ? `${ann.avg_duration_minutes.toFixed(0)}m`
                    : "N/A"
                }
              />
            </div>
          </div>
        </div>

        <div style={s.panel}>
          <div style={{ ...s.section, borderBottom: "1px solid var(--border-subtle)" }}>
            <div style={s.label}>Recent Trades</div>
            {trades.length === 0 ? (
              <div
                style={{
                  padding: "12px 0",
                  textAlign: "center",
                  color: "var(--text-disabled)",
                  fontSize: 10,
                }}
              >
                NO TRADES YET
              </div>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    fontSize: 9,
                  }}
                >
                  <thead>
                    <tr style={{ color: "var(--text-disabled)", textTransform: "uppercase" }}>
                      <th style={{ padding: "3px 6px", textAlign: "left" }}>Time</th>
                      <th style={{ padding: "3px 6px", textAlign: "left" }}>Side</th>
                      <th style={{ padding: "3px 6px", textAlign: "right" }}>Entry</th>
                      <th style={{ padding: "3px 6px", textAlign: "right" }}>Exit</th>
                      <th style={{ padding: "3px 6px", textAlign: "right" }}>P&L</th>
                      <th style={{ padding: "3px 6px", textAlign: "right" }}>Duration</th>
                      <th style={{ padding: "3px 6px", textAlign: "left" }}>Reason</th>
                    </tr>
                  </thead>
                  <tbody>
                    {trades.slice(0, 5).map((t) => (
                      <tr
                        key={t.trade_id}
                        style={{ borderTop: "1px solid var(--border-subtle)" }}
                      >
                        <td style={{ padding: "3px 6px", color: "var(--text-muted)" }}>
                          {t.signal_time
                            ? new Date(t.signal_time).toLocaleTimeString("en-IN", {
                                hour: "2-digit",
                                minute: "2-digit",
                                hour12: false,
                              })
                            : "—"}
                        </td>
                        <td
                          style={{
                            padding: "3px 6px",
                            color: t.side === "BUY" ? "var(--green)" : "var(--red)",
                            fontWeight: 500,
                          }}
                        >
                          {t.side}
                        </td>
                        <td
                          style={{
                            padding: "3px 6px",
                            textAlign: "right",
                            color: "var(--text-secondary)",
                            fontVariantNumeric: "tabular-nums",
                          }}
                        >
                          ₹{t.entry_price?.toLocaleString("en-IN") || "—"}
                        </td>
                        <td
                          style={{
                            padding: "3px 6px",
                            textAlign: "right",
                            color: "var(--text-secondary)",
                            fontVariantNumeric: "tabular-nums",
                          }}
                        >
                          ₹{t.exit_price?.toLocaleString("en-IN") || "—"}
                        </td>
                        <td
                          style={{
                            padding: "3px 6px",
                            textAlign: "right",
                            color: t.net_pnl >= 0 ? "var(--green)" : "var(--red)",
                            fontWeight: 600,
                            fontVariantNumeric: "tabular-nums",
                          }}
                        >
                          {formatINR(t.net_pnl || 0)}
                        </td>
                        <td
                          style={{
                            padding: "3px 6px",
                            textAlign: "right",
                            color: "var(--text-muted)",
                          }}
                        >
                          {t.duration_minutes != null
                            ? `${t.duration_minutes.toFixed(0)}m`
                            : "—"}
                        </td>
                        <td
                          style={{
                            padding: "3px 6px",
                            color: "var(--text-muted)",
                          }}
                        >
                          {t.exit_reason || "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>

      <div style={s.panel}>
        <div style={s.section}>
          <div style={s.label}>Strategy Events</div>
          {events.length === 0 ? (
            <div
              style={{
                padding: "8px 0",
                textAlign: "center",
                color: "var(--text-disabled)",
                fontSize: 10,
              }}
            >
              No recent events
            </div>
          ) : (
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: 3,
                maxHeight: 120,
                overflowY: "auto",
              }}
            >
              {events.slice(0, 15).map((ev, i) => (
                <div
                  key={i}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 8,
                    fontSize: 9,
                  }}
                >
                  <span style={{ color: "var(--text-muted)", fontVariantNumeric: "tabular-nums", minWidth: 50 }}>
                    {ev.timestamp
                      ? new Date(
                          typeof ev.timestamp === "string"
                            ? ev.timestamp
                            : ev.timestamp * 1000
                        ).toLocaleTimeString("en-IN", {
                          hour: "2-digit",
                          minute: "2-digit",
                          second: "2-digit",
                          hour12: false,
                        })
                      : "—"}
                  </span>
                  <span
                    style={{
                      width: 4,
                      height: 4,
                      borderRadius: "50%",
                      background:
                        ev.type?.includes("SIGNAL")
                          ? "var(--green)"
                          : ev.type?.includes("ERROR")
                          ? "var(--red)"
                          : ev.type?.includes("ORDER")
                          ? "var(--blue)"
                          : "var(--text-muted)",
                      flexShrink: 0,
                    }}
                  />
                  <span style={{ color: "var(--text-secondary)" }}>
                    {ev.type?.replace(/_/g, " ") || "—"}
                  </span>
                  {ev.data?.message && (
                    <span style={{ color: "var(--text-muted)" }}>
                      {typeof ev.data.message === "string"
                        ? ev.data.message.slice(0, 60)
                        : ""}
                    </span>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

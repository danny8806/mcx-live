import { useState, useMemo } from "react";
import { formatINR } from "../../lib/utils";

interface StrategyPerf {
  strategy_id: string;
  instrument: string;
  trade_count: number;
  win_rate: number;
  profit_factor: number | null;
  net_pnl: number;
  max_drawdown: number;
  expectancy: number;
  sample_warning?: string;
}

interface EquityPoint {
  timestamp: number;
  equity: number;
}

interface Props {
  strategies: StrategyPerf[];
  selectedIds: string[];
  onToggle: (id: string) => void;
  equityCurves: Record<string, EquityPoint[]>;
  startingEquity: number | null;
}

const COMPARE_COLORS = ["#20D47A", "#4A9EFF", "#F2B84B", "#9A7CFF"];

function normalizeCurve(
  points: EquityPoint[],
  startVal: number | null
): { x: number; y: number }[] {
  if (!points.length) return [];
  // ### OBSERVATION (normalization baseline)
  // Each equity curve's first point is its own baseline; when no explicit
  // starting value is provided the curve's own start is used (never fabricated).
  const base = startVal ?? points[0].equity ?? 1;
  return points.map((p, i) => ({
    x: i,
    y: (p.equity / base) * 100,
  }));
}

export default function StrategyCompare({
  strategies,
  selectedIds,
  onToggle: _onToggle,
  equityCurves,
  startingEquity,
}: Props) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  void hoverIdx; void setHoverIdx;

  const curves = useMemo(() => {
    return selectedIds
      .map((id, ci) => {
        const pts = equityCurves[id] || [];
        const norm = normalizeCurve(pts, startingEquity);
        return {
          id,
          color: COMPARE_COLORS[ci % COMPARE_COLORS.length],
          points: norm,
        };
      })
      .filter((c) => c.points.length > 1);
  }, [selectedIds, equityCurves, startingEquity]);

  const metrics = useMemo(() => {
    return selectedIds
      .map((id) => strategies.find((s) => s.strategy_id === id))
      .filter(Boolean) as StrategyPerf[];
  }, [selectedIds, strategies]);

  const w = 500;
  const h = 120;
  const pad = { top: 10, right: 10, bottom: 10, left: 10 };

  const { minY, maxY, maxLen } = useMemo(() => {
    let mn = 100,
      mx = 100,
      ml = 0;
    curves.forEach((c) => {
      c.points.forEach((p) => {
        if (p.y < mn) mn = p.y;
        if (p.y > mx) mx = p.y;
      });
      if (c.points.length > ml) ml = c.points.length;
    });
    const margin = (mx - mn) * 0.1 || 1;
    return { minY: mn - margin, maxY: mx + margin, maxLen: ml };
  }, [curves]);

  const scaleX = (i: number) => {
    if (maxLen <= 1) return pad.left;
    return pad.left + (i / (maxLen - 1)) * (w - pad.left - pad.right);
  };

  const scaleY = (y: number) => {
    const range = maxY - minY || 1;
    return h - pad.bottom - ((y - minY) / range) * (h - pad.top - pad.bottom);
  };

  if (!selectedIds.length) {
    return (
      <div
        style={{
          background: "var(--bg-panel)",
          border: "1px solid var(--border)",
          borderRadius: 6,
          padding: "12px 16px",
          fontSize: 10,
          color: "var(--text-muted)",
        }}
      >
        Select strategies to compare (up to 4)
      </div>
    );
  }

  return (
    <div
      style={{
        background: "var(--bg-panel)",
        border: "1px solid var(--border)",
        borderRadius: 6,
        overflow: "hidden",
      }}
    >
      <div
        style={{
          padding: "6px 12px",
          borderBottom: "1px solid var(--border-subtle)",
          fontSize: 9,
          fontWeight: 600,
          color: "var(--text-muted)",
          textTransform: "uppercase",
          letterSpacing: "0.5px",
        }}
      >
        NORMALIZED STRATEGY PERFORMANCE
      </div>

      <div style={{ padding: "8px 12px" }}>
        <svg
          viewBox={`0 0 ${w} ${h}`}
          style={{ width: "100%", height: `${h}px` }}
        >
          <line
            x1={pad.left}
            x2={w - pad.right}
            y1={scaleY(100)}
            y2={scaleY(100)}
            stroke="var(--border)"
            strokeWidth={0.5}
            strokeDasharray="3,3"
          />
          {curves.map((curve) => {
            const d = curve.points
              .map(
                (p, i) =>
                  `${i === 0 ? "M" : "L"}${scaleX(p.x)},${scaleY(p.y)}`
              )
              .join(" ");
            return (
              <path
                key={curve.id}
                d={d}
                fill="none"
                stroke={curve.color}
                strokeWidth={1.5}
                strokeLinejoin="round"
              />
            );
          })}
          <text
            x={w - pad.right}
            y={scaleY(100) - 3}
            textAnchor="end"
            fill="var(--text-disabled)"
            fontSize={8}
          >
            100
          </text>
        </svg>
      </div>

      <div
        style={{
          display: "flex",
          gap: 8,
          padding: "4px 12px 8px",
          flexWrap: "wrap",
        }}
      >
        {curves.map((c) => {
          const last = c.points[c.points.length - 1];
          return (
            <div key={c.id} style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 9 }}>
              <span
                style={{
                  width: 6,
                  height: 6,
                  borderRadius: "50%",
                  background: c.color,
                  display: "inline-block",
                }}
              />
              <span style={{ color: "var(--text-secondary)" }}>{c.id}</span>
              <span
                style={{
                  color: last.y >= 100 ? "var(--green)" : "var(--red)",
                  fontWeight: 600,
                }}
              >
                {last.y.toFixed(1)}
              </span>
            </div>
          );
        })}
      </div>

      {metrics.length > 0 && (
        <div style={{ overflowX: "auto", borderTop: "1px solid var(--border-subtle)" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              fontSize: 9,
            }}
          >
            <thead>
              <tr
                style={{
                  color: "var(--text-disabled)",
                  textTransform: "uppercase",
                  letterSpacing: "0.5px",
                }}
              >
                <th style={{ padding: "4px 8px", textAlign: "left" }}>Strategy</th>
                <th style={{ padding: "4px 8px", textAlign: "right" }}>Net P&L</th>
                <th style={{ padding: "4px 8px", textAlign: "right" }}>Win Rate</th>
                <th style={{ padding: "4px 8px", textAlign: "right" }}>Profit Factor</th>
                <th style={{ padding: "4px 8px", textAlign: "right" }}>Trades</th>
                <th style={{ padding: "4px 8px", textAlign: "right" }}>Max DD</th>
                <th style={{ padding: "4px 8px", textAlign: "right" }}>Expectancy</th>
              </tr>
            </thead>
            <tbody>
              {metrics.map((m, i) => (
                <tr key={m.strategy_id} style={{ borderTop: "1px solid var(--border-subtle)" }}>
                  <td style={{ padding: "4px 8px", color: COMPARE_COLORS[i % COMPARE_COLORS.length], fontWeight: 500 }}>
                    {m.strategy_id}
                  </td>
                  <td
                    style={{
                      padding: "4px 8px",
                      textAlign: "right",
                      color: m.net_pnl >= 0 ? "var(--green)" : "var(--red)",
                      fontWeight: 600,
                      fontVariantNumeric: "tabular-nums",
                    }}
                  >
                    {formatINR(m.net_pnl)}
                  </td>
                  <td style={{ padding: "4px 8px", textAlign: "right", color: "var(--text-secondary)" }}>
                    {m.win_rate.toFixed(0)}%
                  </td>
                  <td style={{ padding: "4px 8px", textAlign: "right", color: "var(--text-secondary)" }}>
                    {m.profit_factor != null ? m.profit_factor.toFixed(2) : "N/A"}
                  </td>
                  <td style={{ padding: "4px 8px", textAlign: "right", color: "var(--text-secondary)" }}>
                    {m.trade_count}
                  </td>
                  <td
                    style={{
                      padding: "4px 8px",
                      textAlign: "right",
                      color: m.max_drawdown > 0 ? "var(--red)" : "var(--text-muted)",
                    }}
                  >
                    {m.max_drawdown > 0 ? `₹${m.max_drawdown.toLocaleString("en-IN")}` : "₹0"}
                  </td>
                  <td style={{ padding: "4px 8px", textAlign: "right", color: "var(--text-secondary)" }}>
                    {m.expectancy !== 0 ? formatINR(m.expectancy) : "N/A"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

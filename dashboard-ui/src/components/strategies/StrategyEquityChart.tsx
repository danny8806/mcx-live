import { useMemo, useState, useCallback, useRef } from "react";
import { formatINR } from "../../lib/utils";

interface EquityPoint {
  timestamp: number;
  equity: number;
  trade_id?: string | null;
  cumulative_pnl?: number;
  drawdown?: number;
  trade_count?: number;
}

interface DrawdownPoint {
  timestamp: number;
  equity: number;
  peak: number;
  drawdown: number;
  drawdown_pct: number;
}

interface Props {
  equityCurve: EquityPoint[];
  drawdownCurve: DrawdownPoint[];
  startingEquity: number | null;
  color?: string;
  height?: number;
  showDrawdown?: boolean;
  period: string;
}

const PADDING = { top: 8, right: 8, bottom: 4, left: 8 };
const COLORS = {
  line: "#20D47A",
  lineNegative: "#FF4D5A",
  fill: "rgba(32,212,122,0.08)",
  drawdown: "rgba(255,77,90,0.25)",
  grid: "#1B2430",
  text: "#6F7B8D",
};

function filterByPeriod(points: EquityPoint[], period: string): EquityPoint[] {
  if (period === "ALL" || !points.length) return points;
  const now = Date.now();
  const ms: Record<string, number> = {
    TODAY: 86400000,
    "1W": 604800000,
    "1M": 2592000000,
    "3M": 7776000000,
  };
  const cutoff = now - (ms[period] || 0);
  const filtered = points.filter((p) => p.timestamp * 1000 >= cutoff);
  if (!filtered.length) return points;
  const first = filtered[0];
  return [
    { ...first, equity: first.equity, timestamp: first.timestamp },
    ...filtered.slice(1),
  ];
}

function filterDrawdownByPeriod(
  points: DrawdownPoint[],
  period: string,
  _equityPoints: EquityPoint[]
): DrawdownPoint[] {
  if (period === "ALL" || !points.length) return points;
  const now = Date.now();
  const ms: Record<string, number> = {
    TODAY: 86400000,
    "1W": 604800000,
    "1M": 2592000000,
    "3M": 7776000000,
  };
  const cutoff = now - (ms[period] || 0);
  const filtered = points.filter((p) => p.timestamp * 1000 >= cutoff);
  return filtered.length ? filtered : points;
}

export default function StrategyEquityChart({
  equityCurve,
  drawdownCurve,
  startingEquity,
  color,
  height = 120,
  showDrawdown = true,
  period,
}: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [tooltip, setTooltip] = useState<{
    x: number;
    y: number;
    point: EquityPoint;
    index: number;
  } | null>(null);

  const filteredEquity = useMemo(
    () => filterByPeriod(equityCurve, period),
    [equityCurve, period]
  );

  const filteredDrawdown = useMemo(
    () => filterDrawdownByPeriod(drawdownCurve, period, filteredEquity),
    [drawdownCurve, period, filteredEquity]
  );

  const ddHeight = showDrawdown ? 28 : 0;
  const chartH = height - ddHeight;
  const w = 400;

  const { points, minEq, maxEq, maxDD } = useMemo(() => {
    if (!filteredEquity.length)
      return { points: [], minEq: 0, maxEq: 0, maxDD: 0 };
    const eqs = filteredEquity.map((p) => p.equity);
    const mn = Math.min(...eqs);
    const mx = Math.max(...eqs);
    const ddMx = filteredDrawdown.length
      ? Math.max(...filteredDrawdown.map((d) => d.drawdown))
      : 0;
    return { points: filteredEquity, minEq: mn, maxEq: mx, maxDD: ddMx };
  }, [filteredEquity, filteredDrawdown]);

  const scaleX = useCallback(
    (ts: number) => {
      if (!points.length) return 0;
      const t0 = points[0].timestamp;
      const tN = points[points.length - 1].timestamp;
      if (tN === t0) return PADDING.left;
      const ratio = (ts - t0) / (tN - t0);
      return PADDING.left + ratio * (w - PADDING.left - PADDING.right);
    },
    [points]
  );

  const scaleY = useCallback(
    (eq: number) => {
      const range = maxEq - minEq || 1;
      const ratio = (eq - minEq) / range;
      return chartH - PADDING.bottom - ratio * (chartH - PADDING.top - PADDING.bottom);
    },
    [maxEq, minEq, chartH]
  );

  const pathD = useMemo(() => {
    if (!points.length) return "";
    return points
      .map((p, i) => {
        const x = scaleX(p.timestamp);
        const y = scaleY(p.equity);
        return `${i === 0 ? "M" : "L"}${x},${y}`;
      })
      .join(" ");
  }, [points, scaleX, scaleY]);

  const fillD = useMemo(() => {
    if (!points.length) return "";
    const baseline = chartH - PADDING.bottom;
    const line = points
      .map((p, i) => {
        const x = scaleX(p.timestamp);
        const y = scaleY(p.equity);
        return `${i === 0 ? "M" : "L"}${x},${y}`;
      })
      .join(" ");
    const lastX = scaleX(points[points.length - 1].timestamp);
    const firstX = scaleX(points[0].timestamp);
    return `${line} L${lastX},${baseline} L${firstX},${baseline} Z`;
  }, [points, scaleX, scaleY, chartH]);

  const ddPath = useMemo(() => {
    if (!filteredDrawdown.length || !showDrawdown) return "";
    const ddTop = chartH;
    const ddH = ddHeight - 4;
    return filteredDrawdown
      .map((d, i) => {
        const x = scaleX(d.timestamp);
        const ratio = maxDD > 0 ? d.drawdown / maxDD : 0;
        const y = ddTop + 2 + ratio * ddH;
        return `${i === 0 ? "M" : "L"}${x},${y}`;
      })
      .join(" ");
  }, [filteredDrawdown, showDrawdown, scaleX, chartH, ddHeight, maxDD]);

  const handleMouseMove = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      if (!points.length || !svgRef.current) return;
      const rect = svgRef.current.getBoundingClientRect();
      const mx = e.clientX - rect.left;
      let closest = 0;
      let minDist = Infinity;
      points.forEach((p, i) => {
        const px = scaleX(p.timestamp);
        const d = Math.abs(px - mx);
        if (d < minDist) {
          minDist = d;
          closest = i;
        }
      });
      setTooltip({
        x: scaleX(points[closest].timestamp),
        y: scaleY(points[closest].equity),
        point: points[closest],
        index: closest,
      });
    },
    [points, scaleX, scaleY]
  );

  const tradeCount = points.length > 0 ? points.length - 1 : 0;
  // ### OBSERVATION (P&L baseline)
  // The backend analytics equity curve embeds its baseline in the first point,
  // so P&L is always computed against a real value (explicit startingEquity or
  // the curve's own first point) — never a fabricated number.
  const base = startingEquity ?? (points.length > 0 ? points[0].equity : null);
  const currentEq = points.length > 0 ? points[points.length - 1].equity : null;
  const cumPnl =
    base != null && currentEq != null ? currentEq - base : null;
  const currentDD =
    filteredDrawdown.length > 0
      ? filteredDrawdown[filteredDrawdown.length - 1].drawdown
      : 0;

  return (
    <div style={{ position: "relative" }}>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${w} ${height}`}
        style={{ width: "100%", height: `${height}px`, display: "block" }}
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setTooltip(null)}
      >
        {points.length > 1 && (
          <>
            <path d={fillD} fill={COLORS.fill} stroke="none" />
            <path
              d={pathD}
              fill="none"
              stroke={color || COLORS.line}
              strokeWidth={1.5}
              strokeLinejoin="round"
            />
          </>
        )}
        {ddPath && (
          <path
            d={ddPath}
            fill="none"
            stroke="rgba(255,77,90,0.5)"
            strokeWidth={1}
          />
        )}
        {!points.length && (
          <text
            x={w / 2}
            y={height / 2}
            textAnchor="middle"
            fill={COLORS.text}
            fontSize={10}
          >
            No equity data
          </text>
        )}
        {showDrawdown && ddHeight > 0 && (
          <line
            x1={PADDING.left}
            x2={w - PADDING.right}
            y1={chartH + 1}
            y2={chartH + 1}
            stroke={COLORS.grid}
            strokeWidth={0.5}
          />
        )}
      </svg>
      {tooltip && (
        <div
          style={{
            position: "absolute",
            left: `${(tooltip.x / w) * 100}%`,
            top: `${(tooltip.y / height) * 100}%`,
            transform: "translate(-50%, -120%)",
            background: "#1a2233",
            border: "1px solid #334155",
            borderRadius: 4,
            padding: "6px 8px",
            fontSize: 9,
            color: "#E7ECF4",
            pointerEvents: "none",
            whiteSpace: "nowrap",
            zIndex: 10,
          }}
        >
          <div style={{ color: "#6F7B8D", marginBottom: 2 }}>
            {tooltip.point.timestamp
              ? new Date(tooltip.point.timestamp * 1000).toLocaleTimeString(
                  "en-IN",
                  { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }
                )
              : "—"}
          </div>
          <div>
            Equity:{" "}
            <span style={{ fontWeight: 600 }}>
              ₹{tooltip.point.equity.toLocaleString("en-IN")}
            </span>
          </div>
          <div>
            P&L:{" "}
            {base != null ? (
              <span
                style={{
                  color:
                    tooltip.point.equity - base >= 0
                      ? "#20D47A"
                      : "#FF4D5A",
                }}
              >
                {formatINR(tooltip.point.equity - base)}
              </span>
            ) : (
              <span>—</span>
            )}
          </div>
          {tooltip.point.drawdown != null && (
            <div>
              DD:{" "}
              <span style={{ color: "#FF4D5A" }}>
                ₹{Math.abs(tooltip.point.drawdown).toLocaleString("en-IN")}
              </span>
            </div>
          )}
          <div>Trades: {tradeCount}</div>
        </div>
      )}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          padding: "2px 8px 0",
          fontSize: 9,
          color: "#6F7B8D",
        }}
      >
        <span>Current: {currentEq != null ? `₹${currentEq.toLocaleString("en-IN")}` : "—"}</span>
        <span>
          P&L:{" "}
          {cumPnl != null ? (
            <span style={{ color: cumPnl >= 0 ? "#20D47A" : "#FF4D5A" }}>
              {formatINR(cumPnl)}
            </span>
          ) : (
            <span>—</span>
          )}
        </span>
        <span>
          DD:{" "}
          <span style={{ color: currentDD > 0 ? "#FF4D5A" : "#6F7B8D" }}>
            ₹{currentDD.toLocaleString("en-IN")}
          </span>
        </span>
      </div>
    </div>
  );
}

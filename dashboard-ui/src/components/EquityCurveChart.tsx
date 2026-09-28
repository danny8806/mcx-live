import { useMemo } from "react";
import { formatINR, formatDT } from "../lib/utils";

export interface EquityPoint {
  timestamp: number;
  equity: number;
}

export default function EquityCurveChart({ points, zeroBaseline }: { points: EquityPoint[]; zeroBaseline?: boolean }) {
  const w = 1000;
  const h = 140;
  const pad = { top: 8, right: 8, bottom: 8, left: 8 };

  const { pathD, fillD, startEq, lastEq } = useMemo(() => {
    if (!points.length) return { pathD: "", fillD: "", startEq: 0, lastEq: 0 };
    const eqs = points.map((p) => p.equity);
    const mn = Math.min(...eqs);
    const mx = Math.max(...eqs);
    const t0 = points[0].timestamp;
    const tN = points[points.length - 1].timestamp;
    const sx = (ts: number) => {
      if (tN === t0) return pad.left;
      return pad.left + ((ts - t0) / (tN - t0)) * (w - pad.left - pad.right);
    };
    const sy = (eq: number) => {
      const range = mx - mn || 1;
      return h - pad.bottom - ((eq - mn) / range) * (h - pad.top - pad.bottom);
    };
    const line = points.map((p, i) => `${i === 0 ? "M" : "L"}${sx(p.timestamp).toFixed(2)},${sy(p.equity).toFixed(2)}`).join(" ");
    const base = h - pad.bottom;
    const fill = `${line} L${sx(tN).toFixed(2)},${base} L${sx(t0).toFixed(2)},${base} Z`;
    return { pathD: line, fillD: fill, startEq: points[0].equity, lastEq: points[points.length - 1].equity };
  }, [points]);

  if (!points.length) {
    return (
      <div style={{ padding: "40px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>
        No equity data yet — snapshots are written every 60s
      </div>
    );
  }

  // ### OBSERVATION (per-instrument curve baseline)
  // The per-instrument series is a cumulative realized-P&L journey that starts
  // at the FIRST trade's P&L, not at ₹0.  Showing "Start: ₹X (first trade)" and
  // "Net: last − first" mislabeled the curve.  For journey series the readout
  // should start at ₹0 and Net equals the final cumulative value.
  const effStart = zeroBaseline ? 0 : startEq;
  const net = zeroBaseline ? lastEq : lastEq - startEq;

  return (
    <div>
      <svg viewBox={`0 0 ${w} ${h}`} style={{ width: "100%", height: `${h}px`, display: "block" }}>
        <path d={fillD} fill="rgba(32,212,122,0.08)" stroke="none" />
        <path d={pathD} fill="none" stroke={net >= 0 ? "#20D47A" : "#FF4D5A"} strokeWidth={1.5} strokeLinejoin="round" />
      </svg>
      <div style={{ display: "flex", justifyContent: "space-between", padding: "2px 8px 0", fontSize: 9, color: "var(--text-muted)" }}>
        <span>Points: {points.length}</span>
        <span>Start: {formatINR(effStart)}</span>
        <span>
          Current: <span style={{ color: "var(--text-primary)", fontWeight: 600 }}>{formatINR(lastEq)}</span>
        </span>
        <span>
          Net: <span style={{ color: net >= 0 ? "#20D47A" : "#FF4D5A" }}>{formatINR(net)}</span>
        </span>
        <span>Last: {formatDT(points[points.length - 1].timestamp)}</span>
      </div>
    </div>
  );
}
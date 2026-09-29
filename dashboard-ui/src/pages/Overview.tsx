import { useEffect, useRef, useState } from "react";
import { useDataSelector } from "../store/DataProvider";
import { formatINR, formatPct, pnlColor, statusDot, formatTimestamp, safeNum, safeINR } from "../lib/utils";

const panelStyle: React.CSSProperties = {
  background: "var(--bg-panel)",
  border: "1px solid var(--border)",
  borderRadius: "8px",
  overflow: "hidden",
};

const panelHeader: React.CSSProperties = {
  padding: "8px 12px",
  borderBottom: "1px solid var(--border-subtle)",
  fontSize: "10px",
  fontWeight: 600,
  color: "var(--text-muted)",
  textTransform: "uppercase",
  letterSpacing: "0.5px",
};

/** P&L card that pulses a green/red flash whenever its value changes. */
function MetricCard({ label, value, sub, color, accent }: { label: string; value: string; sub?: string; color?: string; accent?: boolean }) {
  const prev = useRef<string | null>(null);
  const [flash, setFlash] = useState(false);
  const [tone, setTone] = useState("rgba(32,212,122,0.18)");

  useEffect(() => {
    if (prev.current !== null && prev.current !== value) {
      const before = prev.current.replace(/[^0-9.-]/g, "");
      const after = value.replace(/[^0-9.-]/g, "");
      const gotWorse = before && after && after < before;
      setTone(gotWorse ? "rgba(255,77,90,0.16)" : "rgba(32,212,122,0.18)");
      setFlash(true);
      const t = setTimeout(() => setFlash(false), 460);
      return () => clearTimeout(t);
    }
    prev.current = value;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value]);

  return (
    <div
      className={`lift ${flash ? "animate-tick" : ""}`}
      style={{
        background: accent
          ? "linear-gradient(135deg, rgba(74,158,255,0.14) 0%, rgba(49,90,156,0.06) 55%, rgba(32,212,122,0.10) 100%)"
          : "var(--bg-panel)",
        border: `1px solid ${accent ? "var(--border-active)" : "var(--border)"}`,
        borderRadius: "8px",
        padding: "10px 12px",
        cursor: "default",
        ["--flash" as any]: tone,
      }}
    >
      <div style={{ fontSize: "9px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "4px" }}>
        {label}
      </div>
      <div className="tabular-nums" style={{ fontSize: "20px", fontWeight: 700, color: color ?? "var(--text-primary)" }}>
        {value}
      </div>
      {sub && <div style={{ fontSize: "10px", color: "var(--text-muted)", marginTop: "2px" }}>{sub}</div>}
    </div>
  );
}

function MarketCard({ name, ltp, overview }: { name: string; ltp: number; overview: any }) {
  const isLive = ltp > 0;
  return (
    <div className="lift animate-fade-in-up" style={{ ...panelStyle, padding: "12px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
        <span style={{ fontSize: "11px", fontWeight: 600, color: "var(--text-primary)" }}>{name}</span>
        <div style={{ display: "flex", alignItems: "center", gap: "5px" }}>
          <span
            className={isLive ? "animate-pulse-dot" : ""}
            style={{ width: "5px", height: "5px", borderRadius: "50%", background: isLive ? "var(--green)" : "var(--red)", ["--dot" as any]: isLive ? "var(--green)" : "var(--red)" }}
          />
          <span style={{ fontSize: "9px", color: isLive ? "var(--green)" : "var(--red)" }}>
            {isLive ? "LIVE" : "NO DATA"}
          </span>
        </div>
      </div>
      <div className="tabular-nums" style={{ fontSize: "24px", fontWeight: 700, color: "var(--text-primary)", marginBottom: "8px" }}>
        {isLive ? safeINR(ltp) : "—"}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px", fontSize: "10px" }}>
        {overview?.strategies?.map((s: any) => (
          <div key={s.strategy_id} style={{ display: "flex", alignItems: "center", gap: "4px", padding: "3px 6px", background: "var(--bg-table-header)", borderRadius: "4px" }}>
            <span style={{ width: "4px", height: "4px", borderRadius: "50%", background: statusDot(s.status) }} />
            <span style={{ color: "var(--text-secondary)", fontSize: "9px" }}>{s.strategy_id}</span>
            {s.position_side && (
              <span style={{ color: s.position_side === "LONG" ? "var(--green)" : "var(--red)", fontSize: "9px", fontWeight: 600 }}>
                {s.position_side}
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function StrategyRow({ s }: { s: any }) {
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "120px 40px 40px 55px 55px 1fr 70px",
      gap: "8px",
      alignItems: "center",
      padding: "5px 12px",
      fontSize: "10px",
      borderBottom: "1px solid var(--border-subtle)",
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: "5px" }}>
        <span style={{ width: "4px", height: "4px", borderRadius: "50%", background: statusDot(s.state) }} />
        <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{s.strategy_id}</span>
      </div>
      <span style={{ color: "var(--text-muted)" }}>{s.fast_timeframe}</span>
      <span style={{ color: "var(--text-muted)" }}>{s.htf_timeframe}</span>
      <span style={{
        color: s.position_side === "LONG" ? "var(--green)" : s.position_side === "SHORT" ? "var(--red)" : "var(--text-muted)",
        fontWeight: s.position_side ? 600 : 400,
      }}>
        {s.position_side ?? "FLAT"}
      </span>
      <span style={{ color: "var(--text-secondary)" }}>{s.trade_count}</span>
      <span style={{ color: "var(--text-muted)" }}>{(s.win_rate).toFixed(0)}%</span>
      <span className="tabular-nums" style={{ color: pnlColor(s.realized_net), fontWeight: 600, textAlign: "right" }}>
        {formatINR(s.realized_net)}
      </span>
    </div>
  );
}

function PositionRow({ p }: { p: any }) {
  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: "70px 100px 50px 40px 80px 50px 90px",
      gap: "8px",
      alignItems: "center",
      padding: "5px 12px",
      fontSize: "10px",
      borderBottom: "1px solid var(--border-subtle)",
    }}>
      <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{p.instrument}</span>
      <span style={{ color: "var(--text-muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.strategy_id}</span>
      <span style={{ color: p.side === "LONG" ? "var(--green)" : "var(--red)", fontWeight: 600 }}>{p.side}</span>
      <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{p.quantity}</span>
      <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{safeINR(p.average_entry)}</span>
      <span className="tabular-nums" style={{ color: "var(--text-muted)" }}>{p.stop_price ? safeINR(p.stop_price) : "—"}</span>
      <span className="tabular-nums" style={{ color: pnlColor(p.unrealized_pnl), fontWeight: 600, textAlign: "right" }}>
        {p.unrealized_pnl >= 0 ? "+" : ""}₹{safeNum(p.unrealized_pnl).toLocaleString("en-IN", { minimumFractionDigits: 2 })}
      </span>
    </div>
  );
}

function FillRow({ f }: { f: any }) {
  return (
    <div style={{
      display: "flex", alignItems: "center", gap: "10px",
      padding: "4px 12px", fontSize: "10px",
      borderBottom: "1px solid var(--border-subtle)",
    }}>
      <span className="tabular-nums" style={{ color: "var(--text-muted)", width: "55px" }}>
        {formatTimestamp(f.timestamp)}
      </span>
      <span style={{ color: "var(--text-primary)", fontWeight: 500, width: "55px" }}>{f.instrument}</span>
      <span style={{ color: f.side === "BUY" ? "var(--green)" : "var(--red)", fontWeight: 600 }}>{f.side}</span>
      <span style={{ color: "var(--text-secondary)" }}>{f.quantity}</span>
      <span className="tabular-nums" style={{ color: "var(--text-primary)" }}>{safeINR(f.price)}</span>
      <div style={{ flex: 1 }} />
      <span style={{ color: "var(--text-muted)", fontSize: "9px" }}>{f.strategy_id}</span>
    </div>
  );
}

export default function Overview() {
  // Per-slice subscriptions: each card only re-renders when ITS slice changes,
  // not on every WS push.
  const overview = useDataSelector<any>((s) => s.overview);
  const goldOverview = useDataSelector((s) => s.goldOverview);
  const silverOverview = useDataSelector((s) => s.silverOverview);
  const strategies = useDataSelector((s) => s.strategies);
  const positions = useDataSelector((s) => s.positions);
  const fills = useDataSelector((s) => s.fills);
  const connected = useDataSelector((s) => s.connected);

  if (!overview) {
    return (
      <div style={{ padding: "20px", color: "var(--text-muted)" }}>
        <div className="skeleton" style={{ width: "180px", height: "14px", marginBottom: "12px", animation: "shimmer 1.3s ease-in-out infinite" }} />
        <div className="skeleton" style={{ width: "420px", height: "60px" }} />
        <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading dashboard...</div>
      </div>
    );
  }

  const strategyGroups = Array.from(new Set(
    strategies.map((s: any) => s.instrument || "UNKNOWN"),
  )).sort().map((instrument) => ({
    instrument,
    strategies: strategies.filter((s: any) => (s.instrument || "UNKNOWN") === instrument),
  }));
  const openPositions = positions.filter((p: any) => p.is_open);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
      {overview.kill_switch && (
        <div className="animate-fade-in-up" style={{
          background: "var(--red-muted)", border: "1px solid var(--red)",
          borderRadius: "8px", padding: "8px 12px", fontSize: "11px",
          color: "var(--red)", display: "flex", alignItems: "center", gap: "8px",
          boxShadow: "0 0 0 0 rgba(255,77,90,0.4)",
          animation: "pulse-dot 1.6s ease-out 0.3s 3, fade-in-up 0.3s ease-out",
        }}>
          <span
            style={{ width: "7px", height: "7px", borderRadius: "50%", background: "var(--red)", ["--dot" as any]: "var(--red)", animation: "pulse-dot 1.2s ease-out infinite" }}
          />
          KILL SWITCH ACTIVE — All trading halted
        </div>
      )}

      <div className="metric-grid">
        <MetricCard label="STARTING CAPITAL" value={safeINR(overview.starting_capital)} />
        <MetricCard
          accent
          label="NET P&L"
          value={formatINR(overview.total_net_pnl)}
          sub={formatPct(overview.starting_capital > 0 ? (overview.total_net_pnl / overview.starting_capital) * 100 : 0)}
          color={pnlColor(overview.total_net_pnl)}
        />
        <MetricCard label="REALIZED" value={formatINR(overview.realized_pnl)} color={pnlColor(overview.realized_pnl)} />
        <MetricCard label="UNREALIZED" value={formatINR(overview.unrealized_pnl)} color={pnlColor(overview.unrealized_pnl)} />
        <MetricCard label="TODAY P&L" value={formatINR(overview.today_pnl)} color={pnlColor(overview.today_pnl)} />
        <MetricCard label="MARGIN USED" value={safeINR(overview.margin_used)} sub={`Avail: ${safeINR(overview.available_margin)}`} />
        <MetricCard label="OPEN POSITIONS" value={String(overview.open_positions_count)} />
        <MetricCard label="ACTIVE STRATEGIES" value={String(overview.active_strategies_count)} />
      </div>

      <div className="split-grid-2">
        <MarketCard name="GOLD MINI (GOLDM)" ltp={goldOverview?.ltp ?? 0} overview={goldOverview} />
        <MarketCard name="SILVER MINI (SILVERM)" ltp={silverOverview?.ltp ?? 0} overview={silverOverview} />
      </div>

      <div className="split-grid-2">
        {strategyGroups.map(({ instrument, strategies: instrumentStrategies }) => (
          <div key={instrument} className="lift" style={panelStyle}>
            <div style={panelHeader}>{instrument} STRATEGIES ({instrumentStrategies.length})</div>
            <div style={{ maxHeight: "200px", overflow: "auto" }}>
              {instrumentStrategies.map((s: any) => <StrategyRow key={s.strategy_id} s={s} />)}
            </div>
          </div>
        ))}
        {strategyGroups.length === 0 && <div className="lift" style={{ ...panelStyle, padding: "16px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>No strategies</div>}
      </div>

      <div className="lift" style={panelStyle}>
        <div style={panelHeader}>OPEN POSITIONS ({openPositions.length})</div>
        {openPositions.length === 0 ? (
          <div className="animate-fade-in-up" style={{ padding: "20px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>
            No open positions — waiting for signals
          </div>
        ) : (
          <div>
            {openPositions.map((p: any) => <PositionRow key={p.position_id} p={p} />)}
          </div>
        )}
      </div>

      <div className="split-grid-2">
        <div className="lift" style={panelStyle}>
          <div style={panelHeader}>RECENT FILLS</div>
          <div style={{ maxHeight: "180px", overflow: "auto" }}>
            {fills.length === 0 ? (
              <div style={{ padding: "16px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>No fills yet</div>
            ) : (
              fills.slice(0, 10).map((f: any) => <FillRow key={f.fill_id} f={f} />)
            )}
          </div>
        </div>
        <div className="lift" style={panelStyle}>
          <div style={panelHeader}>SYSTEM STATUS</div>
          <div style={{ padding: "12px" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px", fontSize: "10px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                <span style={{ color: "var(--text-muted)" }}>Connection</span>
                <span style={{ color: connected ? "var(--green)" : "var(--red)" }}>{connected ? "LIVE" : "DOWN"}</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                <span style={{ color: "var(--text-muted)" }}>Execution</span>
                <span style={{ color: (overview.execution_mode ?? "") === "LIVE" ? "var(--green)" : "var(--blue)" }}>{overview.execution_mode ?? "—"}</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                <span style={{ color: "var(--text-muted)" }}>Strategies</span>
                <span style={{ color: "var(--text-primary)" }}>{strategies.length}</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                <span style={{ color: "var(--text-muted)" }}>Positions</span>
                <span style={{ color: "var(--text-primary)" }}>{openPositions.length}</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                <span style={{ color: "var(--text-muted)" }}>Fills</span>
                <span style={{ color: "var(--text-primary)" }}>{fills.length}</span>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                <span style={{ color: "var(--text-muted)" }}>Kill Switch</span>
                <span style={{ color: overview.kill_switch ? "var(--red)" : "var(--green)" }}>{overview.kill_switch ? "ON" : "OFF"}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

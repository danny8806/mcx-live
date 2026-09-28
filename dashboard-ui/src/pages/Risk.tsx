import { useDataSelector } from "../store/DataProvider";
import { formatINR, safeINR } from "../lib/utils";

export default function Risk() {
  const risk = useDataSelector<any>((s) => s.risk);
  if (!risk) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "260px", height: "60px", marginBottom: "12px" }} />
      <div className="skeleton" style={{ width: "400px", height: "200px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading risk data...</div>
    </div>
  );

  const marginPct = risk.equity > 0 ? (risk.used_margin / risk.equity) * 100 : 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
      {risk.kill_switch_active && (
        <div className="animate-fade-in-up" style={{
          background: "var(--red-muted)", border: "1px solid var(--red)", borderRadius: "8px",
          padding: "8px 12px", fontSize: "11px", color: "var(--red)",
          display: "flex", alignItems: "center", gap: "8px",
        }}>
          <span style={{ width: "7px", height: "7px", borderRadius: "50%", background: "var(--red)", ["--dot" as any]: "var(--red)", animation: "pulse-dot 1.2s ease-out infinite" }} />
          KILL SWITCH ACTIVE — Trading halted
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "8px" }}>
        {[["EQUITY", safeINR(risk.equity)], ["MARGIN USED", safeINR(risk.used_margin)], ["AVAILABLE", safeINR(risk.available_margin)], ["DAILY P&L", formatINR(risk.daily_pnl)]].map(([k, v]) => (
          <div key={String(k)} className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "10px 12px" }}>
            <div style={{ fontSize: "9px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "4px" }}>{String(k)}</div>
            <div className="tabular-nums" style={{ fontSize: "18px", fontWeight: 700, color: k === "DAILY P&L" ? (risk.daily_pnl >= 0 ? "var(--green)" : "var(--red)") : "var(--text-primary)" }}>{String(v)}</div>
          </div>
        ))}
      </div>

      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px" }}>
        <div style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "8px" }}>Margin Utilization</div>
        <div style={{ width: "100%", height: "6px", background: "var(--bg-table-header)", borderRadius: "3px", overflow: "hidden" }}>
          <div style={{
            height: "100%", borderRadius: "3px",
            background: marginPct > 80 ? "var(--red)" : marginPct > 50 ? "var(--amber)" : "var(--green)",
            width: `${Math.min(100, marginPct)}%`, transition: "width 0.3s",
          }} />
        </div>
        <div style={{ display: "flex", justifyContent: "space-between", fontSize: "9px", color: "var(--text-muted)", marginTop: "4px" }}>
          <span>₹0</span><span>{marginPct.toFixed(1)}%</span><span>{formatINR(risk.equity)}</span>
        </div>
      </div>

      <div className="split-grid-2">
        {[{ title: "LIMITS", rows: [["Max Positions (Total)", `${risk.open_positions} / ${risk.max_positions_total}`], ["Max Positions (Per Strategy)", String(risk.max_positions_per_strategy)], ["Max Daily Loss", formatINR(risk.max_daily_loss)], ["Max Drawdown", `${risk.max_drawdown_pct}%`], ["Daily Loss Remaining", formatINR(risk.daily_loss_remaining)]] },
          { title: "STATUS", rows: [["Open Positions", String(risk.open_positions)], ["Peak Equity", formatINR(risk.peak_equity)], ["Kill Switch", risk.kill_switch_active ? "ACTIVE" : "OFF"], ["Drawdown Limit", `${risk.max_drawdown_pct}%`]] }
        ].map(({ title, rows }) => (
          <div key={title} className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px" }}>
            <div style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "8px" }}>{title}</div>
            {rows.map(([k, v]) => (
              <div key={String(k)} style={{ display: "flex", justifyContent: "space-between", fontSize: "10px", padding: "3px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                <span style={{ color: "var(--text-muted)" }}>{String(k)}</span>
                <span className="tabular-nums" style={{ color: "var(--text-primary)" }}>{String(v)}</span>
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

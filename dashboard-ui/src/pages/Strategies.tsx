import { useState, useCallback } from "react";
import { useDataSelector } from "../store/DataProvider";
import { formatINR, pnlColor, statusDot } from "../lib/utils";
import { api } from "../lib/api";

const panelStyle: React.CSSProperties = { background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" };

export default function Strategies() {
  const strategies = useDataSelector<any[]>((s) => s.strategies);
  const refresh = useDataSelector<(key?: string) => void>((s) => s.refresh);
  const [busy, setBusy] = useState<string | null>(null);

  const toggleControl = useCallback(async (s: any) => {
    const action = s.enabled ? "pause" : "resume";
    setBusy(s.strategy_id);
    try {
      const r = await api.controlStrategy(s.strategy_id, action);
      if (r?.error) console.error(`control ${s.strategy_id}:`, r.error);
      refresh("strategies");
    } catch (e: any) {
      console.error(`control ${s.strategy_id}:`, e?.message || e);
    } finally {
      setBusy(null);
    }
  }, [refresh]);

  if (!strategies) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "300px", height: "120px", marginBottom: "10px" }} />
      <div className="skeleton" style={{ width: "300px", height: "120px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading strategies...</div>
    </div>
  );

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "10px" }}>
      {strategies.map((s: any) => {
        const isLive = s.state != null && s.state !== "flat" && s.state !== "";
        return (
          <div key={s.strategy_id} className="lift animate-fade-in-up" style={{ ...panelStyle, padding: "12px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "8px" }}>
              <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span
                  className={isLive ? "animate-pulse-dot" : ""}
                  style={{ width: "6px", height: "6px", borderRadius: "50%", background: statusDot(s.state), ["--dot" as any]: statusDot(s.state) }}
                />
                <span style={{ fontSize: "11px", fontWeight: 600, color: "var(--text-primary)" }}>{s.strategy_id}</span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                <span style={{
                  fontSize: "9px", padding: "2px 6px", borderRadius: "3px", fontWeight: 600,
                  background: s.position_side === "LONG" ? "var(--green-muted)" : s.position_side === "SHORT" ? "var(--red-muted)" : "var(--bg-table-header)",
                  color: s.position_side === "LONG" ? "var(--green)" : s.position_side === "SHORT" ? "var(--red)" : "var(--text-muted)",
                }}>
                  {s.position_side ?? s.state?.toUpperCase() ?? "UNKNOWN"}
                </span>
                <button
                  onClick={() => toggleControl(s)}
                  disabled={busy === s.strategy_id}
                  style={{
                    fontSize: "9px", padding: "3px 8px", borderRadius: "3px", fontWeight: 600, cursor: busy === s.strategy_id ? "wait" : "pointer",
                    background: s.enabled ? "var(--green-muted)" : "var(--bg-input)",
                    color: s.enabled ? "var(--green)" : "var(--text-muted)",
                    border: "1px solid var(--border)",
                  }}
                >
                  {busy === s.strategy_id ? "..." : s.enabled ? "PAUSE" : "RESUME"}
                </button>
              </div>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "3px", fontSize: "10px" }}>
              {[["Instrument", s.instrument], ["Fast TF", s.fast_timeframe], ["HTF", s.htf_timeframe], ["Qty", s.quantity],
                ["Trades", s.trade_count], ["Win%", `${s.win_rate.toFixed(1)}%`],
                ["P&L", formatINR(s.realized_net)]].map(([k, v]) => (
                <div key={String(k)} style={{ display: "flex", justifyContent: "space-between" }}>
                  <span style={{ color: "var(--text-muted)" }}>{String(k)}</span>
                  <span className="tabular-nums" style={{ color: k === "P&L" ? pnlColor(Number(String(v).replace(/[+₹,]/g, ""))) : "var(--text-primary)", fontWeight: k === "P&L" ? 600 : 400 }}>{String(v)}</span>
                </div>
              ))}
            </div>
          </div>
        );
      })}
      {strategies.length === 0 && <div className="animate-fade-in-up" style={{ gridColumn: "1/-1", padding: "40px", textAlign: "center", color: "var(--text-muted)" }}>No strategies loaded</div>}
    </div>
  );
}

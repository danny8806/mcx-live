import { useState, useCallback } from "react";
import { useDataSelector } from "../store/DataProvider";
import { formatINR, pnlColor, statusDot } from "../lib/utils";
import { api } from "../lib/api";

const panelStyle: React.CSSProperties = { background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" };

export default function Strategies() {
  const strategies = useDataSelector<any[]>((s) => s.strategies);
  const refresh = useDataSelector<(key?: string) => void>((s) => s.refresh);
  const settings = useDataSelector<any>((s) => s.settings);
  const [busy, setBusy] = useState<string | null>(null);
  const [controlError, setControlError] = useState<Record<string, string>>({});

  const toggleControl = useCallback(async (s: any) => {
    const action = s.enabled ? "pause" : "resume";
    setBusy(s.strategy_id);
    try {
      const r = await api.controlStrategy(s.strategy_id, action);
      if (r?.error || r?.success === false) throw new Error(r?.error || "Control rejected");
      setControlError((old) => ({ ...old, [s.strategy_id]: "" }));
      refresh("strategies");
      refresh("settings");
    } catch (e: any) {
      setControlError((old) => ({ ...old, [s.strategy_id]: e?.message || String(e) }));
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
    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(min(100%, 265px), 1fr))", gap: "10px" }}>
      {strategies.map((s: any) => {
        const isLive = ["LONG", "SHORT"].includes(String(s.position_side || s.state || "").toUpperCase());
        const gate = settings?.strategy_gates?.[s.strategy_id];
        const pending = s.pending_entry || s.pending_exit_trigger;
        const canPause = s.enabled && gate?.live_gate === "ON" && !isLive;
        const canResume = !s.enabled && gate?.live_gate === "CLOSE_ONLY" && !isLive;
        const canControl = canPause || canResume;
        const controlLabel = isLive ? "POSITION OPEN" : canPause ? "PAUSE SIGNALS" : canResume ? "RESUME" : "USE SETTINGS";
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
                  disabled={busy === s.strategy_id || !canControl}
                  title={!canControl ? (isLive ? "An open position must remain managed" : "Change the strategy gate in Settings") : undefined}
                  style={{
                    fontSize: "9px", padding: "3px 8px", borderRadius: "3px", fontWeight: 600, cursor: busy === s.strategy_id ? "wait" : canControl ? "pointer" : "not-allowed",
                    background: canPause ? "var(--green-muted)" : "var(--bg-input)",
                    color: canPause ? "var(--green)" : "var(--text-muted)",
                    border: "1px solid var(--border)",
                  }}
                >
                  {busy === s.strategy_id ? "..." : controlLabel}
                </button>
              </div>
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 5, marginBottom: 8, fontSize: 10 }}>
              <span style={{ color: gate?.live_gate === "ON" ? "var(--green)" : "var(--amber)" }}>Gate {gate?.live_gate || "UNKNOWN"}</span>
              <span style={{ color: gate?.entry_enabled ? "var(--green)" : "var(--text-muted)" }}>· Entries {gate?.entry_enabled ? "on" : "off"}</span>
              <span style={{ color: pending ? "var(--amber)" : "var(--text-muted)" }}>· {pending ? `Trigger ${pending.trigger_price ?? "armed"}` : "No trigger"}</span>
            </div>
            {controlError[s.strategy_id] && <div role="alert" style={{ color: "var(--red)", fontSize: 10, marginBottom: 8 }}>{controlError[s.strategy_id]}</div>}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "3px", fontSize: "10px" }}>
              {[["Instrument", s.instrument], ["Fast TF", s.fast_timeframe], ["HTF", s.htf_timeframe], ["Qty", s.quantity],
                ["Trades", s.trade_count], ["Win%", `${Number(s.win_rate ?? 0).toFixed(1)}%`],
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

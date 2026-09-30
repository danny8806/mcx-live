import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { useDataSelector } from "../store/DataProvider";

type StrategyDraft = {
  quantity: number;
  live_gate: string;
  entry_enabled: boolean;
  exit_enabled: boolean;
  reversal_enabled: boolean;
  sl_enabled: boolean;
  close_only: boolean;
};

const gateLabels: Record<string, string> = {
  ON: "ON — entries and exits allowed",
  OFF: "OFF — entries blocked",
  CLOSE_ONLY: "CLOSE ONLY — manage exits, block new entries",
  EMERGENCY_STOP: "EMERGENCY STOP — block entries, keep risk exits",
  LOCKED: "LOCKED — block all strategy actions",
};

const rowStyle: React.CSSProperties = {
  display: "flex", alignItems: "center", justifyContent: "space-between",
  gap: "10px", padding: "7px 0", borderBottom: "1px solid var(--border-subtle)",
  fontSize: "10px",
};

function initialDrafts(settings: any): Record<string, StrategyDraft> {
  const result: Record<string, StrategyDraft> = {};
  for (const [id, cfg] of Object.entries<any>(settings?.strategies ?? {})) {
    const gate = settings?.strategy_gates?.[id] ?? cfg;
    result[id] = {
      quantity: Number(cfg.quantity ?? 1),
      live_gate: String(gate.live_gate ?? "ON").toUpperCase(),
      entry_enabled: Boolean(gate.entry_enabled ?? true),
      exit_enabled: Boolean(gate.exit_enabled ?? true),
      reversal_enabled: Boolean(gate.reversal_enabled ?? true),
      sl_enabled: Boolean(gate.sl_enabled ?? true),
      close_only: Boolean(gate.close_only ?? false),
    };
  }
  return result;
}

export default function Settings() {
  const settings = useDataSelector<any>((s) => s.settings);
  const refresh = useDataSelector<(key?: string) => void>((s) => s.refresh);
  const [drafts, setDrafts] = useState<Record<string, StrategyDraft>>({});
  const [saving, setSaving] = useState<string | null>(null);
  const [messages, setMessages] = useState<Record<string, { ok: boolean; text: string }>>({});

  useEffect(() => setDrafts(initialDrafts(settings)), [settings]);

  if (!settings || Object.keys(settings).length === 0) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "300px", height: "140px", marginBottom: "10px" }} />
      <div className="skeleton" style={{ width: "300px", height: "140px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading settings...</div>
    </div>
  );

  const isLive = !!settings?.system?.live_enabled;
  const sections = [
    { key: "system", label: "System" },
    { key: "instruments", label: "Instruments" },
    { key: "indicators", label: "Indicators" },
    { key: "risk", label: "Risk" },
    { key: "account", label: "Account" },
    ...(!isLive ? [{ key: "paper_execution", label: "Paper Execution" }] : []),
  ];

  const updateDraft = (id: string, patch: Partial<StrategyDraft>) => {
    setDrafts((previous) => ({
      ...previous,
      [id]: { ...previous[id], ...patch },
    }));
    setMessages((previous) => ({ ...previous, [id]: { ok: true, text: "" } }));
  };

  const saveStrategy = async (id: string) => {
    const draft = drafts[id];
    const configured = settings.strategies?.[id] ?? {};
    const currentGate = settings.strategy_gates?.[id] ?? configured;
    const isOpeningEntries = draft.live_gate === "ON" && draft.entry_enabled
      && !(currentGate.live_gate === "ON" && currentGate.entry_enabled);
    const isIncreasingQuantity = draft.quantity > Number(configured.quantity ?? 1);
    if (isLive && (isOpeningEntries || isIncreasingQuantity)
        && !window.confirm("This change can increase or enable future LIVE orders sent to Dhan. Save it?")) {
      return;
    }
    setSaving(id);
    setMessages((previous) => ({ ...previous, [id]: { ok: true, text: "Saving…" } }));
    try {
      await api.updateStrategySettings(id, draft);
      setMessages((previous) => ({ ...previous, [id]: { ok: true, text: "Applied now for subsequent trades." } }));
      refresh("settings");
      refresh("strategies");
    } catch (error: any) {
      setMessages((previous) => ({ ...previous, [id]: { ok: false, text: error?.message || "Save failed." } }));
    } finally {
      setSaving(null);
    }
  };

  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
      <div style={{ gridColumn: "1 / -1", padding: "8px 12px", borderRadius: "8px", fontSize: "10px", fontWeight: 600, letterSpacing: "0.5px", background: isLive ? "var(--green)" : "var(--amber)", color: "#fff" }}>
        EXECUTION MODE: {isLive ? "LIVE — real orders go to Dhan" : "PAPER — simulated execution"}
      </div>

      {isLive && (
        <section className="lift animate-fade-in-up" style={{ gridColumn: "1 / -1", background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px" }}>
          <div style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", marginBottom: "6px" }}>LIVE Strategy Settings</div>
          <div style={{ fontSize: "10px", color: "var(--text-secondary)", marginBottom: "8px" }}>
            Saving sends no order. Quantity changes require a flat strategy with no pending orders. An open position cannot have exits or stop-loss disabled.
          </div>
          <div style={{ fontSize: "10px", color: "var(--text-secondary)", marginBottom: "10px" }}>
            Saved changes apply to the running strategy immediately and affect subsequent eligible trades. They do not alter an open position.
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: "10px" }}>
            {Object.entries<any>(settings.strategies ?? {}).map(([id, cfg]) => {
              const draft = drafts[id] ?? initialDrafts({ strategies: { [id]: cfg } })[id];
              const message = messages[id];
              return (
                <div key={id} style={{ background: "var(--bg-input)", border: "1px solid var(--border-subtle)", borderRadius: "6px", padding: "10px" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
                    <strong style={{ fontSize: "11px", color: "var(--text-primary)" }}>{id} · {cfg.instrument}</strong>
                    <span style={{ color: "var(--text-muted)", fontSize: "9px" }}>{cfg.fast_timeframe} / {cfg.htf_timeframe}</span>
                  </div>
                  <div style={rowStyle}>
                    <label htmlFor={`qty-${id}`}>Quantity</label>
                    <input id={`qty-${id}`} type="number" min={1} step={1} value={draft.quantity} onChange={(event) => updateDraft(id, { quantity: Number(event.target.value) })} style={{ width: "90px", background: "var(--bg-panel)", color: "var(--text-primary)", border: "1px solid var(--border)", borderRadius: "4px", padding: "5px 7px" }} />
                  </div>
                  <div style={rowStyle}>
                    <label htmlFor={`gate-${id}`}>Live gate</label>
                    <select id={`gate-${id}`} value={draft.live_gate} onChange={(event) => {
                      const value = event.target.value;
                      const restrictive = value === "CLOSE_ONLY" || value === "EMERGENCY_STOP" || value === "LOCKED";
                      updateDraft(id, {
                        live_gate: value,
                        close_only: restrictive,
                        entry_enabled: value === "ON",
                        ...(restrictive ? { reversal_enabled: false }
                          : value === "ON" ? { reversal_enabled: true } : {}),
                      });
                    }} style={{ maxWidth: "235px", background: "var(--bg-panel)", color: "var(--text-primary)", border: "1px solid var(--border)", borderRadius: "4px", padding: "5px 7px" }}>
                      {Object.entries(gateLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}
                    </select>
                  </div>
                  {([
                    ["entry_enabled", "New entries"],
                    ["exit_enabled", "Strategy exits"],
                    ["reversal_enabled", "Reversals"],
                    ["sl_enabled", "Stop monitoring"],
                  ] as const).map(([key, label]) => (
                    <label key={key} style={{ ...rowStyle, justifyContent: "flex-start", cursor: "pointer" }}>
                      <input type="checkbox" checked={draft[key]} onChange={(event) => updateDraft(id, { [key]: event.target.checked })} />
                      {label}
                    </label>
                  ))}
                  <div style={{ display: "flex", alignItems: "center", gap: "8px", marginTop: "8px" }}>
                    <button disabled={saving !== null} onClick={() => saveStrategy(id)} style={{ background: "var(--blue-muted)", color: "var(--text-primary)", border: "1px solid var(--border)", borderRadius: "4px", padding: "5px 10px", fontSize: "9px", fontWeight: 600, cursor: saving ? "wait" : "pointer", opacity: saving !== null ? 0.5 : 1 }}>
                      {saving === id ? "SAVING…" : "SAVE SETTINGS"}
                    </button>
                    {message?.text && <span role="status" style={{ color: message.ok ? "var(--green)" : "var(--red)", fontSize: "9px" }}>{message.text}</span>}
                  </div>
                </div>
              );
            })}
          </div>
        </section>
      )}

      {sections.map(({ key, label }) => {
        const data = settings[key];
        if (!data) return null;
        const entries = Object.entries(data).filter(([name]) =>
          !(isLive && key === "system" && ["db_path", "state_path"].includes(name)));
        return (
          <div key={key} className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px" }}>
            <div style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "8px" }}>{label}</div>
            {entries.map(([k, v]) => (
              <div key={k} style={{ display: "flex", justifyContent: "space-between", fontSize: "10px", padding: "3px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                <span style={{ color: "var(--text-muted)" }}>{isLive && key === "system" && k === "live_db_path" ? "active_live_db_path" : isLive && key === "system" && k === "live_state_path" ? "active_live_state_path" : k}</span>
                <span className="tabular-nums" style={{ color: "var(--text-primary)", marginLeft: "8px", overflow: "hidden", textOverflow: "ellipsis", maxWidth: "200px", whiteSpace: "nowrap", textAlign: "right" }}>
                  {typeof v === "object" ? JSON.stringify(v) : String(v ?? "")}
                </span>
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}

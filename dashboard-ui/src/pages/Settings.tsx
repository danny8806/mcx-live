import { useEffect, useState } from "react";
import { api } from "../lib/api";
import { useDataSelector } from "../store/DataProvider";

type Params = { atr_period: number; multiplier: number; source: string; atr_method: string };
type Draft = {
  quantity: number;
  live_gate: string;
  entry_enabled: boolean;
  exit_enabled: boolean;
  reversal_enabled: boolean;
  close_only: boolean;
  supertrend: Params;
};

const defaults = (cfg: any, gate: any): Draft => ({
  quantity: Number(cfg.quantity ?? 1),
  live_gate: String(gate.live_gate ?? "ON").toUpperCase(),
  entry_enabled: Boolean(gate.entry_enabled ?? true),
  exit_enabled: Boolean(gate.exit_enabled ?? true),
  reversal_enabled: Boolean(gate.reversal_enabled ?? true),
  close_only: Boolean(gate.close_only ?? false),
  supertrend: {
    atr_period: Number(cfg.supertrend?.atr_period ?? 10),
    multiplier: Number(cfg.supertrend?.multiplier ?? 3),
    source: String(cfg.supertrend?.source ?? "hl2"),
    atr_method: String(cfg.supertrend?.atr_method ?? "sma_tr"),
  },
});

export default function Settings() {
  const settings = useDataSelector<any>((state) => state.settings);
  const strategies = useDataSelector<any[]>((state) => state.strategies);
  const refresh = useDataSelector<(key?: string) => void>((state) => state.refresh);
  const [drafts, setDrafts] = useState<Record<string, Draft>>({});
  const [saving, setSaving] = useState<string | null>(null);
  const [messages, setMessages] = useState<Record<string, { ok: boolean; text: string }>>({});
  const [masterGateDraft, setMasterGateDraft] = useState("OFF");

  useEffect(() => {
    const next: Record<string, Draft> = {};
    for (const [id, cfg] of Object.entries<any>(settings?.strategies ?? {})) {
      if (cfg.strategy_type !== "supertrend") continue;
      next[id] = defaults(cfg, settings?.strategy_gates?.[id] ?? cfg);
    }
    setDrafts(next);
  }, [settings]);

  useEffect(() => {
    setMasterGateDraft(String(settings?.live_master_gate?.gate ?? "OFF").toUpperCase());
  }, [settings?.live_master_gate?.gate]);

  if (!settings) return <div className="settings-empty">Loading strategy settings…</div>;
  const isLive = Boolean(settings.system?.live_enabled);
  const profiles = Object.entries<any>(settings.strategies ?? {}).filter(([, config]) => config.strategy_type === "supertrend");
  const runtimeIds = new Set(strategies.map((strategy: any) => strategy.strategy_id));

  const saveMasterGate = async () => {
    const current = String(settings.live_master_gate?.gate ?? "OFF").toUpperCase();
    if (masterGateDraft === current) return;
    const message = masterGateDraft === "ON"
      ? "Enable the global live order gate? Eligible signals can send real orders to Dhan."
      : "Disable all live broker orders? This is allowed only when Dhan and the system are flat with no active orders or triggers.";
    if (!window.confirm(message)) return;
    setSaving("global");
    setMessages((value) => ({ ...value, global: { ok: true, text: "Saving…" } }));
    try {
      await api.updateLiveGate(masterGateDraft as "ON" | "OFF");
      setMessages((value) => ({ ...value, global: { ok: true, text: `Global live gate is ${masterGateDraft}.` } }));
      refresh("settings"); refresh("health");
    } catch (error: any) {
      setMessages((value) => ({ ...value, global: { ok: false, text: error?.message || "Save failed." } }));
    } finally { setSaving(null); }
  };

  const save = async (id: string) => {
    const draft = drafts[id];
    const config = settings.strategies[id];
    if (!draft) return;
    const configuredParams = config.supertrend ?? {};
    const paramsChanged = (Object.keys(draft.supertrend) as (keyof Params)[]).some(
      (key) => draft.supertrend[key] !== configuredParams[key],
    );
    const previousGate = settings.strategy_gates?.[id] ?? {};
    const openingEntries = draft.live_gate === "ON" && draft.entry_enabled && !draft.close_only
      && (previousGate.live_gate !== "ON" || !previousGate.entry_enabled || previousGate.close_only);
    const increasingQuantity = draft.quantity > Number(config.quantity ?? 1);
    if (isLive && (openingEntries || increasingQuantity) && !window.confirm("This change can enable or increase future live orders sent to Dhan. Save it?")) return;
    setSaving(id); setMessages((current) => ({ ...current, [id]: { ok: true, text: "Saving…" } }));
    try {
      const { supertrend, ...controls } = draft;
      await api.updateStrategySettings(id, {
        ...controls,
        ...(paramsChanged ? { supertrend } : {}),
      });
      setMessages((current) => ({ ...current, [id]: { ok: true, text: "Applied for subsequent candles. No order was sent." } }));
      refresh("settings"); refresh("strategies");
    } catch (error: any) {
      setMessages((current) => ({ ...current, [id]: { ok: false, text: error?.message || "Save failed." } }));
    } finally { setSaving(null); }
  };

  return <div className="settings-workspace">
    <section className="settings-mode-banner"><span className="eyebrow">EXECUTION MODE</span><strong>{isLive ? "LIVE · Dhan orders" : "PAPER · simulated orders"}</strong><p>Only the GOLDM and SILVERM 15-minute SuperTrend profiles are configurable here. Completed signal candles submit market orders after close; there is no separate strategy stop-loss.</p></section>
    <section className="settings-summary-card settings-global-gate">
      <div><span className="eyebrow">GLOBAL LIVE MASTER GATE</span><strong>{settings.live_master_gate?.broker_gate_enabled ? "LIVE ORDERS ENABLED" : "LIVE ORDERS BLOCKED"}</strong>
        <p>This gate controls the broker order transport for both strategies. Turning it OFF is accepted only when broker/system positions, active orders, and pending triggers are clear. Use each strategy’s CLOSE ONLY mode to block new entries while managing its position.</p>
        {settings.live_master_gate?.live_trading_enabled === false && <p className="settings-warning">The deployment configuration has live trading disabled, so the master gate cannot be enabled here.</p>}
      </div>
      <div className="settings-master-actions"><label>Master gate<select disabled={!isLive || saving !== null || (masterGateDraft === "ON" && settings.live_master_gate?.live_trading_enabled === false)} value={masterGateDraft} onChange={(event) => setMasterGateDraft(event.target.value)}><option value="ON">ON · allow eligible live orders</option><option value="OFF">OFF · block all live orders</option></select></label><button className="primary-button" disabled={!isLive || saving !== null || masterGateDraft === String(settings.live_master_gate?.gate ?? "OFF").toUpperCase() || (masterGateDraft === "ON" && settings.live_master_gate?.live_trading_enabled === false)} onClick={() => void saveMasterGate()}>{saving === "global" ? "Saving…" : "Save master gate"}</button></div>
      {messages.global && <span role="status" className={messages.global.ok ? "text-good" : "text-bad"}>{messages.global.text}</span>}
    </section>
    <section className="settings-summary-card"><div className="settings-summary-heading"><span>Strategy profiles</span><small>Parameters apply to future candles; change only while flat</small></div>
      {profiles.length === 0 ? <div className="settings-empty">No SuperTrend profiles are exposed by the active runtime.</div> : <div className="settings-profile-grid">
        {profiles.map(([id, config]) => {
          const draft = drafts[id] ?? defaults(config, settings.strategy_gates?.[id] ?? config);
          const available = runtimeIds.has(id);
          const message = messages[id];
          const updateParams = (patch: Partial<Params>) => setDrafts((current) => ({ ...current, [id]: { ...draft, supertrend: { ...draft.supertrend, ...patch } } }));
          const update = (patch: Partial<Draft>) => setDrafts((current) => ({ ...current, [id]: { ...draft, ...patch } }));
          const updateGateMode = (mode: string) => {
            const modes: Record<string, Pick<Draft, "entry_enabled" | "exit_enabled" | "reversal_enabled" | "close_only">> = {
              ON: { entry_enabled: true, exit_enabled: true, reversal_enabled: true, close_only: false },
              OFF: { entry_enabled: false, exit_enabled: true, reversal_enabled: true, close_only: false },
              CLOSE_ONLY: { entry_enabled: false, exit_enabled: true, reversal_enabled: true, close_only: true },
              EMERGENCY_STOP: { entry_enabled: false, exit_enabled: true, reversal_enabled: true, close_only: true },
              LOCKED: { entry_enabled: false, exit_enabled: false, reversal_enabled: false, close_only: true },
            };
            update({ live_gate: mode, ...modes[mode] });
          };
          return <article className="settings-profile" key={id}>
            <header><div><strong>{config.instrument}</strong><small>{id} · 15m SuperTrend</small></div><span className={`runtime-chip ${available ? "ready" : "missing"}`}>{available ? "Runtime attached" : "Runtime unavailable"}</span></header>
            <div className="settings-input-grid">
              <label>Quantity<input type="number" min={1} max={100} step={1} disabled={!available || saving !== null} value={draft.quantity} onChange={(event) => update({ quantity: Number(event.target.value) })}/><small>1–100; changes apply while flat with no pending order.</small></label>
              <label>ATR period<input type="number" min={1} max={500} step={1} disabled={!available || saving !== null} value={draft.supertrend.atr_period} onChange={(event) => updateParams({ atr_period: Number(event.target.value) })}/></label>
              <label>ATR multiplier<input type="number" min={0.1} step={0.1} disabled={!available || saving !== null} value={draft.supertrend.multiplier} onChange={(event) => updateParams({ multiplier: Number(event.target.value) })}/></label>
              <label>Price source<select disabled={!available || saving !== null} value={draft.supertrend.source} onChange={(event) => updateParams({ source: event.target.value })}>{["open", "high", "low", "close", "hl2", "hlc3", "ohlc4"].map((source) => <option key={source}>{source}</option>)}</select></label>
              <label>ATR calculation<select disabled={!available || saving !== null} value={draft.supertrend.atr_method} onChange={(event) => updateParams({ atr_method: event.target.value })}><option value="sma_tr">SMA of True Range</option><option value="wilder">Wilder ATR</option></select></label>
              <label>Strategy gate mode<select disabled={!available || saving !== null} value={draft.live_gate} onChange={(event) => updateGateMode(event.target.value)}><option value="ON">ON · gate open</option><option value="OFF">OFF · entry gate closed</option><option value="CLOSE_ONLY">CLOSE ONLY · no new entries</option><option value="EMERGENCY_STOP">EMERGENCY STOP · block entries</option><option value="LOCKED">LOCKED · block all strategy actions</option></select><small>Mode selects safe defaults; individual switches below can be adjusted.</small></label>
            </div>
            <div className="settings-gate-controls" aria-label={`${config.instrument} strategy gate switches`}>
              <strong>Independent gate switches</strong>
              <label><input type="checkbox" disabled={!available || saving !== null} checked={draft.entry_enabled} onChange={(event) => update({ entry_enabled: event.target.checked })}/>Entries enabled</label>
              <label><input type="checkbox" disabled={!available || saving !== null || (draft.live_gate === "LOCKED" && draft.exit_enabled)} checked={draft.exit_enabled} onChange={(event) => update({ exit_enabled: event.target.checked })}/>Exits enabled</label>
              <label><input type="checkbox" disabled={!available || saving !== null} checked={draft.reversal_enabled} onChange={(event) => update({ reversal_enabled: event.target.checked })}/>Reversal processing enabled</label>
              <label><input type="checkbox" disabled={!available || saving !== null} checked={draft.close_only} onChange={(event) => update({ close_only: event.target.checked })}/>Close-only flag</label>
              <small>On this SuperTrend strategy, an opposite flip is both the position exit and reversal event. Disabling reversal processing can therefore prevent that exit. Open positions cannot be saved with exits disabled or the strategy LOCKED. The global master gate OFF blocks all broker orders, including exits.</small>
            </div>
            <div className="settings-fixed-execution"><b>Fixed execution</b><span>Market after completed 15m signal candle</span><span>Opposite flip: exit, confirm flat, then enter</span><span>No separate SL or target</span></div>
            <footer><span role="status" className={message ? message.ok ? "text-good" : "text-bad" : "settings-hint"}>{message?.text ?? "Changes take effect on subsequent strategy processing."}</span><button className="primary-button" disabled={!available || saving !== null} onClick={() => void save(id)}>{saving === id ? "Saving…" : "Save profile"}</button></footer>
          </article>;
        })}
      </div>}
    </section>
  </div>;
}

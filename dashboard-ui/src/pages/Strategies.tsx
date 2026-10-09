import { useCallback, useState } from "react";
import { useDataSelector } from "../store/DataProvider";
import { api } from "../lib/api";
import { formatINR, pnlColor, statusDot } from "../lib/utils";

export default function Strategies() {
  const strategies = useDataSelector<any[]>((state) => state.strategies);
  const settings = useDataSelector<any>((state) => state.settings);
  const refresh = useDataSelector<(key?: string) => void>((state) => state.refresh);
  const [busy, setBusy] = useState<string | null>(null);
  const [errors, setErrors] = useState<Record<string, string>>({});

  const toggle = useCallback(async (strategy: any) => {
    const action = strategy.enabled ? "pause" : "resume";
    setBusy(strategy.strategy_id);
    try {
      const result = await api.controlStrategy(strategy.strategy_id, action);
      if (result?.error || result?.success === false) throw new Error(result?.error || "Strategy control was rejected");
      setErrors((current) => ({ ...current, [strategy.strategy_id]: "" }));
      refresh("strategies"); refresh("settings");
    } catch (error: any) {
      setErrors((current) => ({ ...current, [strategy.strategy_id]: error?.message || String(error) }));
    } finally { setBusy(null); }
  }, [refresh]);

  if (!strategies) return <div className="settings-empty">Loading runtime strategies…</div>;

  return <div className="strategy-workspace">
    <section className="strategy-intro">
      <div><span className="eyebrow">ACTIVE SIGNAL ENGINE</span><h2>15-minute SuperTrend</h2><p>Only the configured GOLDM and SILVERM profiles are active. Signals use completed native 15-minute candles and are sent as market orders after candle close.</p></div>
      <div className="strategy-execution-rules"><span>Entry <b>Market after candle close</b></span><span>Opposite flip <b>Exit, confirm flat, then enter</b></span><span>Separate SL <b>None</b></span></div>
    </section>
    <div className="strategy-card-grid">
      {strategies.filter((strategy: any) => strategy.strategy_type === "supertrend").map((strategy: any) => {
        const gate = settings?.strategy_gates?.[strategy.strategy_id] ?? {};
        const livePosition = ["LONG", "SHORT"].includes(String(strategy.position_side ?? "").toUpperCase());
        const indicator = strategy.supertrend ?? {};
        const value = indicator.value ?? {};
        const trend = Number(value.trend);
        return <article className="strategy-profile-card" key={strategy.strategy_id}>
          <header><div className="strategy-profile-title"><i style={{ background: statusDot(strategy.state) }}/><div><strong>{strategy.instrument}</strong><small>{strategy.strategy_id} · 15-minute chart</small></div></div><span className={`strategy-state ${livePosition ? "in-position" : "flat"}`}>{livePosition ? strategy.position_side : strategy.state ?? "FLAT"}</span></header>
          <div className="strategy-status-row"><span className={`trend-pill ${trend === 1 ? "bullish" : trend === -1 ? "bearish" : "neutral"}`}>{trend === 1 ? "UPTREND" : trend === -1 ? "DOWNTREND" : "WARMING UP"}</span><span className={gate.live_gate === "ON" ? "text-good" : "text-warn"}>Gate {gate.live_gate ?? "UNKNOWN"}</span><span>{strategy.enabled ? "Runtime enabled" : "Runtime paused"}</span></div>
          <div className="strategy-profile-metrics">
            <div><small>Quantity</small><b>{strategy.quantity ?? "—"}</b></div>
            <div><small>ATR</small><b>{value.atr == null ? "—" : formatINR(value.atr)}</b></div>
            <div><small>Upper band</small><b>{value.upper == null ? "—" : formatINR(value.upper)}</b></div>
            <div><small>Lower band</small><b>{value.lower == null ? "—" : formatINR(value.lower)}</b></div>
            <div><small>Closed trades</small><b>{strategy.trade_count ?? 0}</b></div>
            <div><small>Realized P&amp;L</small><b style={{ color: pnlColor(strategy.realized_net) }}>{formatINR(strategy.realized_net)}</b></div>
          </div>
          <div className="strategy-parameters">ATR {indicator.atr_period ?? "—"} · Multiplier {indicator.multiplier ?? "—"} · {String(indicator.source ?? "—").toUpperCase()} · {indicator.atr_method === "sma_tr" ? "SMA True Range" : indicator.atr_method ?? "—"}</div>
          {errors[strategy.strategy_id] && <div role="alert" className="strategy-error">{errors[strategy.strategy_id]}</div>}
          <footer><span>{strategy.pending_entry?.metadata?.is_reversal_entry ? "Reversal entry waiting for broker flat confirmation" : livePosition ? "Position open · no separate strategy SL" : "Waiting for completed candle flip"}</span><button className="secondary-button" disabled={busy !== null || livePosition} title={livePosition ? "The open position remains under its existing lifecycle" : undefined} onClick={() => void toggle(strategy)}>{busy === strategy.strategy_id ? "Applying…" : strategy.enabled ? "Pause signals" : "Resume strategy"}</button></footer>
        </article>;
      })}
      {strategies.filter((strategy: any) => strategy.strategy_type === "supertrend").length === 0 && <div className="settings-empty">No SuperTrend profiles are attached to this runtime.</div>}
    </div>
  </div>;
}

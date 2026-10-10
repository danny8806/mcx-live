import { useDataSelector } from "../store/DataProvider";
import { formatTimestamp, safeINR } from "../lib/utils";
import { indicatorDisplayState } from "../store/strategySnapshotMerge.js";

function Metric({ label, value }: { label: string; value: React.ReactNode }) {
  return <div className="settings-summary-item"><small>{label}</small><strong className="tabular-nums">{value}</strong></div>;
}

export default function Indicators() {
  const strategies = useDataSelector<any[]>((state) => state.strategies);
  const status = useDataSelector<any>((state) => state.snapshotStatus);
  const active = strategies.filter((strategy) => strategy.strategy_type === "supertrend");

  return <div className="indicator-workspace">
    <section className="settings-summary-card">
      <div className="settings-summary-heading"><span>15-minute SuperTrend</span><small>Values from each strategy’s own completed-candle calculation</small></div>
      <p className="indicator-explainer">The strategy updates from completed native 15-minute candles. A flip is submitted after that candle closes. The bands are indicator values; they are not broker orders or stop-loss instructions.</p>
      {status?.strategies !== "live" ? <div className="settings-empty">Waiting for a confirmed runtime strategy snapshot.</div> : active.length === 0 ? <div className="settings-empty">No SuperTrend strategy is present in the current runtime.</div> : <div className="indicator-card-grid">
        {active.map((strategy) => {
          const config = strategy.supertrend ?? {};
          const value = config.value ?? {};
          const calculation = indicatorDisplayState(config);
          const latestSignal = !value || value.trend == null
            ? "Unavailable"
            : value.buy_signal ? "BUY flip" : value.sell_signal ? "SELL flip" : "No flip on latest candle";
          return <article className="indicator-card" key={strategy.strategy_id}>
            <header><div><strong>{strategy.instrument}</strong><small>{strategy.strategy_id} · 15m</small></div><span className={`trend-pill ${calculation.kind}`}>{calculation.label}</span></header>
            <div className="indicator-metrics">
              <Metric label="ATR" value={value.atr == null ? "—" : safeINR(value.atr)} />
              <Metric label="Upper band" value={value.upper == null ? "—" : safeINR(value.upper)} />
              <Metric label="Lower band" value={value.lower == null ? "—" : safeINR(value.lower)} />
              <Metric label="Input source" value={String(config.source ?? "—").toUpperCase()} />
              <Metric label="ATR period" value={config.atr_period ?? "—"} />
              <Metric label="Multiplier" value={config.multiplier ?? "—"} />
              <Metric label="ATR method" value={config.atr_method === "sma_tr" ? "SMA · TR" : config.atr_method ?? "—"} />
              <Metric label="Candles processed" value={config.bars ?? strategy.bars_processed ?? "—"} />
            </div>
            <footer><span>Latest completed candle: {config.last_candle_end_ts ? formatTimestamp(config.last_candle_end_ts) : "Timestamp not exposed"}</span><span>Signal state: {latestSignal}</span></footer>
            {calculation.kind === "unavailable" && <div className="indicator-data-warning" role="status">{calculation.message}</div>}
          </article>;
        })}
      </div>}
    </section>
  </div>;
}

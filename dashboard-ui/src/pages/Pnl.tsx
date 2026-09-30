import { useState, useCallback } from "react";
import { useDataSelector } from "../store/DataProvider";
import { formatINR, pnlColor, formatINRShort } from "../lib/utils";
import EquityCurveChart from "../components/EquityCurveChart";
import { api } from "../lib/api";

export default function Pnl() {
  const pnl = useDataSelector<any>((s) => s.pnl);
  const pnlByInstrument = useDataSelector<Record<string, any>>((s) => s.pnlByInstrument);
  const equityCurve = useDataSelector<any[]>((s) => s.equityCurve);
  const [selectedInst, setSelectedInst] = useState<string | null>(null);
  const [instDetail, setInstDetail] = useState<any>(null);
  const [strategyDetail, setStrategyDetail] = useState<string | null>(null);
  const [strategyData, setStrategyData] = useState<any>(null);
  const [instCurve, setInstCurve] = useState<any[]>([]);
  const [loadingInst, setLoadingInst] = useState(false);

  // ### OBSERVATION (Rules of Hooks)
  // These useCallbacks were declared BELOW the early `if (!pnl) return`, so the
  // component mounted with 7 hooks and incremented to 9 once data arrived —
  // React threw "Rendered more hooks than during the previous render" (#310)
  // and the page crashed/blanked.  Hoisting them above the early return keeps
  // the hook count stable across renders.
  const loadInstrument = useCallback(async (inst: string) => {
    setLoadingInst(true);
    setInstDetail(null);
    setStrategyData(null);
    setStrategyDetail(null);
    setSelectedInst(inst);
    setLoadingInst(false);
    try {
      const [d, eq] = await Promise.all([
        api.pnlInstrument(inst),
        api.equityCurveInstrument(inst),
      ]);
      if (d?.error) return;
      setInstDetail(d);
      const pts = (eq?.equity_curve ?? []).map((r: any) => ({
        timestamp: Number(r.timestamp ?? 0),
        equity: Number(r.equity ?? 0),
      })).sort((a: any, b: any) => a.timestamp - b.timestamp);
      setInstCurve(pts);
    } catch { /* ignore */ }
  }, []);

  const loadStrategy = useCallback(async (inst: string, sid: string) => {
    setStrategyDetail(sid);
    setStrategyData(null);
    try {
      const d = await api.pnlStrategy(inst, sid);
      if (d?.error) return;
      setStrategyData(d);
    } catch { /* ignore */ }
  }, []);

  if (!pnl) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "100%", height: "52px", marginBottom: "8px" }} />
      <div className="skeleton" style={{ width: "100%", height: "200px", marginBottom: "10px" }} />
      <div className="skeleton" style={{ width: "100%", height: "120px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading P&L data...</div>
    </div>
  );

  const instruments = Object.entries(pnlByInstrument);
  const mode = String(pnl.execution_mode ?? "").toUpperCase();
  const portfolioSource = mode === "LIVE" ? "Account snapshot · live mode" : "Engine account snapshot · paper mode";

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
      <div className="pnl-source-banner"><div><strong>{mode || "ENGINE"} P&amp;L SOURCE BREAKDOWN</strong><span>Realized totals come from strategy P&amp;L engines. Unrealized P&amp;L, net P&amp;L, charges, and equity come from the {portfolioSource.toLowerCase()}. These sources can differ while broker and local ledgers reconcile.</span></div><span className="pnl-source-chip">{portfolioSource}</span></div>
      <div className="metric-grid">
        {[
          ["NET P&L · ACCOUNT SNAPSHOT", pnl.net_pnl],
          ["REALIZED · STRATEGY LEDGER", pnl.realized_pnl],
          ["UNREALIZED · ACCOUNT SNAPSHOT", pnl.unrealized_pnl],
          ["CHARGES · ACCOUNT SNAPSHOT", pnl.charges],
          ["EQUITY · ACCOUNT SNAPSHOT", pnl.equity],
        ].map(([label, val]) => (
          <div key={String(label)} className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "10px 12px" }}>
            <div style={{ fontSize: "9px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "4px" }}>{String(label)}</div>
            <div className="tabular-nums" style={{ fontSize: "20px", fontWeight: 700, color: pnlColor(Number(val)) }}>
              {formatINR(Number(val))}
            </div>
          </div>
        ))}
      </div>

      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" }}>
        <div style={{ padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)", fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>
          EQUITY CURVE {selectedInst ? `— ${selectedInst}` : ""}
        </div>
        <div style={{ padding: "12px" }}>
          <EquityCurveChart points={selectedInst && instCurve.length ? instCurve : (equityCurve ?? [])} zeroBaseline={!!selectedInst && instCurve.length > 0} />
        </div>
      </div>

      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" }}>
        <div style={{ padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)", fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>
          BY INSTRUMENT
        </div>
        {instruments.length === 0 ? (
          <div className="animate-fade-in-up" style={{ padding: "24px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>No instrument data</div>
        ) : (
          <div className="ledger-grid-viewport">
            <div className="ledger-grid-head pnl-grid" style={{ display: "grid", gridTemplateColumns: "100px 1fr 1fr 1fr 70px 60px 70px", gap: "10px", padding: "8px 14px", fontSize: "9px", color: "var(--text-disabled)", textTransform: "uppercase", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-table-header)", position: "sticky", top: 0, zIndex: 1 }}>
              <span>Instrument</span><span style={{ textAlign: "right" }}>Gross</span><span style={{ textAlign: "right" }}>Charges</span><span style={{ textAlign: "right" }}>Net</span><span style={{ textAlign: "right" }}>Trades</span><span style={{ textAlign: "right" }}>Wins</span><span style={{ textAlign: "right" }}>Win%</span>
            </div>
            {instruments.map(([inst, data]: [string, any]) => (
              <div key={inst}>
                <div className="ledger-grid-row pnl-grid hover-row" onClick={() => loadInstrument(inst)} style={{ display: "grid", gridTemplateColumns: "100px 1fr 1fr 1fr 70px 60px 70px", gap: "10px", padding: "9px 14px", fontSize: "10px", borderBottom: "1px solid var(--border-subtle)", alignItems: "center", cursor: "pointer" }}>
                  <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{inst}</span>
                  <span className="tabular-nums" style={{ color: pnlColor(data.realized_gross), textAlign: "right" }}>{formatINR(data.realized_gross)}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-muted)", textAlign: "right" }}>{formatINR(data.realized_charges)}</span>
                  <span className="tabular-nums" style={{ color: pnlColor(data.realized_net), fontWeight: 600, textAlign: "right" }}>{formatINR(data.realized_net)}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-secondary)", textAlign: "right" }}>{data.trade_count}</span>
                  <span className="tabular-nums" style={{ color: "var(--green)", textAlign: "right" }}>{data.wins}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-secondary)", textAlign: "right" }}>{(data.win_rate * 100).toFixed(1)}%</span>
                </div>
                {selectedInst === inst && (
                  <div style={{ padding: "8px 12px 10px 24px", background: "var(--bg-table-header)", borderBottom: "1px solid var(--border-subtle)" }}>
                    {loadingInst ? (
                      <div style={{ fontSize: "10px", color: "var(--text-muted)" }}>Loading {inst} details...</div>
                    ) : instDetail?.error ? (
                      <div style={{ fontSize: "10px", color: "var(--red)" }}>{instDetail.error}</div>
                    ) : (
                      <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
                        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(130px, 1fr))", gap: "8px" }}>
                          {[
                            ["REALIZED GROSS", instDetail?.realized?.realized_gross],
                            ["REALIZED NET", instDetail?.realized?.realized_net],
                            ["UNREALIZED", instDetail?.unrealized],
                            ["TRADES", instDetail?.realized?.trade_count],
                            ["WINS", instDetail?.realized?.wins],
                            ["WIN RATE", instDetail?.realized?.win_rate != null ? `${(instDetail.realized.win_rate * 100).toFixed(1)}%` : "—"],
                          ].map(([k, v]: any) => (
                            <div key={k} style={{ background: "var(--bg-panel)", border: "1px solid var(--border-subtle)", borderRadius: 4, padding: "6px 8px" }}>
                              <div style={{ fontSize: 8, color: "var(--text-disabled)", textTransform: "uppercase", letterSpacing: "0.4px" }}>{k}</div>
                              <div className="tabular-nums" style={{ fontSize: 12, fontWeight: 600, color: typeof v === "number" ? pnlColor(v) : "var(--text-primary)" }}>
                                {typeof v === "number" ? formatINR(v) : String(v ?? "—")}
                              </div>
                            </div>
                          ))}
                        </div>
                        <div style={{ fontSize: "10px", color: "var(--text-muted)", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.5px" }}>
                          STRATEGIES
                        </div>
                        {(instDetail?.strategies ? Object.keys(instDetail.strategies).length : 0) > 0 ? (
                          <div style={{ display: "flex", flexWrap: "wrap", gap: "6px" }}>
                            {(Object.entries(instDetail.strategies) as [string, any][]).map(([sid, sdata]: [string, any]) => (
                              <button
                                key={sid}
                                onClick={() => loadStrategy(inst, sid)}
                                style={{
                                  background: strategyDetail === sid ? "var(--bg-panel-active)" : "var(--bg-panel)",
                                  border: `1px solid ${strategyDetail === sid ? "var(--border-active)" : "var(--border-subtle)"}`,
                                  borderRadius: 4,
                                  color: strategyDetail === sid ? "var(--text-primary)" : "var(--text-muted)",
                                  padding: "4px 10px",
                                  fontSize: 9,
                                  fontWeight: 600,
                                  cursor: "pointer",
                                  display: "flex",
                                  alignItems: "center",
                                  gap: 6,
                                }}
                              >
                                <span>{sid}</span>
                                <span className="tabular-nums" style={{ color: pnlColor(sdata?.realized?.realized_net ?? 0) }}>
                                  {formatINRShort(sdata?.realized?.realized_net ?? 0)}
                                </span>
                              </button>
                            ))}
                          </div>
                        ) : (
                          <div style={{ fontSize: 10, color: "var(--text-muted)" }}>No per-strategy breakdown for {inst}</div>
                        )}
                        {strategyDetail && strategyData && (
                          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(130px, 1fr))", gap: "8px", marginTop: 4 }}>
                            {[
                              ["STATUS", strategyData.positions && Object.keys(strategyData).length ? (strategyData.position_count > 0 ? "IN POSITION" : "FLAT") : "FLAT"],
                              ["REALIZED NET", strategyData?.realized?.realized_net],
                              ["REALIZED GROSS", strategyData?.realized?.realized_gross],
                              ["TRADES", strategyData?.realized?.trade_count],
                              ["WINS", strategyData?.realized?.wins],
                              ["UNREALIZED", strategyData?.unrealized],
                            ].map(([k, v]: any) => (
                              <div key={k} style={{ background: "var(--bg-panel)", border: "1px solid var(--border-subtle)", borderRadius: 4, padding: "6px 8px" }}>
                                <div style={{ fontSize: 8, color: "var(--text-disabled)", textTransform: "uppercase", letterSpacing: "0.4px" }}>{k}</div>
                                <div className="tabular-nums" style={{ fontSize: 12, fontWeight: 600, color: typeof v === "number" ? pnlColor(v) : "var(--text-primary)" }}>
                                  {typeof v === "number" ? formatINR(v) : String(v ?? "—")}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

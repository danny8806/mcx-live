import { useState, useCallback } from "react";
import { useDataSelector } from "../store/DataProvider";
import { api } from "../lib/api";

export default function Indicators() {
  const indicators = useDataSelector<any>((s) => s.indicators);
  const htf = useDataSelector<any>((s) => s.htf);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [expandedHtf, setExpandedHtf] = useState<string | null>(null);
  const [indDetail, setIndDetail] = useState<any>(null);
  const [htfDetail, setHtfDetail] = useState<any>(null);
  const [busyInd, setBusyInd] = useState(false);
  const [busyHtf, setBusyHtf] = useState(false);

  const toggleIndicator = useCallback(async (key: string, inst?: string) => {
    if (expandedKey === key) {
      setExpandedKey(null);
      setIndDetail(null);
      return;
    }
    setExpandedKey(key);
    setIndDetail(null);
    setBusyInd(true);
    try {
      const d = await api.indicatorsInstrument(inst || key);
      if (!d?.error) setIndDetail(d);
    } catch { /* ignore */ } finally {
      setBusyInd(false);
    }
  }, [expandedKey]);

  const toggleHtf = useCallback(async (key: string, inst?: string) => {
    if (expandedHtf === key) {
      setExpandedHtf(null);
      setHtfDetail(null);
      return;
    }
    setExpandedHtf(key);
    setHtfDetail(null);
    setBusyHtf(true);
    try {
      const d = await api.htfInstrument(inst || key);
      if (!d?.error) setHtfDetail(d);
    } catch { /* ignore */ } finally {
      setBusyHtf(false);
    }
  }, [expandedHtf]);

  if (!indicators) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "100%", height: "120px", marginBottom: "10px" }} />
      <div className="skeleton" style={{ width: "100%", height: "120px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading indicators...</div>
    </div>
  );

  const entries = Object.entries(indicators);
  const htfEntries = Object.entries(htf);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" }}>
        <div style={{ padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)", fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>
          DEMA-ATR INDICATORS ({entries.length})
        </div>
        {entries.length === 0 ? (
          <div className="animate-fade-in-up" style={{ padding: "24px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>No indicator data</div>
        ) : (
          <>
            <div style={{ display: "grid", gridTemplateColumns: "110px 80px 80px 80px 50px 50px", gap: "8px", padding: "5px 12px", fontSize: "9px", color: "var(--text-disabled)", textTransform: "uppercase", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-table-header)", position: "sticky", top: 0, zIndex: 1 }}>
              <span>Key</span><span style={{ textAlign: "right" }}>DEMA</span><span style={{ textAlign: "right" }}>ATR</span><span style={{ textAlign: "right" }}>Prev</span><span style={{ textAlign: "right" }}>Count</span><span>Init</span>
            </div>
            {entries.map(([key, data]: [string, any]) => (
              <div key={key}>
                <div className="hover-row" onClick={() => toggleIndicator(key, data.instrument)} style={{ display: "grid", gridTemplateColumns: "110px 80px 80px 80px 50px 50px", gap: "8px", padding: "5px 12px", fontSize: "10px", borderBottom: "1px solid var(--border-subtle)", alignItems: "center", cursor: "pointer" }}>
                  <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{key}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-primary)", textAlign: "right" }}>{data.dema_value?.toFixed(2) ?? "—"}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-primary)", textAlign: "right" }}>{data.atr_value?.toFixed(2) ?? "—"}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-muted)", textAlign: "right" }}>{data.prev_output?.toFixed(2) ?? "—"}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-secondary)", textAlign: "right" }}>{data.count}</span>
                  <span style={{ color: data.initialized ? "var(--green)" : "var(--text-disabled)" }}>{data.initialized ? "YES" : "NO"}</span>
                </div>
                {expandedKey === key && (
                  <div style={{ padding: "6px 12px 8px 24px", background: "var(--bg-table-header)", borderBottom: "1px solid var(--border-subtle)", fontSize: "9px" }}>
                    <div style={{ color: "var(--text-disabled)", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.4px", marginBottom: "4px" }}>
                      INDICATOR DETAIL {busyInd && <span style={{ fontWeight: 400 }}>loading...</span>}
                    </div>
                    {indDetail?.error ? (
                      <div style={{ color: "var(--red)" }}>{indDetail.error}</div>
                    ) : indDetail ? (
                      <div>
                        <div style={{ color: "var(--text-muted)", marginBottom: "6px" }}>
                          Instrument: {indDetail.instrument} — {Object.keys(indDetail.indicators ?? {}).length} indicator(s)
                        </div>
                        {Object.entries(indDetail.indicators ?? {}).map(([ik, iv]: [string, any]) => (
                          <div key={ik} style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))", gap: "6px", padding: "4px 0", borderTop: "1px solid var(--border-subtle)" }}>
                            <div style={{ gridColumn: "1/-1", color: "var(--text-primary)", fontWeight: 600 }}>{ik}</div>
                            {[
                              ["DEMA", iv.dema_value != null ? iv.dema_value.toFixed(2) : "—"],
                              ["ATR", iv.atr_value != null ? iv.atr_value.toFixed(2) : "—"],
                              ["Prev", iv.prev_output != null ? iv.prev_output.toFixed(2) : "—"],
                              ["Count", String(iv.count ?? "—")],
                              ["Initialized", iv.initialized ? "YES" : "NO"],
                            ].map(([k, v]) => (
                              <div key={String(k)}>
                                <span style={{ color: "var(--text-disabled)" }}>{String(k)}: </span>
                                <span className="tabular-nums" style={{ color: "var(--text-primary)" }}>{String(v)}</span>
                              </div>
                            ))}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div style={{ color: "var(--text-muted)" }}>{busyInd ? "Fetching..." : "Detail unavailable"}</div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </>
        )}
      </div>

      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" }}>
        <div style={{ padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)", fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>
          HTF CONFIRMATION ({htfEntries.length})
        </div>
        {htfEntries.length === 0 ? (
          <div className="animate-fade-in-up" style={{ padding: "24px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>No HTF data</div>
        ) : (
          <>
            <div style={{ display: "grid", gridTemplateColumns: "110px 80px 80px 80px 50px", gap: "8px", padding: "5px 12px", fontSize: "9px", color: "var(--text-disabled)", textTransform: "uppercase", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-table-header)", position: "sticky", top: 0, zIndex: 1 }}>
              <span>Key</span><span style={{ textAlign: "right" }}>Confirmed</span><span style={{ textAlign: "right" }}>Prev</span><span style={{ textAlign: "right" }}>Source</span><span>Init</span>
            </div>
            {htfEntries.map(([key, data]: [string, any]) => (
              <div key={key}>
                <div className="hover-row" onClick={() => toggleHtf(key, data.instrument)} style={{ display: "grid", gridTemplateColumns: "110px 80px 80px 80px 50px", gap: "8px", padding: "5px 12px", fontSize: "10px", borderBottom: "1px solid var(--border-subtle)", alignItems: "center", cursor: "pointer" }}>
                  <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{key}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-primary)", textAlign: "right" }}>{data.last_confirmed_value?.toFixed(2) ?? "—"}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-muted)", textAlign: "right" }}>{data.prev_confirmed_value?.toFixed(2) ?? "—"}</span>
                  <span style={{ color: "var(--text-muted)", fontSize: "9px", textAlign: "right" }}>{data.source_timestamp ? new Date(data.source_timestamp * 1000).toLocaleTimeString("en-IN", { hour12: false }) : "—"}</span>
                  <span style={{ color: data.indicator?.initialized ? "var(--green)" : "var(--text-disabled)" }}>{data.indicator?.initialized ? "YES" : "NO"}</span>
                </div>
                {expandedHtf === key && (
                  <div style={{ padding: "6px 12px 8px 24px", background: "var(--bg-table-header)", borderBottom: "1px solid var(--border-subtle)", fontSize: "9px" }}>
                    <div style={{ color: "var(--text-disabled)", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.4px", marginBottom: "4px" }}>
                      HTF DETAIL {busyHtf && <span style={{ fontWeight: 400 }}>loading...</span>}
                    </div>
                    {htfDetail?.error ? (
                      <div style={{ color: "var(--red)" }}>{htfDetail.error}</div>
                    ) : htfDetail ? (
                      <div>
                        <div style={{ color: "var(--text-muted)", marginBottom: "6px" }}>
                          Instrument: {htfDetail.instrument} — {Object.keys(htfDetail.htf ?? {}).length} HTF(s)
                        </div>
                        {Object.entries(htfDetail.htf ?? {}).map(([hk, hv]: [string, any]) => (
                          <div key={hk} style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))", gap: "6px", padding: "4px 0", borderTop: "1px solid var(--border-subtle)" }}>
                            <div style={{ gridColumn: "1/-1", color: "var(--text-primary)", fontWeight: 600 }}>{hk}</div>
                            {[
                              ["Confirmed", hv.last_confirmed_value != null ? hv.last_confirmed_value.toFixed(2) : "—"],
                              ["Prev", hv.prev_confirmed_value != null ? hv.prev_confirmed_value.toFixed(2) : "—"],
                              ["Timeframe", hv.timeframe ?? "—"],
                              ["Initialized", hv.indicator?.initialized ? "YES" : "NO"],
                              ["DEMA", hv.indicator?.dema_value != null ? hv.indicator.dema_value.toFixed(2) : "—"],
                              ["ATR", hv.indicator?.atr_value != null ? hv.indicator.atr_value.toFixed(2) : "—"],
                            ].map(([k, v]) => (
                              <div key={String(k)}>
                                <span style={{ color: "var(--text-disabled)" }}>{String(k)}: </span>
                                <span className="tabular-nums" style={{ color: "var(--text-primary)" }}>{String(v)}</span>
                              </div>
                            ))}
                          </div>
                        ))}
                      </div>
                    ) : (
                      <div style={{ color: "var(--text-muted)" }}>{busyHtf ? "Fetching..." : "Detail unavailable"}</div>
                    )}
                  </div>
                )}
              </div>
            ))}
          </>
        )}
      </div>
    </div>
  );
}

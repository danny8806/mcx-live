import { useState, useCallback } from "react";
import { useDataSelector } from "../store/DataProvider";
import { safeINR } from "../lib/utils";
import { api } from "../lib/api";

export default function MarketData() {
  const marketData = useDataSelector<any>((s) => s.marketData);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [detail, setDetail] = useState<any>(null);
  const [busy, setBusy] = useState(false);

  const toggleDetail = useCallback(async (name: string) => {
    if (expanded === name) {
      setExpanded(null);
      setDetail(null);
      return;
    }
    setExpanded(name);
    setDetail(null);
    setBusy(true);
    try {
      const d = await api.marketDataInstrument(name);
      if (!d?.error) setDetail(d);
    } catch { /* ignore */ } finally {
      setBusy(false);
    }
  }, [expanded]);

  if (!marketData) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "300px", height: "32px", marginBottom: "12px" }} />
      <div className="skeleton" style={{ width: "100%", height: "160px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading market data...</div>
    </div>
  );

  const instruments = marketData.instruments ?? {};
  const adapterStats = marketData.adapter_stats ?? {};

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
      <div className="lift animate-fade-in-up" style={{ display: "flex", gap: "20px", alignItems: "center", background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "8px 12px", fontSize: "10px", color: "var(--text-muted)" }}>
        <span>Feed: <span style={{ color: marketData.ws_connected ? "var(--green)" : "var(--red)", fontWeight: 600 }}>{marketData.ws_connected ? "CONNECTED" : "DISCONNECTED"}</span></span>
        <span>Session ticks: <span className="tabular-nums" style={{ color: "var(--text-primary)" }}>{String(adapterStats.tick_count ?? 0)}</span></span>
        {adapterStats.error_count ? <span>Errors: <span style={{ color: "var(--red)" }}>{adapterStats.error_count}</span></span> : null}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
        {Object.entries(instruments).map(([name, data]: [string, any]) => {
          const isLive = data.ltp > 0;
          const isExpanded = expanded === name;
          return (
            <div key={name} className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px", cursor: "pointer" }} onClick={() => toggleDetail(name)}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "10px" }}>
                <span style={{ fontSize: "12px", fontWeight: 600, color: "var(--text-primary)" }}>{name}</span>
                <div style={{ display: "flex", alignItems: "center", gap: "4px" }}>
                  <span
                    className={isLive ? "animate-pulse-dot" : ""}
                    style={{ width: "5px", height: "5px", borderRadius: "50%", background: isLive ? "var(--green)" : "var(--red)", ["--dot" as any]: isLive ? "var(--green)" : "var(--red)" }}
                  />
                  <span style={{ fontSize: "9px", color: isLive ? "var(--green)" : "var(--red)" }}>{isLive ? "LIVE" : "NO DATA"}</span>
                </div>
              </div>
              <div className="tabular-nums" style={{ fontSize: "28px", fontWeight: 700, color: "var(--text-primary)", marginBottom: "12px" }}>
                {isLive ? safeINR(data.ltp) : "—"}
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px", fontSize: "10px" }}>
                {[["Spread", data.spread > 0 ? `₹${data.spread}` : "—"], ["Ticks", String(data.tick_count ?? 0)], ["Last Update", data.timestamp > 0 ? new Date(data.timestamp * 1000).toLocaleTimeString("en-IN", { hour12: false }) : "—"]].map(([k, v]) => (
                  <div key={String(k)} style={{ display: "flex", justifyContent: "space-between", padding: "3px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                    <span style={{ color: "var(--text-muted)" }}>{String(k)}</span>
                    <span className="tabular-nums" style={{ color: "var(--text-primary)" }}>{String(v)}</span>
                  </div>
                ))}
              </div>
              {isExpanded && (
                <div style={{ marginTop: "10px", paddingTop: "10px", borderTop: "1px solid var(--border-subtle)", fontSize: "9px" }}>
                  <div style={{ color: "var(--text-disabled)", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.4px", marginBottom: "6px" }}>
                    INSTRUMENT DETAIL
                    {busy && <span style={{ marginLeft: "6px", fontWeight: 400 }}>loading...</span>}
                  </div>
                  {detail?.error ? (
                    <div style={{ color: "var(--red)" }}>{detail.error}</div>
                  ) : detail ? (
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: "6px" }}>
                      {[
                        ["LTP", detail.ltp ? safeINR(detail.ltp) : "—"],
                        ["Security ID", detail.config?.security_id ?? "—"],
                        ["Exchange", detail.config?.exchange_segment ?? "—"],
                        ["Segment", detail.config?.instrument ?? "—"],
                        ["Symbol", detail.config?.symbol ?? "—"],
                        ["Multiplier", String(detail.config?.multiplier ?? detail.config?.lot_size ?? "—")],
                        ["Updated", detail.timestamp ? new Date(detail.timestamp * 1000).toLocaleTimeString("en-IN", { hour12: false }) : "—"],
                      ].map(([k, v]) => (
                        <div key={String(k)}>
                          <span style={{ color: "var(--text-disabled)" }}>{String(k)}: </span>
                          <span className="tabular-nums" style={{ color: "var(--text-primary)" }}>{String(v)}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div style={{ color: "var(--text-muted)" }}>{busy ? "Fetching..." : "Detail unavailable"}</div>
                  )}
                </div>
              )}
            </div>
          );
        })}
        {Object.keys(instruments).length === 0 && (
          <div className="animate-fade-in-up" style={{ gridColumn: "1/-1", padding: "40px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>No market data available</div>
        )}
      </div>
    </div>
  );
}

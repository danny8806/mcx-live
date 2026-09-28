import { useEffect, useState, useCallback } from "react";
import { useDataSelector } from "../store/DataProvider";
import { api } from "../lib/api";

export default function Health() {
  const healthComponents = useDataSelector<any[]>((s) => s.healthComponents);
  const overallHealth = useDataSelector<string>((s) => s.overallHealth);
  const [live, setLive] = useState<any>(null);
  const [liveError, setLiveError] = useState<string | null>(null);

  const fetchLive = useCallback(async () => {
    try {
      const d = await api.health();
      if (d?.status) setLive(d);
    } catch (e: any) {
      setLiveError(e?.message || String(e));
    }
  }, []);

  useEffect(() => {
    fetchLive();
    const t = window.setInterval(fetchLive, 10000);
    return () => window.clearInterval(t);
  }, [fetchLive]);

  if (!healthComponents) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "250px", height: "36px", marginBottom: "12px" }} />
      <div className="skeleton" style={{ width: "100%", height: "120px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading health data...</div>
    </div>
  );

  function formatName(name: string): string {
    return name.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
  }

  const systemOk = live?.status === "ok";
  const wsCount = live?.ws_connections ?? 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
      <div className="lift animate-fade-in-up" style={{
        background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px",
        padding: "12px", display: "flex", alignItems: "center", gap: "10px",
      }}>
        <span
          className={overallHealth === "healthy" ? "animate-pulse-dot" : ""}
          style={{ width: "8px", height: "8px", borderRadius: "50%", background: overallHealth === "healthy" ? "var(--green)" : overallHealth === "degraded" ? "var(--amber)" : "var(--red)", ["--dot" as any]: overallHealth === "healthy" ? "var(--green)" : overallHealth === "degraded" ? "var(--amber)" : "var(--red)" }}
        />
        <span style={{ fontSize: "13px", fontWeight: 600, color: "var(--text-primary)" }}>
          System Status: {(overallHealth ?? "unknown").toUpperCase()}
        </span>
      </div>

      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "10px 12px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
          <span style={{ fontSize: "11px", fontWeight: 500, color: "var(--text-primary)" }}>Live Service Check</span>
          <div style={{ display: "flex", alignItems: "center", gap: "4px" }}>
            <span
              className={systemOk ? "animate-pulse-dot" : ""}
              style={{ width: "5px", height: "5px", borderRadius: "50%", background: systemOk && !liveError ? "var(--green)" : "var(--red)", ["--dot" as any]: systemOk && !liveError ? "var(--green)" : "var(--red)" }}
            />
            <span style={{ fontSize: "9px", color: systemOk && !liveError ? "var(--green)" : "var(--red)" }}>
              {liveError ? "ERROR" : systemOk ? "OK" : "UNKNOWN"}
            </span>
          </div>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(130px, 1fr))", gap: "8px" }}>
          {[
            ["ENGINE", live?.engine != null ? (live.engine ? "UP" : "DOWN") : "—", live?.engine ? "var(--green)" : "var(--red)"],
            ["PERSISTENCE", live?.persistence != null ? (live.persistence ? "UP" : "DOWN") : "—", live?.persistence ? "var(--green)" : "var(--red)"],
            ["WS CONNECTIONS", String(wsCount ?? "—"), wsCount > 0 ? "var(--green)" : "var(--text-muted)"],
            ["EVENT BUS SIZE", String(live?.event_bus?.size ?? "—"), "var(--text-primary)"],
            ["LAST CHECK", live?.timestamp ? new Date(live.timestamp).toLocaleTimeString("en-IN", { hour12: false }) : "—", "var(--text-muted)"],
          ].map(([label, val, color]) => (
            <div key={String(label)} style={{ background: "var(--bg-input)", border: "1px solid var(--border-subtle)", borderRadius: "6px", padding: "8px" }}>
              <div style={{ fontSize: "8px", color: "var(--text-disabled)", textTransform: "uppercase", letterSpacing: "0.4px", marginBottom: "3px" }}>{String(label)}</div>
              <div className="tabular-nums" style={{ fontSize: "14px", fontWeight: 600, color: String(color) }}>{String(val)}</div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "8px" }}>
        {healthComponents.map((c: any) => {
          const isHealthy = c.status === "healthy";
          return (
            <div key={c.name} className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "10px 12px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
                <span style={{ fontSize: "11px", fontWeight: 500, color: "var(--text-primary)" }}>{formatName(c.name)}</span>
                <div style={{ display: "flex", alignItems: "center", gap: "4px" }}>
                  <span
                    className={isHealthy ? "animate-pulse-dot" : ""}
                    style={{ width: "5px", height: "5px", borderRadius: "50%", background: c.status === "healthy" ? "var(--green)" : c.status === "degraded" ? "var(--amber)" : "var(--red)", ["--dot" as any]: c.status === "healthy" ? "var(--green)" : c.status === "degraded" ? "var(--amber)" : "var(--red)" }}
                  />
                  <span style={{ fontSize: "9px", color: c.status === "healthy" ? "var(--green)" : c.status === "degraded" ? "var(--amber)" : "var(--red)" }}>
                    {c.status?.toUpperCase()}
                  </span>
                </div>
              </div>
              <div style={{ fontSize: "9px", color: "var(--text-muted)" }}>
                {c.errors > 0 && <div style={{ color: "var(--red)" }}>{c.errors} errors</div>}
                {c.uptime > 0 && <div>Uptime: {c.uptime < 60 ? `${c.uptime}s` : c.uptime < 3600 ? `${Math.floor(c.uptime / 60)}m` : `${Math.floor(c.uptime / 3600)}h`}</div>}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

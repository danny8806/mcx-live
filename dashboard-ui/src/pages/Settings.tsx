import { useDataSelector } from "../store/DataProvider";

export default function Settings() {
  const settings = useDataSelector<any>((s) => s.settings);
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
    { key: "strategies", label: "Strategies" },
    { key: "indicators", label: "Indicators" },
    { key: "risk", label: "Risk" },
    { key: "account", label: "Account" },
    ...(!isLive ? [{ key: "paper_execution", label: "Paper Execution" }] : []),
  ];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "10px" }}>
      <div style={{ gridColumn: "1 / -1", padding: "8px 12px", borderRadius: "8px", fontSize: "10px", fontWeight: 600, letterSpacing: "0.5px", background: isLive ? "var(--green)" : "var(--amber)", color: "#fff" }}>
        EXECUTION MODE: {isLive ? "LIVE — real orders go to Dhan" : "PAPER — simulated execution"}
      </div>
      {sections.map(({ key, label }) => {
        const data = settings[key];
        if (!data) return null;
        return (
          <div key={key} className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px" }}>
            <div style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: "8px" }}>{label}</div>
            {typeof data === "object" && data !== null ? (
              Object.entries(data).map(([k, v]) => (
                <div key={k} style={{ display: "flex", justifyContent: "space-between", fontSize: "10px", padding: "3px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                  <span style={{ color: "var(--text-muted)" }}>{k}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-primary)", marginLeft: "8px", overflow: "hidden", textOverflow: "ellipsis", maxWidth: "200px", whiteSpace: "nowrap", textAlign: "right" }}>
                    {typeof v === "object" ? JSON.stringify(v) : String(v ?? "")}
                  </span>
                </div>
              ))
            ) : (
              <div style={{ color: "var(--text-secondary)", fontSize: "10px" }}>{String(data)}</div>
            )}
          </div>
        );
      })}
    </div>
  );
}

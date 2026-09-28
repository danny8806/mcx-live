import { useDataSelector } from "../store/DataProvider";
import { formatTimestamp } from "../lib/utils";

export default function AuditLog() {
  const audit = useDataSelector<any[]>((s) => s.audit);
  if (!audit) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "200px", height: "14px", marginBottom: "12px" }} />
      <div className="skeleton" style={{ width: "100%", height: "200px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading audit log...</div>
    </div>
  );

  return (
    <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" }}>
      <div style={{ padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)", fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>
        AUDIT LOG ({audit.length})
      </div>
      {audit.length === 0 ? (
        <div className="animate-fade-in-up" style={{ padding: "40px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>No audit events — system just started</div>
      ) : (
        <div style={{ maxHeight: "500px", overflow: "auto" }}>
          {audit.map((e: any, i: number) => {
            const evt = e.event_type || e.type || "UNKNOWN";
            return (
              <div key={i} className="hover-row" style={{ display: "flex", alignItems: "center", gap: "10px", padding: "4px 12px", fontSize: "10px", borderBottom: "1px solid var(--border-subtle)" }}>
                <span className="tabular-nums" style={{ color: "var(--text-muted)", width: "55px" }}>{formatTimestamp(e.timestamp)}</span>
                <span style={{
                  fontSize: "8px", padding: "1px 5px", borderRadius: "2px", fontWeight: 600,
                  background: evt === "SIGNAL" ? "var(--blue-muted)" : evt === "ORDER" ? "var(--amber-muted)" : evt === "FILL" ? "var(--green-muted)" : evt === "RISK" ? "rgba(154,124,255,0.1)" : evt === "ERROR" ? "var(--red-muted)" : "var(--bg-table-header)",
                  color: evt === "SIGNAL" ? "var(--blue)" : evt === "ORDER" ? "var(--amber)" : evt === "FILL" ? "var(--green)" : evt === "RISK" ? "var(--purple)" : evt === "ERROR" ? "var(--red)" : "var(--text-muted)",
                }}>{evt}</span>
                <span style={{ color: "var(--text-secondary)", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {typeof e.data === "string" ? e.data : JSON.stringify(e.data)}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

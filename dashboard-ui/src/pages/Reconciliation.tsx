import { useState, useCallback } from "react";
import { useDataSelector } from "../store/DataProvider";
import { api } from "../lib/api";

export default function Reconciliation() {
  const reconciliation = useDataSelector<any>((s) => s.reconciliation);
  const [scanResult, setScanResult] = useState<any>(null);
  const [running, setRunning] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const runScan = useCallback(async (kind: "orphan" | "lifecycle") => {
    setRunning(kind);
    setError(null);
    setScanResult(null);
    try {
      const d = kind === "orphan" ? await api.orphanScan() : await api.lifecycleReconcile();
      if (d?.error) setError(d.error);
      else setScanResult({ kind, data: d });
    } catch (e: any) {
      setError(e?.message || String(e));
    } finally {
      setRunning(null);
    }
  }, []);

  if (!reconciliation || Object.keys(reconciliation).length === 0) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "350px", height: "48px", marginBottom: "12px" }} />
      <div className="skeleton" style={{ width: "100%", height: "80px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading reconciliation data...</div>
    </div>
  );

  const stats: Record<string, any> = {
    // ### OBSERVATION (stats source mismatch)
    // The page read only `reconciliation.stats`, but the route returns its
    // aggregate figures as `summary` (stats exist only as `checks[].stats`),
    // so the stat tiles never rendered.  Merge both sources.
    ...(reconciliation.summary || {}),
    ...(reconciliation.stats || {}),
  };
  const errors: any[] = reconciliation.errors || [];
  const warnings: any[] = reconciliation.warnings || [];
  const isConsistent = reconciliation.is_consistent === true;
  const toText = (x: any): string =>
    typeof x === "string" ? x : (JSON.stringify(x) ?? String(x));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px", display: "flex", justifyContent: "space-between", alignItems: "center", gap: "10px" }}>
        <div>
          <div style={{ fontSize: "12px", fontWeight: 600, color: "var(--text-primary)" }}>
            Reconciliation — {(reconciliation.phase || "live").toUpperCase()}
          </div>
          <div style={{ fontSize: "9px", color: "var(--text-muted)", marginTop: "2px" }}>
            Compares orders, fills, positions, trades, P&L and account state across the persisted DB and in-memory engine
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
          <span
            className={isConsistent ? "animate-pulse-dot" : ""}
            style={{ width: "8px", height: "8px", borderRadius: "50%", background: isConsistent ? "var(--green)" : "var(--red)", ["--dot" as any]: isConsistent ? "var(--green)" : "var(--red)" }}
          />
          <span style={{ fontSize: "10px", fontWeight: 600, color: isConsistent ? "var(--green)" : "var(--red)" }}>
            {isConsistent ? "CONSISTENT" : "INCONSISTENT"}
          </span>
        </div>
      </div>

      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "10px 12px", display: "flex", alignItems: "center", gap: "8px" }}>
        <div style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-primary)" }}>Actions</div>
        <button
          onClick={() => runScan("orphan")}
          disabled={running !== null}
          style={{
            background: running === "orphan" ? "var(--bg-panel-active)" : "var(--bg-input)",
            border: "1px solid var(--border)",
            borderRadius: 4,
            color: "var(--text-primary)",
            padding: "4px 12px",
            fontSize: 9,
            fontWeight: 600,
            cursor: running !== null ? "wait" : "pointer",
          }}
        >
          {running === "orphan" ? "Scanning..." : "Run Orphan Scan"}
        </button>
        <button
          onClick={() => runScan("lifecycle")}
          disabled={running !== null}
          style={{
            background: running === "lifecycle" ? "var(--bg-panel-active)" : "var(--bg-input)",
            border: "1px solid var(--border)",
            borderRadius: 4,
            color: "var(--text-primary)",
            padding: "4px 12px",
            fontSize: 9,
            fontWeight: 600,
            cursor: running !== null ? "wait" : "pointer",
          }}
        >
          {running === "lifecycle" ? "Reconciling..." : "Run Lifecycle Reconcile"}
        </button>
        {error && <span style={{ fontSize: "9px", color: "var(--red)" }}>{error}</span>}
      </div>

      {scanResult && (
        <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "10px 12px" }}>
          <div style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-primary)", marginBottom: "6px" }}>
            {scanResult.kind === "orphan" ? "ORPHAN SCAN" : "LIFECYCLE RECONCILE"} — {scanResult.kind === "orphan" ? (scanResult.data.orphans != null ? `${scanResult.data.orphans.length} orphan(s)` : "complete") : "complete"}
          </div>
          {scanResult.kind === "orphan" ? (
            <div style={{ fontSize: "9px", fontFamily: "monospace", color: "var(--text-secondary)", whiteSpace: "pre-wrap" }}>
              {JSON.stringify(scanResult.data, null, 2)}
            </div>
          ) : (
            <div style={{ fontSize: "9px", fontFamily: "monospace", color: "var(--text-secondary)", whiteSpace: "pre-wrap" }}>
              {JSON.stringify(scanResult.data, null, 2)}
            </div>
          )}
        </div>
      )}

      {Object.keys(stats).length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(150px, 1fr))", gap: "8px" }}>
          {Object.entries(stats).map(([k, v]) => (
            <div key={k} className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "10px 12px" }}>
              <div style={{ fontSize: "9px", color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>{k.replace(/_/g, " ")}</div>
              <div className="tabular-nums" style={{ fontSize: "15px", fontWeight: 600, color: "var(--text-primary)" }}>{String(v)}</div>
            </div>
          ))}
        </div>
      )}

      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px" }}>
        <div style={{ fontSize: "11px", fontWeight: 600, color: "var(--text-primary)", marginBottom: "6px" }}>Errors ({errors.length})</div>
        {errors.length === 0 ? (
          <div className="animate-fade-in-up" style={{ fontSize: "10px", color: "var(--text-muted)" }}>No errors</div>
        ) : (
          errors.map((e: any, i: number) => (
            <div key={i} style={{ fontSize: "10px", color: "var(--red)", marginBottom: "4px" }}>• {toText(e)}</div>
          ))
        )}
      </div>

      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px" }}>
        <div style={{ fontSize: "11px", fontWeight: 600, color: "var(--text-primary)", marginBottom: "6px" }}>Warnings ({warnings.length})</div>
        {warnings.length === 0 ? (
          <div className="animate-fade-in-up" style={{ fontSize: "10px", color: "var(--text-muted)" }}>No warnings</div>
        ) : (
          warnings.map((w: any, i: number) => (
            <div key={i} style={{ fontSize: "10px", color: "var(--amber)", marginBottom: "4px" }}>• {toText(w)}</div>
          ))
        )}
      </div>

      {reconciliation.timestamp && (
        <div style={{ fontSize: "9px", color: "var(--text-muted)", textAlign: "right" }}>
          Last checked: {typeof reconciliation.timestamp === "number" ? new Date((reconciliation.timestamp as number) * 1000).toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "Asia/Kolkata" }) : String(reconciliation.timestamp)}
        </div>
      )}
    </div>
  );
}

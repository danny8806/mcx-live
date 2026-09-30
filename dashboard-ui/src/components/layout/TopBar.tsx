import { useEffect, useState } from "react";
import { PanelLeftClose } from "lucide-react";
import { useDataSelector } from "../../store/DataProvider";
import { safeINR } from "../../lib/utils";

function marketSession(now: Date, instrument: any): "OPEN" | "CLOSED" | "UNKNOWN" {
  const open = String(instrument?.session_open ?? "").match(/^(\d{2}):(\d{2})$/);
  const close = String(instrument?.session_close ?? "").match(/^(\d{2}):(\d{2})$/);
  if (!open || !close) return "UNKNOWN";
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata", weekday: "short", hour: "2-digit",
    minute: "2-digit", hourCycle: "h23",
  }).formatToParts(now);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  if (values.weekday === "Sat" || values.weekday === "Sun") return "CLOSED";
  const current = Number(values.hour) * 60 + Number(values.minute);
  const start = Number(open[1]) * 60 + Number(open[2]);
  const end = Number(close[1]) * 60 + Number(close[2]);
  return current >= start && current < end ? "OPEN" : "CLOSED";
}

function StatusPill({ label, status, color }: { label: string; status: string; color: "green" | "red" | "amber" }) {
  const colors = {
    green: { bg: "var(--green-muted)", text: "var(--green)", dot: "var(--green)" },
    red: { bg: "var(--red-muted)", text: "var(--red)", dot: "var(--red)" },
    amber: { bg: "var(--amber-muted)", text: "var(--amber)", dot: "var(--amber)" },
  };
  const c = colors[color];
  const live = status === "LIVE" || status === "OPEN" || status === "RUNNING";
  return (
    <div style={{
      display: "flex", alignItems: "center", gap: "6px",
      padding: "3px 8px", borderRadius: "4px",
      background: c.bg, fontSize: "10px", fontWeight: 500,
    }}>
      <span
        className={live ? "animate-pulse-dot" : ""}
        style={{ width: "5px", height: "5px", borderRadius: "50%", background: c.dot, ["--dot" as any]: c.dot }}
      />
      <span style={{ color: "var(--text-muted)", fontWeight: 400 }}>{label}</span>
      <span style={{ color: c.text }}>{status}</span>
    </div>
  );
}

export default function TopBar({ onToggleSidebar }: { onToggleSidebar: () => void }) {
  const connected = useDataSelector<boolean>((s) => s.connected);
  const overallHealth = useDataSelector<string>((s) => s.overallHealth);
  const goldOverview = useDataSelector<any>((s) => s.goldOverview);
  const silverOverview = useDataSelector<any>((s) => s.silverOverview);
  const settings = useDataSelector<any>((s) => s.settings);
  const [time, setTime] = useState(new Date());

  useEffect(() => {
    const t = setInterval(() => setTime(new Date()), 1000);
    return () => clearInterval(t);
  }, []);

  const ist = time.toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata", hour12: false });
  const istDate = time.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata", weekday: "short", day: "2-digit", month: "short" });
  const session = marketSession(time, settings?.instruments?.GOLDM);

  const goldLtp = goldOverview?.ltp ?? 0;
  const silverLtp = silverOverview?.ltp ?? 0;
  const executionMode = useDataSelector<string | null>((s) => s.overview?.execution_mode ?? null);

  const headerStyle: React.CSSProperties = {
    height: "var(--topbar-height)",
    background: "var(--bg-topbar)",
    borderBottom: "1px solid var(--border)",
    display: "flex",
    alignItems: "center",
    padding: "0 16px",
    gap: "8px",
    fontSize: "11px",
    flexShrink: 0,
  };

  const divider = <div style={{ width: "1px", height: "20px", background: "var(--border-subtle)" }} />;

  return (
    <header style={headerStyle}>
      <button
        onClick={onToggleSidebar}
        aria-label="Toggle sidebar"
        title="Toggle sidebar"
        style={{
          background: "transparent", border: "none", color: "var(--text-muted)",
          cursor: "pointer", padding: "4px", display: "flex", alignItems: "center",
          justifyContent: "center", marginRight: "4px",
        }}
      >
        <PanelLeftClose style={{ width: "16px", height: "16px" }} />
      </button>

      <div style={{ display: "flex", alignItems: "center", gap: "8px", marginRight: "4px" }}>
        <div style={{
          width: "22px", height: "22px", borderRadius: "4px",
          background: "rgba(242,184,75,0.12)", display: "flex",
          alignItems: "center", justifyContent: "center",
        }}>
          <span style={{ color: "var(--amber)", fontWeight: 700, fontSize: "10px" }}>M</span>
        </div>
        <span style={{ fontWeight: 700, color: "var(--text-primary)", fontSize: "11px", letterSpacing: "0.3px" }}>
          MCX TRADER
        </span>
      </div>

      {divider}

      <StatusPill label="MARKET" status={session} color={session === "OPEN" ? "green" : "amber"} />
      <StatusPill label="WS" status={connected ? "LIVE" : "DOWN"} color={connected ? "green" : "red"} />
      <StatusPill label="ENGINE" status={overallHealth === "healthy" ? "RUNNING" : (overallHealth ?? "unknown").toUpperCase()} color={overallHealth === "healthy" ? "green" : "amber"} />

      <div style={{
        display: "flex", alignItems: "center", gap: "4px",
        padding: "3px 8px", borderRadius: "4px",
        background: "var(--blue-muted)", fontSize: "10px", fontWeight: 600,
      }}>
        <span style={{ color: "var(--amber)" }}>⚠</span>
        <span style={{ color: executionMode === "LIVE" ? "var(--green)" : "var(--blue)" }}>{executionMode ?? "—"}</span>
      </div>

      <div style={{ flex: 1 }} />

      {goldLtp > 0 && (
        <div style={{ display: "flex", alignItems: "baseline", gap: "4px" }}>
          <span style={{ color: "var(--text-muted)", fontSize: "10px" }}>GOLD</span>
          <span style={{ color: "var(--text-primary)", fontWeight: 600, fontSize: "12px", fontVariantNumeric: "tabular-nums" }}>
            {safeINR(goldLtp)}
          </span>
        </div>
      )}

      {goldLtp > 0 && silverLtp > 0 && <div style={{ width: "1px", height: "14px", background: "var(--border)" }} />}

      {silverLtp > 0 && (
        <div style={{ display: "flex", alignItems: "baseline", gap: "4px" }}>
          <span style={{ color: "var(--text-muted)", fontSize: "10px" }}>SILVERM</span>
          <span style={{ color: "var(--text-primary)", fontWeight: 600, fontSize: "12px", fontVariantNumeric: "tabular-nums" }}>
            {safeINR(silverLtp)}
          </span>
        </div>
      )}

      {divider}

      <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
        <span style={{ color: "var(--text-muted)", fontVariantNumeric: "tabular-nums", fontWeight: 500, fontSize: "10px" }}>
          {istDate}
        </span>
        <span style={{ color: "var(--text-muted)" }}>•</span>
        <span className="tabular-nums" style={{ color: "var(--text-primary)", fontWeight: 600, fontSize: "11px" }}>
          {ist}
        </span>
      </div>
    </header>
  );
}

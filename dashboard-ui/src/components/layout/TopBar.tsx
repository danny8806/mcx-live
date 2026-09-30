import { useEffect, useState } from "react";
import { PanelLeft, RadioTower } from "lucide-react";
import { useDataSelector } from "../../store/DataProvider";

export default function TopBar({ onToggleSidebar }: { onToggleSidebar: () => void }) {
  const connected = useDataSelector<boolean>((s) => s.connected);
  const health = useDataSelector<string>((s) => s.overallHealth);
  const mode = useDataSelector<string | null>((s) => s.overview?.execution_mode ?? null);
  const market = useDataSelector<any>((s) => s.marketData);
  const [now, setNow] = useState(new Date());
  useEffect(() => { const timer = window.setInterval(() => setNow(new Date()), 1000); return () => window.clearInterval(timer); }, []);
  const clock = now.toLocaleString("en-IN", { timeZone: "Asia/Kolkata", weekday: "short", day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
  return <header className="app-topbar">
    <button className="mobile-nav-button" onClick={onToggleSidebar} aria-label="Toggle navigation"><PanelLeft size={19}/></button>
    <div className="topbar-title"><RadioTower size={16}/><span>MCX live operations</span></div>
    <div className="topbar-statuses">
      <span title={`Dashboard ${connected ? "connected" : "offline"}`} className={`top-status ${connected ? "ok" : "bad"}`}><i/>Dashboard {connected ? "connected" : "offline"}</span>
      <span title={`Engine ${health || "unknown"}`} className={`top-status ${health === "healthy" ? "ok" : "warn"}`}><i/>Engine {health || "unknown"}</span>
      <span title={`Market ${market?.ws_connected ? "live" : "unknown"}`} className={`top-status ${market?.ws_connected ? "ok" : "warn"}`}><i/>Market {market?.ws_connected ? "live" : "unknown"}</span>
    </div>
    <div className={`topbar-execution ${mode === "LIVE" ? "real" : "sim"}`}><span>{mode || "UNKNOWN"}</span><small>execution</small></div>
    <time className="topbar-clock">{clock} IST</time>
  </header>;
}

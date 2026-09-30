import { useMemo, useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import Sidebar from "./Sidebar";
import TopBar from "./TopBar";
import { useDataSelector } from "../../store/DataProvider";

const titles: Record<string, [string, string]> = {
  "/": ["Live desk", "A real-time view of positions, triggers, broker orders, and protection."],
  "/live": ["Live trading", "Instrument prices and active trade lifecycle."], "/live-ops": ["Operations", "Broker connectivity, market data, engine state, and reconciliation."],
  "/strategies": ["Strategies", "Runtime state, strategy performance, and controls."], "/matrix": ["Strategy matrix", "Compare running strategies and their current state."],
  "/positions": ["Positions", "Broker-linked open and closed position records."], "/orders": ["Orders", "Order requests and broker execution outcomes."], "/trades": ["Trade history", "Entry-to-exit trade lifecycle and realized results."],
  "/pnl": ["P&L analytics", "Realized, unrealized, costs, and equity history."], "/risk": ["Risk", "Capital, margin, limits, and current exposure."],
  "/market-data": ["Market data", "Live instrument prices and feed status."], "/indicators": ["Indicators", "Strategy indicator values and higher-timeframe confirmation."],
  "/reconciliation": ["Reconciliation", "Compare persisted state, runtime state, and broker data."], "/reversals": ["Reversal lifecycle", "Trace old-position exit and opposite-entry processing."],
  "/alerts": ["Alerts", "Runtime events and actionable system warnings."], "/health": ["System health", "Engine, persistence, and service health checks."],
  "/settings": ["Settings", "Runtime strategy gates, quantity, and system configuration."], "/audit": ["Audit history", "Recent configuration and lifecycle events."],
};

export default function Layout() {
  const connected = useDataSelector<boolean>((s) => s.connected);
  const location = useLocation();
  const [collapsed, setCollapsed] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [title, description] = useMemo(() => titles[location.pathname] ?? ["MCX Terminal", "Live system workspace."], [location.pathname]);
  const width = collapsed ? 76 : 252;
  return <div className={`app-frame ${mobileOpen ? "mobile-open" : ""}`}>
    <Sidebar connected={connected} collapsed={collapsed} onToggle={() => setCollapsed(value => !value)} onNavigate={() => setMobileOpen(false)} width={width}/>
    {mobileOpen && <button className="mobile-scrim" aria-label="Close navigation" onClick={() => setMobileOpen(false)}/>}
    <div className="app-main" style={{ marginLeft: width }}>
      <TopBar onToggleSidebar={() => setMobileOpen(value => !value)}/>
      <main className="app-content">
        {location.pathname !== "/" && <div className="page-heading"><div><div className="page-kicker">MARKET OPERATIONS / {title.toUpperCase()}</div><h1>{title}</h1><p>{description}</p></div></div>}
        <Outlet/>
        <footer className="app-footer"><span>MCX Terminal</span><span>Values update from connected runtime and broker APIs</span></footer>
      </main>
    </div>
  </div>;
}

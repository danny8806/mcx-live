import { NavLink } from "react-router-dom";
import { useDataSelector } from "../../store/DataProvider";
import { Activity, BarChart3, Bell, BookOpen, BriefcaseBusiness, CandlestickChart, ChevronLeft, ChevronRight, CircleDollarSign, ClipboardList, FileClock, Gauge, GitCompareArrows, LayoutDashboard, RotateCcw, Settings2, ShieldCheck, SlidersHorizontal, Waves } from "lucide-react";

const groups = [
  { label: "WORKSPACE", items: [{ to: "/", label: "Live desk", icon: LayoutDashboard }, { to: "/live-ops", label: "Operations", icon: Activity }] },
  { label: "TRADING", items: [{ to: "/positions", label: "Positions", icon: BriefcaseBusiness }, { to: "/orders", label: "Orders", icon: ClipboardList }, { to: "/trades", label: "Trade history", icon: BookOpen }, { to: "/strategies", label: "Strategies", icon: SlidersHorizontal }, { to: "/reversals", label: "Reversals", icon: RotateCcw }] },
  { label: "ANALYSIS", items: [{ to: "/pnl", label: "P&L analytics", icon: CircleDollarSign }, { to: "/risk", label: "Risk", icon: ShieldCheck }, { to: "/market-data", label: "Market data", icon: Waves }, { to: "/indicators", label: "Indicators", icon: Gauge }, { to: "/matrix", label: "Strategy matrix", icon: BarChart3 }] },
  { label: "SYSTEM", items: [{ to: "/reconciliation", label: "Reconciliation", icon: GitCompareArrows }, { to: "/health", label: "System health", icon: CandlestickChart }, { to: "/alerts", label: "Alerts", icon: Bell }, { to: "/audit", label: "Audit log", icon: FileClock }, { to: "/settings", label: "Settings", icon: Settings2 }] },
];

export default function Sidebar({ connected, collapsed, onToggle, onNavigate, width }: { connected: boolean; collapsed: boolean; onToggle: () => void; onNavigate: () => void; width: number }) {
  const mode = useDataSelector<string | null>((s) => s.overview?.execution_mode ?? null);
  const health = useDataSelector<string>((s) => s.overallHealth);
  return <aside className={`app-sidebar ${collapsed ? "is-collapsed" : ""}`} style={{ width }}>
    <div className="brand-lockup"><div className="brand-mark">M</div>{!collapsed && <div><strong>MCX TERMINAL</strong><small>Live trading workspace</small></div>}</div>
    <div className={`sidebar-connection ${connected ? "connected" : "disconnected"}`}><i />{!collapsed && <span>Dashboard {connected ? "connected" : "disconnected"}</span>}</div>
    <nav className="sidebar-nav">{groups.map(group => <div className="nav-group" key={group.label}>{!collapsed && <div className="nav-group-label">{group.label}</div>}{group.items.map(item => <NavLink key={item.to} to={item.to} end={item.to === "/"} title={collapsed ? item.label : undefined} onClick={onNavigate} className={({ isActive }) => `nav-item ${isActive ? "active" : ""}`}><item.icon size={17} strokeWidth={1.8}/>{!collapsed && <span>{item.label}</span>}</NavLink>)}</div>)}</nav>
    {!collapsed && <div className="sidebar-mode"><span className="mode-label">RUNTIME</span><strong className={mode === "LIVE" ? "live-mode" : "paper-mode"}>{mode || "UNKNOWN"}</strong><span className={`health-label ${health === "healthy" ? "" : "health-warn"}`}><i/> {health || "unknown"}</span></div>}
    <button className="sidebar-toggle" onClick={onToggle} aria-label={collapsed ? "Expand navigation" : "Collapse navigation"}>{collapsed ? <ChevronRight size={16}/> : <ChevronLeft size={16}/>}</button>
  </aside>;
}

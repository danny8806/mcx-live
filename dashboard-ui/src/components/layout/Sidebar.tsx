import { NavLink } from "react-router-dom";
import { useDataSelector } from "../../store/DataProvider";
import {
  LayoutDashboard, Activity, Columns3, Briefcase,
  ShoppingCart, BookOpen, TrendingUp, Shield,
  Database, Gauge,   GitCompare, Bell, Heart,
  Settings, FileText, Radio, Zap, ChevronsLeft, ChevronsRight, ScrollText, RotateCcw,
} from "lucide-react";

const navItems = [
  { to: "/", icon: LayoutDashboard, label: "Overview" },
  { to: "/live", icon: Radio, label: "Live Trading" },
  { to: "/live-ops", icon: ScrollText, label: "LIVE Ops" },
  { to: "/strategies", icon: Activity, label: "Strategies" },
  { to: "/matrix", icon: Columns3, label: "Strategy Matrix" },
  { to: "/positions", icon: Briefcase, label: "Positions" },
  { to: "/orders", icon: ShoppingCart, label: "Orders" },
  { to: "/trades", icon: BookOpen, label: "Trades" },
  { to: "/pnl", icon: TrendingUp, label: "P&L Analytics" },
  { to: "/risk", icon: Shield, label: "Risk" },
  { to: "/market-data", icon: Database, label: "Market Data" },
  { to: "/indicators", icon: Gauge, label: "Indicators" },
  { to: "/reconciliation", icon: GitCompare, label: "Reconciliation" },
  { to: "/reversals", icon: RotateCcw, label: "Reversals" },
  { to: "/alerts", icon: Bell, label: "Alerts" },
  { to: "/health", icon: Heart, label: "System Health" },
  { to: "/settings", icon: Settings, label: "Settings" },
  { to: "/audit", icon: FileText, label: "Audit Log" },
];

interface SidebarProps {
  connected: boolean;
  collapsed: boolean;
  onToggle: () => void;
  width: number;
}

export default function Sidebar({ connected, collapsed, onToggle, width }: SidebarProps) {
  const executionMode = useDataSelector<string | null>((s) => s.overview?.execution_mode ?? null);
  const settings = useDataSelector<any>((s) => s.settings);
  const version = settings?.system?.version ?? "—";
  return (
    <aside style={{
      width,
      height: "100vh",
      background: "var(--bg-sidebar)",
      borderRight: "1px solid var(--border)",
      display: "flex",
      flexDirection: "column",
      position: "fixed",
      left: 0,
      top: 0,
      zIndex: 40,
      transition: "width 0.18s ease",
      overflow: "hidden",
    }}>
      <div style={{ padding: collapsed ? "12px 8px" : "14px 14px 10px", borderBottom: "1px solid var(--border-subtle)" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "8px", justifyContent: collapsed ? "center" : "flex-start" }}>
          {!collapsed && (
            <div style={{
              width: "28px", height: "28px", borderRadius: "6px",
              background: "rgba(242,184,75,0.12)", display: "flex",
              alignItems: "center", justifyContent: "center", flexShrink: 0,
            }}>
              <Zap style={{ width: "14px", height: "14px", color: "var(--amber)" }} />
            </div>
          )}
          {!collapsed && (
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: "12px", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "0.5px", whiteSpace: "nowrap" }}>
                MCX TRADER
              </div>
              <div style={{ fontSize: "9px", color: "var(--text-muted)", marginTop: "1px", whiteSpace: "nowrap" }}>
                Professional Trading Terminal
              </div>
            </div>
          )}
          {collapsed && (
            <Zap style={{ width: "16px", height: "16px", color: "var(--amber)", margin: "0 auto" }} />
          )}
        </div>
        {!collapsed && (
          <div style={{ display: "flex", alignItems: "center", gap: "6px", marginTop: "10px" }}>
            <span
              className={connected ? "animate-pulse-dot" : ""}
              style={{ width: "6px", height: "6px", borderRadius: "50%", background: connected ? "var(--green)" : "var(--red)", ["--dot" as any]: connected ? "var(--green)" : "var(--red)" }}
            />
            <span style={{ fontSize: "10px", color: "var(--text-muted)" }}>
              {connected ? "Connected" : "Disconnected"}
            </span>
          </div>
        )}
      </div>

      <nav style={{ flex: 1, overflowY: "auto", padding: "6px 0" }}>
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            title={collapsed ? item.label : undefined}
            style={({ isActive }) => ({
              display: "flex",
              alignItems: "center",
              justifyContent: collapsed ? "center" : "flex-start",
              gap: "10px",
              padding: collapsed ? "0" : "0 14px",
              height: "34px",
              fontSize: "11px",
              fontWeight: isActive ? 500 : 400,
              color: isActive ? "var(--blue)" : "var(--text-secondary)",
              background: isActive ? "var(--bg-panel-active)" : "transparent",
              textDecoration: "none",
              borderLeft: isActive && !collapsed ? "2px solid var(--blue)" : "2px solid transparent",
              transition: "background 0.12s ease, color 0.12s ease",
            })}
            className="sidebar-link"
          >
            <item.icon style={{ width: "15px", height: "15px", flexShrink: 0 }} />
            {!collapsed && <span style={{ whiteSpace: "nowrap" }}>{item.label}</span>}
          </NavLink>
        ))}
      </nav>

      <div style={{
        padding: "8px",
        borderTop: "1px solid var(--border-subtle)",
        display: "flex",
        alignItems: "center",
        justifyContent: collapsed ? "center" : "space-between",
        gap: "6px",
      }}>
        {!collapsed && (
          <div style={{ fontSize: "9px", color: "var(--text-disabled)", lineHeight: 1.4 }}>
            <div style={{ fontWeight: 500 }}>MCX Trader</div>
            <div>v{version} • {executionMode ?? "—"} Mode</div>
          </div>
        )}
        <button
          onClick={onToggle}
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          title={collapsed ? "Expand" : "Collapse"}
          style={{
            background: "var(--bg-panel-hover)", border: "1px solid var(--border)",
            borderRadius: "4px", color: "var(--text-muted)", cursor: "pointer",
            width: "24px", height: "24px", display: "flex", alignItems: "center",
            justifyContent: "center", flexShrink: 0, padding: 0,
          }}
        >
          {collapsed ? <ChevronsRight style={{ width: "14px", height: "14px" }} /> : <ChevronsLeft style={{ width: "14px", height: "14px" }} />}
        </button>
      </div>
    </aside>
  );
}
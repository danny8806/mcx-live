import { useState } from "react";
import { Outlet } from "react-router-dom";
import Sidebar from "./Sidebar";
import TopBar from "./TopBar";
import { useDataSelector } from "../../store/DataProvider";

const COLLAPSED_W = 52;
const EXPANDED_W = 190;

export default function Layout() {
  const connected = useDataSelector<boolean>((s) => s.connected);
  const [collapsed, setCollapsed] = useState(false);
  const w = collapsed ? COLLAPSED_W : EXPANDED_W;

  return (
    <div style={{ display: "flex", height: "100vh", overflow: "hidden" }}>
      <Sidebar connected={connected} collapsed={collapsed} onToggle={() => setCollapsed(c => !c)} width={w} />
      <div style={{ marginLeft: w, flex: 1, display: "flex", flexDirection: "column", minWidth: 0, transition: "margin-left 0.18s ease" }}>
        <TopBar onToggleSidebar={() => setCollapsed(c => !c)} />
        <main style={{ flex: 1, overflow: "auto", padding: "12px" }}>
          <Outlet />
        </main>
      </div>
    </div>
  );
}
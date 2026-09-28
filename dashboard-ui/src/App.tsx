import { BrowserRouter, Routes, Route } from "react-router-dom";
import { DataProvider } from "./store/DataProvider";
import Layout from "./components/layout/Layout";
import Overview from "./pages/Overview";
import LiveTrading from "./pages/LiveTrading";
import LiveOps from "./pages/LiveOps";
import Strategies from "./pages/Strategies";
import StrategyMatrix from "./pages/StrategyMatrix";
import Positions from "./pages/Positions";
import Orders from "./pages/Orders";
import Trades from "./pages/Trades";
import Pnl from "./pages/Pnl";
import Risk from "./pages/Risk";
import MarketData from "./pages/MarketData";
import Indicators from "./pages/Indicators";
import Reconciliation from "./pages/Reconciliation";
import Reversals from "./pages/Reversals";
import Alerts from "./pages/Alerts";
import Health from "./pages/Health";
import Settings from "./pages/Settings";
import AuditLog from "./pages/AuditLog";

export default function App() {
  return (
    <DataProvider>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<Overview />} />
            <Route path="/live" element={<LiveTrading />} />
            <Route path="/live-ops" element={<LiveOps />} />
            <Route path="/strategies" element={<Strategies />} />
            <Route path="/matrix" element={<StrategyMatrix />} />
            <Route path="/positions" element={<Positions />} />
            <Route path="/orders" element={<Orders />} />
            <Route path="/trades" element={<Trades />} />
            <Route path="/pnl" element={<Pnl />} />
            <Route path="/risk" element={<Risk />} />
            <Route path="/market-data" element={<MarketData />} />
            <Route path="/indicators" element={<Indicators />} />
            <Route path="/reconciliation" element={<Reconciliation />} />
            <Route path="/reversals" element={<Reversals />} />
            <Route path="/alerts" element={<Alerts />} />
            <Route path="/health" element={<Health />} />
            <Route path="/settings" element={<Settings />} />
            <Route path="/audit" element={<AuditLog />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </DataProvider>
  );
}

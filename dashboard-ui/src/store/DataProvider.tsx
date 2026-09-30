import {
  createContext, useContext, useEffect, useRef, useState, useCallback,
  useSyncExternalStore, useMemo, type ReactNode,
} from "react";
import { api } from "../lib/api";
import { connectDashboardSocket } from "../lib/realtime";

export type DataState = "loading" | "live" | "stale" | "empty" | "error";
type SnapshotKey = "overview" | "strategies" | "positions" | "orders" | "fills" | "trades" | "marketData";
type SnapshotState = "loading" | "live" | "error";

interface DataContextType {
  connected: boolean;
  snapshotStatus: Record<SnapshotKey, SnapshotState>;
  overview: any;
  goldOverview: any;
  silverOverview: any;
  strategies: any[];
  positions: any[];
  orders: any[];
  fills: any[];
  trades: any[];
  pnl: any;
  pnlByInstrument: Record<string, any>;
  risk: any;
  indicators: Record<string, any>;
  htf: Record<string, any>;
  healthComponents: any[];
  overallHealth: string;
  reconciliation: any;
  brokerPnl: any;
  settings: any;
  audit: any[];
  alerts: any[];
  equityCurve: any[];
  marketData: any;
  wsEvents: any[];
  wsState: any;
  refresh: (key?: string) => void;
  lastError: string | null;
}

/**
 * Minimally invasive re-render isolation.  Components that want to re-render
 * only when a specific slice changes can subscribe via `useDataSelector`.  The
 * store mirrors the provider's context value; `useDataSelector` compares the
 * *selected* value with `Object.is` so subscribers only re-render when their
 * slice actually changes (not on every WS push / poll).  `useData()` remains
 * unchanged for existing consumers.
 */
type Selector<T> = (state: DataContextType) => T;

/** Defaults mirroring the provider's initial state, so selectors never read
 *  an `undefined` slice on the very first render (before the sync effect runs). */
const DEFAULT_STATE: DataContextType = {
  connected: false,
  snapshotStatus: { overview: "loading", strategies: "loading", positions: "loading", orders: "loading", fills: "loading", trades: "loading", marketData: "loading" },
  overview: null, goldOverview: null, silverOverview: null,
  strategies: [], positions: [], orders: [], fills: [], trades: [],
  pnl: null, pnlByInstrument: {},
  risk: null, indicators: {}, htf: {},
  healthComponents: [], overallHealth: "unknown",
  reconciliation: null, brokerPnl: null, settings: null, audit: [], alerts: [],
  equityCurve: [], marketData: null, wsEvents: [], wsState: null,
  refresh: () => {}, lastError: null,
};

const selectorStore: {
  state: DataContextType;
  listeners: Set<() => void>;
} = {
  state: DEFAULT_STATE,
  listeners: new Set<() => void>(),
};

function useDataSelector<T>(selector: Selector<T>): T {
  const subscribe = useCallback((listener: () => void) => {
    selectorStore.listeners.add(listener);
    return () => { selectorStore.listeners.delete(listener); };
  }, []);

  const getSnapshot = useCallback((): T => selector(selectorStore.state), [selector]);

  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}

const DataContext = createContext<DataContextType>({} as DataContextType);
// eslint-disable-next-line react-refresh/only-export-components
export { useDataSelector };

function sv<T>(setter: (v: T) => void, mounted: React.MutableRefObject<boolean>) {
  return (data: T) => { if (mounted.current) setter(data); };
}

function extractVal(obj: any, key: string): any {
  if (!obj || typeof obj !== "object") return undefined;
  const v = obj[key];
  if (v && typeof v === "object" && "value" in v) return v.value;
  return v;
}

export function DataProvider({ children }: { children: ReactNode }) {
  const [connected, setConnected] = useState(false);
  const [snapshotStatus, setSnapshotStatus] = useState<Record<SnapshotKey, SnapshotState>>(DEFAULT_STATE.snapshotStatus);
  const [overview, setOverview] = useState<any>(null);
  const [goldOverview, setGoldOverview] = useState<any>(null);
  const [silverOverview, setSilverOverview] = useState<any>(null);
  const [strategies, setStrategies] = useState<any[]>([]);
  const [positions, setPositions] = useState<any[]>([]);
  const [orders, setOrders] = useState<any[]>([]);
  const [fills, setFills] = useState<any[]>([]);
  const [trades, setTrades] = useState<any[]>([]);
  const [pnl, setPnl] = useState<any>(null);
  const [pnlByInstrument, setPnlByInstrument] = useState<Record<string, any>>({});
  const [risk, setRisk] = useState<any>(null);
  const [indicators, setIndicators] = useState<Record<string, any>>({});
  const [htf, setHtf] = useState<Record<string, any>>({});
  const [healthComponents, setHealthComponents] = useState<any[]>([]);
  const [overallHealth, setOverallHealth] = useState("unknown");
  const [reconciliation, setReconciliation] = useState<any>(null);
  const [brokerPnl, setBrokerPnl] = useState<any>(null);
  const [settings, setSettings] = useState<any>(null);
  const [audit, setAudit] = useState<any[]>([]);
  const [alerts, setAlerts] = useState<any[]>([]);
  const [equityCurve, setEquityCurve] = useState<any[]>([]);
  const [marketData, setMarketData] = useState<any>(null);
  const [wsEvents, setWsEvents] = useState<any[]>([]);
  const [wsState, setWsState] = useState<any>(null);
  const [lastError, setLastError] = useState<string | null>(null);

  const timersRef = useRef<Record<string, number>>({});
  const mountedRef = useRef(true);
  const wsActiveRef = useRef(false);
  const wsEngineAtRef = useRef(0);

  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; };
  }, []);

  const safe = useCallback(<T,>(setter: (v: T) => void) => sv(setter, mountedRef), []);

  const fetchOverview = useCallback(async () => {
    try {
      const d = await api.overview() as any;
      if (!d || d.error) throw new Error(d?.error || "No overview payload");
      safe(setOverview)({
        execution_mode: extractVal(d, "execution_mode") ?? null,
        total_equity: extractVal(d, "total_equity") ?? 0,
        starting_capital: extractVal(d, "starting_capital") ?? 0,
        book_starting_capital: extractVal(d, "book_starting_capital") ?? 0,
        today_pnl: extractVal(d, "today_pnl") ?? 0,
        total_net_pnl: extractVal(d, "total_net_pnl") ?? 0,
        realized_pnl: extractVal(d, "realized_pnl") ?? 0,
        unrealized_pnl: extractVal(d, "unrealized_pnl") ?? 0,
        margin_used: extractVal(d, "margin_used") ?? 0,
        available_margin: extractVal(d, "available_margin") ?? 0,
        open_positions_count: extractVal(d, "open_positions_count") ?? 0,
        active_orders_count: extractVal(d, "active_orders_count") ?? 0,
        active_strategies_count: extractVal(d, "active_strategies_count") ?? 0,
        kill_switch: extractVal(d, "kill_switch") ?? false,
      });
      setSnapshotStatus((previous) => ({ ...previous, overview: "live" }));
    } catch (e: any) {
      setSnapshotStatus((previous) => ({ ...previous, overview: "error" }));
      setLastError(`overview: ${e?.message || e}`);
      // Only set defaults when WS is NOT active — avoid overwriting
      // live WS data with zeros on a transient REST failure.
      // Keep the last known snapshot (or null on first load). A failed request
      // must never be rendered as a valid all-zero account state.
    }
  }, [safe]);

  const fetchGoldOverview = useCallback(async () => {
    try { safe(setGoldOverview)(await api.overviewInstrument("GOLDM")); } catch (e: any) { setLastError(`gold: ${e?.message || e}`); }
  }, [safe]);

  const fetchSilverOverview = useCallback(async () => {
    try { safe(setSilverOverview)(await api.overviewInstrument("SILVERM")); } catch (e: any) { setLastError(`silver: ${e?.message || e}`); }
  }, [safe]);

  const fetchStrategies = useCallback(async () => {
    try {
      const d = await api.strategies() as any;
      if (!Array.isArray(d?.strategies)) throw new Error(d?.error || "Invalid strategy payload");
      setSnapshotStatus((previous) => ({ ...previous, strategies: "live" }));
      if (wsActiveRef.current && Date.now() - wsEngineAtRef.current < 10_000) return;
      safe(setStrategies)(d.strategies);
    } catch (e: any) {
      if (!(wsActiveRef.current && Date.now() - wsEngineAtRef.current < 10_000))
        setSnapshotStatus((previous) => ({ ...previous, strategies: "error" }));
      setLastError(`strategies: ${e?.message || e}`);
    }
  }, [safe]);

  const fetchPositions = useCallback(async () => {
    try {
      const d = await api.positions() as any;
      if (!Array.isArray(d?.positions)) throw new Error(d?.error || "Invalid position payload");
      setSnapshotStatus((previous) => ({ ...previous, positions: "live" }));
      if (wsActiveRef.current && Date.now() - wsEngineAtRef.current < 10_000) return;
      safe(setPositions)(d.positions);
    } catch (e: any) {
      if (!(wsActiveRef.current && Date.now() - wsEngineAtRef.current < 10_000))
        setSnapshotStatus((previous) => ({ ...previous, positions: "error" }));
      setLastError(`positions: ${e?.message || e}`);
    }
  }, [safe]);

  const fetchOrders = useCallback(async () => {
    try {
      const d = await api.orders() as any;
      if (!Array.isArray(d?.orders)) throw new Error(d?.error || "Invalid order payload");
      safe(setOrders)(d.orders);
      setSnapshotStatus((previous) => ({ ...previous, orders: "live" }));
    } catch (e: any) { setSnapshotStatus((previous) => ({ ...previous, orders: "error" })); setLastError(`orders: ${e?.message || e}`); }
  }, [safe]);

  const fetchFills = useCallback(async () => {
    try {
      const d = await api.fills() as any;
      if (!Array.isArray(d?.fills)) throw new Error(d?.error || "Invalid fill payload");
      safe(setFills)(d.fills);
      setSnapshotStatus((previous) => ({ ...previous, fills: "live" }));
    } catch (e: any) { setSnapshotStatus((previous) => ({ ...previous, fills: "error" })); setLastError(`fills: ${e?.message || e}`); }
  }, [safe]);

  const fetchTrades = useCallback(async () => {
    try {
      const d = await api.trades() as any;
      if (!Array.isArray(d?.trades)) throw new Error(d?.error || "Invalid trade payload");
      safe(setTrades)(d.trades);
      setSnapshotStatus((previous) => ({ ...previous, trades: "live" }));
    } catch (e: any) {
      setSnapshotStatus((previous) => ({ ...previous, trades: "error" }));
      setLastError(`trades: ${e?.message || e}`);
    }
  }, [safe]);

  const fetchPnl = useCallback(async () => {
    try {
      const d = await api.pnl() as any;
      safe(setPnl)(d?.portfolio ? { ...d.portfolio, execution_mode: d.execution_mode ?? null } : null);
      safe(setPnlByInstrument)(d?.by_instrument ?? {});
    } catch (e: any) { setLastError(`pnl: ${e?.message || e}`); }
  }, [safe]);

  const fetchRisk = useCallback(async () => {
    try { safe(setRisk)(await api.risk()); } catch (e: any) { setLastError(`risk: ${e?.message || e}`); }
  }, [safe]);

  const fetchIndicators = useCallback(async () => {
    try {
      const d = await api.indicators() as any;
      safe(setIndicators)(d?.indicators ?? d ?? {});
    } catch (e: any) { setLastError(`indicators: ${e?.message || e}`); }
  }, [safe]);

  const fetchHtf = useCallback(async () => {
    try {
      const d = await api.htf() as any;
      safe(setHtf)(d?.htf ?? d ?? {});
    } catch (e: any) { setLastError(`htf: ${e?.message || e}`); }
  }, [safe]);

  const fetchHealth = useCallback(async () => {
    try {
      const d = await api.healthSystem() as any;
      safe(setHealthComponents)(d?.components ?? []);
      safe(setOverallHealth)(d?.overall ?? "unknown");
    } catch (e: any) { setLastError(`health: ${e?.message || e}`); }
  }, [safe]);

  const fetchReconciliation = useCallback(async () => {
    try {
      const result = await api.reconciliation() as any;
      if (!result || result.error) throw new Error(result?.error || "Invalid reconciliation payload");
      safe(setReconciliation)({ ...result, _fetched_at: Date.now() / 1000, _fetch_error: null });
    } catch (e: any) {
      const message = e?.message || String(e);
      if (mountedRef.current) setReconciliation((previous: any) => ({ ...previous, _fetch_error: message }));
      setLastError(`reconciliation: ${message}`);
    }
  }, [safe]);

  const fetchBrokerPnl = useCallback(async () => {
    try {
      const result = await api.livePnl() as any;
      if (!result || result.error) throw new Error(result?.error || "Invalid broker P&L payload");
      safe(setBrokerPnl)({ ...result, _fetched_at: Date.now() / 1000, _fetch_error: null });
    } catch (e: any) {
      const message = e?.message || String(e);
      if (mountedRef.current) setBrokerPnl((previous: any) => ({ ...previous, _fetch_error: message }));
      setLastError(`broker P&L: ${message}`);
    }
  }, [safe]);

  const fetchSettings = useCallback(async () => {
    try { safe(setSettings)(await api.settings()); } catch (e: any) { setLastError(`settings: ${e?.message || e}`); }
  }, [safe]);

  const fetchAudit = useCallback(async () => {
    try {
      const d = await api.audit() as any;
      safe(setAudit)(d?.entries ?? []);
    } catch (e: any) { setLastError(`audit: ${e?.message || e}`); }
  }, [safe]);

  const fetchAlerts = useCallback(async () => {
    try {
      const d = await api.alerts() as any;
      safe(setAlerts)(d?.alerts ?? []);
    } catch (e: any) { setLastError(`alerts: ${e?.message || e}`); }
  }, [safe]);

  const fetchEquityCurve = useCallback(async () => {
    try {
      const d = await api.equityCurve() as any;
      const pts = (d?.equity_curve ?? []).map((r: any) => {
        const ts = typeof r.timestamp === "number" ? r.timestamp : ((Date.parse(r.timestamp) / 1000) || 0);
        return { timestamp: ts, equity: Number(r.equity ?? 0) };
      }).sort((a: any, b: any) => a.timestamp - b.timestamp);
      safe(setEquityCurve)(pts);
    } catch (e: any) { setLastError(`equity: ${e?.message || e}`); }
  }, [safe]);

  const fetchMarketData = useCallback(async () => {
    try {
      const data = await api.marketData() as any;
      if (!data || data.error) throw new Error(data?.error || "Invalid market data payload");
      safe(setMarketData)(data);
      setSnapshotStatus((previous) => ({ ...previous, marketData: "live" }));
    } catch (e: any) { setSnapshotStatus((previous) => ({ ...previous, marketData: "error" })); setLastError(`market: ${e?.message || e}`); }
  }, [safe]);

  const refresh = useCallback((key?: string) => {
    const map: Record<string, () => void> = {
      overview: fetchOverview, goldOverview: fetchGoldOverview, silverOverview: fetchSilverOverview,
      strategies: fetchStrategies, positions: fetchPositions, orders: fetchOrders,
      fills: fetchFills, trades: fetchTrades, pnl: fetchPnl, risk: fetchRisk, indicators: fetchIndicators,
      htf: fetchHtf, health: fetchHealth, reconciliation: fetchReconciliation,
      brokerPnl: fetchBrokerPnl,
      settings: fetchSettings, audit: fetchAudit, alerts: fetchAlerts,
      equityCurve: fetchEquityCurve, marketData: fetchMarketData,
    };
    if (key && map[key]) map[key]();
    else Object.values(map).forEach(fn => fn());
  }, [fetchOverview, fetchGoldOverview, fetchSilverOverview, fetchStrategies, fetchPositions, fetchOrders, fetchFills, fetchTrades, fetchPnl, fetchRisk, fetchIndicators, fetchHtf, fetchHealth, fetchReconciliation, fetchBrokerPnl, fetchSettings, fetchAudit, fetchAlerts, fetchEquityCurve, fetchMarketData]);

  useEffect(() => {
    fetchOverview(); fetchGoldOverview(); fetchSilverOverview();
    fetchStrategies(); fetchPositions(); fetchOrders(); fetchFills();
    fetchTrades();
    fetchPnl(); fetchRisk(); fetchIndicators(); fetchHtf();
    fetchHealth(); fetchReconciliation(); fetchBrokerPnl(); fetchSettings();
    fetchAudit(); fetchAlerts(); fetchEquityCurve(); fetchMarketData();
  }, []);

  useEffect(() => {
    timersRef.current = {
      overview: window.setInterval(fetchOverview, 3000),
      gold: window.setInterval(fetchGoldOverview, 3000),
      silver: window.setInterval(fetchSilverOverview, 3000),
      strategies: window.setInterval(fetchStrategies, 3000),
      positions: window.setInterval(fetchPositions, 2000),
      orders: window.setInterval(fetchOrders, 3000),
      fills: window.setInterval(fetchFills, 3000),
      trades: window.setInterval(fetchTrades, 5000),
      pnl: window.setInterval(fetchPnl, 5000),
      risk: window.setInterval(fetchRisk, 3000),
      indicators: window.setInterval(fetchIndicators, 5000),
      htf: window.setInterval(fetchHtf, 5000),
      health: window.setInterval(fetchHealth, 10000),
      reconciliation: window.setInterval(fetchReconciliation, 15000),
      brokerPnl: window.setInterval(fetchBrokerPnl, 15000),
      audit: window.setInterval(fetchAudit, 10000),
      alerts: window.setInterval(fetchAlerts, 5000),
      equityCurve: window.setInterval(fetchEquityCurve, 10000),
      marketData: window.setInterval(fetchMarketData, 2000),
    };
    return () => { Object.values(timersRef.current).forEach(clearInterval); timersRef.current = {}; };
  }, [fetchOverview, fetchGoldOverview, fetchSilverOverview, fetchStrategies, fetchPositions, fetchOrders, fetchFills, fetchTrades, fetchPnl, fetchRisk, fetchIndicators, fetchHtf, fetchHealth, fetchReconciliation, fetchBrokerPnl, fetchAudit, fetchAlerts, fetchEquityCurve, fetchMarketData]);

  useEffect(() => {
    return connectDashboardSocket((msg) => {
        try {
          if (msg.type === "parse_error") {
            setLastError(String(msg.data));
            return;
          }
          if (msg.type === "engine_state") {
            const s = msg.data as any;
            safe(setWsState)(s);
            if (s?.account) {
              wsActiveRef.current = true;
              wsEngineAtRef.current = Date.now();
            }
            if (s?.risk && (s.risk.daily_pnl !== undefined || s.risk.kill_switch_active !== undefined)) {
            }
            if (s?.strategies) {
              const list = Object.entries(s.strategies).map(([name, snap]: [string, any]) => ({
                strategy_id: snap.strategy_id ?? name,
                instrument: snap.instrument ?? "",
                fast_timeframe: snap.fast_timeframe ?? "",
                htf_timeframe: snap.htf_timeframe ?? "",
                quantity: snap.quantity ?? null,
                enabled: snap.enabled ?? true,
                state: snap.state ?? "unknown",
                position_side: snap.position_side,
                stop_price: snap.stop_price,
                pending_entry: snap.pending_entry,
                pending_exit_trigger: snap.pending_exit_trigger,
                bars_processed: snap.bars_processed ?? 0,
                trade_count: snap.trade_count ?? 0,
                wins: snap.wins ?? 0,
                losses: snap.losses ?? 0,
                win_rate: snap.win_rate ?? 0,
                realized_net: snap.realized_net ?? 0,
                realized_gross: snap.realized_gross ?? 0,
                // ### OBSERVATION (realized_charges)
                // The WS strategy projection dropped this field while the REST
                // /api/strategies route carries it, so the strategy cards lost
                // the charges figure as soon as the WS connected.
                realized_charges: snap.realized_charges ?? 0,
              }));
              safe(setStrategies)(list);
              setSnapshotStatus((previous) => ({ ...previous, strategies: "live" }));
            }
            if (s?.positions) {
              const openPos = s.positions?.open_positions ?? {};
              safe(setPositions)(Object.values(openPos) as any[]);
              setSnapshotStatus((previous) => ({ ...previous, positions: "live" }));
            }
          }
          if (msg.type === "events") {
            const events = (msg.data ?? []) as any[];
            safe(setWsEvents)((prev: any[]) => [...events, ...prev].slice(0, 200));
          }
        } catch (e: any) { setLastError(`ws: ${e?.message || e}`); }
      }, (isConnected) => {
        if (!isConnected) { wsActiveRef.current = false; wsEngineAtRef.current = 0; }
        if (mountedRef.current) setConnected(isConnected);
      });
  }, [safe]);

  const contextValue = useMemo<DataContextType>(() => ({
    connected, snapshotStatus, overview, goldOverview, silverOverview, strategies, positions,
    orders, fills, trades, pnl, pnlByInstrument, risk, indicators, htf,
    healthComponents, overallHealth, reconciliation, brokerPnl, settings, audit,
    alerts, equityCurve, marketData, wsEvents, wsState, refresh, lastError,
  }), [
    connected, snapshotStatus, overview, goldOverview, silverOverview, strategies, positions,
    orders, fills, trades, pnl, pnlByInstrument, risk, indicators, htf,
    healthComponents, overallHealth, reconciliation, brokerPnl, settings, audit,
    alerts, equityCurve, marketData, wsEvents, wsState, refresh, lastError,
  ]);

  // Keep the selector store in sync after each commit so useDataSelector
  // subscribers get notified when their slice changes.
  useEffect(() => {
    selectorStore.state = contextValue;
    selectorStore.listeners.forEach(l => l());
  }, [contextValue]);

  return (
    <DataContext.Provider value={contextValue}>
      {children}
    </DataContext.Provider>
  );
}

export function useData() {
  return useContext(DataContext);
}

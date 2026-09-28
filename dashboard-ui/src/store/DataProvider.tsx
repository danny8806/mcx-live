import {
  createContext, useContext, useEffect, useRef, useState, useCallback,
  useSyncExternalStore, useMemo, type ReactNode,
} from "react";
import { api } from "../lib/api";
import { connectDashboardSocket } from "../lib/realtime";

export type DataState = "loading" | "live" | "stale" | "empty" | "error";

interface DataContextType {
  connected: boolean;
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
  overview: null, goldOverview: null, silverOverview: null,
  strategies: [], positions: [], orders: [], fills: [], trades: [],
  pnl: null, pnlByInstrument: {},
  risk: null, indicators: {}, htf: {},
  healthComponents: [], overallHealth: "unknown",
  reconciliation: null, settings: null, audit: [], alerts: [],
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

  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; };
  }, []);

  const safe = useCallback(<T,>(setter: (v: T) => void) => sv(setter, mountedRef), []);

  const fetchOverview = useCallback(async () => {
    try {
      const d = await api.overview() as any;
      if (wsActiveRef.current) return;
      safe(setOverview)({
        execution_mode: extractVal(d, "execution_mode") ?? null,
        total_equity: extractVal(d, "total_equity") ?? 0,
        starting_capital: extractVal(d, "starting_capital") ?? 0,
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
    } catch (e: any) {
      setLastError(`overview: ${e?.message || e}`);
      // Only set defaults when WS is NOT active — avoid overwriting
      // live WS data with zeros on a transient REST failure.
      if (!wsActiveRef.current) {
        safe(setOverview)({
          execution_mode: null, total_equity: 0, starting_capital: 0,
          today_pnl: 0, total_net_pnl: 0, realized_pnl: 0,
          unrealized_pnl: 0, margin_used: 0, available_margin: 0,
          open_positions_count: 0, active_orders_count: 0,
          active_strategies_count: 0, kill_switch: false,
        });
      }
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
      if (wsActiveRef.current) return;
      safe(setStrategies)(Array.isArray(d?.strategies) ? d.strategies : []);
    } catch (e: any) { setLastError(`strategies: ${e?.message || e}`); }
  }, [safe]);

  const fetchPositions = useCallback(async () => {
    try {
      const d = await api.positions() as any;
      if (wsActiveRef.current) return;
      safe(setPositions)(Array.isArray(d?.positions) ? d.positions : []);
    } catch (e: any) { setLastError(`positions: ${e?.message || e}`); }
  }, [safe]);

  const fetchOrders = useCallback(async () => {
    try {
      const d = await api.orders() as any;
      safe(setOrders)(Array.isArray(d?.orders) ? d.orders : []);
    } catch (e: any) { setLastError(`orders: ${e?.message || e}`); }
  }, [safe]);

  const fetchFills = useCallback(async () => {
    try {
      const d = await api.fills() as any;
      safe(setFills)(Array.isArray(d?.fills) ? d.fills : []);
    } catch (e: any) { setLastError(`fills: ${e?.message || e}`); }
  }, [safe]);

  const fetchTrades = useCallback(async () => {
    try {
      const d = await api.trades() as any;
      safe(setTrades)(Array.isArray(d?.trades) ? d.trades : []);
    } catch (e: any) { setLastError(`trades: ${e?.message || e}`); }
  }, [safe]);

  const fetchPnl = useCallback(async () => {
    try {
      const d = await api.pnl() as any;
      safe(setPnl)(d?.portfolio ?? null);
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
    try { safe(setReconciliation)(await api.reconciliation()); } catch (e: any) { setLastError(`reconciliation: ${e?.message || e}`); }
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
    try { safe(setMarketData)(await api.marketData()); } catch (e: any) { setLastError(`market: ${e?.message || e}`); }
  }, [safe]);

  const refresh = useCallback((key?: string) => {
    const map: Record<string, () => void> = {
      overview: fetchOverview, goldOverview: fetchGoldOverview, silverOverview: fetchSilverOverview,
      strategies: fetchStrategies, positions: fetchPositions, orders: fetchOrders,
      fills: fetchFills, trades: fetchTrades, pnl: fetchPnl, risk: fetchRisk, indicators: fetchIndicators,
      htf: fetchHtf, health: fetchHealth, reconciliation: fetchReconciliation,
      settings: fetchSettings, audit: fetchAudit, alerts: fetchAlerts,
      equityCurve: fetchEquityCurve, marketData: fetchMarketData,
    };
    if (key && map[key]) map[key]();
    else Object.values(map).forEach(fn => fn());
  }, [fetchOverview, fetchGoldOverview, fetchSilverOverview, fetchStrategies, fetchPositions, fetchOrders, fetchFills, fetchTrades, fetchPnl, fetchRisk, fetchIndicators, fetchHtf, fetchHealth, fetchReconciliation, fetchSettings, fetchAudit, fetchAlerts, fetchEquityCurve, fetchMarketData]);

  useEffect(() => {
    fetchOverview(); fetchGoldOverview(); fetchSilverOverview();
    fetchStrategies(); fetchPositions(); fetchOrders(); fetchFills();
    fetchTrades();
    fetchPnl(); fetchRisk(); fetchIndicators(); fetchHtf();
    fetchHealth(); fetchReconciliation(); fetchSettings();
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
      audit: window.setInterval(fetchAudit, 10000),
      alerts: window.setInterval(fetchAlerts, 5000),
      equityCurve: window.setInterval(fetchEquityCurve, 10000),
      marketData: window.setInterval(fetchMarketData, 2000),
    };
    return () => { Object.values(timersRef.current).forEach(clearInterval); timersRef.current = {}; };
  }, [fetchOverview, fetchGoldOverview, fetchSilverOverview, fetchStrategies, fetchPositions, fetchOrders, fetchFills, fetchTrades, fetchPnl, fetchRisk, fetchIndicators, fetchHtf, fetchHealth, fetchAudit, fetchAlerts, fetchEquityCurve, fetchMarketData]);

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
              setOverview((prev: any) => {
                const base = prev ?? {
                  execution_mode: null, total_equity: 0, starting_capital: 0,
                  today_pnl: 0, total_net_pnl: 0, realized_pnl: 0,
                  unrealized_pnl: 0, margin_used: 0, available_margin: 0,
                  open_positions_count: 0, active_orders_count: 0,
                  active_strategies_count: 0, kill_switch: false,
                };
                return {
                  ...base,
                  execution_mode: s.execution_mode ?? base.execution_mode,
                  total_equity: s.account.equity ?? base.total_equity,
                  starting_capital: s.account.starting_capital ?? base.starting_capital,
                  realized_pnl: s.account.realized_pnl ?? base.realized_pnl,
                  unrealized_pnl: s.account.unrealized_pnl ?? base.unrealized_pnl,
                  margin_used: s.account.used_margin ?? base.margin_used,
                  available_margin: s.account.available_margin ?? base.available_margin,
                  total_net_pnl: s.account.net_pnl != null
                    ? s.account.net_pnl
                    : ((s.account.realized_pnl ?? 0) + (s.account.unrealized_pnl ?? 0)),
                };
              });
            }
            if (s?.risk && (s.risk.daily_pnl !== undefined || s.risk.kill_switch_active !== undefined)) {
              setOverview((prev: any) => {
                const base = prev ?? {
                  execution_mode: null, total_equity: 0, starting_capital: 0,
                  today_pnl: 0, total_net_pnl: 0, realized_pnl: 0,
                  unrealized_pnl: 0, margin_used: 0, available_margin: 0,
                  open_positions_count: 0, active_orders_count: 0,
                  active_strategies_count: 0, kill_switch: false,
                };
                return {
                  ...base,
                  today_pnl: s.risk.daily_pnl ?? base.today_pnl,
                  kill_switch: s.risk.kill_switch_active ?? base.kill_switch,
                };
              });
            }
            if (s?.positions?.open_positions) {
              setOverview((prev: any) => {
                const base = prev ?? {
                  execution_mode: null, total_equity: 0, starting_capital: 0,
                  today_pnl: 0, total_net_pnl: 0, realized_pnl: 0,
                  unrealized_pnl: 0, margin_used: 0, available_margin: 0,
                  open_positions_count: 0, active_orders_count: 0,
                  active_strategies_count: 0, kill_switch: false,
                };
                return {
                  ...base,
                  open_positions_count: Object.keys(s.positions.open_positions).length,
                };
              });
            }
            if (s?.strategies) {
              setOverview((prev: any) => {
                const base = prev ?? {
                  execution_mode: null, total_equity: 0, starting_capital: 0,
                  today_pnl: 0, total_net_pnl: 0, realized_pnl: 0,
                  unrealized_pnl: 0, margin_used: 0, available_margin: 0,
                  open_positions_count: 0, active_orders_count: 0,
                  active_strategies_count: 0, kill_switch: false,
                };
                return {
                  ...base,
                  active_strategies_count: Object.keys(s.strategies).length,
                };
              });
            }
            if (s?.strategies) {
              const list = Object.entries(s.strategies).map(([name, snap]: [string, any]) => ({
                strategy_id: snap.strategy_id ?? name,
                instrument: snap.instrument ?? "",
                fast_timeframe: snap.fast_timeframe ?? "",
                htf_timeframe: snap.htf_timeframe ?? "",
                quantity: snap.quantity ?? 1,
                enabled: snap.enabled ?? true,
                state: snap.state ?? "unknown",
                position_side: snap.position_side,
                stop_price: snap.stop_price,
                pending_entry: snap.pending_entry,
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
            }
            if (s?.positions) {
              const openPos = s.positions?.open_positions ?? {};
              safe(setPositions)(Object.values(openPos) as any[]);
            }
          }
          if (msg.type === "events") {
            const events = (msg.data ?? []) as any[];
            safe(setWsEvents)((prev: any[]) => [...events, ...prev].slice(0, 200));
          }
        } catch (e: any) { setLastError(`ws: ${e?.message || e}`); }
      }, (isConnected) => {
        if (!isConnected) wsActiveRef.current = false;
        if (mountedRef.current) setConnected(isConnected);
      });
  }, [safe]);

  const contextValue = useMemo<DataContextType>(() => ({
    connected, overview, goldOverview, silverOverview, strategies, positions,
    orders, fills, trades, pnl, pnlByInstrument, risk, indicators, htf,
    healthComponents, overallHealth, reconciliation, settings, audit,
    alerts, equityCurve, marketData, wsEvents, wsState, refresh, lastError,
  }), [
    connected, overview, goldOverview, silverOverview, strategies, positions,
    orders, fills, trades, pnl, pnlByInstrument, risk, indicators, htf,
    healthComponents, overallHealth, reconciliation, settings, audit,
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

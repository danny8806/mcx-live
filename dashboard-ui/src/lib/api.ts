declare global {
  interface Window {
    APP_API_BASE?: string;
    APP_WS_BASE?: string;
  }
}

const API_BASE = (typeof window !== "undefined" && window.APP_API_BASE || "").replace(/\/$/, "");

type Query = Record<string, string | number | boolean | null | undefined>;

export class ApiError extends Error {
  readonly status: number;
  readonly path: string;

  constructor(
    message: string,
    status: number,
    path: string,
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.path = path;
  }
}

function withQuery(path: string, query?: Query): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query ?? {})) {
    if (value !== undefined && value !== null && value !== "") {
      params.set(key, String(value));
    }
  }
  const suffix = params.toString();
  return suffix ? `${path}?${suffix}` : path;
}

function item(path: string, id: string): string {
  return `${path}/${encodeURIComponent(id)}`;
}

async function requestJSON<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, init);
  const contentType = res.headers.get("content-type") ?? "";
  const payload: unknown = contentType.includes("json")
    ? await res.json().catch(() => null)
    : await res.text().catch(() => "");
  if (!res.ok) {
    const detail = payload && typeof payload === "object" && "detail" in payload
      ? String((payload as { detail: unknown }).detail)
      : typeof payload === "string" && payload.trim()
        ? payload.trim()
        : `HTTP ${res.status}`;
    throw new ApiError(detail, res.status, path);
  }
  return payload as T;
}

const fetchJSON = requestJSON;

async function postJSON<T>(path: string, body?: unknown): Promise<T> {
  return requestJSON<T>(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

export const api = {
  health: () => fetchJSON<any>("/api/health"),
  overview: () => fetchJSON<any>("/api/overview"),
  overviewInstrument: (inst: string) => fetchJSON<any>(item("/api/overview", inst)),
  strategies: (params?: Query) => fetchJSON<any>(withQuery("/api/strategies", params)),
  strategy: (id: string) => fetchJSON<any>(item("/api/strategies", id)),
  strategyParams: (id: string) => fetchJSON<any>(`${item("/api/strategies", id)}/parameters`),
  controlStrategy: (id: string, action: string) => postJSON<any>(`${item("/api/strategies", id)}/control`, { action }),
  positions: (params?: Query) => fetchJSON<any>(withQuery("/api/positions", params)),
  position: (id: string) => fetchJSON<any>(item("/api/positions", id)),
  positionPnl: (id: string) => fetchJSON<any>(`${item("/api/positions", id)}/pnl`),
  orders: (params?: Query) => fetchJSON<any>(withQuery("/api/orders", params)),
  fills: (params?: Query) => fetchJSON<any>(withQuery("/api/fills", params)),
  trades: (params?: Query) => fetchJSON<any>(withQuery("/api/trades", params)),
  trade: (id: string) => fetchJSON<any>(item("/api/trades", id)),
  pnl: () => fetchJSON<any>("/api/pnl"),
  pnlInstrument: (inst: string) => fetchJSON<any>(item("/api/pnl", inst)),
  pnlStrategy: (inst: string, strategyId: string) =>
    fetchJSON<any>(item(`${item("/api/pnl", inst)}/strategy`, strategyId)),
  equityCurve: () => fetchJSON<any>("/api/equity-curve"),
  equityCurveInstrument: (inst: string) => fetchJSON<any>(item("/api/equity-curve", inst)),
  marketData: () => fetchJSON<any>("/api/market-data"),
  marketDataInstrument: (inst: string) => fetchJSON<any>(item("/api/market-data", inst)),
  risk: () => fetchJSON<any>("/api/risk"),
  healthSystem: () => fetchJSON<any>("/api/health/system"),
  indicators: () => fetchJSON<any>("/api/indicators"),
  indicatorsInstrument: (inst: string) => fetchJSON<any>(item("/api/indicators", inst)),
  htf: () => fetchJSON<any>("/api/htf"),
  htfInstrument: (inst: string) => fetchJSON<any>(`/api/htf/${inst}`),
  alerts: (params?: Query) => fetchJSON<any>(withQuery("/api/alerts", params)),
  reconciliation: () => fetchJSON<any>("/api/reconciliation"),
  orphanScan: () => fetchJSON<any>("/api/trades/orphan-scan"),
  lifecycleReconcile: () => fetchJSON<any>("/api/trades/lifecycle-reconcile"),
  settings: () => fetchJSON<any>("/api/settings"),
  refreshSettings: () => postJSON<any>("/api/settings/refresh"),
  audit: (params?: Query) => fetchJSON<any>(withQuery("/api/audit", params)),
  liveDashboard: () => fetchJSON<any>("/api/live/dashboard"),
  liveOrders: (params?: Query) => fetchJSON<any>(withQuery("/api/live/orders", params)),
  liveOrder: (id: string) => fetchJSON<any>(item("/api/live/order", id)),
  liveTimeline: (params?: Query) => fetchJSON<any>(withQuery("/api/live/timeline", params)),
  livePositions: () => fetchJSON<any>("/api/live/positions"),
  livePnl: () => fetchJSON<any>("/api/live/pnl"),
  liveRecon: () => fetchJSON<any>("/api/live/recon"),
  liveSignals: (limit?: number) => fetchJSON<any>(withQuery("/api/live/signals", { limit })),
  liveCandles: () => fetchJSON<any>("/api/live/candles"),
  liveFunds: () => fetchJSON<any>("/api/live/funds"),
  liveProfile: () => fetchJSON<any>("/api/live/profile"),
  liveSync: () => fetchJSON<any>("/api/live/sync"),
  liveTelegram: () => fetchJSON<any>("/api/live/telegram"),
  reversals: (id?: string) => fetchJSON<any>(id ? item("/api/reversals", id) : "/api/reversals"),
  analyticsStrategies: () => fetchJSON<any>("/api/analytics/strategies"),
  analyticsStrategy: (id: string) => fetchJSON<any>(`/api/analytics/strategies/${encodeURIComponent(id)}`),
  analyticsStrategyTrades: (id: string, limit?: number) =>
    fetchJSON<any>(withQuery(`${item("/api/analytics/strategies", id)}/trades`, { limit })),
  analyticsStrategyEquity: (id: string, startingEquity?: number) =>
    fetchJSON<any>(
      withQuery(`${item("/api/analytics/strategies", id)}/equity`, {
        starting_equity: startingEquity,
      })
    ),
  analyticsStrategyDrawdown: (id: string) =>
    fetchJSON<any>(`/api/analytics/strategies/${encodeURIComponent(id)}/drawdown`),
  analyticsEvents: (params?: Record<string, string>) => {
    return fetchJSON<any>(withQuery("/api/analytics/events", params));
  },
};

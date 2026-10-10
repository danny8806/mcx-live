export function quoteIsFresh(quote: any, connected: boolean): boolean {
  return connected && quote?.feed_healthy === true && quote?.receive_timestamp != null;
}

export function marketSessionActive(status: unknown): boolean {
  return ["market_open", "live_trading", "market_close"].includes(
    String(status ?? "").toLowerCase(),
  );
}

export function marketSessionClosed(status: unknown): boolean {
  return ["overnight", "after_market", "pre_market"].includes(
    String(status ?? "").toLowerCase(),
  );
}

export function marketStatusLabel(status: unknown): string {
  const value = String(status ?? "unknown").toLowerCase();
  if (value === "live_trading" || value === "market_open") return "Market open";
  if (value === "market_close") return "Market closing";
  if (value === "pre_market") return "Pre-market";
  if (value === "overnight" || value === "after_market") return "Market closed";
  return "Session unknown";
}

export function quoteStatus(quote: any, connected: boolean, marketActive = true): string {
  if (!quote || !(Number(quote.ltp) > 0)) return "No quote";
  if (!marketActive) return "Market closed";
  if (quoteIsFresh(quote, connected)) return "Fresh tick";
  if (!connected) return "Feed disconnected";
  return quote?.receive_timestamp == null ? "Tick time unknown" : "Stale tick";
}

export function tickAge(quote: any): string {
  const seconds = Number(quote?.tick_age_seconds);
  if (quote?.tick_age_seconds == null || !Number.isFinite(seconds)) return "Age unknown";
  if (seconds < 60) return `${Math.floor(seconds)}s old`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m old`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m old`;
}

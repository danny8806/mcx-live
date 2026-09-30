export function quoteIsFresh(quote: any, connected: boolean): boolean {
  return connected && quote?.feed_healthy === true && quote?.receive_timestamp != null;
}

export function quoteStatus(quote: any, connected: boolean): string {
  if (!quote || !(Number(quote.ltp) > 0)) return "No quote";
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

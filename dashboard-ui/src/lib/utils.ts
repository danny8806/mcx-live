export function safeNum(val: any, fallback = 0): number {
  if (val == null || isNaN(val)) return fallback;
  return Number(val);
}

export function safeINR(val: any): string {
  return `₹${safeNum(val).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export function formatINR(val: number | null | undefined, showSign = true): string {
  if (val == null || isNaN(val)) return "—";
  const sign = val > 0 ? "+" : val < 0 ? "-" : "";
  const abs = Math.abs(val);
  return `${showSign ? sign : ""}₹${abs.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export function formatINRShort(val: number | null | undefined): string {
  if (val == null || isNaN(val)) return "₹0";
  const sign = val >= 0 ? "+" : "";
  return `${sign}₹${Math.abs(val).toLocaleString("en-IN", { maximumFractionDigits: 0 })}`;
}

export function formatPct(val: number | null | undefined): string {
  if (val == null || isNaN(val)) return "0.00%";
  return `${val >= 0 ? "+" : ""}${val.toFixed(2)}%`;
}

export function formatTimestamp(ts: number): string {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleTimeString("en-IN", {
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "Asia/Kolkata",
  });
}

export function formatDT(ts: any): string {
  if (ts == null || ts === "") return "—";
  let ms: number;
  if (typeof ts === "number") {
    if (!Number.isFinite(ts) || ts <= 0) return "—";
    ms = ts * 1000;
  }
  else if (typeof ts === "string") {
    ms = Date.parse(ts);
    if (isNaN(ms)) return ts;
    if (ms <= 0) return "—";
  } else return "—";
  return new Date(ms).toLocaleString("en-IN", {
    day: "2-digit", month: "short",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "Asia/Kolkata",
  });
}

export function pnlColor(val: number): string {
  if (val > 0) return "var(--green)";
  if (val < 0) return "var(--red)";
  return "var(--text-muted)";
}

export function statusDot(status: string): string {
  const s = status.toLowerCase();
  if (s === "running" || s === "healthy" || s === "connected" || s === "live") return "var(--green)";
  if (s === "flat" || s === "idle") return "var(--text-muted)";
  if (s === "paused" || s === "warning" || s === "degraded") return "var(--amber)";
  if (s === "error" || s === "offline" || s === "disconnected") return "var(--red)";
  return "var(--text-muted)";
}

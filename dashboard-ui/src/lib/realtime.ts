export interface DashboardMessage {
  type: string;
  data?: unknown;
}

function socketUrl(): string {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const base = (window.APP_WS_BASE ?? "").replace(/\/$/, "");
  return `${protocol}//${window.location.host}${base}/ws`;
}

/** Owns socket setup, subscription, and reconnect cleanup for the dashboard. */
export function connectDashboardSocket(
  onMessage: (message: DashboardMessage) => void,
  onConnectionChange: (connected: boolean) => void,
): () => void {
  let stopped = false;
  let reconnectTimer: number | undefined;
  let socket: WebSocket | undefined;

  const connect = () => {
    if (stopped) return;
    socket = new WebSocket(socketUrl());
    socket.onopen = () => {
      socket?.send(JSON.stringify({ action: "subscribe", channels: ["all"] }));
      onConnectionChange(true);
    };
    socket.onclose = () => {
      onConnectionChange(false);
      if (!stopped) reconnectTimer = window.setTimeout(connect, 3000);
    };
    socket.onerror = () => socket?.close();
    socket.onmessage = (event) => {
      try {
        onMessage(JSON.parse(event.data) as DashboardMessage);
      } catch {
        onMessage({ type: "parse_error", data: "Invalid realtime message" });
      }
    };
  };

  connect();
  return () => {
    stopped = true;
    if (reconnectTimer !== undefined) window.clearTimeout(reconnectTimer);
    socket?.close();
    onConnectionChange(false);
  };
}

import { useEffect, useState } from "react";
import { useDataSelector } from "../store/DataProvider";
import { api } from "../lib/api";
import { formatTimestamp } from "../lib/utils";

const categories = ["ALL", "SIGNAL", "ORDER", "FILL", "POSITION", "SL", "EXIT", "REVERSAL", "RECONCILIATION", "WEBSOCKET", "LTP", "RISK", "SYSTEM", "TELEGRAM", "ERROR", "CRITICAL"];
const deliveries = ["ALL", "SENT", "FAILED", "QUEUED"];

function detailText(row: any): string {
  if (row.error) return String(row.error);
  if (row.payload_sanitized) {
    try {
      const payload = typeof row.payload_sanitized === "string" ? JSON.parse(row.payload_sanitized) : row.payload_sanitized;
      const message = typeof payload?.message === "string" ? payload.message : null;
      if (message) {
        const lines = message.replace(/<[^>]*>/g, "").split(/\r?\n/).map((line: string) => line.trim()).filter(Boolean);
        const subject = lines.find((line: string) => /^(Message|Reason):/i.test(line));
        return subject ? subject.replace(/^(Message|Reason):\s*/i, "") : lines.slice(0, 2).join(" · ");
      }
    } catch { /* Older ledger rows may contain plain text. */ }
    if (typeof row.payload_sanitized === "string") return row.payload_sanitized.slice(0, 220);
  }
  const transition = [row.status_before, row.status_after].filter(Boolean).join(" → ");
  if (transition) return transition;
  if (row.trigger_price != null) return `Trigger ${row.trigger_price}`;
  if (row.price != null) return `Price ${row.price}`;
  return "Event recorded";
}

export default function Alerts() {
  const runtimeEvents = useDataSelector<any[]>((s) => s.alerts);
  const [category, setCategory] = useState("ALL");
  const [delivery, setDelivery] = useState("ALL");
  const [rows, setRows] = useState<any[]>([]);
  const [stats, setStats] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [updatedAt, setUpdatedAt] = useState<number | null>(null);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const [ledger, counts] = await Promise.all([
          api.alertLedger({ limit: 100, event_type: category === "ALL" ? undefined : category, telegram_status: delivery === "ALL" ? undefined : delivery }),
          api.alertLedgerStats(),
        ]);
        if (!alive) return;
        if (ledger?.error || !Array.isArray(ledger?.items)) throw new Error(ledger?.error || "Invalid alert ledger response");
        setRows(ledger.items);
        setStats(counts?.stats ?? counts);
        setUpdatedAt(Date.now());
        setError(null);
        setLoaded(true);
      } catch (e: any) {
        if (alive) { setError(e?.message || String(e)); setLoaded(true); }
      }
    };
    load();
    const timer = window.setInterval(load, 10_000);
    return () => { alive = false; window.clearInterval(timer); };
  }, [category, delivery]);

  return <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
    <section className="desk-section">
      <div className="desk-section-heading"><div><div className="eyebrow">DURABLE ALERT LEDGER</div><h3>Lifecycle and Telegram delivery</h3></div><span className="source-note">{updatedAt ? `Updated ${formatTimestamp(updatedAt / 1000)} · ` : ""}Last 100 matching events</span></div>
      {stats && <div className="alert-stats"><span>Total <b>{stats.total ?? "—"}</b></span><span>Sent <b>{stats.sent ?? "—"}</b></span><span>Failed <b>{stats.failed ?? "—"}</b></span><span>Queued <b>{stats.queued ?? "—"}</b></span></div>}
      <div className="alert-filters">
        <label>Category <select value={category} onChange={(e) => setCategory(e.target.value)}>{categories.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
        <label>Telegram <select value={delivery} onChange={(e) => setDelivery(e.target.value)}>{deliveries.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
      </div>
      {error && <div role="alert" className="desk-warning">Alert ledger unavailable: {error}. Runtime events are listed below.</div>}
      {!loaded && <div className="empty-state">Loading durable alerts…</div>}
      {loaded && !error && rows.length === 0 && <div className="empty-state">No matching durable alerts.</div>}
      {rows.map((row) => <article className="alert-event" key={row.event_id}>
        <div className="alert-event-head"><strong>{row.event_type || "EVENT"}</strong><span>{row.telegram_status || "NOT SENT"}</span><time>{formatTimestamp(row.event_timestamp)}</time></div>
        <div className="alert-event-main">{detailText(row)}</div>
        <div className="alert-event-meta">{[row.strategy_id, row.side, row.quantity != null ? `qty ${row.quantity}` : null, row.broker_order_id ? `Dhan ${row.broker_order_id}` : null, row.local_order_id ? `Local ${row.local_order_id}` : null].filter(Boolean).join(" · ") || row.event_source || "LOCAL"}</div>
        {row.payload_sanitized && <details><summary>Recorded detail</summary><pre>{String(row.payload_sanitized)}</pre></details>}
      </article>)}
    </section>
    <section className="desk-section"><div className="desk-section-heading"><div><div className="eyebrow">CURRENT RUNTIME</div><h3>Recent engine events</h3></div><span className="source-note">In-memory event bus; resets on restart</span></div>
      {runtimeEvents?.length ? runtimeEvents.map((event: any, index: number) => <div className="alert-event" key={event.id ?? index}><div className="alert-event-head"><strong>{event.type || "EVENT"}</strong><span>{event.severity || "info"}</span><time>{formatTimestamp(event.timestamp)}</time></div><details><summary>Event detail</summary><pre>{typeof event.data === "string" ? event.data : JSON.stringify(event.data, null, 2)}</pre></details></div>) : <div className="empty-state">No recent runtime events. Check the durable ledger above for history.</div>}
    </section>
  </div>;
}

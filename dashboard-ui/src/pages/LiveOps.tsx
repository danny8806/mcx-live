import { useEffect, useRef, useState } from "react";
import { api } from "../lib/api";
import { formatTimestamp } from "../lib/utils";

/**
 * LIVE OPS — consolidated LIVE panel.
 *
 * Renders only data the LIVE backend actually exposes (broker truth from Dhan,
 * engine decisions, DB audit).  No simulated values anywhere.  Every section
 * shows its freshness (age) so STALE data is visible, never silently hidden.
 */

function useLiveDashboard(ms = 5000) {
  const [data, setData] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
  const load = () => {
    api.liveDashboard()
      .then((d) => { if (mounted.current) { setData(d); setError(null); } })
      .catch((e: any) => { if (mounted.current) setError(e?.message || String(e)); })
      .finally(() => { if (mounted.current) setLoading(false); });
  };
  useEffect(() => {
    load();
    const t = window.setInterval(load, ms);
    return () => window.clearInterval(t);
  }, [ms]);
  return { data, error, loading, refresh: load };
}

const card: React.CSSProperties = {
  background: "var(--bg-panel)", border: "1px solid var(--border)",
  borderRadius: "8px", overflow: "hidden",
};
const header: React.CSSProperties = {
  padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)",
  fontSize: "10px", fontWeight: 600, color: "var(--text-muted)",
  textTransform: "uppercase", letterSpacing: "0.5px",
  display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px",
};
const cell: React.CSSProperties = {
  padding: "5px 12px", fontSize: "10px",
  borderBottom: "1px solid var(--border-subtle)",
};
const thStyle: React.CSSProperties = {
  fontSize: "9px", color: "var(--text-disabled)", textTransform: "uppercase",
  padding: "5px 12px", borderBottom: "1px solid var(--border-subtle)",
  background: "var(--bg-table-header)",
};

function Panel({ title, right, children }: { title: string; right?: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="lift animate-fade-in-up" style={card}>
      <div style={header}>
        <span>{title}</span>
        {right && <span>{right}</span>}
      </div>
      {children}
    </div>
  );
}

function Badge({ ok, label, warn }: { ok?: boolean; label: string; warn?: boolean }) {
  const color = warn ? "var(--amber)" : ok === false ? "var(--red)" : ok === true ? "var(--green)" : "var(--text-muted)";
  return (
    <span style={{
      display: "inline-block", padding: "1px 6px", borderRadius: "4px",
      fontSize: "9px", fontWeight: 600, color, background: `${color}1a`,
      border: `1px solid ${color}40`, whiteSpace: "nowrap",
    }}>{label}</span>
  );
}

function Age({ ts }: { ts: number | null | undefined }) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(t);
  }, []);
  if (ts == null) return <Badge warn label="AGE UNKNOWN" />;
  const age = (now / 1000) - Number(ts);
  if (age < 0) return <Badge ok label="fresh" />;
  const stale = age > 90;
  return <Badge warn={stale} ok={!stale} label={stale ? `STALE ${Math.round(age)}s` : `Updated ${Math.round(age)}s ago`} />;
}

function fmt(v: any) {
  if (v == null || v === "") return "—";
  const n = Number(v);
  if (Number.isNaN(n)) return String(v);
  return n.toFixed(2);
}

function secs(v: any): number | undefined {
  if (v == null || v === "") return undefined;
  const n = Number(v);
  if (!Number.isNaN(n)) return n;
  const t = new Date(String(v)).getTime();
  return Number.isNaN(t) ? undefined : t / 1000;
}

function Side({ side }: { side?: string }) {
  const s = String(side || "").toUpperCase();
  const color = s === "BUY" || s === "LONG" ? "var(--green)" : s === "SELL" || s === "SHORT" ? "var(--red)" : "var(--text-muted)";
  return <span style={{ color, fontWeight: 600 }}>{s || "—"}</span>;
}

function Stat({ label, value, sub }: { label: string; value: React.ReactNode; sub?: React.ReactNode }) {
  return (
    <div style={{ padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)" }}>
      <div style={{ fontSize: "9px", color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.4px" }}>{label}</div>
      <div className="tabular-nums" style={{ fontSize: "16px", fontWeight: 700, color: "var(--text-primary)", marginTop: "2px" }}>{value}</div>
      {sub && <div style={{ fontSize: "9px", color: "var(--text-disabled)", marginTop: "2px" }}>{sub}</div>}
    </div>
  );
}

function StateColor(state?: string) {
  switch ((state || "").toLowerCase()) {
    case "filled": case "matched": return "var(--green)";
    case "rejected": case "cancelled": case "canceled": case "expired": return "var(--red)";
    case "created": case "submitted": case "acknowledged": case "partially_filled": case "pending": return "var(--amber)";
    default: return "var(--amber)";
  }
}

export default function LiveOps() {
  const { data, error, loading, refresh } = useLiveDashboard(5000);
  if (loading && !data) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "200px", height: "14px", marginBottom: "12px" }} />
      <div className="skeleton" style={{ width: "100%", height: "300px" }} />
    </div>
  );
  const err = error || data?.error;
  const p = data?.profile;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
      <div className="lift animate-fade-in-up" style={{ ...card, padding: "12px 14px", display: "flex", alignItems: "center", justifyContent: "space-between", gap: "10px", flexWrap: "wrap" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          <Badge ok label="LIVE" />
          <span style={{ fontSize: "13px", fontWeight: 700, color: "var(--text-primary)" }}>LIVE Dhan Terminal — Consolidated Panel</span>
          {p && <Badge ok label={`gate ${p.gate}`} />}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          {err && <Badge warn={false} ok={false} label="ERROR" />}
          <span style={{ fontSize: "9px", color: "var(--text-disabled)" }}>
            {data ? (new Date(data.generated_at * 1000).toLocaleTimeString()) : ""}
          </span>
          <button onClick={refresh} style={{
            background: "var(--bg-panel-hover)", border: "1px solid var(--border)",
            borderRadius: "4px", color: "var(--text-muted)", cursor: "pointer",
            fontSize: "10px", padding: "3px 10px",
          }}>Refresh</button>
        </div>
      </div>

      {err && (
        <div className="lift animate-fade-in-up" style={{ ...card, padding: "10px 14px", color: "var(--red)", fontSize: "11px", borderColor: "var(--red)" }}>
          {typeof err === "string" ? err : JSON.stringify(err)}
        </div>
      )}

      {/* ── Row 1: Profile + Funds + Sync ── */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: "12px" }}>
        <Panel title="DHAN PROFILE" right={p && <>{p.order_ws?.connected ? <Badge ok label="order-ws" /> : <Badge ok={false} label="order-ws" />}{" "}{p.data_ws?.connected ? <Badge ok label="data-ws" /> : <Badge ok={false} label="data-ws" />}</>}>
          {p ? (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0" }}>
              <Stat label="Client ID" value={p.client_id} />
              <Stat label="Product Type" value={p.product_type || "—"} />
              <Stat label="Broker" value={p.broker || "—"} />
              <Stat label="Execution Model" value={p.execution_model || "—"} sub={`entries ${p.live_trading_enabled ? "ENABLED" : "DISABLED"}`} />
              <Stat label="Order WS" value={p.order_ws?.connected ? "CONNECTED" : "DISCONNECTED"} sub={p.order_ws?.url || ""} />
              <Stat label="Data WS" value={p.data_ws?.connected ? "CONNECTED" : "DISCONNECTED"} sub={`fallback ${p.order_watcher?.market_fallback_enabled ? "ON" : "OFF"} / age ${(p.order_watcher?.max_order_age_ms ?? 0) / 1000}s`} />
            </div>
          ) : <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no profile</div>}
        </Panel>

        <Panel title="FUNDS / MARGIN" right={data?.funds && <span style={{ display: "flex", gap: "4px", alignItems: "center" }}><Badge ok label={data?.funds?.source || "DHAN"} /><Age ts={data?.funds?.last_updated} /></span>}>
          {data?.funds ? (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0" }}>
              <Stat label="Total Equity" value={`₹${fmt(data.funds.equity)}`} sub={`Dhan funds ${data.funds.fields?.source || ""}`} />
              <Stat label="Broker-reported P&L" value={`₹${fmt(data.funds.net_pnl)}`} sub="Dhan positions P&L; local charges shown separately" />
              <Stat label="Used Margin" value={`₹${fmt(data.funds.used_margin)}`} />
              <Stat label="Available Margin" value={`₹${fmt(data.funds.available_margin)}`} />
              <Stat label="Realized P&L" value={`₹${fmt(data.funds.realized_pnl)}`} />
              <Stat label="Unrealized P&L" value={`₹${fmt(data.funds.unrealized_pnl)}`} />
            </div>
          ) : <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no funds</div>}
        </Panel>

        <Panel title="BROKER SYNC + CONNECTIVITY" right={data?.sync?.sync?.service && (
          <span style={{ display: "flex", gap: "4px" }}>
            <Badge ok={!!data.sync.sync.service.healthy} label={data.sync.sync.service.healthy ? "HEALTHY" : "UNHEALTHY"} />
            <Badge ok={data.sync.sync.service.ws_connected} label={data.sync.sync.service.ws_connected ? "WS" : "ws-down"} />
          </span>
        )}>
          {data?.sync?.sync ? (
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0" }}>
              <Stat label="REST Worker" value={data.sync.sync.service.worker_alive ? "ALIVE" : "DEAD"} />
              <Stat label="Service" value={data.sync.sync.service.running ? "RUNNING" : "STOPPED"} />
              {Object.entries(data.sync.sync.stale_tasks ?? {}).map(([t, s]: any) => (
                <div key={t} style={{ padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)" }}>
                  <div style={{ fontSize: "9px", color: "var(--text-muted)", textTransform: "uppercase" }}>{t} poll</div>
                  <div style={{ fontSize: "11px", fontWeight: 600, color: s?.is_stale ? "var(--red)" : "var(--green)" }}>
                    {s?.is_stale ? (s?.age_seconds != null ? `STALE ${Math.round(s.age_seconds)}s` : "STALE") : "fresh"}
                  </div>
                </div>
              ))}
              <Stat label="Data WS" value={data.sync.data_ws?.connected ? "CONNECTED" : "DISCONNECTED"} sub={`ticks ${data.sync.data_ws?.stats?.tick_count ?? "—"}`} />
            </div>
          ) : <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no sync data</div>}
        </Panel>
      </div>

      {/* ── Row 2: Candles ── */}
      <Panel title="CANDLES — CLOSED (Dhan REST) vs FORMING (Dhan REST forming bar)">
        {data?.candles?.instruments ? (
          Object.entries(data.candles.instruments).map(([inst, ix]: any) => (
            <div key={inst} style={{ borderBottom: "1px solid var(--border-subtle)" }}>
              <div style={{ padding: "6px 12px", display: "flex", alignItems: "center", gap: "8px", flexWrap: "wrap" }}>
                <span style={{ fontWeight: 700, fontSize: "11px", color: "var(--text-primary)" }}>{inst}</span>
                <span style={{ fontSize: "9px", color: "var(--text-muted)", fontFamily: "monospace" }}>SEC {ix.security_id}</span>
                <span style={{ fontSize: "9px", color: "var(--text-muted)" }}>{ix.symbol}</span>
                <span style={{ fontSize: "9px", color: "var(--text-muted)" }}>{ix.exchange_segment} / {ix.instrument_type}</span>
                {ix.ltp && (
                  <span className="tabular-nums" style={{ fontSize: "12px", fontWeight: 700, color: "var(--amber)" }}>
                    LTP {fmt(ix.ltp.ltp)}
                    {ix.ltp_age_seconds != null && <span style={{ fontSize: "9px", color: ix.ltp_age_seconds > 60 ? "var(--red)" : "var(--text-muted)", fontWeight: 400 }}> {" "}({Math.round(ix.ltp_age_seconds)}s)</span>}
                  </span>
                )}
              </div>
              <div style={{ padding: "0 12px" }}>
                <div style={{ display: "grid", gridTemplateColumns: "70px 1fr 1fr", gap: "8px", ...thStyle }}>
                  <span>TF</span><span>CLOSED (o/h/l/c · v)</span><span>FORMING (o/h/l/c · LTP)</span>
                </div>
                {Object.entries(ix.candles ?? {}).map(([tf, c]: any) => (
                  <div key={tf} style={{ display: "grid", gridTemplateColumns: "70px 1fr 1fr", gap: "8px", fontSize: "10px", padding: "4px 0", borderBottom: "1px solid var(--border-subtle)" }}>
                    <span style={{ color: "var(--text-muted)" }}>{tf} <Age ts={c?.last_updated} /></span>
                    {c?.closed ? (
                      <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>
                        {fmt(c.closed.open)} {fmt(c.closed.high)} {fmt(c.closed.low)} {fmt(c.closed.close)} · {c.closed.volume}
                        <span style={{ color: "var(--text-disabled)", fontSize: "9px" }}> close@{formatTimestamp(secs(c.closed.end_ts) as number)}</span>
                      </span>
                    ) : <span style={{ color: "var(--text-disabled)", fontSize: "9px" }}>no closed candle (market hours?)</span>}
                    {c?.forming ? (
                      <span className="tabular-nums" style={{ color: "var(--amber)" }}>
                        {fmt(c.forming.open)} {fmt(c.forming.high)} {fmt(c.forming.low)} {fmt(c.forming.close)} · {c.forming.source?.includes("dhan") ? fmt(c.forming.volume ?? 0) : fmt(c.forming.ltp)}
                        <span style={{ color: "var(--text-disabled)", fontSize: "9px" }}> ({c.forming.source?.includes("dhan") ? "Dhan forming" : "LTP snapshot"})</span>
                      </span>
                    ) : <span style={{ color: "var(--text-disabled)", fontSize: "9px" }}>no forming bar</span>}
                  </div>
                ))}
              </div>
            </div>
          ))
        ) : <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no candle data</div>}
      </Panel>

      {/* ── Row 3: Signals ── */}
      <Panel title="SIGNALS (LIVE)" right={data?.signals && <Age ts={data.signals.latest_at} />}>
        <div style={{ display: "grid", gridTemplateColumns: "150px 70px 60px 70px 70px 50px 1fr", gap: "8px", ...thStyle }}>
          <span>Strategy</span><span>Side</span><span>Time</span><span>Trigger</span><span>SL</span><span>Qty</span><span>Reason · Candle</span>
        </div>
        {(data?.signals?.signals ?? []).map((s: any) => (
          <div key={s.signal_id} className="hover-row" style={{ display: "grid", gridTemplateColumns: "150px 70px 60px 70px 70px 50px 1fr", gap: "8px", ...cell, alignItems: "center" }}>
            <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{s.strategy_id}</span>
            <Side side={s.side} />
            <span className="tabular-nums" style={{ color: "var(--text-muted)" }}>{formatTimestamp(secs(s.signal_timestamp) as number)}</span>
            <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{fmt(s.trigger_price)}</span>
            <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{fmt(s.stop_price)}</span>
            <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{s.quantity ?? "—"}</span>
            <span style={{ color: "var(--text-muted)", fontSize: "9px", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
              {s.signal_reason || ""} · TF {s.timeframe || ""} · candle {s.open != null ? `${fmt(s.open)} ${fmt(s.high)} ${fmt(s.low)} ${fmt(s.close)}` : "—"}
            </span>
          </div>
        ))}
        {(data?.signals?.signals ?? []).length === 0 && <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no live signals yet</div>}
      </Panel>

      {/* ── Row 4: Orders ── */}
      <Panel title="ORDERS — DB lineage + Dhan broker-confirmed state" right={data?.orders && <Age ts={data.orders.latest_at} />}>
        <div style={{ display: "grid", gridTemplateColumns: "120px 150px 60px 60px 60px 60px 60px 80px 1fr", gap: "8px", ...thStyle }}>
          <span>Order ID</span><span>Broker Order ID</span><span>Side</span><span>Qty</span><span>Filled</span><span>Rem.</span><span>Type</span><span>State</span><span>Watcher</span>
        </div>
        {(data?.orders?.orders ?? []).map((o: any) => (
          <div key={o.order_id} className="hover-row" style={{ display: "grid", gridTemplateColumns: "120px 150px 60px 60px 60px 60px 60px 80px 1fr", gap: "8px", ...cell, alignItems: "center" }}>
            <span title={o.order_id} style={{ color: "var(--text-muted)", fontFamily: "monospace", fontSize: "9px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{o.order_id}</span>
            <span title={o.broker_order_id || ""} style={{ color: "var(--amber)", fontFamily: "monospace", fontSize: "9px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {o.broker_order_id || "—"}
              <span style={{ color: "var(--text-disabled)" }}>{o.exchange_order_id ? ` / exc ${o.exchange_order_id}` : ""}</span>
            </span>
            <Side side={o.side} />
            <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{o.quantity}</span>
            <span className="tabular-nums" style={{ color: o.filled_quantity && o.filled_quantity >= o.quantity ? "var(--green)" : "var(--text-secondary)" }}>{o.filled_quantity ?? 0}</span>
            <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{o.remaining_quantity ?? "—"}</span>
            <span style={{ color: "var(--text-muted)" }}>{o.order_type}{o.order_role && o.order_role !== "ENTRY" && o.order_role !== "PENDING_ENTRY" ? ` · ${o.order_role}` : ""}</span>
            <span style={{ color: StateColor(o.state), fontWeight: 600 }}>{o.state}</span>
            <span style={{ fontSize: "9px", color: "var(--text-muted)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
              {o.watcher_status ? `${o.watcher_status}${o.rest_verified ? " ✓rest" : " (no rest)"}${o.last_error ? ` ✗${o.last_error_code || ""}` : ""}` : "not watched"}
              {o.submitted_at ? ` · sub ${formatTimestamp(secs(o.submitted_at) as number)}` : ""}
              {o.product_type ? ` · ${o.product_type}` : ""}
            </span>
          </div>
        ))}
        {(data?.orders?.orders ?? []).length === 0 && <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no live orders yet</div>}
      </Panel>

      {/* ── Row 5: Positions + P&L + Reconciliation ── */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: "12px" }}>
        <Panel title="POSITIONS — LOCAL vs DHAN" right={data?.positions && <span style={{ display: "flex", gap: "4px" }}>
          <Badge ok={!data.positions.counts.MISMATCH} label={`${data.positions.counts.MISMATCH} mismatch`} warn />
        </span>}>
          <div style={{ display: "grid", gridTemplateColumns: "120px 60px 90px 90px 60px", gap: "8px", ...thStyle }}>
            <span>Strategy</span><span>Status</span><span>LOCAL</span><span>DHAN</span><span>Δqty</span>
          </div>
          {(data?.positions?.positions ?? []).map((r: any) => {
            const col = r.status === "MATCHED" ? "var(--green)" : r.status === "MISMATCH" ? "var(--red)" : "var(--amber)";
            return (
              <div key={`${r.strategy_id}:${r.instrument}`} className="hover-row" style={{ display: "grid", gridTemplateColumns: "120px 60px 90px 90px 60px", gap: "8px", ...cell, alignItems: "center" }}>
                <span style={{ color: "var(--text-primary)", fontWeight: 500 }} title={r.instrument}>{r.strategy_id}<span style={{ display: "block", fontSize: "8px", color: "var(--text-disabled)" }}>{r.instrument}</span></span>
                <span style={{ color: col, fontWeight: 600, fontSize: "9px" }}>{r.status}</span>
                <span className="tabular-nums" style={{ color: "var(--text-secondary)", fontSize: "9px" }}>
                  {r.local ? `${r.local.side || ""} ${r.local.quantity} @${fmt(r.local.average_entry_price)}` : "—"}
                  <span style={{ color: "var(--text-disabled)" }}>{r.local?.sl_state ? ` · sl ${r.local.sl_state}` : ""}</span>
                </span>
                <span className="tabular-nums" style={{ color: "var(--amber)", fontSize: "9px" }}>
                  {r.dhan ? `${r.dhan.side || ""} ${r.dhan.quantity} @${fmt(r.dhan.average_entry_price)}` : "—"}
                </span>
                <span style={{ color: r.delta_qty === 0 ? "var(--text-muted)" : "var(--red)", fontSize: "9px" }}>{r.delta_qty}</span>
              </div>
            );
          })}
          {(data?.positions?.positions ?? []).length === 0 && <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no open positions (local or broker)</div>}
        </Panel>

        <Panel title="P&L — DHAN vs LOCAL" right={data?.pnl?.dhan && <Age ts={data.pnl.dhan.last_updated} />}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "0" }}>
            <Stat label="DHAN realized (reported)" value={`₹${fmt(data?.pnl?.dhan?.realized_pnl)}`} sub="positions realizedProfit" />
            <Stat label="DHAN unrealized" value={`₹${fmt(data?.pnl?.dhan?.unrealized_pnl)}`} />
            <Stat label="DHAN total (reported)" value={`₹${fmt(data?.pnl?.dhan?.net_pnl)}`} />
            <Stat label="LOCAL realized net" value={`₹${fmt(data?.pnl?.local?.realized_pnl)}`} sub="after recorded local charges" />
            <Stat label="LOCAL unrealized" value={`₹${fmt(data?.pnl?.local?.unrealized_pnl)}`} sub="local position book" />
            <Stat label="LOCAL net" value={`₹${fmt(data?.pnl?.local?.net_pnl)}`} />
          </div>
          {data?.pnl?.difference && (
            <div style={{ padding: "8px 12px", borderTop: "1px solid var(--border-subtle)" }}>
              <span style={{ fontSize: "9px", color: "var(--text-muted)", textTransform: "uppercase" }}>Difference (Dhan − Local): </span>
              <span className="tabular-nums" style={{ fontSize: "11px", fontWeight: 600, color: "var(--amber)" }}>
                ₹{fmt(data.pnl.difference.net_pnl)}
              </span>
              <span style={{ fontSize: "9px", color: "var(--text-disabled)" }}> {" "}{data.pnl.difference.note}</span>
            </div>
          )}
        </Panel>

        <Panel title="RECONCILIATION (LOCAL vs DHAN ledger)">
          <div style={{ display: "flex", gap: "6px", padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)", flexWrap: "wrap" }}>
            {Object.entries(data?.recon?.summary ?? {}).map(([k, v]: any) => (
              <span key={k} style={{ fontSize: "9px", color: "var(--text-muted)" }}>
                {k}: <b style={{ color: k === "MISMATCH" ? "var(--red)" : "var(--text-primary)" }}>{v}</b>
              </span>
            ))}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "110px 90px 60px 60px 50px 60px", gap: "8px", ...thStyle }}>
            <span>Broker ID</span><span>Order ID</span><span>Broker Qty</span><span>Local Qty</span><span>Gap</span><span>Status</span>
          </div>
          {(data?.recon?.reconciliation ?? []).map((r: any) => (
            <div key={r.broker_order_id} className="hover-row" style={{ display: "grid", gridTemplateColumns: "110px 90px 60px 60px 50px 60px", gap: "8px", ...cell, alignItems: "center" }}>
              <span title={r.broker_order_id} style={{ color: "var(--amber)", fontFamily: "monospace", fontSize: "9px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.broker_order_id}</span>
              <span title={r.order_id || ""} style={{ color: "var(--text-muted)", fontFamily: "monospace", fontSize: "9px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {r.order_id || r.strategy_id || "—"}
              </span>
              <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{r.broker_cumulative_qty}</span>
              <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{r.local_cumulative_qty}</span>
              <span className="tabular-nums" style={{ color: r.gap_qty ? "var(--red)" : "var(--text-muted)" }}>{r.gap_qty}</span>
              <span style={{ color: StateColor(r.status), fontWeight: 600, fontSize: "9px" }}>{r.status}</span>
            </div>
          ))}
          {(data?.recon?.reconciliation ?? []).length === 0 && <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no reconciliation rows yet</div>}
        </Panel>
      </div>

      {/* ── Row 6: Timeline ── */}
      <Panel title="TIMELINE — recent engine/broker events (latest 50)">
        {data?.timeline && data.timeline.length > 0 ? (
          <div style={{ maxHeight: 160, overflowY: "auto", fontSize: "10px" }}>
            {data.timeline.map((ev: any, i: number) => (
              <div key={i} style={{ display: "grid", gridTemplateColumns: "70px 50px 80px 80px 1fr", gap: "6px", padding: "3px 8px", borderBottom: "1px solid var(--border-subtle)", alignItems: "center" }}>
                <span style={{ color: "var(--text-muted)", fontFamily: "monospace" }}>{formatTimestamp(secs(ev.at) as number)}</span>
                <span style={{ color: ev.source === "ALERT" ? "var(--red)" : ev.source === "BROKER" ? "var(--amber)" : "var(--text-secondary)", fontWeight: 600 }}>{ev.source}</span>
                <span style={{ color: "var(--text-secondary)" }}>{ev.kind}</span>
                <span style={{ color: "var(--text-muted)", fontSize: "9px" }}>{ev.strategy_id || ev.instrument || ""}</span>
                <span style={{ color: "var(--text-muted)", fontSize: "9px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{ev.detail}</span>
              </div>
            ))}
          </div>
        ) : <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no timeline events yet</div>}
      </Panel>

      {/* ── Row 7: Telegram ── */}
      <Panel title="TELEGRAM" right={data?.telegram && <Badge ok={!!data.telegram.enabled} label={data.telegram.enabled ? "ENABLED" : "DISABLED"} />}>
        {data?.telegram ? (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: "0" }}>
            <Stat label="Chat ID" value={data.telegram.chat_id || "—"} />
            <Stat label="Bot Token" value={data.telegram.bot_token_configured ? "configured (masked)" : "not set"} />
              <Stat label="Sent / Failed" value={`${data.telegram.stats?.sent_count ?? 0} / ${data.telegram.stats?.error_count ?? 0}`} />
            <Stat label="Queue" value={`${data.telegram.stats?.queue_size ?? 0}`} />
          </div>
        ) : <div style={{ padding: "14px", color: "var(--text-muted)", fontSize: "10px" }}>no telegram data</div>}
      </Panel>
    </div>
  );
}

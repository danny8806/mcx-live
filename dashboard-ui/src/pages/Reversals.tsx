import { useEffect, useState } from "react";
import { api } from "../lib/api";

interface Reversal {
  reversal_id: string;
  signal_id: string | null;
  strategy_id: string | null;
  strategy_name: string | null;
  instrument: string | null;
  security_id: string | null;
  signal_timestamp: number | string | null;
  side: string | null;
  trigger_price: number | null;
  old_trade_id: string | null;
  old_position_id: string | null;
  old_exit_order_id: string | null;
  old_broker_order_id: string | null;
  old_exit_status: string | null;
  old_exit_fill_price: number | null;
  old_exit_filled_quantity: number | null;
  old_sl_order_id: string | null;
  old_sl_status: string | null;
  new_trade_id: string | null;
  new_position_id: string | null;
  new_entry_order_id: string | null;
  new_broker_order_id: string | null;
  new_entry_status: string | null;
  new_entry_fill_price: number | null;
  new_entry_filled_quantity: number | null;
  new_sl_order_id: string | null;
  new_sl_status: string | null;
  exit_verified_at: string | null;
  entry_fill_confirmed_at: string | null;
  fallback_used: boolean;
  fallback_status: string | null;
  status: string | null;
  complete: boolean;
  chain: { step: string; value: any; detail?: string; ok: boolean }[];
  created_at: string | null;
  updated_at: string | null;
}

const POLL_MS = 5000;
const STALE_MS = 30_000;

const fmtTs = (x: number | string | null | undefined): string => {
  if (x === null || x === undefined || x === "") return "—";
  if (typeof x === "number") {
    if (x < 1e12) x = x * 1000;
    const d = new Date(x);
    if (Number.isNaN(d.getTime())) return String(x);
    return d.toLocaleString("en-IN", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "Asia/Kolkata" });
  }
  const d = new Date(x);
  if (Number.isNaN(d.getTime())) return x;
  return d.toLocaleString("en-IN", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "Asia/Kolkata" });
};

export default function Reversals() {
  const [data, setData] = useState<{ reversals: Reversal[]; count?: number; execution_mode?: string } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [lastFetch, setLastFetch] = useState<number>(0);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const d = await api.reversals();
        if (!alive) return;
        if (d?.error) setError(String(d.error));
        else {
          setError(null);
          setData(d);
        }
        setLastFetch(Date.now());
      } catch (e: any) {
        if (alive) setError(e?.message || String(e));
      }
    };
    load();
    const t = window.setInterval(load, POLL_MS);
    return () => {
      alive = false;
      window.clearInterval(t);
    };
  }, []);

  const ageMs = lastFetch ? Date.now() - lastFetch : 0;
  const stale = ageMs > STALE_MS;
  const list: Reversal[] = data?.reversals || [];

  if (!data && !error) {
    return (
      <div style={{ padding: "20px", color: "var(--text-muted)" }}>
        <div className="skeleton" style={{ width: "350px", height: "48px", marginBottom: "12px" }} />
        <div className="skeleton" style={{ width: "100%", height: "120px" }} />
        <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading LIVE reversals...</div>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
      <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "12px", display: "flex", justifyContent: "space-between", alignItems: "center", gap: "10px" }}>
        <div>
          <div style={{ fontSize: "12px", fontWeight: 600, color: "var(--text-primary)" }}>
            LIVE REVERSALS — {data?.execution_mode?.toUpperCase() ?? "LIVE"}
          </div>
          <div style={{ fontSize: "9px", color: "var(--text-muted)", marginTop: "2px" }}>
            Full 9-step lifecycle: SIGNAL → TRIGGER → OLD EXIT → OLD POSITION FLAT → OLD LOCAL SL CLEARED → NEW ENTRY → NEW FILL → NEW POSITION → NEW LOCAL SL. Broker-linked IDs only; never collapsed into one status.
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
          <span style={{ fontSize: "9px", color: "var(--text-muted)" }}>{list.length} record(s)</span>
          {stale ? (
            <span style={{ fontSize: "9px", fontWeight: 700, color: "var(--red)", border: "1px solid var(--red)", borderRadius: "4px", padding: "2px 6px" }}>
              STALE {Math.round(ageMs / 1000)}s
            </span>
          ) : (
            <span style={{ fontSize: "9px", color: "var(--green)" }}>LIVE · updated {Math.round(ageMs / 1000)}s ago</span>
          )}
        </div>
      </div>

      {error && (
        <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--red)", borderRadius: "8px", padding: "8px 12px", fontSize: "10px", color: "var(--red)" }}>
          API error: {error}
        </div>
      )}

      {list.length === 0 && (
        <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", padding: "24px", textAlign: "center" }}>
          <div style={{ fontSize: "11px", color: "var(--text-primary)", fontWeight: 600 }}>No reversals recorded</div>
          <div style={{ fontSize: "10px", color: "var(--text-muted)", marginTop: "6px" }}>
            When a REVERSAL signal fires, the full old-exit / new-entry chain is rendered here from the durable reversals table (SOURCE: DATABASE, DHAN-linked order IDs).
          </div>
        </div>
      )}

      {list.map((rev) => (
        <div key={rev.reversal_id} className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: `1px solid ${rev.complete ? "var(--green)" : "var(--amber)"}`, borderRadius: "8px", padding: "12px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
            <div>
              <div style={{ fontSize: "11px", fontWeight: 700, color: "var(--text-primary)" }}>
                {rev.strategy_name || rev.strategy_id || "—"} · {rev.instrument || "—"}
                {rev.security_id ? <span style={{ color: "var(--text-muted)" }}> · {rev.security_id}</span> : null}
              </div>
              <div style={{ fontSize: "9px", color: "var(--text-muted)", marginTop: "2px", fontFamily: "monospace" }}>
                signal {rev.signal_id || "—"} · reversal {rev.reversal_id}
              </div>
              {typeof rev.signal_timestamp === "number" || typeof rev.signal_timestamp === "string" ? (
                <div style={{ fontSize: "9px", color: "var(--text-muted)" }}>signal time {fmtTs(rev.signal_timestamp as any)}</div>
              ) : null}
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: "6px" }}>
              <span style={{ fontSize: "9px", fontWeight: 600, color: rev.complete ? "var(--green)" : "var(--amber)", border: `1px solid ${rev.complete ? "var(--green)" : "var(--amber)"}`, borderRadius: "4px", padding: "2px 6px" }}>
                {rev.complete ? (rev.new_entry_order_id?.startsWith("IMPORT-") ? "COMPLETE · MANUAL ENTRY" : "COMPLETE") : (rev.status || "PENDING_EXIT")}
              </span>
              {rev.fallback_used && (
                <span style={{ fontSize: "9px", fontWeight: 600, color: "var(--amber)", border: "1px solid var(--amber)", borderRadius: "4px", padding: "2px 6px" }}>
                  MARKET FALLBACK {rev.fallback_status || ""}
                </span>
              )}
            </div>
          </div>

          <div style={{ fontSize: "9px", color: "var(--text-muted)", marginTop: "4px" }}>
            SOURCE: DATABASE (reversals) · broker IDs: DHAN
          </div>
          {rev.new_entry_order_id?.startsWith("IMPORT-") && <div style={{ marginTop: 6, color: "var(--amber)", fontSize: 11 }}>The opposite entry was placed manually and later imported. This record does not prove automatic reversal entry succeeded.</div>}
          <div style={{ marginTop: 4, color: "var(--text-muted)", fontSize: 10 }}>Stops are monitored locally; an empty SL order ID means no resting Dhan stop order.</div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(120px, 1fr))", gap: "8px", marginTop: "10px" }}>
            <div>
              <div style={{ fontSize: "8px", color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>Trigger price</div>
              <div className="tabular-nums" style={{ fontSize: "14px", fontWeight: 600, color: "var(--amber)" }}>{rev.trigger_price ?? "—"}</div>
            </div>
            <div>
              <div style={{ fontSize: "8px", color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>Old trade</div>
              <div className="tabular-nums" style={{ fontSize: "11px", color: "var(--text-primary)" }}>{rev.old_trade_id || "—"}</div>
            </div>
            <div>
              <div style={{ fontSize: "8px", color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>New trade</div>
              <div className="tabular-nums" style={{ fontSize: "11px", color: "var(--text-primary)" }}>{rev.new_trade_id || "—"}</div>
            </div>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: "10px", marginTop: "10px" }}>
            <div style={{ border: "1px solid var(--border-subtle)", borderRadius: "6px", padding: "8px" }}>
              <div style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-primary)", marginBottom: "6px" }}>OLD TRADE (exit)</div>
              {[
                ["Trade ID", rev.old_trade_id],
                ["Position ID", rev.old_position_id],
                ["Exit order ID", rev.old_exit_order_id],
                ["Dhan order ID", rev.old_broker_order_id],
                ["Exit status", rev.old_exit_status],
                ["Exit fill", rev.old_exit_fill_price != null ? `${rev.old_exit_fill_price} @ ${rev.old_exit_filled_quantity ?? "—"} qty` : "—"],
                ["SL order ID", rev.old_sl_order_id],
                ["SL status", rev.old_sl_status],
                ["Flat verified", rev.exit_verified_at ? fmtTs(rev.exit_verified_at) : "pending"],
              ].map(([k, v]) => (
                <div key={String(k)} style={{ display: "flex", justifyContent: "space-between", gap: "8px", fontSize: "9px", padding: "2px 0" }}>
                  <span style={{ color: "var(--text-muted)" }}>{k}</span>
                  <span style={{ color: "var(--text-secondary)", fontFamily: "monospace", textAlign: "right", wordBreak: "break-all" }}>{v || "—"}</span>
                </div>
              ))}
            </div>
            <div style={{ border: "1px solid var(--border-subtle)", borderRadius: "6px", padding: "8px" }}>
              <div style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-primary)", marginBottom: "6px" }}>NEW TRADE (entry)</div>
              {[
                ["Trade ID", rev.new_trade_id],
                ["Position ID", rev.new_position_id],
                ["Entry order ID", rev.new_entry_order_id],
                ["Dhan order ID", rev.new_broker_order_id],
                ["Entry status", rev.new_entry_status],
                ["Entry fill", rev.new_entry_fill_price != null ? `${rev.new_entry_fill_price} @ ${rev.new_entry_filled_quantity ?? "—"} qty` : "—"],
                ["SL order ID", rev.new_sl_order_id],
                ["SL status", rev.new_sl_status],
                ["Fill confirmed", rev.entry_fill_confirmed_at ? fmtTs(rev.entry_fill_confirmed_at) : "pending"],
              ].map(([k, v]) => (
                <div key={String(k)} style={{ display: "flex", justifyContent: "space-between", gap: "8px", fontSize: "9px", padding: "2px 0" }}>
                  <span style={{ color: "var(--text-muted)" }}>{k}</span>
                  <span style={{ color: "var(--text-secondary)", fontFamily: "monospace", textAlign: "right", wordBreak: "break-all" }}>{v || "—"}</span>
                </div>
              ))}
            </div>
          </div>

          <div style={{ display: "flex", flexWrap: "wrap", gap: "4px", marginTop: "10px" }}>
            {rev.chain.map((s) => (
              <div key={s.step} style={{ display: "flex", alignItems: "center", gap: "4px", border: "1px solid var(--border)", background: s.ok ? "rgba(34,197,94,0.08)" : "var(--bg-panel-hover)", borderRadius: "4px", padding: "3px 6px" }}>
                <span style={{ width: "5px", height: "5px", borderRadius: "50%", background: s.ok ? "var(--green)" : "var(--muted-color, var(--text-muted))", flexShrink: 0 }} />
                <span style={{ fontSize: "8px", fontWeight: 600, color: "var(--text-secondary)" }}>{s.step}</span>
                {s.value != null && <span style={{ fontSize: "8px", color: "var(--text-muted)", fontFamily: "monospace", wordBreak: "break-all" }}>{String(s.value)}</span>}
                {s.detail ? <span style={{ fontSize: "8px", color: s.ok ? "var(--green)" : "var(--text-muted)" }}>· {s.detail}</span> : null}
              </div>
            ))}
          </div>

          <div style={{ fontSize: "9px", color: "var(--text-muted)", marginTop: "8px", display: "flex", gap: "12px", flexWrap: "wrap" }}>
            <span>created {fmtTs(rev.created_at)}</span>
            <span>updated {fmtTs(rev.updated_at)}</span>
            {rev.side ? <span>side {rev.side}</span> : null}
          </div>
        </div>
      ))}
    </div>
  );
}

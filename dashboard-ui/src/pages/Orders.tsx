import { useDataSelector } from "../store/DataProvider";
import { formatTimestamp } from "../lib/utils";

export default function Orders() {
  const orders = useDataSelector<any>((s) => s.orders);
  if (!orders) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "180px", height: "14px", marginBottom: "12px" }} />
      <div className="skeleton" style={{ width: "100%", height: "200px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading orders...</div>
    </div>
  );

  const stateColor = (state: string) => {
    switch (state?.toLowerCase()) {
      case "filled": return "var(--green)";
      case "rejected": return "var(--red)";
      case "cancelled": return "var(--text-muted)";
      case "created":
      case "submitted":
      case "acknowledged":
      case "partially_filled": return "var(--amber)";
      default: return "var(--amber)";
    }
  };

  return (
    <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" }}>
      <div style={{ padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)", fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>
        ORDERS ({orders.length})
      </div>
      {orders.length === 0 ? (
        <div className="animate-fade-in-up" style={{ padding: "40px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>No orders placed</div>
      ) : (
        <div className="ledger-grid-viewport">
          <div className="ledger-grid-head" style={{ display: "grid", gridTemplateColumns: "55px 95px 95px 70px 90px 50px 40px 46px 65px 95px 55px 75px", gap: "8px", padding: "8px 12px", fontSize: "9px", color: "var(--text-disabled)", textTransform: "uppercase", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-table-header)", position: "sticky", top: 0, zIndex: 1 }}>
            <span>Time</span><span>Local ID</span><span>Dhan ID</span><span>Instrument</span><span>Strategy</span><span>Side</span><span>Qty</span><span>Filled</span><span>Type</span><span>Limit / plan</span><span>Avg</span><span>Status</span>
          </div>
          {orders.map((o: any) => (
            <div key={o.order_id} className="ledger-grid-row hover-row" style={{ display: "grid", gridTemplateColumns: "55px 95px 95px 70px 90px 50px 40px 46px 65px 95px 55px 75px", gap: "8px", padding: "9px 12px", fontSize: "10px", borderBottom: "1px solid var(--border-subtle)", alignItems: "center" }}>
              <span className="tabular-nums" style={{ color: "var(--text-muted)" }}>{formatTimestamp(o.created_at)}</span>
              <span title={o.order_id} style={{ color: "var(--text-muted)", fontFamily: "monospace", fontSize: "9px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{o.order_id}</span>
              <span title={o.broker_order_id || "Not accepted by Dhan"} style={{ color: "var(--text-muted)", fontFamily: "monospace", fontSize: "9px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{o.broker_order_id || "—"}</span>
              <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{o.instrument}</span>
              <span style={{ color: "var(--text-muted)" }}>{o.strategy_id}</span>
              <span style={{ color: o.side === "BUY" ? "var(--green)" : "var(--red)", fontWeight: 600 }}>{o.side}</span>
              <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{o.quantity}</span>
              <span className="tabular-nums" style={{ color: o.filled_quantity && o.filled_quantity >= o.quantity ? "var(--green)" : "var(--text-secondary)" }}>{o.filled_quantity ?? 0}</span>
              <span style={{ color: "var(--text-muted)" }}>{o.order_type}</span>
              <span className="tabular-nums" title={o.price != null ? "Order limit price" : o.planned_entry_price != null ? "Planned price; actual order price unavailable" : "Price unavailable"} style={{ color: "var(--text-muted)" }}>{o.price != null ? o.price : o.planned_entry_price != null ? `${o.planned_entry_price} plan` : "—"}</span>
              <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{o.average_fill_price != null ? o.average_fill_price : "—"}</span>
              <span title={o.reason || ""} style={{
                color: stateColor(o.state),
                fontWeight: 600, fontSize: "9px",
              }}>{o.state}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

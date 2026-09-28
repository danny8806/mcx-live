import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../lib/api";
import { formatDT, formatINR, pnlColor, safeNum, safeINR } from "../lib/utils";

type View = "open" | "closed" | "all";

export default function Positions() {
  const [view, setView] = useState<View>("open");
  const [list, setList] = useState<any[] | null>(null);
  const [counts, setCounts] = useState<Record<string, number>>({ open: 0, closed: 0, all: 0 });
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<any>(null);
  const [detailBusy, setDetailBusy] = useState(false);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);

  useEffect(() => {
    (async () => {
      try {
        const [o, c, a] = await Promise.all([
          api.positions({ status: "open" }),
          api.positions({ status: "closed" }),
          api.positions({ status: "all" }),
        ]);
        if (mounted.current) {
          setCounts({
            open: o?.count ?? 0,
            closed: c?.count ?? 0,
            all: a?.count ?? 0,
          });
        }
      } catch { /* ignore */ }
    })();
  }, []);

  useEffect(() => {
    let timer: number;
    const load = async () => {
      try {
        const d = await api.positions({ status: view }) as any;
        if (mounted.current) setList(Array.isArray(d?.positions) ? d.positions : []);
      } catch { /* ignore */ }
    };
    load();
    timer = window.setInterval(load, 5000);
    return () => window.clearInterval(timer);
  }, [view]);

  const toggleDetail = useCallback(async (positionId: string) => {
    if (expandedId === positionId) {
      setExpandedId(null);
      setDetail(null);
      return;
    }
    setExpandedId(positionId);
    setDetail(null);
    setDetailBusy(true);
    try {
      const [p, pnl] = await Promise.all([
        api.position(positionId),
        api.positionPnl(positionId),
      ]);
      if (mounted.current) setDetail({ position: p, pnl });
    } catch { /* ignore */ } finally {
      if (mounted.current) setDetailBusy(false);
    }
  }, [expandedId]);

  if (!list) return (
    <div style={{ padding: "20px", color: "var(--text-muted)" }}>
      <div className="skeleton" style={{ width: "220px", height: "36px", marginBottom: "12px" }} />
      <div className="skeleton" style={{ width: "100%", height: "180px" }} />
      <div style={{ fontSize: "11px", marginTop: "8px" }}>Loading positions...</div>
    </div>
  );

  const tabs: { key: View; label: string }[] = [
    { key: "open", label: `Open (${counts.open})` },
    { key: "closed", label: `Closed (${counts.closed})` },
    { key: "all", label: `All (${counts.all})` },
  ];

  return (
    <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" }}>
      <div style={{ padding: "8px 12px", borderBottom: "1px solid var(--border-subtle)", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: "10px", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px" }}>
          POSITIONS ({list.length})
        </span>
        <div style={{ display: "flex", gap: "4px" }}>
          {tabs.map((t) => (
            <button
              key={t.key}
              onClick={() => setView(t.key)}
              style={{
                padding: "3px 10px", fontSize: "9px", borderRadius: "4px", cursor: "pointer",
                background: view === t.key ? "var(--blue)" : "transparent",
                color: view === t.key ? "#fff" : "var(--text-muted)",
                border: "1px solid var(--border)", fontWeight: 600,
                transition: "background 0.12s ease, color 0.12s ease",
              }}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>
      {list.length === 0 ? (
        <div className="animate-fade-in-up" style={{ padding: "40px", textAlign: "center", color: "var(--text-muted)", fontSize: "10px" }}>
          {view === "open" ? "No open positions — all flat" : "No positions in this view"}
        </div>
      ) : (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "70px 110px 50px 40px 80px 80px 55px 70px 60px 90px", gap: "8px", padding: "5px 12px", fontSize: "9px", color: "var(--text-disabled)", textTransform: "uppercase", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-table-header)", position: "sticky", top: 0, zIndex: 1 }}>
            <span>Instrument</span><span>Strategy</span><span>Side</span><span>Qty</span><span>Entry</span><span>LTP/Exit</span><span>SL</span><span>Margin</span><span>Status</span><span style={{ textAlign: "right" }}>P&L</span>
          </div>
          {list.map((p: any) => {
            const closed = p.status === "closed";
            const lastExit = (p.exit_fills ?? []).length ? (p.exit_fills[p.exit_fills.length - 1] as any).price : null;
            const mark = closed ? lastExit : p.current_mark;
            const pnl = closed ? p.realized_pnl : p.unrealized_pnl;
            const expanded = expandedId === p.position_id;
            return (
              <div key={p.position_id}>
                <div className="hover-row" onClick={() => toggleDetail(p.position_id)} style={{ display: "grid", gridTemplateColumns: "70px 110px 50px 40px 80px 80px 55px 70px 60px 90px", gap: "8px", padding: "5px 12px", fontSize: "10px", borderBottom: "1px solid var(--border-subtle)", alignItems: "center", cursor: "pointer" }}>
                  <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>{p.instrument}</span>
                  <span style={{ color: "var(--text-muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.strategy_id}</span>
                  <span style={{ color: p.side === "LONG" ? "var(--green)" : "var(--red)", fontWeight: 600 }}>{p.side}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{p.quantity}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-secondary)" }}>{safeINR(p.average_entry)}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-primary)" }}>{mark ? safeINR(mark) : "—"}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-muted)" }}>{p.stop_price ? safeINR(p.stop_price) : "—"}</span>
                  <span className="tabular-nums" style={{ color: "var(--text-muted)" }}>{formatINR(p.margin, false)}</span>
                  <span style={{ color: closed ? "var(--text-muted)" : "var(--amber)", fontFamily: "monospace", fontSize: "9px" }}>{closed ? (p.exit_reason || "closed") : "open"}</span>
                  <span className="tabular-nums" style={{ color: pnlColor(pnl), fontWeight: 600, textAlign: "right" }}>
                    {pnl >= 0 ? "+" : ""}₹{safeNum(pnl).toLocaleString("en-IN", { minimumFractionDigits: 2 })}
                  </span>
                </div>
                {expanded && (
                  <div style={{ padding: "8px 12px 10px 24px", background: "var(--bg-table-header)", borderBottom: "1px solid var(--border-subtle)" }}>
                    <div style={{ fontSize: "10px", color: "var(--text-primary)", fontWeight: 600, marginBottom: 6 }}>
                      POSITION DETAIL — {p.position_id}
                    </div>
                    {detailBusy ? (
                      <div style={{ fontSize: "9px", color: "var(--text-muted)" }}>Loading details...</div>
                    ) : detail?.position?.error ? (
                      <div style={{ fontSize: "9px", color: "var(--red)" }}>{detail.position.error}</div>
                    ) : detail ? (
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: "8px" }}>
                        {[
                          ["Position ID", detail.position?.position_id ?? "—"],
                          ["Trade ID", detail.position?.trade_id ?? "—"],
                          ["Entry Price", safeINR(detail.position?.average_entry)],
                          ["Mark Price", safeINR(detail.position?.current_mark)],
                          ["Realized P&L", `${safeNum(detail.position?.realized_pnl) >= 0 ? "+" : ""}₹${safeNum(detail.position?.realized_pnl).toLocaleString("en-IN", { minimumFractionDigits: 2 })}`],
                          ["Unrealized P&L", `${safeNum(detail.position?.unrealized_pnl) >= 0 ? "+" : ""}₹${safeNum(detail.position?.unrealized_pnl).toLocaleString("en-IN", { minimumFractionDigits: 2 })}`],
                          ["Multiplier", String(detail.position?.multiplier ?? "—")],
                          ["Margin", `${safeNum(detail.position?.margin).toLocaleString("en-IN", { minimumFractionDigits: 2 })}`],
                          ["Entry Time", formatDT(detail.position?.entry_timestamp)],
                          ["Exit Time", (() => { const ef = detail.position?.exit_fills ?? []; return ef.length ? formatDT(ef[ef.length - 1].timestamp) : "—"; })()],
                          ["Entry Fills", `${(detail.position?.entry_fill_ids ?? []).length}`],
                          ["Exit Fills", `${(detail.position?.exit_fills ?? []).length}`],
                        ].map(([k, v]) => (
                          <div key={String(k)} style={{ background: "var(--bg-panel)", border: "1px solid var(--border-subtle)", borderRadius: 4, padding: "6px 8px" }}>
                            <div style={{ fontSize: 8, color: "var(--text-disabled)", textTransform: "uppercase", letterSpacing: "0.4px" }}>{String(k)}</div>
                            <div className="tabular-nums" style={{ fontSize: 11, fontWeight: 500, color: k === "Realized P&L" || k === "Unrealized P&L" ? pnlColor(safeNum(detail.position?.[k === "Realized P&L" ? "realized_pnl" : "unrealized_pnl"])) : "var(--text-primary)", fontFamily: k === "Realized P&L" || k === "Unrealized P&L" ? "monospace" : undefined }}>
                              {String(v)}
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </div>
                )}
              </div>
            );
          })}
        </>
      )}
    </div>
  );
}

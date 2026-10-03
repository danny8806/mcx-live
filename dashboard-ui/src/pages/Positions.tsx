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
  const [loadError, setLoadError] = useState<string | null>(null);
  const [lastSuccessAt, setLastSuccessAt] = useState<number | null>(null);
  const [brokerSnapshot, setBrokerSnapshot] = useState<any>(null);
  const [brokerError, setBrokerError] = useState<string | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);

  useEffect(() => {
    let timer: number;
    const load = async () => {
      const [localResult, brokerResult] = await Promise.allSettled([
        api.positions({ status: "all" }),
        api.livePositions(),
      ]);
      if (brokerResult.status === "fulfilled") {
        if (mounted.current) {
          setBrokerSnapshot(brokerResult.value);
          setBrokerError(null);
        }
      } else if (mounted.current) {
        setBrokerError(brokerResult.reason?.message || String(brokerResult.reason));
      }
      try {
        if (localResult.status === "rejected") throw localResult.reason;
        const d = localResult.value as any;
        if (!Array.isArray(d?.positions)) throw new Error(d?.error || "Invalid positions response");
        if (mounted.current) {
          const all = d.positions;
          const open = all.filter((position: any) => position.is_open);
          const closed = all.filter((position: any) => !position.is_open);
          setCounts({ open: open.length, closed: closed.length, all: all.length });
          setList(view === "open" ? open : view === "closed" ? closed : all);
          setLoadError(null);
          setLastSuccessAt(Date.now());
        }
      } catch (error: any) { if (mounted.current) setLoadError(error?.message || String(error)); }
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

  if (!list && loadError) return <div role="alert" className="desk-warning">Positions unavailable: {loadError}</div>;
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
  const brokerUnmatched = (brokerSnapshot?.positions ?? []).filter(
    (row: any) => row.status !== "MATCHED",
  );

  return (
    <div className="lift animate-fade-in-up" style={{ background: "var(--bg-panel)", border: "1px solid var(--border)", borderRadius: "8px", overflow: "hidden" }}>
      {loadError && <div role="alert" className="desk-warning">Position refresh failed. Showing the last successful snapshot from {lastSuccessAt ? new Date(lastSuccessAt).toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata" }) : "unknown time"}: {loadError}</div>}
      {brokerError ? <div role="alert" className="desk-warning">Dhan position comparison is unavailable: {brokerError}. An empty local list does not confirm that the broker account is flat.</div> : brokerUnmatched.length > 0 && <div role="alert" className="desk-warning" style={{ display: "block" }}>
        <strong>Broker and local positions do not match — do not treat this account as flat.</strong>
        <div style={{ marginTop: 6, fontSize: 10 }}>Dhan shows exposure without a matching open local position. Stop monitoring and ownership need operator reconciliation; this dashboard will not invent a local trade or send an order.</div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 6, marginTop: 8 }}>
          {brokerUnmatched.map((row: any) => <div key={row.instrument} style={{ border: "1px solid var(--border-subtle)", borderRadius: 4, padding: "6px 8px", background: "var(--bg-panel)" }}>
            <b>{row.instrument}</b> · <span style={{ color: "var(--red)", fontWeight: 700 }}>{row.status}</span>
            <div style={{ marginTop: 3 }}>Dhan: {row.dhan?.side ?? "—"} {row.dhan?.quantity ?? "—"} @ {safeINR(row.dhan?.average_entry_price)}</div>
            <div>Local: {row.local ? `${row.local.side} ${row.local.quantity}` : "no open position record"}</div>
            {row.dhan?.ltp != null && <div>LTP: {safeINR(row.dhan.ltp)}</div>}
          </div>)}
        </div>
        <div style={{ marginTop: 6, fontSize: 9, color: "var(--text-muted)" }}>Dhan snapshot age: {brokerSnapshot?.dhan_position_age_seconds == null ? "unknown" : `${Math.max(0, Math.floor(brokerSnapshot.dhan_position_age_seconds))}s`}</div>
      </div>}
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
          {view === "open" ? (brokerError ? "No local position records; broker state unavailable" : brokerUnmatched.length ? "No local open positions — broker exposure is listed above" : "No open local positions") : "No positions in this view"}
        </div>
      ) : (
        <div className="ledger-grid-viewport">
          <div className="ledger-grid-head" style={{ display: "grid", gridTemplateColumns: "70px 110px 50px 40px 80px 80px 55px 70px 60px 90px", gap: "8px", padding: "8px 12px", fontSize: "9px", color: "var(--text-disabled)", textTransform: "uppercase", borderBottom: "1px solid var(--border-subtle)", background: "var(--bg-table-header)", position: "sticky", top: 0, zIndex: 1 }}>
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
                <div className="ledger-grid-row hover-row" onClick={() => toggleDetail(p.position_id)} style={{ display: "grid", gridTemplateColumns: "70px 110px 50px 40px 80px 80px 55px 70px 60px 90px", gap: "8px", padding: "9px 12px", fontSize: "10px", borderBottom: "1px solid var(--border-subtle)", alignItems: "center", cursor: "pointer" }}>
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
        </div>
      )}
    </div>
  );
}

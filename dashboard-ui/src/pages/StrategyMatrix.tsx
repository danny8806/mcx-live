import { useState, useMemo, useCallback, memo, useEffect } from "react";
import { useDataSelector } from "../store/DataProvider";
import { formatINR, pnlColor, statusDot } from "../lib/utils";
import { api } from "../lib/api";
import StrategyDetail from "../components/strategies/StrategyDetail";
import StrategyCompare from "../components/strategies/StrategyCompare";

type SortKey = "net_pnl" | "win_rate" | "profit_factor" | "max_drawdown" | "trade_count" | null;
type SortDir = "asc" | "desc";
type InstrumentFilter = "ALL" | "GOLDM" | "SILVERM";
type StatusFilter = "ALL" | "FLAT" | "IN_POSITION" | "PENDING" | "EXITING";
type SignalFilter = "ALL" | "LONG" | "SHORT" | "FLAT";
type PerfFilter = "ALL" | "PROFITABLE" | "LOSING";

function StrategyRow({
  s,
  expanded,
  onToggle,
  marketData,
  sortBy: _sortBy,
  sortDir: _sortDir,
  onSort: _onSort,
}: {
  s: any;
  expanded: boolean;
  onToggle: () => void;
  marketData: any;
  sortBy: SortKey;
  sortDir: SortDir;
  onSort: (key: SortKey) => void;
}) {
  const side = s.position_side;
  const sideColor =
    side === "LONG"
      ? "var(--green)"
      : side === "SHORT"
      ? "var(--red)"
      : "var(--text-muted)";
  const stateColor = statusDot(s.state || "flat");

  return (
    <>
      <div
        onClick={onToggle}
        style={{
          display: "grid",
          gridTemplateColumns:
            "130px 65px 40px 40px 65px 50px 65px 45px 45px 55px 40px 48px 60px",
          gap: 0,
          fontSize: 10,
          padding: "5px 12px",
          borderBottom: "1px solid var(--border-subtle)",
          alignItems: "center",
          cursor: "pointer",
          background: expanded ? "var(--bg-panel-hover)" : "transparent",
          transition: "background 0.1s",
        }}
        onMouseEnter={(e) => {
          if (!expanded) e.currentTarget.style.background = "var(--bg-panel-hover)";
        }}
        onMouseLeave={(e) => {
          if (!expanded) e.currentTarget.style.background = "transparent";
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
          <span
            style={{
              width: 5,
              height: 5,
              borderRadius: "50%",
              background: stateColor,
              flexShrink: 0,
            }}
          />
          <span style={{ color: "var(--text-primary)", fontWeight: 500 }}>
            {s.strategy_id}
          </span>
        </div>
        <span style={{ color: "var(--text-secondary)", fontSize: 9 }}>
          {s.instrument}
        </span>
        <span style={{ color: "var(--text-muted)", fontSize: 9 }}>
          {s.fast_timeframe}
        </span>
        <span style={{ color: "var(--text-muted)", fontSize: 9 }}>
          {s.htf_timeframe}
        </span>
        <span
          style={{
            color: stateColor,
            fontSize: 8,
            fontWeight: 600,
            textTransform: "uppercase",
          }}
        >
          {s.state || "FLAT"}
        </span>
        <span
          style={{
            color: sideColor,
            fontSize: 9,
            fontWeight: 500,
          }}
        >
          {side || "—"}
        </span>
        <span
          style={{
            color: side ? sideColor : "var(--text-muted)",
            fontSize: 9,
            textAlign: "center",
          }}
        >
          {side ? `${s.quantity || 1} ${side}` : "0"}
        </span>
        <span
          style={{
            color: "var(--text-secondary)",
            fontVariantNumeric: "tabular-nums",
            textAlign: "right",
            fontSize: 9,
          }}
        >
          {s.trade_count}
        </span>
        <span
          style={{
            color: "var(--text-secondary)",
            textAlign: "right",
            fontSize: 9,
          }}
        >
          {s.win_rate.toFixed(0)}%
        </span>
        <span
          style={{
            color: pnlColor(s.realized_net),
            fontWeight: 600,
            fontVariantNumeric: "tabular-nums",
            textAlign: "right",
            fontSize: 9,
          }}
        >
          {formatINR(s.realized_net)}
        </span>
        <span
          style={{
            color: "var(--text-muted)",
            textAlign: "right",
            fontSize: 9,
          }}
        >
          {s.bars_processed}
        </span>
        <span
          style={{
            color: s.realized_gross > 0 ? "var(--green)" : s.realized_gross < 0 ? "var(--red)" : "var(--text-muted)",
            textAlign: "right",
            fontSize: 9,
          }}
        >
          {s.realized_gross ? formatINR(s.realized_gross) : "—"}
        </span>
        <span
          style={{
            color: "var(--text-muted)",
            textAlign: "right",
            fontSize: 9,
          }}
        >
          {s.enabled ? "ON" : "OFF"}
        </span>
      </div>
      {expanded && (
        <div
          style={{
            background: "var(--bg-root)",
            borderBottom: "1px solid var(--border-subtle)",
          }}
        >
          <StrategyDetail strategyId={s.strategy_id} marketData={marketData} />
        </div>
      )}
    </>
  );
}

const MemoRow = memo(StrategyRow, (prev, next) => {
  return (
    prev.s === next.s &&
    prev.expanded === next.expanded &&
    prev.marketData === next.marketData
  );
});

export default function StrategyMatrix() {
  const strategies = useDataSelector<any[]>((s) => s.strategies);
  const marketData = useDataSelector<any>((s) => s.marketData);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [instrumentFilter, setInstrumentFilter] = useState<InstrumentFilter>("ALL");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("ALL");
  const [signalFilter, setSignalFilter] = useState<SignalFilter>("ALL");
  const [perfFilter, setPerfFilter] = useState<PerfFilter>("ALL");
  const [search, setSearch] = useState("");
  const [sortBy, setSortBy] = useState<SortKey>(null);
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [compareIds, setCompareIds] = useState<string[]>([]);
  const [showCompare, setShowCompare] = useState(false);
  // ### OBSERVATION (compare data source)
  // The compare dialog used to receive placeholder metrics (profit_factor null,
  // max_drawdown 0, expectancy 0) plus `equityCurves={{}}`, so every strategy
  // showed "N/A"/"₹0" and the curves never rendered.  The analytics module
  // already computes real per-strategy performance from the trades ledger;
  // load it (and the per-strategy equity curves on demand) so the compare
  // table and the normalized curves show truthful values.
  const [analytics, setAnalytics] = useState<Record<string, any>>({});
  const [equityCurves, setEquityCurves] = useState<Record<string, any[]>>({});
  const [startingCapital, setStartingCapital] = useState<number | null>(null);

  useEffect(() => {
    let alive = true;
    api.analyticsStrategies()
      .then((d: any) => {
        if (!alive || !d?.strategies) return;
        const map: Record<string, any> = {};
        d.strategies.forEach((s: any) => { if (s?.strategy_id) map[s.strategy_id] = s; });
        setAnalytics(map);
      })
      .catch(() => { /* analytics may be uninitialized; fall back to live snapshot */ });
    api.overview()
      .then((d: any) => {
        if (!alive) return;
        const raw = d?.starting_capital;
        const cap = raw && typeof raw === "object" && "value" in raw ? raw.value : raw;
        if (typeof cap === "number" && cap > 0) setStartingCapital(cap);
      })
      .catch(() => {});
    return () => { alive = false; };
  }, []);

  useEffect(() => {
    if (!compareIds.length || !showCompare) return;
    let alive = true;
    compareIds.forEach((id) => {
      if (equityCurves[id]) return;
      api.analyticsStrategyEquity(id)
        .then((d: any) => {
          if (!alive || !d?.equity_curve) return;
          const pts = (d.equity_curve as any[])
            .map((r: any) => ({ timestamp: Number(r.timestamp ?? 0), equity: Number(r.equity ?? 0) }))
            .sort((a: any, b: any) => a.timestamp - b.timestamp);
          setEquityCurves((prev) => ({ ...prev, [id]: pts }));
        })
        .catch(() => {});
    });
    return () => { alive = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [compareIds, showCompare]);

  const handleSort = useCallback(
    (key: SortKey) => {
      if (sortBy === key) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
      } else {
        setSortBy(key);
        setSortDir("desc");
      }
    },
    [sortBy]
  );

  const toggleCompare = useCallback((id: string) => {
    setCompareIds((prev) =>
      prev.includes(id)
        ? prev.filter((x) => x !== id)
        : prev.length < 4
        ? [...prev, id]
        : prev
    );
  }, []);

  const filtered = useMemo(() => {
    if (!strategies?.length) return [];
    let list = [...strategies];

    if (instrumentFilter !== "ALL") {
      list = list.filter((s: any) => s.instrument === instrumentFilter);
    }
    if (statusFilter !== "ALL") {
      list = list.filter((s: any) => {
        const st = (s.state || "flat").toLowerCase();
        if (statusFilter === "FLAT") return st === "flat";
        if (statusFilter === "IN_POSITION") return st === "long_position" || st === "short_position";
        if (statusFilter === "PENDING") return st.startsWith("pending_") || st === "signal_long" || st === "signal_short" || st === "entry_triggered";
        if (statusFilter === "EXITING") return st === "exit_pending" || st === "exit_order_submitted" || st === "stop_active";
        return true;
      });
    }
    if (signalFilter !== "ALL") {
      if (signalFilter === "FLAT") {
        list = list.filter((s: any) => !s.position_side);
      } else {
        list = list.filter((s: any) => s.position_side === signalFilter);
      }
    }
    if (perfFilter !== "ALL") {
      if (perfFilter === "PROFITABLE") {
        list = list.filter((s: any) => s.realized_net > 0);
      } else {
        list = list.filter((s: any) => s.realized_net < 0);
      }
    }
    if (search) {
      const q = search.toLowerCase();
      list = list.filter((s: any) => s.strategy_id?.toLowerCase().includes(q));
    }
    if (sortBy) {
      list.sort((a: any, b: any) => {
        const av = a[sortBy] ?? 0;
        const bv = b[sortBy] ?? 0;
        return sortDir === "asc" ? av - bv : bv - av;
      });
    }
    return list;
  }, [
    strategies,
    instrumentFilter,
    statusFilter,
    signalFilter,
    perfFilter,
    search,
    sortBy,
    sortDir,
  ]);

  const summary = useMemo(() => {
    if (!strategies?.length)
      return {
        total: 0,
        running: 0,
        long: 0,
        short: 0,
        flat: 0,
        profitable: 0,
        losing: 0,
      };
    return {
      total: strategies.length,
      running: strategies.filter(
        (s: any) =>
          (s.state || "").toLowerCase() !== "stopped" &&
          (s.state || "").toLowerCase() !== "error"
      ).length,
      long: strategies.filter((s: any) => s.position_side === "LONG").length,
      short: strategies.filter((s: any) => s.position_side === "SHORT").length,
      flat: strategies.filter((s: any) => !s.position_side).length,
      profitable: strategies.filter((s: any) => s.realized_net > 0).length,
      losing: strategies.filter((s: any) => s.realized_net < 0).length,
    };
  }, [strategies]);

  const analyticsStrategies = useMemo(() => {
    return filtered.map((s: any) => {
      const a = analytics[s.strategy_id];
      return {
        strategy_id: s.strategy_id,
        instrument: s.instrument,
        trade_count: a?.trade_count ?? s.trade_count ?? 0,
        win_rate: a?.win_rate ?? s.win_rate ?? 0,
        profit_factor: a?.profit_factor != null ? a.profit_factor : null,
        net_pnl: a?.net_pnl ?? s.realized_net ?? 0,
        max_drawdown: a?.max_drawdown ?? 0,
        expectancy: a?.expectancy ?? 0,
        sample_warning: a?.sample_warning,
      };
    });
  }, [filtered, analytics]);

  const filterBtn = (active: boolean) => ({
    background: active ? "var(--bg-panel-active)" : "transparent",
    border: `1px solid ${active ? "var(--border-active)" : "var(--border-subtle)"}`,
    borderRadius: 3,
    color: active ? "var(--text-primary)" : "var(--text-muted)",
    padding: "2px 8px",
    fontSize: 9,
    fontWeight: active ? 600 : 400,
    cursor: "pointer" as const,
    textTransform: "uppercase" as const,
  });

  if (!strategies || strategies.length === 0) {
    return (
      <div
        style={{
          background: "var(--bg-panel)",
          border: "1px solid var(--border)",
          borderRadius: 6,
          padding: "40px 20px",
          textAlign: "center",
          color: "var(--text-disabled)",
          fontSize: 11,
        }}
      >
        NO STRATEGIES CONFIGURED
      </div>
    );
  }

  return (
    <div
      style={{
        background: "var(--bg-panel)",
        border: "1px solid var(--border)",
        borderRadius: 6,
        overflow: "hidden",
      }}
    >
      <div
        style={{
          padding: "6px 12px",
          borderBottom: "1px solid var(--border-subtle)",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <div
          style={{
            fontSize: 10,
            fontWeight: 600,
            color: "var(--text-muted)",
            textTransform: "uppercase",
            letterSpacing: "0.5px",
          }}
        >
          STRATEGY MATRIX ({filtered.length})
        </div>
        <button
          onClick={() => setShowCompare(!showCompare)}
          style={{
            background: showCompare ? "var(--blue-muted)" : "transparent",
            border: `1px solid ${showCompare ? "var(--blue)" : "var(--border-subtle)"}`,
            borderRadius: 3,
            color: showCompare ? "var(--blue)" : "var(--text-muted)",
            padding: "2px 8px",
            fontSize: 8,
            fontWeight: 600,
            cursor: "pointer",
            textTransform: "uppercase" as const,
          }}
        >
          COMPARE
        </button>
      </div>

      <div
        style={{
          padding: "5px 12px",
          borderBottom: "1px solid var(--border-subtle)",
          display: "flex",
          alignItems: "center",
          gap: 6,
          flexWrap: "wrap",
        }}
      >
        <div style={{ display: "flex", gap: 3, marginRight: 6 }}>
          {(["ALL", "GOLDM", "SILVERM"] as InstrumentFilter[]).map((f) => (
            <button
              key={f}
              onClick={() => setInstrumentFilter(f)}
              style={filterBtn(instrumentFilter === f)}
            >
              {f}
            </button>
          ))}
        </div>

        <input
          type="text"
          placeholder="Search strategy..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            background: "var(--bg-input)",
            border: "1px solid var(--border-subtle)",
            borderRadius: 3,
            color: "var(--text-primary)",
            padding: "2px 8px",
            fontSize: 9,
            width: 110,
            outline: "none",
          }}
        />

        <div style={{ display: "flex", gap: 3, marginLeft: 4 }}>
          {(["ALL", "FLAT", "IN_POSITION", "PENDING", "EXITING"] as StatusFilter[]).map((f) => (
            <button
              key={f}
              onClick={() => setStatusFilter(f)}
              style={filterBtn(statusFilter === f)}
            >
              {f}
            </button>
          ))}
        </div>

        <div style={{ display: "flex", gap: 3, marginLeft: 4 }}>
          {(["ALL", "LONG", "SHORT", "FLAT"] as SignalFilter[]).map((f) => (
            <button
              key={f}
              onClick={() => setSignalFilter(f)}
              style={filterBtn(signalFilter === f)}
            >
              {f}
            </button>
          ))}
        </div>

        <div style={{ display: "flex", gap: 3, marginLeft: 4 }}>
          {(["ALL", "PROFITABLE", "LOSING"] as PerfFilter[]).map((f) => (
            <button
              key={f}
              onClick={() => setPerfFilter(f)}
              style={filterBtn(perfFilter === f)}
            >
              {f}
            </button>
          ))}
        </div>
      </div>

      <div
        style={{
          padding: "4px 12px",
          borderBottom: "1px solid var(--border-subtle)",
          display: "flex",
          alignItems: "center",
          gap: 10,
          fontSize: 10,
          color: "var(--text-secondary)",
          flexWrap: "wrap",
        }}
      >
        <span>
          <span style={{ color: "var(--text-primary)", fontWeight: 600 }}>
            {summary.total}
          </span>{" "}
          Strategies
        </span>
        <span style={{ color: "var(--border)" }}>|</span>
        <span>
          <span style={{ color: "var(--green)", fontWeight: 600 }}>
            {summary.running}
          </span>{" "}
          Running
        </span>
        <span style={{ color: "var(--border)" }}>|</span>
        <span>
          <span style={{ color: "var(--green)", fontWeight: 600 }}>
            {summary.long}
          </span>{" "}
          Long
        </span>
        <span style={{ color: "var(--border)" }}>|</span>
        <span>
          <span style={{ color: "var(--red)", fontWeight: 600 }}>
            {summary.short}
          </span>{" "}
          Short
        </span>
        <span style={{ color: "var(--border)" }}>|</span>
        <span>
          <span style={{ color: "var(--text-muted)", fontWeight: 600 }}>
            {summary.flat}
          </span>{" "}
          Flat
        </span>
        <span style={{ color: "var(--border)" }}>|</span>
        <span>
          <span style={{ color: "var(--green)", fontWeight: 600 }}>
            {summary.profitable}
          </span>{" "}
          Profitable
        </span>
        <span style={{ color: "var(--border)" }}>|</span>
        <span>
          <span style={{ color: "var(--red)", fontWeight: 600 }}>
            {summary.losing}
          </span>{" "}
          Losing
        </span>
      </div>

      {showCompare && (
        <div
          style={{
            padding: "8px 12px",
            borderBottom: "1px solid var(--border-subtle)",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              marginBottom: 6,
              flexWrap: "wrap",
            }}
          >
            <span
              style={{
                fontSize: 9,
                fontWeight: 600,
                color: "var(--text-disabled)",
                textTransform: "uppercase",
              }}
            >
              Compare:
            </span>
            {filtered.map((s: any) => {
              const active = compareIds.includes(s.strategy_id);
              return (
                <button
                  key={s.strategy_id}
                  onClick={() => toggleCompare(s.strategy_id)}
                  style={{
                    background: active ? "var(--blue-muted)" : "transparent",
                    border: `1px solid ${active ? "var(--blue)" : "var(--border-subtle)"}`,
                    borderRadius: 3,
                    color: active ? "var(--blue)" : "var(--text-muted)",
                    padding: "1px 6px",
                    fontSize: 8,
                    cursor: "pointer",
                  }}
                >
                  {s.strategy_id}
                </button>
              );
            })}
          </div>
          <StrategyCompare
            strategies={analyticsStrategies}
            selectedIds={compareIds}
            onToggle={toggleCompare}
            equityCurves={equityCurves}
            startingEquity={startingCapital}
          />
        </div>
      )}

      <div style={{ overflowX: "auto" }}>
        <div
          style={{
            display: "grid",
            gridTemplateColumns:
              "130px 65px 40px 40px 65px 50px 65px 45px 45px 55px 40px 48px 60px",
            gap: 0,
            fontSize: 9,
            color: "var(--text-disabled)",
            textTransform: "uppercase",
            padding: "4px 12px",
            borderBottom: "1px solid var(--border-subtle)",
            background: "var(--bg-table-header)",
            letterSpacing: "0.3px",
          }}
        >
          <span>Strategy</span>
          <span>Inst</span>
          <span>TF</span>
          <span>HTF</span>
          <span>Status</span>
          <span>Side</span>
          <span style={{ textAlign: "center" }}>Position</span>
          <span
            style={{
              textAlign: "right",
              cursor: "pointer",
            }}
            onClick={() => handleSort("trade_count")}
          >
            Trades {sortBy === "trade_count" ? (sortDir === "asc" ? "↑" : "↓") : ""}
          </span>
          <span
            style={{
              textAlign: "right",
              cursor: "pointer",
            }}
            onClick={() => handleSort("win_rate")}
          >
            Win% {sortBy === "win_rate" ? (sortDir === "asc" ? "↑" : "↓") : ""}
          </span>
          <span
            style={{
              textAlign: "right",
              cursor: "pointer",
            }}
            onClick={() => handleSort("net_pnl")}
          >
            P&L {sortBy === "net_pnl" ? (sortDir === "asc" ? "↑" : "↓") : ""}
          </span>
          <span style={{ textAlign: "right" }}>Bars</span>
          <span style={{ textAlign: "right" }}>Gross</span>
          <span style={{ textAlign: "right" }}>En</span>
        </div>

        {filtered.length === 0 ? (
          <div
            style={{
              padding: "20px 12px",
              textAlign: "center",
              color: "var(--text-disabled)",
              fontSize: 10,
            }}
          >
            No strategies match filters
          </div>
        ) : (
          filtered.map((s: any) => (
            <MemoRow
              key={s.strategy_id}
              s={s}
              expanded={expandedId === s.strategy_id}
              onToggle={() =>
                setExpandedId((prev) =>
                  prev === s.strategy_id ? null : s.strategy_id
                )
              }
              marketData={marketData}
              sortBy={sortBy}
              sortDir={sortDir}
              onSort={handleSort}
            />
          ))
        )}
      </div>
    </div>
  );
}

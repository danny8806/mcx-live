import { Activity, ArrowDownRight, ArrowUpRight, CircleCheck, Clock3, ShieldAlert, Waypoints } from "lucide-react";
import { formatINR, formatTimestamp, pnlColor, safeINR, safeNum } from "../lib/utils";
import { useDataSelector } from "../store/DataProvider";
import { quoteIsFresh, quoteStatus, tickAge } from "../lib/market";

const activeOrderStates = new Set(["submitted", "acknowledged", "partially_filled", "partial", "partially filled", "pending", "open", "transit", "part_traded"]);

function orderState(order: any): string {
  return String(order.state ?? order.status ?? order.order_status ?? "").trim().toLowerCase().replace(/[ -]+/g, "_");
}

function StateTag({ children, kind = "neutral" }: { children: React.ReactNode; kind?: "good" | "bad" | "warn" | "neutral" }) {
  return <span className={`state-tag ${kind}`}><i/>{children}</span>;
}

function Value({ label, value, emphasis }: { label: string; value: React.ReactNode; emphasis?: string }) {
  return <div className="desk-value"><small>{label}</small><strong className={emphasis}>{value}</strong></div>;
}

export default function LiveTrading() {
  const overview = useDataSelector<any>((s) => s.overview);
  const pnl = useDataSelector<any>((s) => s.pnl);
  const strategies = useDataSelector<any[]>((s) => s.strategies);
  const positions = useDataSelector<any[]>((s) => s.positions);
  const orders = useDataSelector<any[]>((s) => s.orders);
  const fills = useDataSelector<any[]>((s) => s.fills);
  const marketData = useDataSelector<any>((s) => s.marketData);
  const gold = useDataSelector<any>((s) => s.goldOverview);
  const silver = useDataSelector<any>((s) => s.silverOverview);
  const connected = useDataSelector<boolean>((s) => s.connected);
  const snapshotStatus = useDataSelector<any>((s) => s.snapshotStatus);
  const lastError = useDataSelector<string | null>((s) => s.lastError);
  const reconciliation = useDataSelector<any>((s) => s.reconciliation);
  const brokerPnl = useDataSelector<any>((s) => s.brokerPnl);
  if (!overview || snapshotStatus.overview !== "live") return <div className="desk-unavailable"><ShieldAlert size={21}/><div><strong>Current account snapshot unavailable</strong><p>The dashboard has not received a successful account snapshot, so positions, triggers, and P&amp;L cannot be confirmed.</p>{lastError && <small>{lastError}</small>}</div></div>;

  const open = positions.filter((position: any) => position.is_open);
  const overviewKnown = Boolean(overview.execution_mode && !["UNKNOWN", "UNAVAILABLE"].includes(String(overview.execution_mode).toUpperCase()));
  const positionsKnown = snapshotStatus.positions === "live";
  const strategiesKnown = snapshotStatus.strategies === "live";
  const ordersKnown = snapshotStatus.orders === "live";
  const fillsKnown = snapshotStatus.fills === "live";
  const getLtp = (instrument: string) => safeNum(marketData?.instruments?.[instrument]?.ltp ?? (instrument === "GOLDM" ? gold?.ltp : instrument === "SILVERM" ? silver?.ltp : undefined));
  const instruments = [...new Set([...Object.keys(marketData?.instruments ?? {}), ...strategies.map((s: any) => s.instrument), ...open.map((p: any) => p.instrument)].filter(Boolean))].sort();
  const activeTriggers = strategiesKnown ? strategies.flatMap((strategy: any) => [
    strategy.pending_exit_trigger ? { ...strategy.pending_exit_trigger, strategy, triggerKind: "REVERSAL EXIT" } : null,
    strategy.pending_entry ? { ...strategy.pending_entry, strategy, triggerKind: strategy.pending_entry?.metadata?.is_reversal_entry ? "REVERSAL ENTRY" : "ENTRY" } : null,
  ].filter(Boolean) as any[]) : [];
  const workingOrders = orders.filter((order: any) => activeOrderStates.has(orderState(order)));
  const unclassifiedOrders = orders.filter((order: any) => !activeOrderStates.has(orderState(order)) && !new Set(["created", "filled", "rejected", "cancelled", "canceled", "expired", "complete", "completed"]).has(orderState(order)));

  return <div className="live-floor">
    <section className="desk-hero">
      <div><div className="eyebrow">REAL-TIME EXECUTION</div><h2>Live execution desk</h2><p>Runtime state, armed triggers, broker orders, open positions, and local stop monitoring.</p></div>
      <div className="desk-hero-status"><StateTag kind={connected ? "good" : "bad"}>{connected ? "Dashboard socket connected" : "Dashboard socket disconnected"}</StateTag><StateTag kind={marketData?.ws_connected ? "good" : "warn"}>{marketData?.ws_connected ? "Market feed connected" : "Market feed disconnected"}</StateTag><StateTag kind={overview.execution_mode === "LIVE" ? "bad" : "neutral"}>{overview.execution_mode || "MODE UNKNOWN"}</StateTag></div>
    </section>

    {overview.kill_switch && <div className="desk-warning"><ShieldAlert size={20}/><div><b>Kill switch is active</b><small>Trading is halted according to the current runtime state.</small></div></div>}
    {reconciliation?.is_consistent === false && <div className="desk-warning"><ShieldAlert size={20}/><div><b>Broker and local ledgers differ</b><small>{reconciliation.summary?.total_errors ?? reconciliation.errors?.length ?? "?"} reconciliation error(s). Check Reconciliation before relying on local fills or P&amp;L.</small></div></div>}
    {reconciliation?._fetch_error && <div className="desk-warning"><ShieldAlert size={20}/><div><b>Reconciliation refresh failed</b><small>Last confirmed snapshot: {reconciliation._fetched_at ? formatTimestamp(reconciliation._fetched_at) : "unknown"}. Check Reconciliation.</small></div></div>}

    <section className="desk-metrics">
      <Value label="Open positions" value={positionsKnown ? open.length : "—"}/>
      <Value label="Armed triggers" value={strategiesKnown ? activeTriggers.length : "—"}/>
      <Value label="Working broker orders" value={ordersKnown ? workingOrders.length : "—"}/>
      <Value label="Local unrealized P&L" value={pnl ? formatINR(pnl.unrealized_pnl) : "—"} emphasis={pnlColor(pnl?.unrealized_pnl)}/>
      <Value label="Local realized P&L" value={pnl ? formatINR(pnl.realized_pnl) : "—"} emphasis={pnlColor(pnl?.realized_pnl)}/>
      <Value label="Dhan available margin" value={overviewKnown ? safeINR(overview.available_margin) : "—"}/>
    </section>

    <section className="desk-section">
      <div className="desk-section-heading"><div><div className="eyebrow">P&amp;L SOURCES</div><h3>Dhan and local book</h3></div><span className="source-note">{brokerPnl?._fetched_at ? `Fetched ${formatTimestamp(brokerPnl._fetched_at)}` : "Awaiting broker data"}</span></div>
      {brokerPnl?._fetch_error && <div role="alert" className="desk-warning">Broker P&amp;L refresh failed. Values below are from the last successful fetch: {brokerPnl._fetch_error}</div>}
      {brokerPnl?.dhan && brokerPnl?.local ? <div className="account-grid">
        <Value label="Dhan reported net" value={formatINR(brokerPnl.dhan.net_pnl)} emphasis={pnlColor(brokerPnl.dhan.net_pnl)}/>
        <Value label="Local net after charges" value={formatINR(brokerPnl.local.net_pnl)} emphasis={pnlColor(brokerPnl.local.net_pnl)}/>
        <Value label="Dhan minus local" value={formatINR(brokerPnl.difference?.net_pnl)} emphasis={pnlColor(brokerPnl.difference?.net_pnl)}/>
        <Value label="Dhan data age" value={brokerPnl.dhan.age_seconds == null ? "Unknown" : `${Math.floor(brokerPnl.dhan.age_seconds)}s`}/>
      </div> : <div className="empty-state">Broker P&amp;L comparison has not been confirmed.</div>}
    </section>

    <section className="desk-section">
      <div className="desk-section-heading"><div><div className="eyebrow">MARKET WATCH</div><h3>Instruments</h3></div><StateTag kind={marketData?.ws_connected ? "good" : "warn"}>{marketData?.ws_connected ? "Feed connected" : "Feed status unknown"}</StateTag></div>
      <div className="instrument-grid">{instruments.length ? instruments.map((instrument) => {
        const quote = marketData?.instruments?.[instrument];
        const ltp = getLtp(instrument);
        const linked = strategies.filter((strategy: any) => strategy.instrument === instrument);
        const fresh = quoteIsFresh(quote, Boolean(marketData?.ws_connected));
        return <article className="instrument-card" key={instrument}><div className="instrument-card-head"><div><span className="instrument-symbol">{instrument}</span><small>{linked.length ? linked.map((strategy: any) => strategy.strategy_id).join(" · ") : "No strategy assigned"}</small></div><StateTag kind={fresh ? "good" : "warn"}>{quoteStatus(quote, Boolean(marketData?.ws_connected))}</StateTag></div><strong className="instrument-price tabular-nums">{ltp > 0 ? safeINR(ltp) : "—"}</strong><div className="instrument-foot"><span>Last tick <b>{quote?.receive_timestamp ? formatTimestamp(quote.receive_timestamp) : "—"}</b> · {tickAge(quote)}</span><span>{linked.length} strategy{linked.length === 1 ? "" : "ies"}</span></div></article>;
      }) : <div className="empty-state"><div><b>Instrument state is not confirmed.</b><small>No feed or strategy snapshot has arrived.</small></div></div>}</div>
    </section>

    <section className="desk-section">
      <div className="desk-section-heading"><div><div className="eyebrow">POSITION EXPOSURE</div><h3>Open positions <span className="count-chip">{positionsKnown ? open.length : "—"}</span></h3></div><div className="source-note">P&amp;L below is the engine’s local calculation</div></div>
      {open.length === 0 ? <div className="empty-state">{positionsKnown ? <CircleCheck size={18}/> : <ShieldAlert size={18}/>}<div><b>{positionsKnown ? "No open local positions." : "Position state is not confirmed."}</b><small>{positionsKnown ? "No position records are open in the latest runtime snapshot." : "The positions source has not returned successfully. Do not treat this as a confirmed flat account."}</small></div></div> : <div className="position-list">
        {open.map((position: any) => {
          const ltp = getLtp(position.instrument);
          const sl = position.stop_price;
          const stopState = String(position.sl_state || position.stop_state || "").toUpperCase();
          const protectedState = stopState.includes("ARM") || stopState === "ACTIVE" || stopState === "MONITORING";
          return <article className="position-card" key={position.position_id}>
            <div className="position-card-instrument"><span className={`side-mark ${position.side === "LONG" ? "long" : "short"}`}>{position.side === "LONG" ? <ArrowUpRight size={17}/> : <ArrowDownRight size={17}/>}</span><div><strong>{position.instrument}</strong><small>{position.strategy_id} · {String(position.position_id || "").slice(0, 8)}</small></div></div>
            <Value label="Quantity" value={position.quantity}/><Value label="Average entry" value={safeINR(position.average_entry)}/><Value label="Market price" value={ltp > 0 ? safeINR(ltp) : "—"}/>
            <div className="desk-value stop-value"><small>Local stop monitor</small><strong>{sl ? safeINR(sl) : "No stop price"}</strong><StateTag kind={protectedState ? "good" : "warn"}>{protectedState ? stopState : stopState || "STATE NOT EXPOSED"}</StateTag></div>
            <div className="position-card-pnl"><small>Local unrealized P&amp;L</small><strong style={{ color: pnlColor(position.unrealized_pnl) }}>{formatINR(position.unrealized_pnl)}</strong></div>
          </article>;
        })}
      </div>}
    </section>

    <section className="desk-section">
      <div className="desk-section-heading"><div><div className="eyebrow">SIGNAL TO EXECUTION</div><h3>Active trigger queue <span className="count-chip">{strategiesKnown ? activeTriggers.length : "—"}</span></h3></div><span className="source-note">Strategy runtime state · trigger queue is not a broker order</span></div>
      {!activeTriggers.length ? <div className="empty-state">{strategiesKnown ? <CircleCheck size={18}/> : <ShieldAlert size={18}/>}<div><b>{strategiesKnown ? "No armed entry or reversal trigger." : "Trigger state is not confirmed."}</b><small>{strategiesKnown ? "Latest strategy state contains no pending trigger. Orders already sent to Dhan appear in the working orders panel below." : "No strategy snapshot has arrived, so the empty queue does not prove that no trigger is pending."}</small></div></div> : <div className="trigger-list">{activeTriggers.map((item: any, index: number) => {
        const side = String(item.side || item.direction || item.strategy?.position_side || "").toUpperCase();
        const ltp = getLtp(item.strategy.instrument);
        const triggerPrice = safeNum(item.trigger_price ?? item.trigger);
        const distance = ltp && triggerPrice ? Math.abs(triggerPrice - ltp) : null;
        return <article className="trigger-card" key={`${item.strategy.strategy_id}-${item.triggerKind}-${index}`}>
          <div className="trigger-card-head"><div><span className="trigger-badge">{item.triggerKind}</span><strong>{item.strategy.instrument} · {item.strategy.strategy_id}</strong></div><StateTag kind="warn">{item.trigger_state || "ARMED"}</StateTag></div>
          <div className="trigger-values"><Value label="Direction" value={side || "—"}/><Value label="Trigger price" value={triggerPrice ? safeINR(triggerPrice) : "—"}/><Value label="Current price" value={ltp ? safeINR(ltp) : "—"}/><Value label="Distance" value={distance === null ? "—" : safeINR(distance)}/><Value label="Stop level" value={item.stop_price ? safeINR(item.stop_price) : "—"}/><Value label="Signal candle" value={item.signal_candle_start ? formatTimestamp(item.signal_candle_start) : "—"}/></div>
          {(item.signal_candle_open || item.signal_candle_high || item.signal_candle_low || item.signal_candle_close) && <div className="candle-strip">{[["O",item.signal_candle_open],["H",item.signal_candle_high],["L",item.signal_candle_low],["C",item.signal_candle_close]].map(([label,value]: any)=><span key={label}><small>{label}</small><b>{value ? safeINR(value) : "—"}</b></span>)}</div>}
        </article>;
      })}</div>}
    </section>

    <section className="desk-section">
      <div className="desk-section-heading"><div><div className="eyebrow">DHAN ORDER LIFECYCLE</div><h3>Working broker orders <span className="count-chip">{ordersKnown ? workingOrders.length : "—"}</span></h3></div><span className="source-note">Filled/rejected/cancelled orders are in Orders</span></div>
      {!ordersKnown ? <div className="empty-state"><ShieldAlert size={18}/><div><b>Broker order state is not confirmed.</b><small>The order source has not returned successfully; the working-order count is unknown.</small></div></div> : workingOrders.length === 0 && unclassifiedOrders.length === 0 ? <div className="empty-state"><CircleCheck size={18}/><div><b>No working broker orders.</b><small>All orders in the latest successful snapshot are terminal.</small></div></div> : <div className="order-list">{[...workingOrders, ...unclassifiedOrders].map((order: any)=><div className="order-row" key={order.order_id}><span><b>{order.instrument}</b><small>{order.strategy_id} · {String(order.order_id || "").slice(0, 12)}</small></span><b className={order.side === "BUY" ? "positive" : "negative"}>{order.side}</b><span>{order.filled_quantity ?? 0} / {order.quantity ?? "—"} filled</span><span>{order.order_type || order.type || "—"}</span><StateTag kind={activeOrderStates.has(orderState(order)) ? "warn" : "neutral"}>{order.state || order.status || order.order_status || "UNCLASSIFIED"}</StateTag></div>)}</div>}
    </section>

    <div className="desk-bottom-grid">
      <section className="desk-section"><div className="desk-section-heading"><div><div className="eyebrow">LATEST EXECUTIONS</div><h3>Recent fills</h3></div><Waypoints size={17}/></div>{!fillsKnown ? <div className="empty-state">Fill state is not confirmed.</div> : fills.slice(0,8).length ? <div className="order-list">{fills.slice(0,8).map((fill: any)=><div className="order-row fill-row" key={fill.fill_id}><span><b>{fill.instrument}</b><small>{fill.strategy_id} · {formatTimestamp(fill.timestamp)}</small></span><b className={fill.side === "BUY" ? "positive" : "negative"}>{fill.side}</b><span>{fill.quantity} filled</span><strong className="tabular-nums">{safeINR(fill.price)}</strong></div>)}</div> : <div className="empty-state">No fills in the current snapshot.</div>}</section>
      <section className="desk-section"><div className="desk-section-heading"><div><div className="eyebrow">ACCOUNT SNAPSHOT</div><h3>Broker and runtime</h3></div><Activity size={17}/></div><div className="account-grid"><Value label="Execution mode" value={overview.execution_mode || "—"}/><Value label="Margin used" value={overviewKnown ? safeINR(overview.margin_used) : "—"}/><Value label="Available margin" value={overviewKnown ? safeINR(overview.available_margin) : "—"}/><Value label="Orders reported" value={ordersKnown ? orders.length : "—"}/><Value label="Updated feed" value={marketData?.updated_at ? formatTimestamp(marketData.updated_at) : "Shown per instrument"}/><Value label="Position source" value={positionsKnown ? "Runtime local state" : "Not confirmed"}/></div><div className="desk-disclaimer"><Clock3 size={14}/>Local engine values are labeled separately from Dhan account P&amp;L. Broker positions and feed diagnostics are available in Operations.</div></section>
    </div>
  </div>;
}

"""Telegram message formatters for trading events."""
from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

IST = timezone(timedelta(hours=5, minutes=30))


def _ist(ts: Optional[float] = None) -> str:
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(ts, tz=IST).strftime("%Y-%m-%d %H:%M:%S IST")


def _inr(val: float) -> str:
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:,.0f}"


def format_new_trade(fill: dict, strategy: dict, account: dict) -> str:
    direction = fill.get("side", "BUY")
    emoji = "\U0001f534" if direction == "BUY" else "\U0001f535"
    instrument = fill.get("instrument", "?")
    strategy_id = fill.get("strategy_id", "?")
    price = fill.get("price", 0)
    qty = fill.get("quantity", 0)
    multiplier = fill.get("multiplier", 1)
    value = price * qty * multiplier
    entry_value = strategy.get("entry_value", value)
    stop = strategy.get("stop_price", 0)
    htf_val = strategy.get("htf_dema_atr", 0)
    equity = account.get("equity", 0)
    margin = account.get("used_margin", 0)
    mode = fill.get("execution_mode", "PAPER")

    # When the account block is sourced from the LIVE Dhan broker (engine
    # passed real /fundlimit + P&L), render the true account numbers.
    account_lines = []
    if account.get("source") == "dhan":
        avail = account.get("available_margin", 0)
        realized = account.get("realized_pnl", 0)
        unrealized = account.get("unrealized_pnl", 0)
        net_pnl = account.get("net_pnl", 0)
        client = account.get("dhan_client_id", "")
        masked = f"...{client[-4:]}" if client else "?"
        account_lines.append(f"<b>Account Equity (Dhan):</b> {_inr(equity)}")
        account_lines.append(f"<b>Available Margin:</b> {_inr(avail)}")
        account_lines.append(f"<b>Margin Used:</b> {_inr(margin)}")
        account_lines.append(f"<b>Realized P&L:</b> {_inr(realized)}")
        account_lines.append(f"<b>Unrealized P&L:</b> {_inr(unrealized)}")
        account_lines.append(f"<b>Client ID:</b> {masked}")
    else:
        account_lines.append(f"<b>Account Equity:</b> {_inr(equity)}")
        account_lines.append(f"<b>Margin Used:</b> {_inr(margin)}")

    return (
        f"{emoji} <b>NEW TRADE</b>\n\n"
        f"<b>Instrument:</b> {instrument}\n"
        f"<b>Strategy:</b> {strategy_id}\n"
        f"<b>Direction:</b> {direction}\n\n"
        f"<b>Signal Time:</b> {_ist()}\n"
        f"<b>Entry Fill:</b> {price:,.0f}\n"
        f"<b>Quantity:</b> {qty}\n"
        f"<b>Stop:</b> {stop:,.0f}\n\n"
        f"<b>Entry Value:</b> {_inr(entry_value)}\n"
        f"<b>HTF DEMA-ATR:</b> {htf_val:,.0f}\n\n"
        f"{chr(10).join(account_lines)}\n\n"
        f"<b>Order ID:</b> {fill.get('order_id', '?')}\n"
        f"<b>Mode:</b> {mode}\n"
        f"<b>Timestamp:</b> {_ist()}"
    )


def _account_block(account: dict) -> str:
    """Render the account section, honoring the REAL Dhan broker block."""
    equity = account.get("equity", 0)
    margin = account.get("used_margin", 0)
    if account.get("source") == "dhan":
        avail = account.get("available_margin", 0)
        realized = account.get("realized_pnl", 0)
        unrealized = account.get("unrealized_pnl", 0)
        net = account.get("net_pnl", 0)
        client = account.get("dhan_client_id", "")
        masked = f"...{client[-4:]}" if client else "?"
        return (
            f"<b>Account Equity (Dhan):</b> {_inr(equity)}\n"
            f"<b>Available Margin:</b> {_inr(avail)}\n"
            f"<b>Margin Used:</b> {_inr(margin)}\n"
            f"<b>Realized P&L:</b> {_inr(realized)}\n"
            f"<b>Unrealized P&L:</b> {_inr(unrealized)}\n"
            f"<b>Net P&L:</b> {_inr(net)}\n"
            f"<b>Client ID:</b> {masked}"
        )
    return (
        f"<b>Account Equity:</b> {_inr(equity)}\n"
        f"<b>Margin Used:</b> {_inr(margin)}"
    )


def format_signal_alert(signal_data: dict) -> str:
    """Signal-candle alert: the candle that produced the cross AND the candle
    the trade was actually placed on (may be a later bar / tick)."""
    direction = signal_data.get("side", "LONG")
    emoji = "\U0001f4c8" if direction == "LONG" else "\U0001f4c9"
    instrument = signal_data.get("instrument", "?")
    strategy_id = signal_data.get("strategy_id", "?")

    def _fmt(field: str) -> Optional[str]:
        val = signal_data.get(field)
        if val is None:
            return None
        return f"{float(val):,.0f}"

    sig_time = signal_data.get("signal_candle_time")
    sig_close = _fmt("signal_candle_close")
    sig_high = _fmt("signal_candle_high")
    sig_low = _fmt("signal_candle_low")
    sig_htf = _fmt("signal_htf_dema_atr")
    sig_mid = _fmt("signal_mid_dema_atr")
    sig_fast = _fmt("signal_fast_dema_atr")
    sig_trigger = _fmt("signal_trigger_price")
    sig_stop = _fmt("stop_price")
    sig_qty = signal_data.get("quantity")
    sig_mode = signal_data.get("mode")
    signal_ts = signal_data.get("signal_time")
    if signal_ts and not sig_time:
        try:
            sig_time = datetime.fromtimestamp(float(signal_ts), tz=IST).strftime("%H:%M %d-%b")
        except Exception:  # noqa: BLE001
            sig_time = str(signal_ts)

    place_time = signal_data.get("placement_candle_time")
    place_fill = _fmt("fill_price")

    lines = [
        f"{emoji} <b>SIGNAL CANDLE ALERT</b>\n",
        f"<b>Instrument:</b> {instrument}",
        f"<b>Strategy:</b> {strategy_id}",
        f"<b>Signal:</b> {direction}\n",
        f"<b>— Signal Candle —</b>",
        f"<b>Time:</b> {sig_time or '?'}",
    ]
    if sig_close is not None:
        lines.append(f"<b>Close:</b> {sig_close}")
    if sig_high is not None:
        lines.append(f"<b>High:</b> {sig_high}")
    if sig_low is not None:
        lines.append(f"<b>Low:</b> {sig_low}")
    if sig_trigger is not None:
        lines.append(f"<b>Trigger Level:</b> {sig_trigger}")
    if sig_stop is not None:
        lines.append(f"<b>Stop Level:</b> {sig_stop}")
    if sig_qty is not None:
        lines.append(f"<b>Quantity:</b> {sig_qty}")
    if sig_fast is not None:
        lines.append(f"<b>Fast DEMA-ATR:</b> {sig_fast}")
    if sig_htf is not None:
        lines.append(f"<b>1H DEMA-ATR:</b> {sig_htf}")
    if sig_mid is not None:
        lines.append(f"<b>15m DEMA-ATR:</b> {sig_mid}")

    lines.append("")
    lines.append(f"<b>— Trade Placement —</b>")
    lines.append(f"<b>Time:</b> {place_time or '?'}")
    if place_fill is not None:
        lines.append(f"<b>Entry Fill:</b> {place_fill}")

    account = signal_data.get("account") if isinstance(signal_data.get("account"), dict) else {}
    if account:
        lines.append("")
        lines.append(_account_block(account))
    if sig_mode:
        lines.append("")
        lines.append(f"<b>Mode:</b> {sig_mode}")
    lines.append(f"<b>Timestamp:</b> {_ist()}")

    return "\n".join(lines)


def format_trade_exit(close_data: dict) -> str:
    instrument = close_data.get("instrument", "?")
    strategy_id = close_data.get("strategy_id", "?")
    side = close_data.get("side", "?")
    entry = close_data.get("entry_price", 0)
    exit_p = close_data.get("exit_price", 0)
    pnl = close_data.get("net_pnl", 0)
    emoji = "\u2705" if pnl >= 0 else "\u274c"
    duration = close_data.get("duration", "")
    exit_reason = close_data.get("exit_reason", "signal")

    return (
        f"{emoji} <b>TRADE CLOSED</b>\n\n"
        f"<b>Instrument:</b> {instrument}\n"
        f"<b>Strategy:</b> {strategy_id}\n"
        f"<b>Direction:</b> {side}\n"
        f"<b>Entry:</b> {entry:,.0f}\n"
        f"<b>Exit:</b> {exit_p:,.0f}\n"
        f"<b>P&L:</b> {_inr(pnl)}\n"
        f"<b>Exit Reason:</b> {exit_reason}\n"
        f"<b>Duration:</b> {duration}\n"
        f"<b>Timestamp:</b> {_ist()}"
    )


def format_reversal_complete(data: dict) -> str:
    """Format a broker-confirmed reversal whose new local stop is armed."""
    old_side = data.get("old_side", "?")
    new_side = data.get("new_side", data.get("side", "?"))
    instrument = data.get("instrument", "?")
    strategy_id = data.get("strategy_id", "?")
    quantity = data.get("quantity", 0)
    exit_price = data.get("old_exit_fill_price")
    entry_price = data.get("new_entry_fill_price")
    stop_price = data.get("stop_price")
    lines = [
        "🔄 <b>REVERSAL COMPLETE</b>",
        "",
        f"<b>Instrument:</b> {instrument}",
        f"<b>Strategy:</b> {strategy_id}",
        f"<b>Direction:</b> {old_side} → {new_side}",
        f"<b>Filled Quantity:</b> {quantity}",
    ]
    if exit_price is not None:
        lines.append(f"<b>Old Exit Fill:</b> {float(exit_price):,.2f}")
    if entry_price is not None:
        lines.append(f"<b>New Entry Fill:</b> {float(entry_price):,.2f}")
    if stop_price is not None:
        lines.append(f"<b>New Local Stop:</b> {float(stop_price):,.2f} (ARMED)")
    if data.get("old_exit_order_id"):
        lines.append(f"<b>Exit Order:</b> {data['old_exit_order_id']}")
    if data.get("new_broker_order_id"):
        lines.append(f"<b>Entry Broker Order:</b> {data['new_broker_order_id']}")
    lines.append(f"<b>Timestamp:</b> {_ist()}")
    return "\n".join(lines)


def format_risk_alert(alert_data: dict) -> str:
    severity = alert_data.get("severity", "WARNING")
    emoji = "\u26a0\ufe0f" if severity == "WARNING" else "\U0001f6a8"
    lines = [
        f"{emoji} <b>RISK ALERT</b>\n",
        f"<b>Type:</b> {alert_data.get('type', 'unknown')}",
        f"<b>Message:</b> {alert_data.get('message', '')}",
    ]
    # Add extra details if present
    for field in ['strategy_id', 'instrument', 'side', 'trigger_price', 'stop_price',
                  'quantity', 'value', 'limit', 'equity', 'available_margin']:
        val = alert_data.get(field)
        if val is not None and val != '':
            label = field.replace('_', ' ').title()
            lines.append(f"<b>{label}:</b> {val}")
    lines.append(f"<b>Timestamp:</b> {_ist()}")
    return "\n".join(lines)


def format_error_alert(error_data: dict) -> str:
    return (
        f"\U0001f6a8 <b>ERROR ALERT</b>\n\n"
        f"<b>Component:</b> {error_data.get('component', 'unknown')}\n"
        f"<b>Error:</b> {error_data.get('message', '')}\n"
        f"<b>Timestamp:</b> {_ist()}"
    )


def format_startup_alert(data: dict) -> str:
    strategies = data.get("strategies", [])
    count = data.get("strategy_count", len(strategies))
    strat_list = data.get("strategy_list", ", ".join(strategies))
    account = data.get("account") or {}
    lines = [
        f"\U0001f680 <b>LIVE ENGINE STARTED</b>\n",
        f"<b>Strategies:</b> {count}",
        f"<b>List:</b> {strat_list}",
    ]
    if account.get("equity"):
        avail = account.get("available_margin", 0)
        used = account.get("used_margin", 0)
        realized = account.get("realized_pnl", 0)
        unrealized = account.get("unrealized_pnl", 0)
        client = account.get("dhan_client_id", "")
        masked = f"...{client[-4:]}" if client else "?"
        lines.append("\n\u2014 <b>Dhan Account</b> \u2014")
        lines.append(f"<b>Equity (if sold today):</b> {_inr(account['equity'])}")
        lines.append(f"<b>Available Margin:</b> {_inr(avail)}")
        lines.append(f"<b>Margin Used:</b> {_inr(used)}")
        lines.append(f"<b>Realized P&L:</b> {_inr(realized)}")
        lines.append(f"<b>Unrealized P&L:</b> {_inr(unrealized)}")
        lines.append(f"<b>Client ID:</b> {masked}")
    lines.append(f"<b>Timestamp:</b> {_ist()}")
    return "\n".join(lines)


def format_shutdown_alert(data: dict) -> str:
    reason = data.get("reason", "normal")
    return (
        f"\U0001f6d1 <b>LIVE ENGINE STOPPED</b>\n\n"
        f"<b>Reason:</b> {reason}\n"
        f"<b>Strategies:</b> {data.get('strategy_count', 0)}\n"
        f"<b>Timestamp:</b> {_ist()}"
    )


def format_order_lifecycle(data: dict) -> str:
    """Format order lifecycle events (ENTRY_SENT, TRIGGER_CROSSED,
    MARKET_FALLBACK, SL_*, REVERSAL, etc.) into a compact Telegram alert."""
    event_type = data.get("event_type", data.get("lifecycle_event", "UNKNOWN"))
    strategy = data.get("strategy_id", "?")
    instrument = data.get("instrument", "?")
    side = data.get("side", data.get("order_side", "?"))
    order_id = data.get("order_id", data.get("local_order_id", "?"))
    broker_oid = data.get("broker_order_id", "?")
    status = data.get("status", data.get("new_status", ""))
    error = data.get("error", "")
    quantity = data.get("quantity", "")
    price = data.get("price", data.get("fill_price", ""))
    trigger = data.get("trigger_price", "")

    emoji_map = {
        "ENTRY_SENT": "\U0001f4e4",
        "ENTRY_FILLED": "\u2705",
        "ENTRY_REJECTED": "\u274c",
        "TRIGGER_CROSSED": "\U0001f534",
        "LIMIT_CANCEL_REQUESTED": "\u23f3",
        "LIMIT_CANCEL_CONFIRMED": "\u2705",
        "MARKET_FALLBACK_SENT": "\u26a1",
        "MARKET_FALLBACK_FILLED": "\u2705",
        "PARTIAL_FILL": "\U0001f537",
        "SL_SENT": "\U0001f6e1",
        "SL_CONFIRMED": "\u2705",
        "SL_FAILURE": "\U0001f6a8",
        "REVERSAL": "\U0001f504",
        "POSITION_FLAT": "\U0001f4c9",
    }
    emoji = emoji_map.get(event_type, "\U0001f4cb")
    label = event_type.replace("_", " ")

    lines = [
        f"{emoji} <b>{label}</b>\n",
        f"<b>Strategy:</b> {strategy}",
        f"<b>Instrument:</b> {instrument}",
        f"<b>Side:</b> {side}",
    ]
    if quantity:
        lines.append(f"<b>Qty:</b> {quantity}")
    if price:
        lines.append(f"<b>Price:</b> {price}")
    if trigger:
        lines.append(f"<b>Trigger:</b> {trigger}")
    if broker_oid and broker_oid != "?":
        lines.append(f"<b>Broker Order:</b> {broker_oid}")
    if status:
        lines.append(f"<b>Status:</b> {status}")
    if error:
        lines.append(f"<b>Error:</b> {error}")
    lines.append(f"<b>Timestamp:</b> {_ist()}")
    return "\n".join(lines)


def format_daily_summary(account: dict, pnl_data: dict, risk: dict) -> str:
    equity = account.get("equity", 0)
    starting = account.get("starting_capital", 0)
    net_pnl = equity - starting
    daily = risk.get("daily_pnl", 0)
    return (
        f"\U0001f4ca <b>DAILY SUMMARY</b>\n\n"
        f"<b>Equity:</b> {_inr(equity)}\n"
        f"<b>Starting:</b> {_inr(starting)}\n"
        f"<b>Net P&L:</b> {_inr(net_pnl)}\n"
        f"<b>Today P&L:</b> {_inr(daily)}\n"
        f"<b>Kill Switch:</b> {'ACTIVE' if risk.get('kill_switch_active') else 'OFF'}\n"
        f"<b>Timestamp:</b> {_ist()}"
    )

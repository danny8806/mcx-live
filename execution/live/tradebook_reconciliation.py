"""Read-only comparison of Dhan executions with locally persisted fills."""
from __future__ import annotations


def _first(row: dict, *names, default=None):
    for name in names:
        value = row.get(name)
        if value is not None and value != "":
            return value
    return default


def _int(value) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compare_tradebook(broker_orders, broker_trades, local_orders, local_fills):
    """Compare today's system-owned broker executions against persisted fills.

    Manual/external orders are counted but never treated as local failures.
    Missing or divergent fills for MCX-/EMER-correlated orders are explicit
    mismatches; this function never imports or mutates lifecycle state.
    """
    broker_order_by_id = {
        str(_first(row, "broker_order_id", "orderId", "order_id", default="")): row
        for row in (broker_orders or []) if isinstance(row, dict)
        if _first(row, "broker_order_id", "orderId", "order_id", default="")
    }
    local_order_by_id = {}
    local_order_to_broker = {}
    for row in (local_orders or []):
        if not isinstance(row, dict):
            continue
        oid = str(_first(row, "order_id", "id", default=""))
        bid = str(_first(row, "broker_order_id", default="") or "")
        if oid:
            local_order_by_id[oid] = row
            if bid:
                local_order_to_broker[oid] = bid

    local_fills_by_broker = {}
    for fill in (local_fills or []):
        if not isinstance(fill, dict):
            continue
        oid = str(_first(fill, "order_id", default=""))
        bid = str(_first(fill, "broker_order_id", default="") or
                  local_order_to_broker.get(oid, ""))
        if not bid and oid.startswith("IMPORT-"):
            bid = oid.removeprefix("IMPORT-")
        if not bid:
            continue
        row = local_fills_by_broker.setdefault(
            bid, {"quantity": 0, "notional": 0.0, "instrument": ""})
        qty = _int(_first(fill, "quantity", "tradedQuantity", default=0))
        price = _float(_first(fill, "price", "tradedPrice"))
        row["quantity"] += qty
        if price is not None:
            row["notional"] += qty * price
        row["instrument"] = row["instrument"] or str(fill.get("instrument") or "")

    broker_fills_by_order = {}
    for fill in (broker_trades or []):
        if not isinstance(fill, dict):
            continue
        bid = str(_first(fill, "orderId", "broker_order_id", "order_id", default=""))
        if not bid:
            continue
        row = broker_fills_by_order.setdefault(
            bid, {"quantity": 0, "notional": 0.0,
                  "instrument": str(_first(fill, "tradingSymbol", "symbol", default=""))})
        qty = _int(_first(fill, "tradedQuantity", "tradedQty", "quantity", default=0))
        price = _float(_first(fill, "tradedPrice", "price", "averageTradedPrice"))
        row["quantity"] += qty
        if price is not None:
            row["notional"] += qty * price

    mismatches = []
    manual_executions = 0
    checked_system_orders = 0
    system_ids = set()
    for broker_id, order in broker_order_by_id.items():
        cid = str(_first(order, "correlation_id", "correlationId", default="") or "")
        if not (cid.startswith("MCX-") or cid.startswith("EMER-")):
            continue
        system_ids.add(broker_id)
        expected = broker_fills_by_order.get(broker_id)
        if expected is None:
            # Ignore unfilled and terminal orders.  A trade without a tradebook
            # row is only suspicious when the order itself claims a fill.
            expected_qty = _int(_first(order, "filled_quantity", "filledQty", default=0))
            if expected_qty <= 0:
                continue
            expected = {"quantity": expected_qty, "notional": 0.0,
                        "instrument": str(order.get("instrument") or "")}
            missing_tradebook = True
        else:
            missing_tradebook = False
        checked_system_orders += 1
        actual = local_fills_by_broker.get(broker_id, {"quantity": 0, "notional": 0.0})
        broker_qty = int(expected["quantity"])
        local_qty = int(actual["quantity"])
        detail = {
            "broker_order_id": broker_id,
            "correlation_id": cid,
            "instrument": (expected.get("instrument") or order.get("instrument") or
                           actual.get("instrument") or ""),
            "side": str(_first(order, "side", "transactionType", default="") or ""),
            "broker_quantity": broker_qty,
            "local_quantity": local_qty,
            "broker_average_price": round(expected["notional"] / broker_qty, 4)
                if broker_qty and expected["notional"] else None,
            "local_average_price": round(actual["notional"] / local_qty, 4)
                if local_qty and actual["notional"] else None,
        }
        if missing_tradebook:
            detail["type"] = "BROKER_TRADEBOOK_FILL_MISSING"
            mismatches.append(detail)
        elif broker_qty > local_qty:
            detail["type"] = "BROKER_FILL_MISSING_LOCAL"
            mismatches.append(detail)
        elif local_qty > broker_qty:
            detail["type"] = "LOCAL_FILL_AHEAD_OF_BROKER"
            mismatches.append(detail)
        elif broker_qty and actual["notional"] and expected["notional"]:
            broker_avg = expected["notional"] / broker_qty
            local_avg = actual["notional"] / local_qty
            if abs(broker_avg - local_avg) > 0.01:
                detail["type"] = "FILL_AVERAGE_PRICE_MISMATCH"
                mismatches.append(detail)

    for broker_id in broker_fills_by_order:
        if broker_id not in system_ids:
            manual_executions += 1

    return {
        "status": "MISMATCH" if mismatches else "MATCHED",
        "checked_system_orders": checked_system_orders,
        "broker_execution_orders": len(broker_fills_by_order),
        "manual_or_unclassified_execution_orders": manual_executions,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
    }

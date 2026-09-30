"""Reconciliation routes.

Runs both the legacy reconciliation engine AND the new lifecycle-based
reconciliation for a comprehensive consistency check.
"""
from __future__ import annotations
import asyncio
import time
from fastapi import APIRouter
router = APIRouter()
_engine = None

def init(engine, event_bus):
    global _engine
    _engine = engine

def _run_reconciliation_sync():
    if not _engine:
        return {"error": "Engine not initialized"}
    results = {
        "timestamp": time.time(),
        "checks": [],
        "is_consistent": True,
        "errors": [],
        "warnings": [],
    }

    # 1. Legacy reconciliation engine
    if getattr(_engine, "_persistence", None) is not None:
        try:
            from reconciliation.engine import ReconciliationEngine
            recon = ReconciliationEngine(
                persistence=_engine._persistence,
                position_manager=_engine.position_manager,
                pnl_engines=_engine.pnl_engines,
                account_engines=_engine.account_engines,
                strategies=_engine.strategies,
                order_manager=_engine.order_manager,
            )
            result = recon.reconcile(phase="live")
            results["checks"].append({
                "name": "legacy_reconciliation",
                "is_consistent": result.is_consistent,
                "errors": result.errors,
                "warnings": result.warnings,
                "stats": result.stats,
            })
            if not result.is_consistent:
                results["is_consistent"] = False
                results["errors"].extend(result.errors)
            results["warnings"].extend(result.warnings)
        except Exception as e:
            results["checks"].append({"name": "legacy_reconciliation", "error": str(e)})

    # 2. Lifecycle orphan scan (aggregate over per-strategy lifecycles)
    if hasattr(_engine, "orphan_scan"):
        try:
            orphan_report = _engine.orphan_scan()
            results["checks"].append({
                "name": "lifecycle_orphan_scan",
                "is_clean": orphan_report["is_clean"],
                "total_orphans": orphan_report["total_orphans"],
                "orphan_fills": orphan_report["orphan_fills"],
                "orphan_orders": orphan_report["orphan_orders"],
                "orphan_positions": orphan_report["orphan_positions"],
                "orphan_pending_orders": orphan_report["orphan_pending_orders"],
                "trades_without_signals": orphan_report["trades_without_signals"],
                "trades_without_positions": orphan_report["trades_without_positions"],
            })
            if not orphan_report["is_clean"]:
                results["is_consistent"] = False
                for fill in orphan_report["orphan_fills"]:
                    results["errors"].append({"type": "ORPHAN_FILL", "detail": fill})
                for order in orphan_report["orphan_orders"]:
                    results["errors"].append({"type": "ORPHAN_ORDER", "detail": order})
                for pos in orphan_report["orphan_positions"]:
                    results["errors"].append({"type": "ORPHAN_POSITION", "detail": pos})
                for pend in orphan_report["orphan_pending_orders"]:
                    results["errors"].append({"type": "ORPHAN_PENDING_ORDER", "detail": pend})
        except Exception as e:
            results["checks"].append({"name": "lifecycle_orphan_scan", "error": str(e)})

    # 3. Lifecycle identity consistency (aggregate over per-strategy lifecycles)
    if hasattr(_engine, "reconcile_trades"):
        try:
            lc_result = _engine.reconcile_trades()
            results["checks"].append({
                "name": "lifecycle_identity_consistency",
                "stats": lc_result["stats"],
                "errors": lc_result["errors"],
                "warnings": lc_result["warnings"],
            })
            if lc_result["errors"]:
                results["is_consistent"] = False
                results["errors"].extend(lc_result["errors"])
            results["warnings"].extend(lc_result["warnings"])
        except Exception as e:
            results["checks"].append({"name": "lifecycle_identity_consistency", "error": str(e)})

    # Local DB integrity alone cannot detect executions missing from local
    # history. Include the poller's independent Dhan order-book/trade-book
    # comparison so broker-only fills turn this endpoint red, not false-green.
    try:
        from dashboard.envs import resolve as _resolve_env
        live_env = _resolve_env(_engine, "live")
        sync = getattr(live_env, "sync_service", None) if live_env else None
        sync_stats = sync.stats() if sync is not None else {}
        broker_report = sync_stats.get("tradebook_reconciliation") or {}
        if broker_report.get("status") not in (None, "NOT_CHECKED", "UNSUPPORTED"):
            mismatch_count = int(broker_report.get("mismatch_count") or 0)
            broker_errors = list(broker_report.get("mismatches") or [])
            check_ok = broker_report.get("status") == "MATCHED" and mismatch_count == 0
            results["checks"].append({
                "name": "broker_tradebook_vs_local_fills",
                "is_consistent": check_ok,
                "status": broker_report.get("status"),
                "mismatch_count": mismatch_count,
                "mismatches": broker_errors,
                "checked_at": broker_report.get("checked_at"),
                "broker_trade_rows": broker_report.get("broker_trade_rows"),
                "broker_order_rows": broker_report.get("broker_order_rows"),
            })
            if not check_ok:
                results["is_consistent"] = False
                results["errors"].append({
                    "type": "BROKER_TRADEBOOK_MISMATCH",
                    "detail": broker_errors or broker_report.get("error")
                        or broker_report.get("status"),
                })
    except Exception as e:
        results["checks"].append({
            "name": "broker_tradebook_vs_local_fills", "error": str(e),
            "is_consistent": False,
        })
        results["is_consistent"] = False
        results["errors"].append({"type": "BROKER_TRADEBOOK_CHECK_FAILED",
                                  "detail": str(e)})

    # Summary
    total_errors = len(results["errors"])
    total_warnings = len(results["warnings"])
    results["summary"] = {
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "checks_passed": sum(1 for c in results["checks"] if c.get("is_consistent", True) and "error" not in c),
        "checks_failed": sum(1 for c in results["checks"] if not c.get("is_consistent", True)),
    }

    return results

@router.get("/api/reconciliation")
async def get_reconciliation():
    if not _engine:
        return {"error": "Engine not initialized"}
    return await asyncio.to_thread(_run_reconciliation_sync)

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

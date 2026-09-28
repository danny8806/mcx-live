"""Runtime safety barriers for the three-service architecture.

Every service boots through these guards so a misconfigured container fails
fast (exits non-zero) instead of silently trading against the wrong universe.
"""
from __future__ import annotations

import os
import sys
import time
from typing import Callable, Dict, List, Optional


def _env_flag(name: str, default: str = "0") -> str:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def option_module_enabled() -> bool:
    """Whether the DEMO backend should also host the option scheduler/routes.

    Defaults to ON for backward-compat with the pre-split dashboard; the
    ``demo-mcx`` service sets ``ENABLE_OPTION_MODULE=0`` so the option runtime
    lives only in ``demo-option``.
    """
    return _env_flag("ENABLE_OPTION_MODULE", default="1")


def assert_runtime_mode(expected: str, *, real_order_execution_required: bool = False) -> None:
    """Fail fast unless the process's env matches the expected runtime mode.

    ``expected`` is one of ``LIVE``, ``DEMO``, ``OPTION_DEMO``.
    """
    mode = os.getenv("TRADING_MODE", "").strip()
    real_order = _env_flag("REAL_ORDER_EXECUTION")
    broker = _env_flag("BROKER_ENABLED")

    problems: List[str] = []
    if expected == "LIVE":
        if mode.upper() != "LIVE":
            problems.append(f"TRADING_MODE={mode!r} (expected LIVE)")
        if real_order_execution_required and not real_order:
            problems.append("REAL_ORDER_EXECUTION must be 1 for LIVE")
        if not broker:
            problems.append("BROKER_ENABLED must be 1 for LIVE")
    elif expected == "DEMO":
        if mode.upper() not in {"", "DEMO", "PAPER"}:
            problems.append(f"TRADING_MODE={mode!r} (expected DEMO/PAPER)")
        if real_order:
            problems.append("REAL_ORDER_EXECUTION must NOT be 1 for DEMO")
        if broker:
            problems.append("BROKER_ENABLED must NOT be 1 for DEMO")
    elif expected == "OPTION_DEMO":
        if mode.upper() not in {"", "OPTION_DEMO", "DEMO", "PAPER"}:
            problems.append(f"TRADING_MODE={mode!r} (expected OPTION_DEMO)")
        if real_order:
            problems.append("REAL_ORDER_EXECUTION must NOT be 1 for OPTION_DEMO")
        if broker:
            problems.append("BROKER_ENABLED must NOT be 1 for OPTION_DEMO")
    else:
        problems.append(f"unknown expected mode {expected!r}")

    if problems:
        for p in problems:
            print(f"[safety] FATAL: {p}", file=sys.stderr, flush=True)
        raise SystemExit(f"runtime mode guard failed: {'; '.join(problems)}")


def bind_health_endpoints(
    app,
    *,
    service: str,
    version: str = "1.0.0",
    checks: Optional[Dict[str, Callable[[], bool]]] = None,
    start_wall_clock: float,
) -> None:
    """Bind ``/health``, ``/ready``, ``/metrics`` on a FastAPI app.

    ``/health`` liveness (always 200 once up).  ``/ready`` readiness (200 only
    when all ``checks`` return True, else 503).  ``/metrics`` simple text gauge
    for uptime and per-check status.
    """
    from fastapi.responses import JSONResponse, PlainTextResponse

    checks = checks or {}
    started = start_wall_clock

    async def _now() -> Dict:
        statuses = {name: bool(fn()) for name, fn in checks.items()}
        ready = all(statuses.values())
        return {"service": service, "version": version, "ready": ready,
                "checks": statuses, "uptime_s": round(time.monotonic() - started, 2),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    @app.get("/health", include_in_schema=False)
    async def health() -> Dict:
        info = await _now()
        info.pop("ready", None)
        return {"status": "ok", **info}

    @app.get("/ready", include_in_schema=False)
    async def ready():
        info = await _now()
        if not info["ready"]:
            return JSONResponse(status_code=503, content={"status": "not_ready", **info})
        return {"status": "ready", **info}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> PlainTextResponse:
        info = await _now()
        lines = [
            f"# HELP {service}_uptime_seconds Service uptime",
            f"# TYPE {service}_uptime_seconds gauge",
            f"{service}_uptime_seconds {info['uptime_s']}",
            f"# TYPE {service}_ready gauge",
            f"{service}_ready {1 if info['ready'] else 0}",
        ]
        for name, ok in info["checks"].items():
            lines.append(f"{service}_check_{name} {1 if ok else 0}")
        return PlainTextResponse("\n".join(lines) + "\n")
"""System health monitoring and status reporting."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Optional

IST = timezone(timedelta(hours=5, minutes=30))


class SystemStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class ComponentStatus:
    name: str
    status: SystemStatus
    last_update: float = 0.0
    error_count: int = 0
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status.value,
            "last_update": self.last_update,
            "error_count": self.error_count,
            "message": self.message,
        }


class HealthMonitor:
    """Tracks health of all trading system components."""

    def __init__(self):
        self._components: dict[str, ComponentStatus] = {}
        self._start_time = time.time()
        self._tick_count = 0
        self._bar_count = 0
        self._signal_count = 0
        self._fill_count = 0
        self._error_count = 0

    def register_component(self, name: str) -> None:
        self._components[name] = ComponentStatus(
            name=name, status=SystemStatus.HEALTHY, last_update=time.time(),
        )

    def update_component(self, name: str, status: SystemStatus, message: str = "") -> None:
        if name not in self._components:
            self.register_component(name)
        comp = self._components[name]
        comp.status = status
        comp.last_update = time.time()
        comp.message = message
        if status == SystemStatus.ERROR:
            comp.error_count += 1
            self._error_count += 1

    def mark_all(self, status: SystemStatus, message: str = "") -> None:
        """Set every registered component to the same status."""
        for name in list(self._components):
            self.update_component(name, status, message)

    def record_tick(self) -> None:
        self._tick_count += 1

    def record_bar(self) -> None:
        self._bar_count += 1

    def record_signal(self) -> None:
        self._signal_count += 1

    def record_fill(self) -> None:
        self._fill_count += 1

    def overall_status(self) -> SystemStatus:
        statuses = [c.status for c in self._components.values()]
        if any(s == SystemStatus.ERROR for s in statuses):
            return SystemStatus.ERROR
        if any(s == SystemStatus.STOPPED for s in statuses):
            return SystemStatus.STOPPED
        if any(s == SystemStatus.DEGRADED for s in statuses):
            return SystemStatus.DEGRADED
        if not self._components:
            return SystemStatus.HEALTHY
        return SystemStatus.HEALTHY

    def uptime_seconds(self) -> float:
        return time.time() - self._start_time

    def snapshot(self) -> dict:
        return {
            "overall_status": self.overall_status().value,
            "uptime_seconds": self.uptime_seconds(),
            "tick_count": self._tick_count,
            "bar_count": self._bar_count,
            "signal_count": self._signal_count,
            "fill_count": self._fill_count,
            "error_count": self._error_count,
            "components": {
                name: {**comp.to_dict(), "uptime": max(0.0, time.time() - comp.last_update)}
                for name, comp in self._components.items()
            },
            "timestamp": datetime.now(IST).isoformat(),
        }

    def summary(self) -> str:
        status = self.overall_status().value
        uptime = self.uptime_seconds()
        hours = int(uptime // 3600)
        minutes = int((uptime % 3600) // 60)
        return (
            f"System: {status} | Uptime: {hours}h {minutes}m | "
            f"Ticks: {self._tick_count} | Bars: {self._bar_count} | "
            f"Signals: {self._signal_count} | Fills: {self._fill_count} | "
            f"Errors: {self._error_count}"
        )

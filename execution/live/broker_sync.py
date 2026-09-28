"""Phase 9.8 — BrokerSyncService: single sync point for LIVE environments.

Wraps the existing :class:`LiveBrokerPoller` as the REST-authority worker,
adds optional WS ingest acceleration (:class:`DhanOrderUpdateFeed`), and
provides a health watchdog that surfaces worker death (STOP semantics).

Design:
- The poller owns the REST poll cycles (orders/positions/account/reconcile)
  and all DB persistence.  The service is the *owner* and *health gate* for
  that worker.
- WS ``on_record`` feeds into the transport book via ``ingest_status`` so
  the next REST poll sees fresh statuses immediately; WS records never mint
  fills (parser produces no fill list by design).
- The service exposes ``health()`` / ``stats()`` / ``snapshot()`` for the
  dashboard and engine; ``startup_reconcile()`` is a 9.11 hook.
- Built only when a LIVE env exists; engine calls ``start()`` from
  ``_start_live_pollers`` and ``stop()`` from ``stop()``.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Optional

from config import as_dict

log = logging.getLogger(__name__)

_DEFAULT_HEALTH_TICK = 1.0   # seconds between watchdog checks
# The order-update feed may legitimately sit idle (market closed, no order
# events) and Dhan closes it with code 1000.  Warning on every 1 s tick
# would bury real warnings in ~86k lines/day, so report the edge plus a
# slow heartbeat instead.
_WS_STALE_WARN_INTERVAL = 300.0
_STALE_TICKS = 3             # cycles missed before marking stale


def _resolve_ws_credentials(broker, config: dict):
    """Resolve order-WS ``(token_loader, token, client_id)`` from a broker.

    Priority: ``broker._token_loader`` / ``broker._token`` when present;
    otherwise ``broker._http`` (``DhanRestTransport`` keeps the token inside
    its HTTP client), falling back to the config ``dhan.client_id``.
    """
    token_loader = getattr(broker, "_token_loader", None)
    token = getattr(broker, "_token", "") or ""
    if token_loader is None:
        http = getattr(broker, "_http", None)
        if http is not None:
            renew = getattr(http, "renew_token", None)
            if callable(renew):
                token_loader = renew
            if not token:
                loader = getattr(http, "load_token", None)
                if callable(loader):
                    try:
                        token = loader() or ""
                    except Exception:
                        token = ""
    client_id = str(getattr(broker, "_client_id", "") or
                    getattr(broker, "client_id", "") or "")
    if not client_id:
        client_id = str((config.get("dhan", {}) or {}).get("client_id", ""))
    return token_loader, token, client_id


class BrokerSyncService:
    """Centralized sync service for one LIVE environment."""

    def __init__(
        self,
        env,
        config: Optional[dict] = None,
        clock: Optional[Callable] = None,
        handle_fill: Optional[Callable] = None,
        on_reconcile: Optional[Callable] = None,
        reset_strategy_fn: Optional[Callable] = None,
        wire_now: bool = False,
    ):
        self.env = env
        self.config = config or {}
        self._clock = clock or time.time
        self._handle_fill = handle_fill
        self._on_reconcile = on_reconcile
        self._wire_now = wire_now

        live_cfg = as_dict(config).get("live", {}) or {}
        self._health_tick = _DEFAULT_HEALTH_TICK
        self._stale_threshold = int(live_cfg.get("stale_threshold", 90) or 90)
        self._ws_cfg = live_cfg.get("order_ws") or {}
        self._ws_enabled = bool(self._ws_cfg.get("enabled", False))

        # ── poller (REST worker) ──────────────────────────────────────
        from execution.live.poller import LiveBrokerPoller
        self._poller = LiveBrokerPoller(
            env, config, clock=clock, wire_now=wire_now,
            handle_fill=handle_fill, on_reconcile=on_reconcile,
            reset_strategy_fn=reset_strategy_fn,
        )

        # ── optional WS feed ──────────────────────────────────────────
        self._ws_feed: Any = None
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_stale_active = False
        self._ws_stale_warned_at = 0.0

        # ── health watchdog ───────────────────────────────────────────
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._healthy = True
        self._stop_event = threading.Event()

        # ── staleness tracking (per task, updated by poller cycles) ───
        self._last_cycle: dict[str, float] = {}
        self._consecutive_errors: dict[str, int] = {
            "orders": 0, "positions": 0, "account": 0, "reconcile": 0}

        # ── service-level stats ───────────────────────────────────────
        self._stats: dict[str, int] = {
            "ws_records_ingested": 0,
            "ws_records_applied": 0,
            "health_checks": 0,
            "worker_deaths": 0,
        }

    # ── lifecycle ─────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._healthy = True
        self._stop_event.clear()
        # REST poller
        self._poller.start()
        # optional WS feed
        if self._ws_enabled:
            self._start_ws_feed()
        # health watchdog
        self._thread = threading.Thread(
            target=self._health_loop,
            name=f"broker-sync-{self.env.name}",
            daemon=True,
        )
        self._thread.start()
        log.info("[BrokerSync:%s] service started (ws=%s)",
                 self.env.name, self._ws_enabled)

    def stop(self, timeout: float = 3.0) -> None:
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        # WS feed
        if self._ws_feed is not None:
            try:
                self._ws_feed.stop(timeout=timeout)
            except Exception:
                pass
        # REST poller
        try:
            self._poller.stop(timeout=timeout)
        except Exception:
            pass
        # watchdog thread
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._thread = None
        self._healthy = False
        log.info("[BrokerSync:%s] service stopped", self.env.name)

    @property
    def running(self) -> bool:
        return self._running

    @property
    def healthy(self) -> bool:
        return self._healthy

    # ── WS feed ───────────────────────────────────────────────────────

    def _start_ws_feed(self) -> None:
        """Construct and start DhanOrderUpdateFeed when WS is enabled."""
        try:
            from execution.live.dhan_order_ws import DhanOrderUpdateFeed
        except ImportError:
            log.warning("[BrokerSync:%s] DhanOrderUpdateFeed not available", self.env.name)
            return
        # Token loader: reads from the live broker's token file if available.
        token_loader, token, client_id = _resolve_ws_credentials(
            self.env.broker, self.config)
        self._ws_feed = DhanOrderUpdateFeed(
            client_id=client_id,
            token_loader=token_loader,
            on_record=self._on_ws_record,
            on_status=self._on_ws_status,
            url=self._ws_cfg.get("url", "wss://api-order-update.dhan.co"),
            heartbeat_interval=float(self._ws_cfg.get("heartbeat_interval", 10.0)),
            reconnect_delay=float(self._ws_cfg.get("reconnect_delay", 5.0)),
            stale_threshold=float(self._ws_cfg.get("stale_threshold", 90.0)),
            token=token,
        )
        self._ws_feed.start()

    def _on_ws_record(self, record: dict) -> None:
        """WS record → transport book (accelerator); never mints fills."""
        self._stats["ws_records_ingested"] += 1
        transport = getattr(self.env.broker, "_transport", None)
        if transport is None:
            # DhanRestTransport keeps its public book on the broker object
            # itself (``ingest_status``), so the accelerator works with or
            # without a dedicated ``_transport`` attribute.
            transport = self.env.broker
        ingest = getattr(transport, "ingest_status", None)
        if ingest is None:
            return
        try:
            applied = ingest(record)
            if applied:
                self._stats["ws_records_applied"] += 1
        except Exception as e:
            log.debug("[BrokerSync:%s] WS ingest failed for %s: %s",
                      self.env.name,
                      record.get("broker_order_id", "?"), e)
        # Order Watcher fast path: mirror the WS order alert into local
        # per-order state immediately (never mints fills; REST remains the
        # authority via the watcher's targeted verification).
        watcher = getattr(self, "_order_watcher", None) or \
            getattr(self.env, "order_watcher", None)
        if watcher is not None:
            try:
                watcher.ingest_ws_record(record)
                watcher.scan()
            except Exception as e:
                log.debug("[BrokerSync:%s] order watcher WS ingest failed: %s",
                          self.env.name, e)

    def _on_ws_status(self, status: str) -> None:
        if "error" in status.lower() or "closed" in status.lower():
            log.warning("[BrokerSync:%s] WS status: %s", self.env.name, status)

    # ── fill fast path (order watcher on_fills hook) ───────────────────

    def route_fills(self, fills: list) -> int:
        """Route freshly-minted broker fills into the strategy lifecycle NOW.

        Mirrors the poller's order-cycle fill handling (``router.route_fill``
        with the frozen env-scoped ``_handle_fill`` plus the DB order-row
        upgrade) so fills learned by the order watcher's WS-triggered REST
        verification commit position state and run the post-fill SL compare
        the moment they arrive — no wait for the next 2 s order poll.  The
        engine's broker-fill ledger dedup keeps the path idempotent under the
        WS/poller race; a fill already applied simply routes to a no-op.
        """
        if not fills:
            return 0
        env = self.env
        router = getattr(env, "broker_router", None)
        routed = 0
        for fill in fills:
            oid = getattr(fill, "fill_id", "?")
            try:
                ok = False
                if router is not None:
                    try:
                        ok = bool(router.route_fill(
                            fill,
                            lambda f, es, ix: self._route_fill(f, es, is_exit=ix),
                            entry_signal_id=getattr(fill, "entry_signal_id", None),
                        ))
                    except Exception as e:
                        log.error("[BrokerSync:%s] WS fill route failed for %s: %s",
                                  env.name, oid, e)
                elif self._handle_fill is not None:
                    self._handle_fill(fill, getattr(fill, "entry_signal_id", None))
                    ok = True
                if ok:
                    routed += 1
            except Exception as e:
                log.error("[BrokerSync:%s] WS fill apply failed for %s: %s",
                          env.name, oid, e)
            try:
                self._poller._upgrade_dbs_order_row(fill)
            except Exception as e:
                log.debug("[BrokerSync:%s] WS fill DB upgrade failed: %s",
                          env.name, e)
        if routed:
            self._stats["fills_routed_ws"] = self._stats.get("fills_routed_ws", 0) + routed
        return routed

    def _route_fill(self, fill, signal_id, is_exit=None) -> None:
        if self._handle_fill is not None:
            self._handle_fill(fill, signal_id, is_exit=is_exit)

    # ── health watchdog ───────────────────────────────────────────────

    def _health_loop(self) -> None:
        while self._running and not self._stop_event.is_set():
            try:
                self._health_tick_once()
            except Exception as e:
                log.error("[BrokerSync:%s] health tick failed: %s", self.env.name, e)
            self._stop_event.wait(timeout=self._health_tick)

    def _health_tick_once(self) -> None:
        with self._lock:
            self._stats["health_checks"] += 1
            # Worker thread alive check
            worker_alive = self._poller.running
            if self._running and not worker_alive:
                self._healthy = False
                self._stats["worker_deaths"] += 1
                log.error("[BrokerSync:%s] REST poller worker DIED — stopping sync",
                          self.env.name)
                self._running = False
                self._stop_event.set()
                return
            # WS feed health (when enabled)
            if self._ws_enabled and self._ws_feed is not None:
                ws_alive = self._ws_feed.connected
                stale = self._ws_feed.is_stale() if hasattr(self._ws_feed, "is_stale") else False
                now = self._clock()
                if stale:
                    if (not self._ws_stale_active
                            or (now - self._ws_stale_warned_at) >= _WS_STALE_WARN_INTERVAL):
                        log.warning("[BrokerSync:%s] WS feed stale (heartbeat timeout) "
                                    "connected=%s — retrying every %.0fs",
                                    self.env.name, ws_alive, _WS_STALE_WARN_INTERVAL)
                        self._ws_stale_warned_at = now
                        self._ws_stale_active = True
                elif self._ws_stale_active:
                    log.info("[BrokerSync:%s] WS feed recovered", self.env.name)
                    self._ws_stale_active = False
                    self._ws_stale_warned_at = 0.0
            # Overall health
            self._healthy = worker_alive

    # ── stale detection ───────────────────────────────────────────────

    def record_cycle(self, task: str) -> None:
        """Called by poller cycle hooks to track staleness."""
        with self._lock:
            self._last_cycle[task] = self._clock()

    def last_cycle_time(self, task: str) -> float:
        with self._lock:
            return self._last_cycle.get(task, 0.0)

    def is_stale(self, task: str, since: Optional[float] = None) -> bool:
        """True if task hasn't cycled since ``since`` (default: stale_threshold).

        Falls back to the poller's ``last_run`` timestamps when the service's
        own ``_last_cycle`` dict is empty (prod path: ``record_cycle`` is only
        called in test-facing ``poll_once``).
        """
        if since is None:
            since = self._clock() - self._stale_threshold
        with self._lock:
            lc = self._last_cycle.get(task)
            if lc:
                return lc < since
        # Fallback: poller's own last_run timestamps.
        try:
            lr = self._poller.stats().get("last_run", {})
        except Exception:
            return True
        return lr.get(task, 0.0) < since

    # ── 9.11 startup reconcile hook ──────────────────────────────────

    def startup_reconcile(self) -> dict:
        """§9.11 — pull broker state at boot and adopt OUR OWN day orders.

        Sources (all transport-optional; a non-Dhan broker degrades to a
        status report with empty lists):

        * ``day_order_book()``  — ``GET /orders``: every order the account
          placed today.  Only rows carrying our correlation prefix
          (``MCX-`` / ``EMER-``) are adopted into the transport book so the
          running poller resumes tracking them; a manual broker-side order is
          NEVER adopted, cancelled, or exited.
        * ``positions()`` / ``tradebook()`` — read-only broker truth
          surfaced for the reconcile report.

        The reconcile never places, reverses, or exits anything.
        """
        broker = getattr(self.env, "broker", None)
        transport = getattr(broker, "_transport", None) or broker
        orders: list[dict] = []
        positions: list[dict] = []
        trades: list[dict] = []
        adopted: list[dict] = []
        errors: list[str] = []
        capable = bool(transport is not None
                       and hasattr(transport, "day_order_book")
                       and hasattr(transport, "adopt_order"))

        if transport is not None:
            try:
                if hasattr(transport, "day_order_book"):
                    orders = list(transport.day_order_book() or [])
                if hasattr(transport, "adopt_order"):
                    for row in orders:
                        seeded = transport.adopt_order(row)
                        if seeded is not None:
                            adopted.append(seeded)
            except Exception as e:
                errors.append(f"day_order_book: {e}")
            try:
                if hasattr(transport, "positions"):
                    positions = list(transport.positions() or [])
            except Exception as e:
                errors.append(f"positions: {e}")
            try:
                if hasattr(transport, "tradebook"):
                    trades = list(transport.tradebook() or [])
            except Exception as e:
                errors.append(f"tradebook: {e}")

        outcome = {
            "status": ("ok" if capable else "not_implemented"),
            "orders": orders,
            "adopted": adopted,
            "positions": positions,
            "trades": trades,
            "errors": errors,
        }
        log.info("[BrokerSync:%s] startup_reconcile: %d broker orders, "
                 "%d adopted (ours), %d positions, %d trades",
                 self.env.name, len(orders), len(adopted),
                 len(positions), len(trades))
        return outcome

    # ── aggregated stats / snapshot ───────────────────────────────────

    def stats(self) -> dict:
        with self._lock:
            poller_stats = self._poller.stats()
            poller_stats["worker_alive"] = bool(self._poller.running)
            poller_stats.update(self._stats)
            poller_stats["service_healthy"] = self._healthy
            poller_stats["ws_enabled"] = self._ws_enabled
            poller_stats["ws_connected"] = (
                self._ws_feed.connected if self._ws_feed is not None else False)
            poller_stats["last_cycle"] = dict(self._last_cycle)
            return poller_stats

    def snapshot(self) -> dict:
        poller_snap = self._poller.snapshot()
        poller_snap["service"] = {
            "healthy": self._healthy,
            "ws_enabled": self._ws_enabled,
            "ws_connected": (
                self._ws_feed.connected if self._ws_feed is not None else False),
            "worker_alive": self._poller.running,
        }
        return poller_snap

    # ── test-facing delegations ───────────────────────────────────────

    def poll_once(self, task: str) -> Any:
        """Synchronous one-cycle delegation (tests)."""
        self.record_cycle(task)
        return self._poller.poll_once(task)

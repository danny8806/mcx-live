"""``live.*`` settings must actually reach the components that consume them.

``TradingEngine`` hands the ``Config`` *singleton* (``trading_engine.py:163``)
to BrokerSyncService, LiveBrokerPoller and OrderWatcher, whose signatures say
``dict``.  Those three guarded with ``isinstance(config, dict)``, which is
always False for a Config instance, so ``config["live"]`` was dropped and the
hardcoded defaults ran forever: 2 s order poll, WS feed off, reprice budget 3.
"""
from types import SimpleNamespace

import pytest

from config import Config, as_dict


@pytest.fixture(autouse=True)
def _isolate_singleton():
    saved = Config._config
    yield
    Config._config = saved


# ── as_dict ─────────────────────────────────────────────────────────────


def test_as_dict_passes_a_plain_dict_through_unchanged():
    payload = {"live": {"gate": "ON"}}
    assert as_dict(payload) is payload


def test_as_dict_reads_an_already_loaded_config_singleton():
    class Fake:
        _config = {"live": {"order_poll_interval_seconds": 0.5}}

    assert as_dict(Fake())["live"]["order_poll_interval_seconds"] == 0.5


def test_as_dict_never_triggers_a_lazy_load_of_settings_json():
    # An empty singleton must stay empty rather than pull in config/settings.json.
    Config._config = {}
    assert as_dict(Config()) == {}


def test_as_dict_is_total_for_junk_input():
    assert as_dict(None) == {}
    assert as_dict(object()) == {}

    class NeedsKey:
        def get(self, key, default=None):
            raise AssertionError("as_dict must not call get(key)")

    assert as_dict(NeedsKey()) == {}


# ── LiveBrokerPoller ────────────────────────────────────────────────────


def test_poller_takes_the_order_poll_interval_from_the_live_section():
    from execution.live.poller import LiveBrokerPoller

    Config._config = {
        "live": {
            "order_poll_interval_seconds": 0.5,
            "position_poll_interval_seconds": 7,
            "pnl_poll_interval_seconds": 9,
            "reconcile_interval_seconds": 11,
            "tradebook_reconcile_interval_seconds": 17,
        }
    }
    poller = LiveBrokerPoller(SimpleNamespace(name="live"), Config())

    assert poller.intervals["orders"] == 0.5      # not the 2.0 default
    assert poller.intervals["positions"] == 7
    assert poller.intervals["account"] == 9
    assert poller.intervals["reconcile"] == 11
    assert poller.intervals["tradebook"] == 17


def test_poller_still_falls_back_to_defaults_when_the_section_is_absent():
    from execution.live.poller import LiveBrokerPoller

    Config._config = {"live": {}}
    poller = LiveBrokerPoller(SimpleNamespace(name="live"), Config())

    assert poller.intervals["orders"] == 0.5
    assert poller.intervals["positions"] == 0.5


# ── OrderWatcher ────────────────────────────────────────────────────────


def test_order_watcher_takes_its_budget_from_the_live_section():
    from execution.live.order_watcher import OrderWatcher

    Config._config = {
        "live": {
            "order_watcher": {
                "max_reprices": 2,
                "market_fallback_enabled": True,
                "limit_skip_policy": {"enabled": True},
            }
        }
    }
    watcher = OrderWatcher(config=Config())

    assert watcher._tick_cfg["max_reprices"] == 2          # default is 3
    assert watcher._tick_cfg["market_fallback_enabled"] is True
    assert watcher._tick_cfg["limit_skip_enabled"] is True


# ── BrokerSyncService ───────────────────────────────────────────────────


def test_broker_sync_reads_the_order_ws_flag_from_the_live_section():
    from execution.live.broker_sync import BrokerSyncService

    Config._config = {"live": {"stale_threshold": 45.0,
                               "order_ws": {"enabled": False,
                                            "stale_threshold": 30.0}}}
    sync = BrokerSyncService(SimpleNamespace(name="live"), Config())

    assert sync._ws_enabled is False
    assert sync._stale_threshold == 45.0          # live.stale_threshold
    assert sync._ws_cfg["stale_threshold"] == 30.0   # live.order_ws.stale_threshold
    assert sync._poller.intervals["orders"] == 0.5

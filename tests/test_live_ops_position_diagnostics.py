from types import SimpleNamespace

from dashboard.routes import live_ops


class _Poller:
    def __init__(self, positions):
        self._positions = positions

    def snapshot(self):
        return {"positions": self._positions, "position_mismatches": []}

    def stats(self):
        return {"last_run": {"positions": 1_800_000_000.0}}


def _position(strategy_id, side, quantity, *, instrument="SILVERM", **extra):
    return {
        "strategy_id": strategy_id,
        "instrument": instrument,
        "side": side,
        "quantity": quantity,
        "is_open": True,
        **extra,
    }


def _set_live_env(monkeypatch, env):
    monkeypatch.setattr(live_ops, "_engine", SimpleNamespace(live=env))


def test_position_diagnostics_deduplicate_dhan_instrument_mapping(monkeypatch):
    broker_row = _position(
        "silver_01", "LONG", 1, average_entry_price=229042,
        unrealized_profit=288, ltp=229330,
    )
    env = SimpleNamespace(
        poller=_Poller([
            broker_row,
            {**broker_row, "strategy_id": "silver_02"},
        ]),
        position_manager=SimpleNamespace(snapshot=lambda: {
            "open_positions": {
                "p1": _position(
                    "silver_01", "LONG", 1, average_entry_price=228940,
                    unrealized_pnl=1950, sl_state="ARMED",
                )
            }
        }),
    )
    _set_live_env(monkeypatch, env)

    result = live_ops._get_positions_sync()

    assert result["counts"] == {
        "MATCHED": 1, "MISMATCH": 0, "MISSING_LOCAL": 0, "MISSING_DHAN": 0,
    }
    assert len(result["positions"]) == 1
    row = result["positions"][0]
    assert row["status"] == "MATCHED"
    assert row["local"]["quantity"] == row["dhan"]["quantity"] == 1
    assert row["dhan"]["mapped_strategy_rows"] == 2
    assert row["dhan"]["unrealized_profit"] == 288
    assert row["local"]["sl_state"] == "ARMED"


def test_position_diagnostics_aggregate_local_strategy_owners(monkeypatch):
    env = SimpleNamespace(
        poller=_Poller([_position("silver_01", "LONG", 2)]),
        position_manager=SimpleNamespace(snapshot=lambda: {
            "open_positions": {
                "p1": _position("silver_01", "LONG", 1,
                                 average_entry_price=100, unrealized_pnl=4,
                                 sl_state="ARMED"),
                "p2": _position("silver_02", "LONG", 1,
                                 average_entry_price=102, unrealized_pnl=6,
                                 sl_state="ARMED"),
            }
        }),
    )
    _set_live_env(monkeypatch, env)

    row = live_ops._get_positions_sync()["positions"][0]

    assert row["status"] == "MATCHED"
    assert row["strategy_id"] is None
    assert row["local_owners"] == ["silver_01", "silver_02"]
    assert row["local"]["side"] == row["dhan"]["side"] == "LONG"
    assert row["local"]["quantity"] == row["dhan"]["quantity"] == 2
    assert row["local"]["average_entry_price"] == 101
    assert row["local"]["unrealized_pnl"] == 10


def test_position_diagnostics_do_not_hide_conflicting_broker_rows(monkeypatch):
    env = SimpleNamespace(
        poller=_Poller([
            _position("silver_01", "LONG", 1),
            _position("silver_02", "SHORT", 1),
        ]),
        position_manager=SimpleNamespace(snapshot=lambda: {
            "open_positions": {"p1": _position("silver_01", "LONG", 1)}
        }),
    )
    _set_live_env(monkeypatch, env)

    row = live_ops._get_positions_sync()["positions"][0]

    assert row["status"] == "MISMATCH"
    assert row["dhan"]["side"] is None
    assert row["dhan"]["conflicting_rows"] == 2


def test_sync_reports_flattened_poller_broker_position_count(monkeypatch):
    sync = SimpleNamespace(
        running=True,
        stats=lambda: {
            "intervals": {"positions": 2},
            "last_run": {"positions": 1_800_000_000.0},
            "broker_positions_count": 2,
            "service_healthy": True,
            "worker_alive": True,
            "ws_enabled": True,
            "ws_connected": True,
        },
        snapshot=lambda: {
            "running": True,
            "positions": [_position("silver_01", "LONG", 1),
                          _position("silver_02", "LONG", 1)],
            "accounts": {"available_margin": 100},
            "position_mismatches": [],
        },
    )
    _set_live_env(monkeypatch, SimpleNamespace(sync_service=sync, data_adapter=None))

    result = live_ops._get_sync_sync()

    assert result["sync"]["poller"]["broker_positions_count"] == 2
    assert result["sync"]["poller"]["running"] is True

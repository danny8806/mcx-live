from types import SimpleNamespace

import pytest

from dashboard.routes import orders, trades
from execution.fee_model import MCXFeeModel


def _closed_trade(side="", entry=228647.0, exit_price=226623.0,
                  entry_order="entry", exit_order="cancelled-limit",
                  multiplier=5.0, trade_id="t1"):
    return {
        "trade_id": trade_id,
        "strategy_id": "silver_01",
        "instrument": "SILVERM",
        "side": side,
        "status": "CLOSED",
        "entry_price": entry,
        "exit_price": exit_price,
        "quantity": 1,
        "multiplier": multiplier,
        "charges": 84.37,
        "gross_pnl": exit_price - entry,
        "net_pnl": (exit_price - entry) - 84.37,
        "entry_fill_id": "entry-fill",
        "exit_fill_id": "exit-fill",
        "entry_order_id": entry_order,
        "exit_order_id": exit_order,
    }


def _fills(trade_id="t1", entry_side="BUY", entry=228647.0,
           exit_price=226623.0, exit_order="market-fallback"):
    return [
        {"trade_id": trade_id, "fill_id": "entry-fill", "order_id": "entry",
         "side": entry_side, "quantity": 1, "price": entry,
         "timestamp": "2026-10-01T10:00:00Z"},
        {"trade_id": trade_id, "fill_id": "exit-fill", "order_id": exit_order,
         "side": "SELL" if entry_side == "BUY" else "BUY",
         "quantity": 1, "price": exit_price,
         "timestamp": "2026-10-01T11:00:00Z"},
    ]


@pytest.mark.parametrize(
    "entry_side,entry,exit_price,expected_side,expected_gross",
    [
        ("BUY", 228647.0, 226623.0, "LONG", -10120.0),
        ("SELL", 226555.0, 228000.0, "SHORT", -7225.0),
    ],
)
def test_imported_history_rebuilds_multiplier_and_direction_from_actual_fills(
    entry_side, entry, exit_price, expected_side, expected_gross,
):
    trade_id = f"legacy-{entry_side}"
    trade = _closed_trade(entry=entry, exit_price=exit_price,
                          trade_id=trade_id)
    fills = _fills(trade_id=trade_id, entry_side=entry_side,
                   entry=entry, exit_price=exit_price)
    fee_model = MCXFeeModel(stamp_duty_pct=0.0)

    repaired = trades._reconcile_history_from_fills(trade, fills, fee_model)

    assert repaired["side"] == expected_side
    assert repaired["entry_side"] == expected_side
    assert repaired["entry_action"] == entry_side
    assert repaired["gross_pnl"] == expected_gross
    assert repaired["net_pnl"] == pytest.approx(
        expected_gross - repaired["charges"])
    assert repaired["pnl_reconciled_from_fills"] is True
    assert repaired["charges_basis"] == "system_fee_model_estimate"


def test_history_links_exit_to_filled_fallback_and_keeps_cancelled_limit_lineage():
    trade = _closed_trade()
    fills = _fills(exit_order="filled-market-fallback")

    repaired = trades._reconcile_history_from_fills(
        trade, fills, MCXFeeModel(stamp_duty_pct=0.0))

    assert repaired["exit_order_id"] == "filled-market-fallback"
    assert repaired["parent_exit_order_id"] == "cancelled-limit"


def test_incomplete_fill_set_does_not_fabricate_reconstructed_pnl():
    trade = _closed_trade()
    only_entry = _fills()[:1]

    repaired = trades._reconcile_history_from_fills(trade, only_entry)

    assert repaired["gross_pnl"] == trade["gross_pnl"]
    assert "pnl_reconciled_from_fills" not in repaired


def test_trade_history_merges_persisted_rows_with_runtime_rows(monkeypatch):
    life_rows = [
        {"trade_id": "recent", "strategy_id": "gold_02", "instrument": "GOLDM",
         "status": "REJECTED", "created_at": 3},
        {"trade_id": "duplicate", "strategy_id": "silver_01", "instrument": "SILVERM",
         "status": "CLOSED", "side": "LONG", "created_at": 2},
    ]
    stored_rows = [
        {"trade_id": "old", "strategy_id": "silver_01", "instrument": "SILVERM",
         "status": "CLOSED", "created_at": 1},
        {"trade_id": "duplicate", "strategy_id": "silver_01", "instrument": "SILVERM",
         "status": "CLOSED", "side": "", "net_pnl": -5, "created_at": 2},
    ]
    lifecycle = SimpleNamespace(get_trades_for_api=lambda **_: life_rows)
    persistence = SimpleNamespace(
        get_trades=lambda **_: stored_rows,
        get_fills=lambda: [],
    )
    runtime_registry = SimpleNamespace(all=lambda: [SimpleNamespace(lifecycle=lifecycle)])
    env = SimpleNamespace(mode="LIVE", runtimes=runtime_registry,
                          persistence=persistence, pnl_engines={})
    monkeypatch.setattr(trades, "_engine", SimpleNamespace(live=env))
    monkeypatch.setattr(trades, "_env_for", lambda _env=None: env)

    result = trades._list_trades_sync()

    assert result["source"] == "merged"
    assert result["count"] == 3
    assert {row["trade_id"] for row in result["trades"]} == {
        "recent", "old", "duplicate"}
    duplicate = next(row for row in result["trades"]
                     if row["trade_id"] == "duplicate")
    assert duplicate["side"] == "LONG"


def test_instrument_filter_applies_to_both_history_sources(monkeypatch):
    life = SimpleNamespace(get_trades_for_api=lambda **_: [
        {"trade_id": "life-gold", "instrument": "GOLDM", "created_at": 2},
    ])
    persistence = SimpleNamespace(
        get_trades=lambda **_: [
            {"trade_id": "persist-silver", "instrument": "SILVERM", "created_at": 1},
        ],
        get_fills=lambda: [],
    )
    runtime_registry = SimpleNamespace(all=lambda: [SimpleNamespace(lifecycle=life)])
    env = SimpleNamespace(mode="LIVE", runtimes=runtime_registry,
                          persistence=persistence, pnl_engines={})
    monkeypatch.setattr(trades, "_engine", SimpleNamespace(live=env))
    monkeypatch.setattr(trades, "_env_for", lambda _env=None: env)

    result = trades._list_trades_sync(instrument="SILVERM")

    assert result["count"] == 1
    assert result["trades"][0]["trade_id"] == "persist-silver"


def test_fill_history_sorts_mixed_epoch_and_iso_timestamps(monkeypatch):
    runtime_fill = SimpleNamespace(
        fill_id="runtime-fill", order_id="market-fallback", instrument="SILVERM",
        side="SELL", quantity=1, price=226623.0,
        timestamp="2026-10-01T11:00:00+00:00", strategy_id="silver_01",
        multiplier=5.0)
    persisted_fill = {
        "fill_id": "persisted-fill", "order_id": "entry", "instrument": "SILVERM",
        "side": "BUY", "quantity": 1, "price": 228647.0,
        "timestamp": "2026-10-01T10:00:00+00:00", "strategy_id": "silver_01",
    }
    execution = SimpleNamespace(get_fills=lambda **_: [runtime_fill])
    persistence = SimpleNamespace(get_fills=lambda: [persisted_fill])
    monkeypatch.setattr(orders, "_engine", SimpleNamespace(
        execution_engine=execution, _persistence=persistence))

    result = orders._list_fills_sync(strategy="silver_01", instrument="SILVERM")

    assert result["count"] == 2
    assert [fill["fill_id"] for fill in result["fills"]] == [
        "runtime-fill", "persisted-fill"]
    assert all(isinstance(fill["timestamp"], float) for fill in result["fills"])


def test_live_portfolio_pnl_uses_same_fill_reconciled_rows_as_trade_history(monkeypatch):
    from dashboard.routes import pnl

    trade = _closed_trade(trade_id="closed-1")
    persisted = SimpleNamespace(
        get_trades=lambda **_: [trade],
        get_fills=lambda: _fills(trade_id="closed-1"),
    )
    stale_engine = SimpleNamespace(
        fee_model=MCXFeeModel(stamp_duty_pct=0.0),
        snapshot=lambda: {"realized_gross": -1, "realized_charges": 1,
                          "realized_net": -2, "trade_count": 99,
                          "wins": 90, "losses": 9},
    )
    pm = SimpleNamespace(
        get_positions_by_instrument=lambda _: [SimpleNamespace(
            is_open=True, unrealized_pnl=-50.0)],
    )
    env = SimpleNamespace(
        mode="LIVE", persistence=persisted,
        runtimes=None, pnl_engines={"silver_01": stale_engine},
        account_engine=SimpleNamespace(snapshot=lambda: {
            "realized_pnl": -2, "unrealized_pnl": -999,
            "net_pnl": -1001, "charges": 1, "equity": 500000,
            "starting_capital": 1200000,
        }),
        position_manager=pm,
    )
    engine = SimpleNamespace(
        config={"strategies": {"silver_01": {"instrument": "SILVERM"}}},
        live=env,
    )
    monkeypatch.setattr(trades, "_engine", engine)
    monkeypatch.setattr(trades, "_env_for", lambda _env=None: env)
    monkeypatch.setattr(pnl, "_engine", engine)
    monkeypatch.setattr(pnl, "_env_for", lambda _env=None: env)

    result = pnl._get_portfolio_pnl_sync()

    row = trades._reconcile_history_from_fills(
        trade, _fills(trade_id="closed-1"), stale_engine.fee_model)
    assert result["portfolio"]["realized_pnl"] == row["net_pnl"]
    assert result["portfolio"]["unrealized_pnl"] == -50.0
    assert result["portfolio"]["net_pnl"] == pytest.approx(
        row["net_pnl"] - 50.0)
    assert result["portfolio"]["realized_pnl"] != -2
    assert result["portfolio"]["pnl_source"] == "fill_reconciled_trade_history"
    assert result["portfolio"]["reconciled_trade_count"] == 1
    assert result["by_instrument"]["SILVERM"]["trade_count"] == 1


def test_history_aggregate_excludes_closed_rows_without_fill_reconciliation(monkeypatch):
    from dashboard import history_pnl

    rows = [
        {"trade_id": "verified", "instrument": "SILVERM",
         "strategy_id": "silver_01", "status": "CLOSED",
         "pnl_reconciled_from_fills": True, "gross_pnl": -50,
         "charges": 5, "net_pnl": -55},
        {"trade_id": "legacy-unknown", "instrument": "SILVERM",
         "strategy_id": "silver_01", "status": "CLOSED",
         "gross_pnl": -900, "charges": 20, "net_pnl": -920},
    ]
    monkeypatch.setattr(
        trades, "_list_trades_sync",
        lambda **_: {"trades": rows, "source": "merged"},
    )

    result = history_pnl.trade_history_pnl(SimpleNamespace())

    assert result["total"]["realized_gross"] == -50
    assert result["total"]["realized_charges"] == 5
    assert result["total"]["realized_net"] == -55
    assert result["total"]["trade_count"] == 2
    assert result["total"]["pnl_trade_count"] == 1
    assert result["total"]["reconciled_trade_count"] == 1
    assert result["total"]["unreconciled_trade_count"] == 1

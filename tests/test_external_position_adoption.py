"""External broker-position import uses verified fills and real candle stops."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from core.trade_close import TradeCloseManager
from execution.live.broker_client import StubLiveBroker
from persistence.manager import PersistenceManager
from trading_engine import TradingEngine
from live import api as live_api


BROKER_ORDER = "24826093037104"
BROKER_FILL = "240073750"
CANDLE_TS = 1790745300.0
FILL_TS = 1790746333.0


def _adoption_runtime(tmp_path, monkeypatch, *, live_price=229107.0,
                      omit_prior_from_recent_window=False):
    root = Path(__file__).resolve().parents[1]
    config = json.loads((root / "config" / "live_settings.json").read_text())
    for key, filename in (
        ("live_db_path", "live.db"),
        ("live_state_path", "live-state.json"),
        ("db_path", "paper.db"),
        ("state_path", "paper-state.json"),
    ):
        config["system"][key] = str(tmp_path / filename)
    config["live"]["broker"] = "stub"
    config["live"]["gate"] = "ON"
    config["live"]["live_trading_enabled"] = True
    config["strategies"] = {
        "silver_01": dict(config["strategies"]["silver_01"], quantity=1,
                           enabled=True, live_gate="ON")
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    engine = TradingEngine(config_path=str(config_path), live_only=True)
    env = engine.live
    broker = StubLiveBroker(gate_enabled=True)
    broker.positions = lambda: [
        {"strategy_id": "silver_01", "instrument": "SILVERM",
         "side": "LONG", "quantity": 1},
        {"strategy_id": "silver_02", "instrument": "SILVERM",
         "side": "LONG", "quantity": 1},
    ]
    broker.day_order_book = lambda: [{
        "broker_order_id": BROKER_ORDER, "status": "filled",
        "side": "BUY", "quantity": 1, "filled_quantity": 1,
        "average_fill_price": 228095.0, "order_type": "LIMIT",
        "security_id": "483080",
    }]
    broker.tradebook = lambda: [
        {"orderId": "older-buy", "securityId": "483080",
         "transactionType": "BUY", "tradedQuantity": 1,
         "tradedPrice": 229144.0, "exchangeTradeId": "older-fill",
         "exchangeTime": "2026-09-30 10:15:38"},
        {"orderId": "older-sell", "securityId": "483080",
         "transactionType": "SELL", "tradedQuantity": 1,
         "tradedPrice": 228208.0, "exchangeTradeId": "older-exit",
         "exchangeTime": "2026-09-30 10:46:12"},
        {"orderId": BROKER_ORDER, "securityId": "483080",
         "transactionType": "BUY", "tradedQuantity": 1,
         "tradedPrice": 228940.0, "exchangeTradeId": BROKER_FILL,
         "exchangeTime": "2026-09-30 11:02:13"},
    ]
    env.broker = broker
    env.execution_engine.broker = broker
    prior = [1790744400.0, 228900.0, 229140.0, 228349.0, 228374.0, 316.0]
    signal_candle = [CANDLE_TS, 228374.0, 228900.0, 228100.0,
                     228895.0, 467.0]
    newer_closed = [1790746200.0, 228895.0, 229140.0, 228700.0,
                    228871.0, 300.0]
    recent_signal = [CANDLE_TS, 228374.0, 228900.0, 228400.0,
                     228895.0, 467.0]
    recent_closed = ([recent_signal, newer_closed] if omit_prior_from_recent_window
                     else [prior, signal_candle, newer_closed])
    env.data_adapter = SimpleNamespace(fetch_candle_state=lambda *_args: {
        "closed": recent_closed,
        "forming": [1790747100.0, 228871.0, 229140.0,
                    228732.0, live_price, 300.0],
    })
    if omit_prior_from_recent_window:
        env.data_adapter.fetch_historical_candles = lambda *_args: [prior, signal_candle]
    persistence = PersistenceManager(
        state_path=str(tmp_path / "live-state.json"),
        db_path=str(tmp_path / "live.db"), execution_mode="LIVE")
    engine.set_persistence(persistence, env_name="live")
    env.trade_close_manager = TradeCloseManager(
        position_manager=env.position_manager,
        pnl_engines=env.pnl_engines,
        account_engines=env.account_engines,
        global_account=env.account_engine,
        risk_engine=env.risk_engine,
        persistence=persistence,
        event_store=env.event_store,
        telegram=engine.telegram,
        event_callback=engine._event_callback,
        trade_ledger=env.trade_ledger,
    )
    runtime = env.runtimes.require("silver_01")
    runtime.position_manager  # assert this strategy runtime exists
    monkeypatch.setattr(live_api, "_engine", engine)
    monkeypatch.setattr(live_api, "_persistence", persistence)
    return engine, env, broker, persistence


def test_imports_exact_dhan_fill_and_arms_candle_stop_once(tmp_path, monkeypatch):
    engine, env, broker, persistence = _adoption_runtime(tmp_path, monkeypatch)
    body = {"strategy_id": "silver_01", "broker_order_id": BROKER_ORDER,
            "signal_candle_timestamp": CANDLE_TS}
    try:
        result = live_api._adopt_broker_position(body)
        assert result["adopted"] is True
        assert result["broker_order_sent"] is False
        assert result["trigger_price"] == 228900.0
        assert result["stop_price"] == 228100.0
        assert result["position"]["side"] == "LONG"
        assert result["position"]["quantity"] == 1
        assert result["position"]["average_entry"] == 228940.0
        assert result["position"]["sl_state"] == "ARMED"
        assert len(env.runtimes.require("silver_01").position_manager.open_positions) == 1
        assert len(persistence.get_open_positions("silver_01")) == 1
        assert persistence.fill_by_broker_fill_id(BROKER_FILL)
        assert any(f.fill_id == f"DHAN-{BROKER_FILL}"
                   for f in env.execution_engine._fills)
        assert not broker._orders  # import did not submit a second Dhan order

        env.execution_engine._fills.clear()
        duplicate = live_api._adopt_broker_position(body)
        assert duplicate["already_adopted"] is True
        assert any(f.fill_id == f"DHAN-{BROKER_FILL}"
                   for f in env.execution_engine._fills)
        assert len(persistence.get_fills()) == 1
        assert len(persistence.get_trades("silver_01")) == 1
    finally:
        engine.stop()
        persistence.close()


def test_import_refuses_position_after_stop_cross_without_creating_trade(
        tmp_path, monkeypatch):
    engine, env, broker, persistence = _adoption_runtime(
        tmp_path, monkeypatch, live_price=228000.0)
    try:
        with pytest.raises(HTTPException) as exc:
            live_api._adopt_broker_position({
                "strategy_id": "silver_01", "broker_order_id": BROKER_ORDER,
                "signal_candle_timestamp": CANDLE_TS})
        assert exc.value.status_code == 409
        assert not env.runtimes.require("silver_01").position_manager.open_positions
        assert not persistence.get_trades("silver_01")
        assert not broker._orders
    finally:
        engine.stop()
        persistence.close()


def test_import_fetches_prior_candle_when_recent_window_does_not_include_it(
        tmp_path, monkeypatch):
    engine, env, broker, persistence = _adoption_runtime(
        tmp_path, monkeypatch, omit_prior_from_recent_window=True)
    try:
        result = live_api._adopt_broker_position({
            "strategy_id": "silver_01", "broker_order_id": BROKER_ORDER,
            "signal_candle_timestamp": CANDLE_TS})
        assert result["adopted"] is True
        assert result["stop_price"] == 228100.0
        assert result["position"]["sl_state"] == "ARMED"
        assert not broker._orders
    finally:
        engine.stop()
        persistence.close()


def test_operator_can_attribute_broker_confirmed_manual_fill_to_supplied_signal_candle(
        tmp_path, monkeypatch):
    engine, env, broker, persistence = _adoption_runtime(
        tmp_path, monkeypatch, omit_prior_from_recent_window=True)
    newer_closed = [1790746200.0, 228895.0, 229140.0, 228700.0,
                    228871.0, 300.0]
    env.data_adapter = SimpleNamespace(
        fetch_candle_state=lambda *_args: {"closed": [newer_closed], "forming": None},
        get_live_ltp=lambda _symbol: 229107.0,
    )
    body = {
        "strategy_id": "silver_01", "broker_order_id": BROKER_ORDER,
        "signal_candle_timestamp": CANDLE_TS,
        "operator_confirmed_manual_fill": True,
        "operator_signal_candle": {
            "timestamp": CANDLE_TS, "open": 228374.0, "high": 228900.0,
            "low": 228100.0, "close": 228895.0,
            "trigger_price": 228900.0, "stop_price": 228100.0,
        },
    }
    try:
        result = live_api._adopt_broker_position(body)
        position = env.runtimes.require("silver_01").position_manager.open_positions[0]
        assert result["adopted"] is True
        assert result["broker_order_sent"] is False
        assert result["trigger_price"] == 228900.0
        assert result["stop_price"] == 228100.0
        assert position.side.value == "LONG"
        assert position.sl_state == "ARMED"
        assert position.stop_price == 228100.0
        assert persistence.fill_by_broker_fill_id(BROKER_FILL)
        assert not broker._orders
    finally:
        engine.stop()
        persistence.close()


def test_operator_manual_import_uses_stop_from_exact_persisted_reversal_signal(
        tmp_path, monkeypatch):
    engine, env, broker, persistence = _adoption_runtime(
        tmp_path, monkeypatch, omit_prior_from_recent_window=True)
    source_signal_id = "REVERSAL-LONG-STOP-REFERENCE"
    source_stop = 228050.0
    source_trigger = 228902.0
    persistence.save_signal({
        "signal_id": source_signal_id, "strategy_id": "silver_01",
        "instrument": "SILVERM", "side": "LONG", "signal_type": "entry",
        "timestamp": CANDLE_TS, "candle_timestamp": CANDLE_TS,
        "open": 228374.0, "high": 228900.0, "low": 228100.0,
        "close": 228895.0, "trigger_price": source_trigger,
        "stop_price": source_stop, "quantity": 1,
        "signal_metadata": {"is_reversal_entry": True},
    })
    newer_closed = [1790746200.0, 228895.0, 229140.0, 228700.0,
                    228871.0, 300.0]
    env.data_adapter = SimpleNamespace(
        fetch_candle_state=lambda *_args: {"closed": [newer_closed], "forming": None},
        get_live_ltp=lambda _symbol: 229107.0,
    )
    body = {
        "strategy_id": "silver_01", "broker_order_id": BROKER_ORDER,
        "signal_candle_timestamp": CANDLE_TS,
        "operator_confirmed_manual_fill": True,
        "operator_signal_candle": {
            "timestamp": CANDLE_TS, "open": 228374.0, "high": 228900.0,
            "low": 228100.0, "close": 228895.0,
            "trigger_price": source_trigger, "stop_price": source_stop,
            "source_signal_id": source_signal_id,
        },
    }
    try:
        result = live_api._adopt_broker_position(body)
        position = env.runtimes.require("silver_01").position_manager.open_positions[0]
        assert result["adopted"] is True
        assert result["broker_order_sent"] is False
        assert result["trigger_price"] == source_trigger
        assert result["stop_price"] == source_stop
        assert position.side.value == "LONG"
        assert position.sl_state == "ARMED"
        assert position.stop_price == source_stop
        assert not broker._orders
    finally:
        engine.stop()
        persistence.close()


def test_operator_manual_reversal_import_completes_the_existing_reversal_chain(
        tmp_path, monkeypatch):
    engine, env, broker, persistence = _adoption_runtime(
        tmp_path, monkeypatch, omit_prior_from_recent_window=True)
    parent_id = "REV-EXIT-MANUAL-IMPORT"
    source_signal_id = "REVERSAL-LONG-PAIRED-ENTRY"
    source_trigger = 228902.0
    source_stop = 228050.0
    persistence.save_signal({
        "signal_id": source_signal_id, "strategy_id": "silver_01",
        "instrument": "SILVERM", "side": "LONG", "signal_type": "entry",
        "timestamp": CANDLE_TS, "candle_timestamp": CANDLE_TS,
        "open": 228374.0, "high": 228900.0, "low": 228100.0,
        "close": 228895.0, "trigger_price": source_trigger,
        "stop_price": source_stop, "quantity": 1,
        "signal_metadata": {"is_reversal_entry": True,
                             "reversal_parent_signal_id": parent_id},
    })
    persistence.save_reversal({
        "reversal_id": "RV-MANUAL-IMPORT-1", "signal_id": parent_id,
        "strategy_id": "silver_01", "instrument": "SILVERM",
        "old_exit_order_id": "OLD-EXIT", "old_exit_broker_status": "FILLED",
        "old_exit_fill_price": 228700.0, "old_exit_filled_quantity": 1,
        "exit_verified_at": "2026-09-30T10:00:00+00:00",
        "status": "EXIT_FILLED",
    })
    env.data_adapter = SimpleNamespace(
        fetch_candle_state=lambda *_args: {"closed": [[
            1790746200.0, 228895.0, 229140.0, 228700.0, 228871.0, 300.0]],
            "forming": None},
        get_live_ltp=lambda _symbol: 229107.0,
    )
    body = {
        "strategy_id": "silver_01", "broker_order_id": BROKER_ORDER,
        "signal_candle_timestamp": CANDLE_TS,
        "operator_confirmed_manual_fill": True,
        "operator_signal_candle": {
            "timestamp": CANDLE_TS, "open": 228374.0, "high": 228900.0,
            "low": 228100.0, "close": 228895.0,
            "trigger_price": source_trigger, "stop_price": source_stop,
            "source_signal_id": source_signal_id,
        },
    }
    try:
        result = live_api._adopt_broker_position(body)
        reversal = persistence.get_reversal("RV-MANUAL-IMPORT-1")
        imported = next(iter(env.execution_engine._orders.values()))
        assert result["reversal_parent_signal_id"] == parent_id
        assert imported.order_role == "REVERSAL_ENTRY"
        assert imported.reversal_parent_signal_id == parent_id
        assert reversal["status"] == "COMPLETE"
        assert reversal["new_entry_order_id"] == f"IMPORT-{BROKER_ORDER}"
        assert reversal["new_broker_order_id"] == BROKER_ORDER
        assert reversal["new_entry_broker_status"] == "FILLED"
        assert reversal["new_sl_state"] == "ARMED"
        assert not broker._orders
    finally:
        engine.stop()
        persistence.close()


def test_correct_imported_stop_from_exact_historical_signal_candle(
        tmp_path, monkeypatch):
    engine, env, broker, persistence = _adoption_runtime(tmp_path, monkeypatch)
    try:
        body = {"strategy_id": "silver_01", "broker_order_id": BROKER_ORDER,
                "signal_candle_timestamp": CANDLE_TS}
        live_api._adopt_broker_position(body)
        runtime = env.runtimes.require("silver_01")
        position = runtime.position_manager.get_positions_by_strategy("silver_01")[0]
        order = next(iter(env.execution_engine._orders.values()))
        position.stop_price = 228349.0
        order.planned_sl = 228349.0
        env.sl_monitor = engine._sl_monitor(env)
        env.sl_monitor.arm(position)
        prior = [1790744400.0, 228900.0, 229140.0, 228349.0, 228374.0, 316.0]
        signal = [CANDLE_TS, 228374.0, 228900.0, 228100.0, 228895.0, 467.0]
        env.data_adapter.fetch_historical_candles = lambda *_args: [prior, signal]

        result = live_api._correct_imported_position_stop(body)
        assert result["stop_price"] == 228100.0
        assert result["monitor_state"] == "ARMED"
        assert position.stop_price == 228100.0
        assert order.planned_sl == 228100.0
        assert result["broker_order_sent"] is False
        assert not broker._orders
    finally:
        engine.stop()
        persistence.close()


def _prepare_reversal_short(tmp_path, monkeypatch, *, live_price=229107.0):
    engine, env, broker, persistence = _adoption_runtime(
        tmp_path, monkeypatch, live_price=live_price)
    from strategies.types import Signal, SignalType, freeze_signal_context

    strategy_id = "silver_01"
    parent_id = "REVERSAL-EXIT-1"
    signal_id = "REVERSAL-SHORT-ENTRY-1"
    broker_order_id = "35826093061304"
    broker_fill_id = "240176402"
    broker.positions = lambda: [
        {"strategy_id": "silver_01", "instrument": "SILVERM",
         "side": "SHORT", "quantity": 1},
        {"strategy_id": "silver_02", "instrument": "SILVERM",
         "side": "SHORT", "quantity": 1},
    ]
    broker.day_order_book = lambda: [{
        "broker_order_id": broker_order_id, "status": "filled",
        "side": "SELL", "quantity": 1, "filled_quantity": 1,
        "average_fill_price": 228940.0, "order_type": "LIMIT",
        "security_id": "483080",
    }]
    broker.tradebook = lambda: [
        {"orderId": "older-buy", "securityId": "483080",
         "transactionType": "BUY", "tradedQuantity": 1,
         "tradedPrice": 229144.0, "exchangeTradeId": "older-fill",
         "exchangeTime": "2026-09-30 10:15:38"},
        {"orderId": "older-sell", "securityId": "483080",
         "transactionType": "SELL", "tradedQuantity": 1,
         "tradedPrice": 228208.0, "exchangeTradeId": "older-exit",
         "exchangeTime": "2026-09-30 10:46:12"},
        {"orderId": broker_order_id, "securityId": "483080",
         "transactionType": "SELL", "tradedQuantity": 1,
         "tradedPrice": 228095.0, "exchangeTradeId": broker_fill_id,
         "exchangeTime": "2026-09-30 11:02:13"},
    ]
    env.broker = broker
    env.execution_engine.broker = broker
    signal = Signal(
        signal_type=SignalType.SHORT, instrument="SILVERM",
        strategy_id=strategy_id, timestamp=CANDLE_TS,
        trigger_price=228100.0, stop_price=229140.0,
        quantity=1, side="SHORT",
        metadata={
            "is_reversal": True, "is_reversal_entry": True,
            "entry_after_confirmed_reversal_exit": True,
            "reversal_entry_trigger_level": 228100.0,
            "trigger_state": "FIRED", "trigger_generation": 1,
            "trigger_source": "broker_confirmed_reversal_flat",
            "reversal_parent_signal_id": parent_id,
        },
    )
    freeze_signal_context(signal, timestamp=CANDLE_TS, open_=228374.0,
                          high=228900.0, low=228100.0, close=228895.0)
    engine._persist_signal(signal, "entry", env_name="live")
    persistence.save_signal({
        "signal_id": signal.signal_id, "strategy_id": strategy_id,
        "instrument": "SILVERM", "side": "SHORT", "signal_type": "entry",
        "timestamp": CANDLE_TS, "trigger_price": 228100.0,
        "stop_price": 229140.0, "quantity": 1,
        "candle_timestamp": CANDLE_TS,
        "signal_metadata": signal.metadata,
    })
    runtime = env.runtimes.require(strategy_id)
    trade = runtime.lifecycle.create_trade_from_signal(
        signal, strategy_id, strategy_id, "SILVERM", 1, 5.0)
    # Simulate a restored pending DB row whose legacy signal fields hydrated
    # blank; adoption must recover them only from this exact durable signal.
    trade.entry_side = ""
    trade.entry_trigger_price = 0.0
    runtime.lifecycle.persist_trade(trade)
    persistence.save_signal({
        "signal_id": parent_id, "strategy_id": strategy_id,
        "instrument": "SILVERM", "side": "SHORT", "signal_type": "exit",
        "timestamp": CANDLE_TS, "trigger_price": 228100.0,
        "stop_price": 229305.0, "quantity": 1,
        "signal_metadata": {"exit": True, "is_reversal": True},
    })
    persistence.save_reversal({
        "reversal_id": "RV-TEST-SHORT", "signal_id": parent_id,
        "strategy_id": strategy_id, "instrument": "SILVERM",
        "side": "SHORT",
        "old_trade_id": "OLD-TRADE", "old_position_id": "OLD-POSITION",
        "old_exit_order_id": "OLD-EXIT", "old_exit_broker_status": "FILLED",
        "reversal_trigger_price": 228100.0,
        "exit_verified_at": "2026-09-30T05:32:00+00:00",
        "status": "EXIT_FILLED",
    })
    return engine, env, broker, persistence, signal, trade, broker_order_id


def test_imports_manual_short_into_pending_reversal_and_arms_local_sl(
        tmp_path, monkeypatch):
    engine, env, broker, persistence, signal, trade, broker_order_id = (
        _prepare_reversal_short(tmp_path, monkeypatch))
    try:
        result = live_api._adopt_filled_reversal_short({
            "strategy_id": "silver_01", "broker_order_id": broker_order_id,
            "signal_id": signal.signal_id,
        })
        assert result["adopted"] is True
        assert result["broker_order_sent"] is False
        assert result["position"]["side"] == "SHORT"
        assert result["position"]["quantity"] == 1
        assert result["stop_price"] == 229140.0
        assert result["position"]["sl_state"] == "ARMED"
        assert persistence.get_reversal_by_signal_id("REVERSAL-EXIT-1")["status"] == "COMPLETE"
        assert persistence.fill_by_broker_fill_id("240176402")
        assert not broker._orders
    finally:
        engine.stop()
        persistence.close()


def test_manual_short_adoption_refuses_when_short_stop_is_already_crossed(
        tmp_path, monkeypatch):
    engine, env, broker, persistence, signal, _trade, broker_order_id = (
        _prepare_reversal_short(tmp_path, monkeypatch, live_price=229200.0))
    try:
        with pytest.raises(HTTPException) as exc:
            live_api._adopt_filled_reversal_short({
                "strategy_id": "silver_01", "broker_order_id": broker_order_id,
                "signal_id": signal.signal_id,
            })
        assert exc.value.status_code == 409
        assert not env.runtimes.require("silver_01").position_manager.open_positions
        assert not broker._orders
    finally:
        engine.stop()
        persistence.close()

"""Focused failure-path checks for the journaled expiry carry handoff."""
from datetime import datetime, timezone
from threading import RLock
from types import SimpleNamespace

import pytest

from core.rollover import RolloverService
from indicators.supertrend import SuperTrend
from strategies.types import OrderState
from trading_engine import TradingEngine


class _Broker:
    def __init__(self, *, flat=True, clear=True):
        self.flat = flat
        self.clear = clear
        self.contracts = {}

    def instrument_flat(self, _instrument, _sid):
        return self.flat

    def instrument_orders_clear(self, _instrument, _sid):
        return self.clear

    def update_instrument_contract(self, instrument, config):
        self.contracts[instrument] = dict(config)


def _service(tmp_path):
    return RolloverService(
        config={"enabled": True}, state_path=str(tmp_path / "rollover.json"))


def test_promotion_waits_for_broker_flat_and_terminal_order_book(tmp_path):
    engine = TradingEngine.__new__(TradingEngine)
    service = engine._rollover_service = _service(tmp_path)
    engine.config = {"instruments": {"SILVERM": {
        "symbol": "MCX:SILVERM202611", "security_id": "483080",
        "exchange_segment": "MCX_COMM", "instrument": "FUTCOM",
    }}}
    engine._lock = RLock()
    engine._rollover_candidates = {}
    indicator = SuperTrend(1, 3.7, "low", "sma_tr", first_tr="high_low")
    indicator.update(100, 102, 99, 101, 1)
    engine._rollover_candidates[("live", "SILVERM")] = {
        "alias": "SILVERM__ROLLOVER_900001",
        "instrument_config": {
            **engine.config["instruments"]["SILVERM"],
            "symbol": "MCX:SILVERM202612", "security_id": "900001",
            "lot_size": 1, "tick_size": 1,
        },
        "indicator": indicator,
    }
    service.update_workflow(
        "SILVERM", candidate={"security_id": "900001", "atr_period": 1,
                               "multiplier": 3.7, "source": "low",
                               "atr_method": "sma_tr", "first_tr": "high_low"})
    broker = _Broker(flat=False)
    adapter = SimpleNamespace(promoted=0)
    adapter.promote_rollover_candidate = lambda *_args: setattr(adapter, "promoted", adapter.promoted + 1)
    env = SimpleNamespace(
        name="live", broker=broker, data_adapter=adapter,
        strategies={"silver_01": SimpleNamespace(
            instrument="SILVERM", strategy_type="supertrend", security_id="483080",
            supertrend=SuperTrend(1, 3.7, "low", "sma_tr", first_tr="high_low"),
            position_side=None)},
        position_manager=SimpleNamespace(open_positions=[]),
        candle_fetcher=None, candle_router=None,
    )
    assert engine._activate_rollover_candidate(env, "SILVERM", carry=False) is False
    assert adapter.promoted == 0
    assert engine.config["instruments"]["SILVERM"]["security_id"] == "483080"

    broker.flat = True
    broker.clear = False
    assert engine._activate_rollover_candidate(env, "SILVERM", carry=False) is False
    assert adapter.promoted == 0


def test_carry_direction_and_quantity_use_warmed_trend_and_actual_fill_size(tmp_path):
    engine = TradingEngine.__new__(TradingEngine)
    engine._rollover_service = _service(tmp_path)
    engine.config = {"instruments": {"SILVERM": {
        "security_id": "900001", "lot_size": 1,
    }}}
    engine._lock = RLock()
    engine._rollover_blocked = {("live", "SILVERM")}
    engine.market_data_health = None
    strategy = SimpleNamespace(
        instrument="SILVERM", strategy_type="supertrend", strategy_id="silver_01",
        supertrend=SuperTrend(1, 3.7, "low", "sma_tr", first_tr="high_low"),
    )
    strategy.supertrend.update(100, 102, 99, 101, 900)
    expected_side = "LONG" if strategy.supertrend.value.trend == 1 else "SHORT"
    broker = _Broker(flat=True, clear=True)
    adapter = SimpleNamespace(get_live_ltp=lambda _metal: 101.5)
    execution = SimpleNamespace(_orders={})
    env = SimpleNamespace(
        name="live", is_live=True, data_adapter=adapter, broker=broker,
        strategies={"silver_01": strategy}, execution_engine=execution,
        position_manager=SimpleNamespace(open_positions=[]), gate_enabled=True,
        market_status=SimpleNamespace(is_trading_allowed=True), safe_mode=None,
    )
    engine._gate_for = lambda _sid: SimpleNamespace(entries_allowed=True)
    submitted = []

    def process(signal, _env_name):
        submitted.append(signal)
        execution._orders["test-order"] = SimpleNamespace(
            entry_signal_id=signal.signal_id,
            state=OrderState.SUBMITTED,
            reason=None,
        )

    engine._process_signal = process
    engine.publish_event = lambda *_args, **_kwargs: None
    engine._rollover_service.update_workflow(
        "SILVERM", phase="CARRY_ENTRY_READY", roll_id="roll-1",
        carry_positions=[{"quantity": 3, "side": "SHORT",
                          "old_security_id": "483080"}])

    engine._try_rollover_carry_entries(env)
    assert len(submitted) == 1
    assert submitted[0].side == expected_side
    assert submitted[0].quantity == 3
    assert submitted[0].metadata["is_rollover_carry_entry"] is True
    workflow = engine._rollover_service.workflow("SILVERM")
    assert workflow["phase"] == "ENTRY_SUBMITTED"
    assert workflow["carry_positions"][0]["entry_expected_side"] == expected_side
    assert workflow["carry_positions"][0]["entry_expected_quantity"] == 3
    assert workflow["carry_positions"][0]["entry_strategy_id"] == "silver_01"

    engine._try_rollover_carry_entries(env)
    assert len(submitted) == 1


def test_ambiguous_carry_submission_is_never_reposted(tmp_path):
    engine = TradingEngine.__new__(TradingEngine)
    service = engine._rollover_service = _service(tmp_path)
    engine.config = {"instruments": {"SILVERM": {"security_id": "900001"}}}
    engine._rollover_blocked = {("live", "SILVERM")}
    engine.publish_event = lambda *_args, **_kwargs: None
    broker = _Broker(flat=True, clear=True)
    env = SimpleNamespace(
        name="live", is_live=True, data_adapter=object(), broker=broker,
        strategies={"silver_01": SimpleNamespace(
            instrument="SILVERM", strategy_type="supertrend")},
        position_manager=SimpleNamespace(open_positions=[]),
        execution_engine=SimpleNamespace(_orders={}),
    )
    service.update_workflow(
        "SILVERM", phase="ENTRY_SUBMITTED", carry_positions=[{
            "quantity": 1, "entry_signal_id": "roll-signal",
            "entry_submitted_at": datetime.now(timezone.utc).timestamp() - 30,
        }])
    engine._try_rollover_carry_entries(env)
    assert service.workflow("SILVERM")["phase"] == "ENTRY_SUBMITTED"
    assert ("live", "SILVERM") in engine._rollover_blocked


def _submitted_carry_engine(tmp_path, *, broker_flat, positions, order_state):
    engine = TradingEngine.__new__(TradingEngine)
    service = engine._rollover_service = _service(tmp_path)
    engine.config = {"instruments": {"SILVERM": {"security_id": "900001"}}}
    engine._rollover_blocked = {("live", "SILVERM")}
    engine.publish_event = lambda *_args, **_kwargs: None
    service.update_workflow(
        "SILVERM", phase="ENTRY_SUBMITTED", roll_id="roll-1",
        carry_positions=[{
            "quantity": 3,
            "side": "SHORT",  # original side is informational; new side is explicit below
            "entry_signal_id": "roll-signal",
            "entry_submitted_at": datetime.now(timezone.utc).timestamp() - 30,
            "entry_expected_side": "SHORT",
            "entry_expected_quantity": 3,
            "entry_strategy_id": "silver_01",
            "entry_security_id": "900001",
        }])
    order = SimpleNamespace(
        entry_signal_id="roll-signal", state=order_state,
        side="SELL", quantity=3, strategy_id="silver_01",
        reason="simulated terminal rejection")
    env = SimpleNamespace(
        name="live", mode="LIVE", is_live=True, data_adapter=object(),
        broker=_Broker(flat=broker_flat, clear=True),
        strategies={"silver_01": SimpleNamespace(
            instrument="SILVERM", strategy_type="supertrend",
            strategy_id="silver_01")},
        position_manager=SimpleNamespace(open_positions=positions),
        execution_engine=SimpleNamespace(_orders={"carry-order": order}),
    )
    return engine, service, env


def test_async_terminal_rejection_is_resolved_and_alerted(tmp_path):
    engine, service, env = _submitted_carry_engine(
        tmp_path, broker_flat=True, positions=[], order_state=OrderState.REJECTED)

    engine._try_rollover_carry_entries(env)

    workflow = service.workflow("SILVERM")
    assert workflow["phase"] == "CARRY_FAILED"
    assert "simulated terminal rejection" in workflow["last_error"]
    assert ("live", "SILVERM") in engine._rollover_blocked


def test_rejection_survives_restart_without_order_cache(tmp_path):
    engine, service, env = _submitted_carry_engine(
        tmp_path, broker_flat=True, positions=[], order_state=OrderState.REJECTED)
    env.execution_engine._orders.clear()
    env.runtimes = SimpleNamespace(require=lambda _sid: SimpleNamespace(
        lifecycle=SimpleNamespace(resolve_trade_from_signal=lambda _signal: SimpleNamespace(
            status="REJECTED", entry_side="SHORT", quantity=3,
            strategy_id="silver_01"))))

    engine._try_rollover_carry_entries(env)

    assert service.workflow("SILVERM")["phase"] == "CARRY_FAILED"
    assert "REJECTED" in service.workflow("SILVERM")["last_error"]


def test_position_confirmation_can_recover_expected_values_from_lifecycle(tmp_path):
    carried = SimpleNamespace(
        is_open=True, instrument="SILVERM", is_long=False, quantity=3,
        strategy_id="silver_01", entry_signal_id="roll-signal",
        position_id="carry-position", trade_id="carry-trade")
    engine, service, env = _submitted_carry_engine(
        tmp_path, broker_flat=False, positions=[carried],
        order_state=OrderState.FILLED)
    row = service.workflow("SILVERM")["carry_positions"][0]
    for key in ("entry_expected_side", "entry_expected_quantity", "entry_strategy_id"):
        row.pop(key, None)
    service.update_workflow("SILVERM", carry_positions=[row])
    env.execution_engine._orders.clear()
    env.runtimes = SimpleNamespace(require=lambda _sid: SimpleNamespace(
        lifecycle=SimpleNamespace(resolve_trade_from_signal=lambda _signal: SimpleNamespace(
            status="OPEN", entry_side="SHORT", quantity=3,
            strategy_id="silver_01"))))

    engine._try_rollover_carry_entries(env)

    assert service.workflow("SILVERM")["phase"] == "COMPLETE"
    assert ("live", "SILVERM") not in engine._rollover_blocked


@pytest.mark.parametrize("position_changes", [
    {"is_long": True},
    {"quantity": 1},
    {"quantity": "corrupt"},
    {"strategy_id": "other_strategy"},
    {"entry_signal_id": "unrelated-signal"},
])
def test_unrelated_position_cannot_confirm_carry_entry(tmp_path, position_changes):
    attrs = {
        "is_open": True, "instrument": "SILVERM", "is_long": False,
        "quantity": 3, "strategy_id": "silver_01",
        "entry_signal_id": "roll-signal", "position_id": "other-position",
        "trade_id": "other-trade",
    }
    attrs.update(position_changes)
    unrelated = SimpleNamespace(**attrs)
    engine, service, env = _submitted_carry_engine(
        tmp_path, broker_flat=False, positions=[unrelated],
        order_state=OrderState.SUBMITTED)

    engine._try_rollover_carry_entries(env)

    assert service.workflow("SILVERM")["phase"] == "ENTRY_SUBMITTED"
    assert ("live", "SILVERM") in engine._rollover_blocked


def test_only_exact_carry_position_confirms_handoff(tmp_path):
    carried = SimpleNamespace(
        is_open=True, instrument="SILVERM", is_long=False, quantity=3,
        strategy_id="silver_01", entry_signal_id="roll-signal",
        position_id="carry-position", trade_id="carry-trade")
    engine, service, env = _submitted_carry_engine(
        tmp_path, broker_flat=False, positions=[carried],
        order_state=OrderState.FILLED)

    engine._try_rollover_carry_entries(env)

    assert service.workflow("SILVERM")["phase"] == "COMPLETE"
    assert ("live", "SILVERM") not in engine._rollover_blocked


def test_final_session_close_is_journaled_before_single_exit_attempt():
    engine = TradingEngine.__new__(TradingEngine)
    state = {"phase": "IDLE", "roll_id": None, "candidate": {},
             "carry_positions": [], "last_error": None}

    class _Service:
        enabled = True

        @staticmethod
        def workflow(_metal):
            return {**state,
                    "candidate": dict(state["candidate"]),
                    "carry_positions": list(state["carry_positions"])}

        @staticmethod
        def update_workflow(_metal, **changes):
            state.update(changes)
            return True

        @staticmethod
        def requires_broker_confirmation(*_args):
            return True

        @staticmethod
        def evaluate(*_args, **_kwargs):
            return ["block_entries", "force_close"]

        @staticmethod
        def alert_once(*_args, **_kwargs):
            pass

        @staticmethod
        def overrides():
            return {}

    engine._rollover_service = _Service()
    engine.config = {"instruments": {"SILVERM": {
        "symbol": "MCX:SILVERM202611", "security_id": "483080"}}}
    engine._rollover_blocked = set()
    engine.publish_event = lambda *_args, **_kwargs: None
    engine._env_for = lambda _name=None: env
    engine._prepare_rollover_candidate = lambda *_args: None
    engine._rollover_metal_open = lambda *_args: True
    position = SimpleNamespace(
        is_open=True, instrument="SILVERM", position_id="p-1", trade_id="t-1",
        strategy_id="silver_01", is_long=True, quantity=2, average_entry=100,
    )
    broker = _Broker(flat=False, clear=True)
    env = SimpleNamespace(
        name="live", is_live=True, broker=broker,
        position_manager=SimpleNamespace(open_positions=[position]),
    )
    exits = []

    def exit_position(_env_name, **kwargs):
        exits.append((dict(state), kwargs))
        return {"closed": False, "errors": []}

    engine.emergency_exit_all = exit_position
    engine._rollover_tick_locked("live")

    assert len(exits) == 1
    journal_at_exit, kwargs = exits[0]
    assert journal_at_exit["phase"] == "EXIT_REQUESTED"
    assert journal_at_exit["carry_positions"][0]["quantity"] == 2
    assert journal_at_exit["carry_positions"][0]["old_security_id"] == "483080"
    assert kwargs["exit_reason"] == "contract_rollover"


def test_workflow_state_rolls_back_if_persistence_fails(tmp_path):
    service = _service(tmp_path)
    before = service.workflow("SILVERM")
    service._save = lambda: False

    assert service.update_workflow(
        "SILVERM", phase="EXIT_REQUESTED", roll_id="roll-failed",
        carry_positions=[{"quantity": 1}], last_error="test") is False
    assert service.workflow("SILVERM") == before


def test_persisted_override_retries_runtime_promotion():
    engine = TradingEngine.__new__(TradingEngine)
    state = {"phase": "PROMOTION_WAIT", "roll_id": "r-1",
             "candidate": {}, "carry_positions": [], "last_error": None}

    class _Service:
        enabled = True

        @staticmethod
        def workflow(_metal):
            return {**state}

        @staticmethod
        def update_workflow(_metal, **changes):
            state.update(changes)
            return True

        @staticmethod
        def requires_broker_confirmation(*_args):
            return True

        @staticmethod
        def evaluate(*_args, **_kwargs):
            # The real service returns only block_entries after switched_on
            # has been durably saved.
            return ["block_entries"]

        @staticmethod
        def overrides():
            return {"SILVERM": {"active_security_id": "900001"}}

        @staticmethod
        def alert_once(*_args, **_kwargs):
            pass

    engine._rollover_service = _Service()
    engine.config = {"instruments": {"SILVERM": {
        "symbol": "MCX:SILVERM202611", "security_id": "483080"}}}
    engine._rollover_blocked = set()
    engine.publish_event = lambda *_args, **_kwargs: None
    engine._env_for = lambda _name=None: env
    engine._recover_rollover_handoff = lambda *_args: None
    staged = {"contract": {"security_id": "900001"}}
    engine._prepare_rollover_candidate = lambda *_args: staged
    engine._rollover_metal_open = lambda *_args: False
    broker = _Broker(flat=True, clear=True)
    env = SimpleNamespace(
        name="live", is_live=True, broker=broker,
        position_manager=SimpleNamespace(open_positions=[]),
    )
    promotions = []
    engine._activate_rollover_candidate = lambda *_args, **_kwargs: promotions.append(True) or True

    engine._rollover_tick_locked("live")

    assert promotions == [True]
